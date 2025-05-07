import asyncio
import logging
import json
import hmac
import hashlib
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from urllib.parse import urlencode
from cachetools import TTLCache

import aiohttp
from aiolimiter import AsyncLimiter
from aiogram.utils.formatting import Text

from config import config
from modules.telegram_notify_async import send_telegram_message_async
from modules.funding_history_parquet import FundingHistoryParquet

logger = logging.getLogger(__name__)

class FundingWatcher:
    def __init__(self):
        self._validate_config()
        
        # Настройка HTTP клиента
        base_url = config.BYBIT_API_URL.rstrip("/") + "/"
        self.session = aiohttp.ClientSession(
            base_url=base_url,
            connector=aiohttp.TCPConnector(limit=50),
            timeout=aiohttp.ClientTimeout(total=config.API_TIMEOUT)
        )

        self.limiter = AsyncLimiter(config.API_MAX_REQUESTS_PER_SECOND, 1)
        self.history = FundingHistoryParquet(config.FUNDING_HISTORY_DIR)
        self.symbol_cache = TTLCache(maxsize=1, ttl=config.SYMBOLS_UPDATE_INTERVAL)
        self.active_symbols: List[str] = []
        self.error_counter = TTLCache(maxsize=1000, ttl=3600)

    def _validate_config(self):
        required_params = ("BYBIT_API_KEY", "BYBIT_API_SECRET")
        for param in required_params:
            if not getattr(config, param):
                raise ValueError(f"Missing config: {param}")

    def _sign(self, params: dict) -> str:
        filtered = {k: v for k, v in params.items() if v is not None}
        query = urlencode(sorted(filtered.items()))
        return hmac.new(
            config.BYBIT_API_SECRET.encode(),
            query.encode(),
            hashlib.sha256
        ).hexdigest()

    async def _api_request(self, endpoint: str, params: dict) -> dict:
        logger.debug(f"API Request: {endpoint} - {params}")
        try:
            endpoint = endpoint.lstrip("/")
            request_params = {
                "api_key": config.BYBIT_API_KEY,
                "timestamp": str(int(datetime.utcnow().timestamp() * 1000))
            }
            request_params.update(params)
            request_params["sign"] = self._sign(request_params)

            async with self.limiter:
                async with self.session.get(endpoint, params=request_params) as resp:
                    response_text = await resp.text()
                    logger.debug(f"API Response: {resp.status} - {response_text}")
                    
                    if resp.status != 200:
                        return {"error": f"HTTP {resp.status}"}
                    
                    return json.loads(response_text)
                    
        except Exception as e:
            logger.error(f"Request failed: {str(e)}", exc_info=True)
            return {"error": str(e)}

    async def _fetch_symbols(self) -> List[str]:
        symbols = []
        cursor = ""
        page = 1
        
        logger.info("Starting symbols fetch...")
        
        while True:
            logger.debug(f"Fetching page {page} with cursor: {cursor}")
            data = await self._api_request(
                "v5/market/tickers",
                {"category": "linear", "cursor": cursor}
            )
            
            if data.get("error"):
                logger.error(f"Symbols fetch error: {data['error']}")
                break
                
            result = data.get("result", {})
            logger.debug(f"API Response: {json.dumps(result, indent=2)}")
            
            if not result.get("list"):
                logger.warning("Empty symbols list in response")
                break
                
            # Фильтрация символов
            batch_symbols = [
                item["symbol"] 
                for item in result["list"] 
                if item.get("symbol", "").endswith("USDT") 
                and item.get("status") == "Trading"
            ]
            
            logger.debug(f"Found {len(batch_symbols)} symbols in batch")
            symbols.extend(batch_symbols)
            
            # Пагинация
            new_cursor = result.get("nextPageCursor")
            if not new_cursor or new_cursor == cursor:
                break
            cursor = new_cursor
            page += 1
        
        logger.info(f"Total symbols found: {len(symbols)}")
        return sorted(set(symbols))

    async def update_symbols(self):
        logger.info("Updating symbols...")
        try:
            raw_symbols = await self._fetch_symbols()
            logger.debug(f"Raw symbols: {raw_symbols}")
            
            self.active_symbols = raw_symbols[:100] if raw_symbols else []
            
            if not self.active_symbols:
                logger.warning("No symbols found! Using fallback.")
                self.active_symbols = config.FALLBACK_SYMBOLS.copy()
                
            logger.info(f"Active symbols: {len(self.active_symbols)}")
            
        except Exception as e:
            logger.critical(f"Symbols update failed: {e}", exc_info=True)
            self.active_symbols = config.FALLBACK_SYMBOLS.copy()

    async def _process_symbol(self, symbol: str) -> Optional[dict]:
        if self.error_counter.get(symbol, 0) >= 3:
            logger.debug(f"Skipping banned symbol: {symbol}")
            return None
            
        try:
            logger.debug(f"Processing symbol: {symbol}")
            data = await self._api_request(
                "v5/market/funding/history",
                {"category": "linear", "symbol": symbol, "limit": 1}
            )
            
            if data.get("error"):
                logger.warning(f"Error fetching funding for {symbol}: {data['error']}")
                self.error_counter[symbol] += 1
                return None
                
            rate_data = data.get("result", {}).get("list", [{}])[0]
            current_rate = float(rate_data.get("fundingRate", 0))
            prev_rate = self.history.get_latest(symbol)
            
            await self.history.add_rate(symbol, current_rate)

            if prev_rate is None:
                logger.debug(f"Initial rate for {symbol}")
                return None
                
            diff = abs(current_rate - prev_rate)
            if diff >= config.FUNDING_RATE_THRESHOLD:
                return {
                    "symbol": symbol,
                    "old": prev_rate,
                    "new": current_rate,
                    "diff": diff
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Error processing {symbol}: {e}", exc_info=True)
            self.error_counter[symbol] += 1
            return None

    async def start(self):
        logger.info("🚀 Starting FundingWatcher")
        await self.update_symbols()
        
        try:
            while True:
                logger.debug("Starting check cycle...")
                anomalies = [res for res in await asyncio.gather(
                    *[self._process_symbol(s) for s in self.active_symbols]
                ) if res]
                
                if anomalies:
                    await self._send_alerts(anomalies)
                    
                await asyncio.sleep(config.FUNDING_POLL_INTERVAL)
                
        except Exception as e:
            logger.critical(f"Main loop failed: {e}", exc_info=True)
        finally:
            await self.close()

    async def _send_alerts(self, anomalies: List[dict]):
        msg = Text("🚨 Funding Rate Alerts\n\n")
        for anomaly in anomalies:
            msg += Text(
                f"• {anomaly['symbol']}: "
                f"{anomaly['old']:.6f} → {anomaly['new']:.6f} "
                f"(Δ {anomaly['diff']:.6f})\n"
            )
        await send_telegram_message_async(msg, "funding")

    async def close(self):
        await self.session.close()
        logger.info("🔌 Service stopped")

if __name__ == "__main__":
    asyncio.run(FundingWatcher().start())
