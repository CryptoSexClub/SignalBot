import asyncio
import logging
import json
import hmac
import hashlib
from datetime import datetime, timedelta
from typing import List, Optional
from urllib.parse import urlencode
import signal

import aiohttp
from aiolimiter import AsyncLimiter
from cachetools import TTLCache
from aiogram.utils.formatting import Text, Bold, TextLink
from aiogram.enums import ParseMode

from config import config
from modules.funding_history_parquet import FundingHistoryParquet
from modules.telegram_notify_async import send_telegram_message_async

logger = logging.getLogger(__name__)

class FundingWatcher:
    def __init__(self):
        self._validate_config()

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
        self._is_closed = False

    def _validate_config(self):
        for param in ("BYBIT_API_KEY", "BYBIT_API_SECRET"):
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
        endpoint = endpoint.lstrip("/")
        request_params = {
            "api_key": config.BYBIT_API_KEY,
            "timestamp": str(int(datetime.utcnow().timestamp() * 1000)),
            **(params or {})
        }
        request_params["sign"] = self._sign(request_params)
        clean_params = {k: v for k, v in request_params.items() if v is not None}

        try:
            async with self.limiter:
                async with self.session.get(endpoint, params=clean_params) as resp:
                    text = await resp.text()
                    logger.debug(f"API Response ({resp.status}): {text}")
                    try:
                        data = json.loads(text)
                    except json.JSONDecodeError:
                        return {"error": "Invalid JSON from API"}
                    ret_code = data.get("retCode", 0)
                    if ret_code != 0:
                        return {"error": f"{data.get('retMsg','Unknown')} (retCode={ret_code})"}
                    return data
        except Exception as e:
            logger.error(f"Request error: {e}", exc_info=True)
            return {"error": str(e)}

    async def _fetch_symbols(self) -> List[str]:
        logger.info("Starting symbols fetch via instruments-info...")
        all_symbols: List[str] = []
        cursor: Optional[str] = None

        while True:
            params = {
                "category": "linear",
                "status": "Trading",
                "limit": 500,
                "cursor": cursor
            }
            resp = await self._api_request("v5/market/instruments-info", params)
            if resp.get("error"):
                logger.error(f"Fetch instruments error: {resp['error']}")
                return config.FALLBACK_SYMBOLS.copy()

            items = resp.get("result", {}).get("list", [])
            if not items:
                break

            batch = [
                item["symbol"]
                for item in items
                if isinstance(item.get("symbol"), str) and item["symbol"].endswith("USDT")
            ]
            all_symbols.extend(batch)

            next_cursor = resp["result"].get("nextPageCursor")
            if not next_cursor or next_cursor == cursor:
                break
            cursor = next_cursor

        unique = sorted(set(all_symbols))
        if not unique:
            logger.warning("No symbols found; using fallback")
            return config.FALLBACK_SYMBOLS.copy()

        logger.info(f"Total symbols fetched: {len(unique)}")
        return unique

    async def update_symbols(self):
        if "symbols" in self.symbol_cache:
            self.active_symbols = self.symbol_cache["symbols"]
            return

        symbols = await self._fetch_symbols()
        self.active_symbols = symbols[:config.API_BATCH_SIZE] or config.FALLBACK_SYMBOLS.copy()
        self.symbol_cache["symbols"] = self.active_symbols
        logger.info(f"Active symbols set: {len(self.active_symbols)}")

    async def _process_symbol(self, symbol: str) -> Optional[dict]:
        if self.error_counter.get(symbol, 0) >= config.API_RETRY_ATTEMPTS:
            return None

        resp = await self._api_request(
            "v5/market/funding/history",
            {"category": "linear", "symbol": symbol, "limit": 1}
        )
        if resp.get("error"):
            self.error_counter[symbol] = self.error_counter.get(symbol, 0) + 1
            logger.warning(f"Error fetching funding for {symbol}: {resp['error']}")
            return None

        entry = resp.get("result", {}).get("list", [{}])[0]
        current_rate = float(entry.get("fundingRate", 0))
        prev_rate = self.history.get_latest(symbol)
        await self.history.add_rate(symbol, current_rate)

        if prev_rate is None:
            return None

        diff = abs(current_rate - prev_rate)
        if diff >= config.FUNDING_RATE_THRESHOLD:
            return {"symbol": symbol, "old": prev_rate, "new": current_rate, "diff": diff}
        return None

    async def _send_alerts(self, anomalies: List[dict]):
        """
        Формирует и отправляет в Telegram сообщение с форматированием.
        """
        now = datetime.utcnow()
        hour = now.hour
        next_base = (hour // 8 + 1) * 8
        next_dt = now.replace(hour=next_base % 24, minute=0, second=0, microsecond=0)
        if next_dt <= now:
            next_dt += timedelta(hours=8)
        countdown = str(next_dt - now).split(".")[0]

        # Собираем сообщение с использованием объектов форматирования
        text = Text(
            Bold("🚨 Funding Rate Alert 🚨"), "\n",
            Bold(f"Threshold: {config.FUNDING_RATE_THRESHOLD*100:.2f}%"), "\n\n"
        )

        for a in anomalies:
            emoji = "🟢" if a["new"] > a["old"] else "🔴"
            pct_old = a["old"] * 100
            pct_new = a["new"] * 100
            diff = a["diff"]
            pct_diff = (diff / abs(a["old"])) * 100 if a["old"] != 0 else diff * 100

            text += Text(
                f"{emoji} {a['symbol']} — Next in: {countdown}\n",
                f"Old: {a['old']:+.5f} ({pct_old:+.2f}%)\n",
                f"New: {a['new']:+.5f} ({pct_new:+.2f}%)\n",
                f"Δ: {diff:+.5f} ({pct_diff:+.1f}%)\n",
                "────────────────────────────\n"
            )

        ts = now.strftime("%H:%M %d.%m.%Y")
        text += Text(f"🕒 {ts}\n")

        # Добавляем ссылки на графики
        chart_links = []
        for a in anomalies:
            url = f"https://www.tradingview.com/chart/?symbol=BYBIT:{a['symbol']}"
            chart_links.append(TextLink(a["symbol"], url=url))
        charts = Text("View Chart: ", *[link + " | " for link in chart_links[:-1]], chart_links[-1])
        text += charts

        # Отправка сообщения
        await send_telegram_message_async(
            text.as_html(),
            telegram_type="funding",
            parse_mode=ParseMode.HTML
        )

    async def start(self):
        logger.info("🚀 Starting FundingWatcher")
        await self.update_symbols()

        loop = asyncio.get_running_loop()
        stop_event = asyncio.Event()
        loop.add_signal_handler(signal.SIGINT,  stop_event.set)
        loop.add_signal_handler(signal.SIGTERM, stop_event.set)

        try:
            while not stop_event.is_set():
                results = await asyncio.gather(
                    *[self._process_symbol(s) for s in self.active_symbols]
                )
                alerts = [x for x in results if x]
                if alerts:
                    await self._send_alerts(alerts)
                await asyncio.sleep(config.FUNDING_POLL_INTERVAL)
        except Exception as e:
            logger.critical(f"Watcher loop failed: {e}", exc_info=True)
        finally:
            await self.close()

    async def close(self):
        if self._is_closed:
            return
        self._is_closed = True
        await self.session.close()
        logger.info("🔌 FundingWatcher stopped")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s"
    )
    watcher = FundingWatcher()
    asyncio.run(watcher.start())
