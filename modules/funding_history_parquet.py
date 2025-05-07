import os
import logging
import pandas as pd
from datetime import datetime
from typing import Optional
from config import config

logger = logging.getLogger(__name__)

class FundingHistoryParquet:
    def __init__(self, storage_dir: str):
        self.storage_dir = os.path.abspath(storage_dir)
        os.makedirs(self.storage_dir, exist_ok=True)
        self.buffer = {}

    def _get_filepath(self, symbol: str) -> str:
        return os.path.join(self.storage_dir, f"{symbol}.parquet")

    async def add_rate(self, symbol: str, rate: float):
        try:
            timestamp = pd.Timestamp.utcnow().tz_localize(None)
            if symbol not in self.buffer:
                self.buffer[symbol] = []
            self.buffer[symbol].append({
                "timestamp": timestamp,
                "rate": float(rate)
            })
            if len(self.buffer[symbol]) >= config.BUFFER_SIZE:
                await self._flush_buffer(symbol)
        except Exception as e:
            logger.error(f"Buffer error: {e}")

    async def _flush_buffer(self, symbol: str):
        try:
            filepath = self._get_filepath(symbol)
            new_data = pd.DataFrame(self.buffer[symbol])
            if os.path.exists(filepath):
                existing = pd.read_parquet(filepath)
                combined = pd.concat([existing, new_data])
            else:
                combined = new_data
            combined = combined.drop_duplicates("timestamp", keep="last")
            combined.to_parquet(filepath, index=False)
            self.buffer[symbol].clear()
        except Exception as e:
            logger.error(f"Flush failed: {e}")

    def get_latest(self, symbol: str) -> Optional[float]:
        try:
            filepath = self._get_filepath(symbol)
            if not os.path.exists(filepath):
                return None
            df = pd.read_parquet(filepath)
            return df.iloc[-1]["rate"] if not df.empty else None
        except Exception as e:
            logger.error(f"Read error: {e}")
            return None
