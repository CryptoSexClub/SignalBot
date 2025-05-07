import os
from pydantic_settings import BaseSettings
from dotenv import load_dotenv

load_dotenv()

class Config(BaseSettings):
    # API
    BYBIT_API_KEY: str
    BYBIT_API_SECRET: str
    BYBIT_API_URL: str = "https://api.bybit.com/"
    
    # Symbols
#    SYMBOL_PATTERN: str = r'^[A-Z0-9]+USDT$'
    SYMBOL_PATTERN: str = r'^.*USDT$'  # Разрешить любые символы, заканчивающиеся на USDT
    SYMBOL_MAX_LENGTH: int = 20
#    SYMBOL_BLACKLIST_PREFIXES: list = ["1000", "10000", "BETA", "TEST"]
    SYMBOL_BLACKLIST_PREFIXES: list = [] # Очистить черный список
    MIN_TURNOVER_24H: float = 1000.0
    FALLBACK_SYMBOLS: list = ["BTCUSDT", "ETHUSDT"]
    
    # Rate Limits
    API_MAX_REQUESTS_PER_SECOND: int = 10
    API_TIMEOUT: int = 15
    API_RETRY_ATTEMPTS: int = 3
    API_BATCH_SIZE: int = 5
    API_RETRY_DELAY_BASE: int = 2
    
    # Funding
    FUNDING_RATE_THRESHOLD: float = 0.001
    FUNDING_POLL_INTERVAL: int = 120
    SYMBOLS_UPDATE_INTERVAL: int = 3600
    
    # Storage
    FUNDING_HISTORY_DIR: str = "parquet_data"
    BUFFER_SIZE: int = 10
    
    # Telegram
    TELEGRAM_ERRORS_ENABLED: bool = True
    TELEGRAM_SETTINGS: dict = {
        "errors": {
            "bot_token": os.getenv("TG_ERROR_BOT_TOKEN"),
            "chat_id": os.getenv("TG_ERROR_CHAT_ID")
        },
        "funding": {
            "bot_token": os.getenv("TG_FUNDING_BOT_TOKEN"),
            "chat_id": os.getenv("TG_FUNDING_CHAT_ID")
        }
    }

    class Settings:
        case_sensitive = True

config = Config()
