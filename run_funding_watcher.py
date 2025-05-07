import asyncio
import logging
import os
import sys
from config import config
from modules.funding_watcher import FundingWatcher
from modules.telegram_notify_async import AsyncTelegramLogHandler, telegram_sender

def setup_logging():
    # Получаем уровень логирования из переменной окружения
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    numeric_level = getattr(logging, log_level, logging.INFO)
    
    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("funding_watcher.log")
        ]
    )
    
    # Настройка уровня для внешних библиотек
    logging.getLogger("aiohttp").setLevel(logging.WARNING)
    logging.getLogger("aiogram").setLevel(logging.WARNING)
    
    # Telegram handler для ошибок
    if config.TELEGRAM_ERRORS_ENABLED:
        tg_handler = AsyncTelegramLogHandler("errors")
        tg_handler.setLevel(logging.ERROR)
        logging.getLogger().addHandler(tg_handler)

async def main():
    setup_logging()
    watcher = FundingWatcher()
    await telegram_sender.start()
    try:
        await watcher.start()
    finally:
        await watcher.close()
        await telegram_sender.stop()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("⏹️ Stopped by user")
    except Exception as e:
        logging.critical(f"💥 Fatal error: {e}", exc_info=True)
        sys.exit(1)
