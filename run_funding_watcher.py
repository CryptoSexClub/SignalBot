import asyncio
import logging
import sys
from config import config
from modules.funding_watcher import FundingWatcher
from modules.telegram_notify_async import AsyncTelegramLogHandler, telegram_sender

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("funding_watcher.log")
        ]
    )
    logging.getLogger("aiohttp").setLevel(logging.WARNING)
    logging.getLogger("aiogram").setLevel(logging.WARNING)
    if config.TELEGRAM_ERRORS_ENABLED:
        tg = AsyncTelegramLogHandler("errors")
        tg.setLevel(logging.ERROR)
        logging.getLogger().addHandler(tg)

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
        logging.critical(f"💥 Fatal error: {e}")
        sys.exit(1)
