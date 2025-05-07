import asyncio
import logging
import sys
from typing import Optional
from aiogram import Bot
from aiogram.utils.formatting import Text
from config import config

logger = logging.getLogger(__name__)

class AsyncTelegramSender:
    def __init__(self):
        self.queue = asyncio.Queue()
        self.bots = {}
        self._running = False

    async def start(self):
        if not self._running:
            self._running = True
            asyncio.create_task(self._sender_loop())

    async def stop(self):
        self._running = False
        await self.queue.join()
        for bot in self.bots.values():
            await bot.close()

    async def send_message(self, message: Text, telegram_type: str, parse_mode: Optional[str] = "HTML"):
        settings = config.TELEGRAM_SETTINGS.get(telegram_type, {})
        await self.queue.put((message, settings, parse_mode))

    async def _sender_loop(self):
        while self._running:
            message, settings, _ = await self.queue.get()
            bot_token = settings.get("bot_token")
            chat_id = settings.get("chat_id")
            if not bot_token or not chat_id:
                self.queue.task_done()
                continue
            try:
                bot = self.bots.get(bot_token) or Bot(token=bot_token)
                self.bots[bot_token] = bot
                send_kwargs = message.as_kwargs(replace_parse_mode=False)
                send_kwargs.update({
                    "chat_id": chat_id,
                    "disable_web_page_preview": True,
                })
                await bot.send_message(**send_kwargs)
            except Exception as e:
                logger.error(f"Telegram error: {e}")
            finally:
                self.queue.task_done()

telegram_sender = AsyncTelegramSender()

async def send_telegram_message_async(text: Text, telegram_type: str = "funding", parse_mode: Optional[str] = "HTML"):
    if not telegram_sender._running:
        await telegram_sender.start()
    await telegram_sender.send_message(text, telegram_type, parse_mode)

class AsyncTelegramLogHandler(logging.Handler):
    def __init__(self, telegram_type: str = "errors"):
        super().__init__()
        self.telegram_type = telegram_type

    def emit(self, record):
        # Разрубаем рекурсию логгирования
        try:
            if record.levelno >= self.level:
                message = Text(
                    f"⚠️ <b>{record.levelname}</b>\n"
                    f"{self.format(record)}\n\n"
                    f"📁 {record.name}:{record.lineno}"
                )
                # Если есть активный loop — шлём асинхронно
                try:
                    loop = asyncio.get_running_loop()
                except RuntimeError:
                    loop = None

                if loop and loop.is_running():
                    asyncio.create_task(
                        send_telegram_message_async(message, self.telegram_type)
                    )
                else:
                    # Во время инициализации/закрытия выводим в stderr
                    print(f"[TELEGRAM {self.telegram_type}] {record.levelname}: {self.format(record)}", file=sys.stderr)
        except Exception:
            # Любые ошибки внутри emit игнорируем, чтобы не провоцировать рекурсию
            pass
