from aiogram.utils import executor

from config import settings
from moon_house_bot.app import dp, on_shutdown, on_startup

if __name__ == '__main__':
    print(f'{settings.webhook.host}{settings.webhook.path}')
    executor.start_webhook(
        dispatcher=dp,
        webhook_path=settings.webhook.path,
        on_startup=on_startup,
        on_shutdown=on_shutdown,
        skip_updates=True,
        host=settings.host,
        port=settings.port,
    )
