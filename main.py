from aiogram.utils import executor

from moon_house_bot.app import dp, scheduler, schedule_daily_notifications

if __name__ == '__main__':
    schedule_daily_notifications()
    scheduler.start()
    executor.start_polling(dp, skip_updates=True)
