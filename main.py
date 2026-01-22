"""
MAIN ENTRY POINT
----------------
Responsible for initializing the Bot, Dispatcher, Middlewares, and background services.
Starts the aiohttp server for polling and manages global application lifecycle.
"""
import asyncio
import logging
import sys
from aiogram import Bot, Dispatcher, types
from aiogram.exceptions import TelegramBadRequest
from aiogram.fsm.storage.memory import MemoryStorage
from config import BOT_TOKEN
from handlers import router
from database import init_db
from scheduler_service import scheduler, load_reminders

# --- Configuration & Logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("main")

async def global_logging_middleware(handler, event, data):
    """
    Middleware to log all incoming updates for auditing and debugging.
    """
    if isinstance(event, types.Update):
        update_info = "Unknown"
        if event.message:
            msg_text = event.message.text or "[Non-text message]"
            update_info = f"Message from {event.message.from_user.id}: {msg_text[:20]}..."
        elif event.callback_query:
            update_info = f"Callback from {event.callback_query.from_user.id}: {event.callback_query.data}"
        logger.info(f"Incoming Update: {update_info}")
    try:
        return await handler(event, data)
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
            return
        raise

async def main():
    """Main execution point for the bot."""
    logger.info("Starting INCREASE STAFF BOT...")
    
    # Initialize Core Components
    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher(storage=MemoryStorage())
    
    # Setup Middlewares & Routers
    dp.update.outer_middleware(global_logging_middleware)
    dp.include_router(router)
    
    # Startup Sequence
    await init_db()
    await load_reminders(bot)
    
    from scheduler_service import start_maintenance_jobs
    start_maintenance_jobs(bot)
    
    if not scheduler.running:
        scheduler.start()
        logger.info("APScheduler started.")

    try:
        # Polling with Chat Member updates enabled
        logger.info("Bot is now online and polling for updates.")
        await dp.start_polling(bot, skip_updates=True, allowed_updates=["message", "callback_query", "chat_member"])
    except Exception as e:
        logger.critical(f"Bot execution stopped due to critical error: {e}")
    finally:
        if scheduler.running:
            scheduler.shutdown()
        await bot.session.close()
        logger.info("Bot session closed. Goodbye!")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot manually stopped.")
