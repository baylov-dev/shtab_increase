"""
SCHEDULER SERVICE
-----------------
Manages all background jobs: sending reminders and weekly system maintenance.
Uses APScheduler (AsyncIOScheduler) to handle cron and date triggers.
"""
import logging
import pytz
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from aiogram import Bot
from aiogram.types import InlineKeyboardButton, FSInputFile
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
import os
from datetime import datetime, timedelta

from config import TIMEZONE, ADMIN_IDS
from database import get_reminders, toggle_reminder_status, get_setting, cleanup_old_logs, delete_reminder, DB_PATH

# Professional Logging setup for Scheduler
logger = logging.getLogger("scheduler")

PARSE_MODE = "Markdown"


def _job_id(reminder_id: int) -> str:
    return f"rem_{reminder_id}"


def _tz_from_name(timezone_name: str):
    try:
        return pytz.timezone(str(timezone_name))
    except Exception:
        return pytz.timezone(TIMEZONE)


def _shift_days(days_str: str, shift_days: int) -> str:
    if days_str == "all" or shift_days == 0:
        return days_str
    parts = [p.strip() for p in str(days_str).split(",") if p.strip()]
    days: list[int] = []
    for p in parts:
        try:
            days.append(int(p))
        except Exception:
            continue
    shifted = [str((d + shift_days) % 7) for d in days]
    return ",".join(shifted) if shifted else days_str


def _convert_bot_time_to_server(time_str: str, offset_minutes: int) -> tuple[int, int, int]:
    hour, minute = map(int, time_str.split(":"))
    base = datetime(2000, 1, 3, hour, minute)
    server_dt = base - timedelta(minutes=offset_minutes)
    shift_days = (server_dt.date() - base.date()).days
    return server_dt.hour, server_dt.minute, shift_days


async def _get_time_offset_minutes() -> int:
    raw = await get_setting("time_offset_minutes", "0")
    try:
        return int(str(raw))
    except Exception:
        return 0


scheduler = AsyncIOScheduler(timezone=_tz_from_name(TIMEZONE))

async def send_reminder_job(bot: Bot, chat_id: int, thread_id: int, text: str, rid: int, needs_confirm: bool = False, is_recurring: bool = True):
    """Sends the reminder message and handles auto-deletion for one-time tasks."""
    builder = InlineKeyboardBuilder()
    if needs_confirm:
        builder.row(InlineKeyboardButton(text="✅ Выполнено", callback_data=f"task_done:{rid}"))
    
    try:
        await bot.send_message(
            chat_id=chat_id,
            message_thread_id=thread_id if thread_id != 1 else None,
            text=f"📝 **ИНКРИС ШТАБ | СЛУЖЕБНОЕ УВЕДОМЛЕНИЕ**\n\n{text}",
            reply_markup=builder.as_markup() if needs_confirm else None,
            parse_mode=PARSE_MODE
        )
        logger.info(f"Successfully sent reminder {rid} to thread {thread_id}")
        
        # Auto-deletion for single-fire reminders
        if not is_recurring:
            await delete_reminder(rid)
            try:
                scheduler.remove_job(_job_id(rid))
            except Exception:
                pass
            logger.info(f"One-time reminder {rid} deleted and unscheduled.")
            
    except TelegramForbiddenError:
        logger.error(f"Bot was kicked from chat {chat_id}. Cannot send reminder {rid}.")
    except TelegramBadRequest as e:
        err_msg = str(e).lower()
        if "thread not found" in err_msg or "chat not found" in err_msg:
            logger.warning(f"Target {thread_id}/{chat_id} is invalid. Disabling reminder {rid}.")
            await toggle_reminder_status(rid)
            try:
                scheduler.remove_job(_job_id(rid))
            except Exception:
                pass
        else:
            logger.error(f"BadRequest in reminder {rid}: {e}")
    except Exception as e:
        logger.exception(f"Unexpected error in reminder job {rid}: {e}")

async def load_reminders(bot: Bot):
    """Fetches all active reminders from DB and schedules them."""
    chat_id = await get_setting("group_chat_id")
    if not chat_id:
        logger.warning("Group chat ID not set. Skipping scheduler load.")
        return
    
    try:
        reminders = await get_reminders()
        count = 0
        for r in reminders:
            # rid, thread_id, text, r_time, days, active, topic_name, needs_confirm, specific_date, is_recurring
            if r[5]: # active
                await add_reminder_to_scheduler(bot, int(chat_id), r[1], r[2], r[0], r[3], r[4], bool(r[7]), r[8], bool(r[9]))
                count += 1
        logger.info(f"Loaded {count} active reminders into scheduler.")
    except Exception as e:
        logger.error(f"Failed to load reminders: {e}")

async def run_maintenance_job(bot: Bot):
    """Weekly maintenance: backup DB and cleanup logs."""
    logger.info("Starting weekly maintenance...")
    try:
        # 1. Cleanup logs
        await cleanup_old_logs(30)
        
        # 2. Send backup to admins
        if os.path.exists(DB_PATH):
            doc = FSInputFile(DB_PATH, filename=f"backup_{datetime.now().strftime('%Y%m%d')}.db")
            for admin_id in ADMIN_IDS:
                try:
                    await bot.send_document(admin_id, doc, caption="📦 Еженедельный бэкап базы данных.")
                except Exception as e:
                    logger.warning(f"Failed to send backup to {admin_id}: {e}")
        
        logger.info("Weekly maintenance completed successfully.")
    except Exception as e:
        logger.error(f"Maintenance job failed: {e}")

def start_maintenance_jobs(bot: Bot):
    """Schedules the weekly maintenance job for Sunday 03:00."""
    scheduler.add_job(
        run_maintenance_job,
        "cron",
        day_of_week="sun",
        hour=3,
        minute=0,
        args=[bot],
        id="weekly_maintenance",
        replace_existing=True
    )
    logger.info("Weekly maintenance job scheduled (Sundays 03:00).")

async def reload_scheduler(bot: Bot) -> None:
    scheduler.remove_all_jobs()
    start_maintenance_jobs(bot)
    await load_reminders(bot)
    if not scheduler.running:
        scheduler.start()

async def add_reminder_to_scheduler(bot: Bot, chat_id: int, thread_id: int, text: str, rid: int, time_str: str, days_str: str, needs_confirm: bool = False, specific_date: str = None, is_recurring: bool = True):
    """Adds a job to the scheduler, deciding between cron (recurring) or date (one-time) triggers."""
    offset_minutes = await _get_time_offset_minutes()
    
    if specific_date:
        # Date-specific reminders are ALWAYS one-time if not specified otherwise
        bot_fire_time = datetime.strptime(f"{specific_date} {time_str}", "%Y-%m-%d %H:%M")
        fire_time = bot_fire_time - timedelta(minutes=offset_minutes)
        tz = scheduler.timezone
        if hasattr(tz, "localize"):
            fire_time = tz.localize(fire_time)
        else:
            fire_time = fire_time.replace(tzinfo=tz)
        
        if fire_time < datetime.now(tz):
            logger.warning(f"Skipping scheduled time in the past: {fire_time}")
            return
            
        scheduler.add_job(
            send_reminder_job,
            'date',
            run_date=fire_time,
            args=[bot, chat_id, thread_id, text, rid, needs_confirm, False], # specific_date is never recurring here
            id=_job_id(rid),
            replace_existing=True
        )
    else:
        hour, minute, shift_days = _convert_bot_time_to_server(time_str, offset_minutes)
        shifted_days_str = _shift_days(days_str, shift_days)
        # Day-of-week based reminders
        kwargs = {
            'trigger': 'cron',
            'hour': hour,
            'minute': minute,
            'args': [bot, chat_id, thread_id, text, rid, needs_confirm, is_recurring],
            'id': _job_id(rid),
            'replace_existing': True
        }
        if shifted_days_str != "all":
            kwargs['day_of_week'] = shifted_days_str
            
        scheduler.add_job(send_reminder_job, **kwargs)
