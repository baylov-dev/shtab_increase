"""
DATABASE MODULE
---------------
This module handles all persistent data storage for the bot using SQLite (aiosqlite).
It manages topics, reminders, system settings, and task completion logs.
Uses WAL mode for high concurrency and an in-memory cache for topic validation.
"""
import aiosqlite
import logging
import os
import pytz
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from typing import List, Tuple, Any, Optional
from config import TIMEZONE

# Professional Logging setup for DB
logger = logging.getLogger("db")

DB_PATH = "data/bot.db"
if not os.path.exists("data"):
    os.makedirs("data")

# Zero DB Hits Strategy Cache
topics_cache = set()

@asynccontextmanager
async def db_session():
    """Context manager for database connections to ensure they are always closed and use WAL mode."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA journal_mode=WAL;")
        yield db

async def init_db():
    """Initializes the database schema and seeds default settings/topics."""
    async with db_session() as db:
        # Core Tables
        await db.execute("""
            CREATE TABLE IF NOT EXISTS topics (
                thread_id INTEGER PRIMARY KEY,
                name TEXT
            )
        """)
        
        await db.execute("""
            CREATE TABLE IF NOT EXISTS reminders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                thread_id INTEGER,
                text TEXT,
                time TEXT,
                days TEXT,
                active INTEGER DEFAULT 1,
                specific_date TEXT DEFAULT NULL,
                needs_confirm INTEGER DEFAULT 0,
                is_recurring INTEGER DEFAULT 1,
                FOREIGN KEY (thread_id) REFERENCES topics(thread_id)
            )
        """)
        
        # Indexes for performance
        await db.execute("CREATE INDEX IF NOT EXISTS idx_rem_thread ON reminders(thread_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_rem_active ON reminders(active)")
        
        await db.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS task_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                reminder_id INTEGER,
                user_id INTEGER,
                user_name TEXT,
                timestamp TEXT,
                FOREIGN KEY (reminder_id) REFERENCES reminders(id)
            )
        """)
        
        # Migrations
        # Add specific_date column if it doesn't exist (for existing DBs)
        try:
            await db.execute('ALTER TABLE reminders ADD COLUMN specific_date TEXT DEFAULT NULL')
        except:
            pass # Already exists
        
        # Add needs_confirm column if it doesn't exist
        try:
            await db.execute('ALTER TABLE reminders ADD COLUMN needs_confirm INTEGER DEFAULT 0')
        except: pass
        
        # Add is_recurring column if it doesn't exist
        try:
            await db.execute('ALTER TABLE reminders ADD COLUMN is_recurring INTEGER DEFAULT 1')
        except: pass

        # Rename is_active to active and change type to INTEGER if it exists as BOOLEAN
        try:
            # Check if is_active column exists
            cursor = await db.execute("PRAGMA table_info(reminders)")
            columns = await cursor.fetchall()
            is_active_exists = any(col[1] == 'is_active' for col in columns)

            if is_active_exists:
                # Rename and convert type
                await db.execute("ALTER TABLE reminders RENAME COLUMN is_active TO active")
                # SQLite doesn't directly support ALTER COLUMN TYPE, but if it was BOOLEAN (0/1), INTEGER is compatible.
                # No explicit type conversion needed if values are already 0 or 1.
        except Exception as e:
            # This might fail if 'active' already exists or other issues.
            # For simplicity, we'll just pass if it's an expected migration error.
            pass
        
        # Load cache
        async with db.execute("SELECT thread_id FROM topics") as cursor:
            async for row in cursor:
                topics_cache.add(row[0])
        
        # Seed Defaults
        defaults = [
            ('welcome_enabled', '0'),
            ('welcome_thread_id', '1'),
            ('welcome_text', 'Привет, {name}! 👋'),
            ('time_offset_minutes', '0'),
        ]
        for key, val in defaults:
            await db.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", (key, val))
        
        await db.execute("INSERT OR IGNORE INTO topics (thread_id, name) VALUES (1, 'General (Общий)')")
        await db.commit()
    logger.info("Database initialized successfully.")

async def set_topic_name(thread_id: int, name: str):
    """Updates or creates a topic entry and refreshes the in-memory cache."""
    async with db_session() as db:
        await db.execute("INSERT OR REPLACE INTO topics (thread_id, name) VALUES (?, ?)", (thread_id, name))
        await db.commit()
    topics_cache.add(thread_id)

async def get_topic_name(thread_id: int) -> str:
    """Returns the name of a topic or a generic fallback if not found."""
    async with db_session() as db:
        async with db.execute("SELECT name FROM topics WHERE thread_id = ?", (thread_id,)) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else f"Топик {thread_id}"

async def add_topic_if_not_exists(thread_id: int, name: str):
    """Adds a new topic to the DB if it doesn't exist, or updates generic names with actual ones."""
    is_generic = name.startswith("Топик ")
    
    async with db_session() as db:
        if thread_id in topics_cache:
            if not is_generic:
                async with db.execute("SELECT name FROM topics WHERE thread_id = ?", (thread_id,)) as cursor:
                    row = await cursor.fetchone()
                    if row and row[0].startswith("Топик "):
                        await db.execute("UPDATE topics SET name = ? WHERE thread_id = ?", (name, thread_id))
                        await db.commit()
            return
        
        await db.execute("INSERT OR IGNORE INTO topics (thread_id, name) VALUES (?, ?)", (thread_id, name))
        await db.commit()
    topics_cache.add(thread_id)

async def delete_topic(thread_id: int):
    """Removes a topic from the database and cache."""
    async with db_session() as db:
        await db.execute("DELETE FROM topics WHERE thread_id = ?", (thread_id,))
        await db.commit()
    if thread_id in topics_cache:
        topics_cache.remove(thread_id)
    logger.info(f"Topic {thread_id} deleted from database.")

async def get_all_topics() -> List[Tuple[int, str]]:
    """Returns a list of all known topics."""
    async with db_session() as db:
        async with db.execute("SELECT thread_id, name FROM topics") as cursor:
            return await cursor.fetchall()

async def add_reminder(thread_id: int, text: str, time: str, days: str, needs_confirm: bool = False, specific_date: str = None, is_recurring: bool = True) -> int:
    """Inserts a new reminder into the database."""
    async with db_session() as db:
        cursor = await db.execute(
            'INSERT INTO reminders (thread_id, text, time, days, needs_confirm, specific_date, is_recurring) VALUES (?, ?, ?, ?, ?, ?, ?)',
            (thread_id, text, time, days, 1 if needs_confirm else 0, specific_date, 1 if is_recurring else 0)
        )
        reminder_id = cursor.lastrowid
        await db.commit()
        return reminder_id

async def get_reminders() -> List[Tuple]:
    """Retrieves all reminders with their associated topic names."""
    async with db_session() as db:
        sql = '''
            SELECT r.id, r.thread_id, r.text, r.time, r.days, r.active, 
                   COALESCE(t.name, 'Thread ' || r.thread_id), r.needs_confirm, r.specific_date, r.is_recurring
            FROM reminders r 
            LEFT JOIN topics t ON r.thread_id = t.thread_id
        '''
        async with db.execute(sql) as cursor:
            return await cursor.fetchall()

async def delete_reminder(reminder_id: int):
    """Permanently removes a reminder from the database."""
    async with db_session() as db:
        await db.execute("DELETE FROM reminders WHERE id = ?", (reminder_id,))
        await db.commit()

async def toggle_reminder_status(reminder_id: int) -> Optional[int]:
    """Toggles the active state of a reminder and returns the new state."""
    async with db_session() as db:
        async with db.execute("SELECT active FROM reminders WHERE id = ?", (reminder_id,)) as cursor:
            row = await cursor.fetchone()
            if row:
                new_val = 0 if row[0] == 1 else 1
                await db.execute("UPDATE reminders SET active = ? WHERE id = ?", (new_val, reminder_id))
                await db.commit()
                return new_val
    return None

async def log_task_completion(reminder_id: int, user_id: int, user_name: str):
    """Records a task completion event for the audit trailer."""
    tz_name = TIMEZONE
    try:
        tz = pytz.timezone(str(tz_name))
    except Exception:
        tz = pytz.timezone(TIMEZONE)
    raw_offset = await get_setting("time_offset_minutes", "0")
    try:
        offset_minutes = int(str(raw_offset))
    except Exception:
        offset_minutes = 0
    now = (datetime.now(tz) + timedelta(minutes=offset_minutes)).strftime("%Y-%m-%d %H:%M:%S")
    async with db_session() as db:
        await db.execute(
            "INSERT INTO task_logs (reminder_id, user_id, user_name, timestamp) VALUES (?, ?, ?, ?)",
            (reminder_id, user_id, user_name, now)
        )
        await db.commit()

async def get_recent_logs(limit: int = 3) -> List[Tuple]:
    """Returns the latest task completion logs."""
    async with db_session() as db:
        query = """
            SELECT l.user_name, r.text, l.timestamp 
            FROM task_logs l
            JOIN reminders r ON l.reminder_id = r.id
            ORDER BY l.id DESC LIMIT ?
        """
        async with db.execute(query, (limit,)) as cursor:
            return await cursor.fetchall()

async def get_setting(key: str, default: Any = None) -> Any:
    """Retrieves a system setting by key."""
    async with db_session() as db:
        async with db.execute("SELECT value FROM settings WHERE key = ?", (key,)) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else default

async def set_setting(key: str, value: Any):
    """Updates or creates a system setting."""
    async with db_session() as db:
        await db.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (key, str(value)))
        await db.commit()

async def get_stats() -> Tuple[int, int]:
    """Returns basic system metrics: total topics and active reminders."""
    async with db_session() as db:
        async with db.execute("SELECT COUNT(*) FROM topics") as c1:
            topics_cnt = (await c1.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM reminders WHERE active = 1") as c2:
            rems_cnt = (await c2.fetchone())[0]
        return topics_cnt, rems_cnt

async def check_db_health() -> bool:
    """Performs a simple query to ensure the database is responsive."""
    try:
        async with db_session() as db:
            async with db.execute("SELECT 1") as cursor:
                return (await cursor.fetchone())[0] == 1
    except Exception as e:
        logger.error(f"Database health check failed: {e}")
        return False
async def cleanup_old_logs(days: int = 30):
    """Deletes logs older than the specified number of days."""
    try:
        tz = pytz.timezone(TIMEZONE)
        raw_offset = await get_setting("time_offset_minutes", "0")
        try:
            offset_minutes = int(str(raw_offset))
        except Exception:
            offset_minutes = 0
        async with db_session() as db:
            limit = (datetime.now(tz) + timedelta(minutes=offset_minutes) - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
            await db.execute('DELETE FROM task_logs WHERE timestamp < ?', (limit,))
            await db.commit()
            logger.info(f"Cleaned up logs older than {days} days.")
    except Exception as e:
        logger.error(f"Cleanup failed: {e}")
