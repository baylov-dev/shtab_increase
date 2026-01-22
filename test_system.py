import asyncio
import os
import sys
import logging
from datetime import datetime

# Add current directory to path
sys.path.append(os.getcwd())

from database import init_db, db_session, get_stats, get_setting, set_setting, check_db_health
from config import TIMEZONE, ADMIN_IDS

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("diagnostic")

async def run_diagnostics():
    print("💎 --- INCREASE STAFF BOT: СИСТЕМНАЯ ДИАГНОСТИКА --- 💎")
    print(f"🕒 Время запуска: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("-" * 50)

    # 1. Environment & Config
    print("⚙️ [1/5] Проверка конфигурации...")
    print(f"├ Timezone: {TIMEZONE}")
    print(f"├ Admin IDs: {ADMIN_IDS}")
    print(f"└ Bot Token: {'Установлен' if os.getenv('BOT_TOKEN') else 'ОТСУТСТВУЕТ'}")
    print("✅ Конфигурация в норме.")
    print("-" * 50)

    # 2. Database Health & Schema
    print("📦 [2/5] Проверка Базы Данных...")
    await init_db()
    
    health = await check_db_health()
    if health:
        print("├ Статус БД: 🟢 Online (Здорова)")
    else:
        print("├ Статус БД: 🔴 ERROR (Недоступна)")
        return

    async with db_session() as db:
        # Check reminders table columns
        async with db.execute("PRAGMA table_info(reminders)") as cursor:
            rows = await cursor.fetchall()
            columns = [row[1] for row in rows]
            required = ['id', 'thread_id', 'text', 'time', 'days', 'active', 'specific_date', 'needs_confirm', 'is_recurring']
            missing = [c for c in required if c not in columns]
            
            if not missing:
                print("├ Схема напоминаний: ✅ Соответствует (Pro Edition)")
            else:
                print(f"├ Схема напоминаний: ⚠️ ОТСУТСТВУЮТ КОЛОНКИ: {missing}")

        # Check settings table
        async with db.execute("SELECT COUNT(*) FROM settings") as c:
            s_count = (await c.fetchone())[0]
            print(f"└ Настройки: `{s_count}` записей в БД")
    print("-" * 50)

    # 3. Features Check
    print("🚀 [3/5] Проверка Активных Функций...")
    
    w_enabled = await get_setting("welcome_enabled", "0") == "1"
    w_text = await get_setting("welcome_text", "Не задано")
    print(f"├ Приветствие: {'🟢 ВКЛ' if w_enabled else '⚪️ ВЫКЛ'}")
    print(f"├ Шаблон: `{w_text[:30]}...`" if len(w_text) > 30 else f"├ Шаблон: `{w_text}`")
    group_chat_id = await get_setting("group_chat_id")
    print(f"├ Привязка к группе (/bind): {'🟢 Есть' if group_chat_id else '⚠️ Нет'}")
    
    t_cnt, r_cnt = await get_stats()
    print(f"├ Топиков: {t_cnt}")
    print(f"└ Активных напоминаний: {r_cnt}")
    print("-" * 50)

    # 4. Scheduler Integrity (Future Jobs)
    print("⏱ [4/5] Проверка планировщика (будущие уведомления)...")
    from apscheduler.triggers.cron import CronTrigger
    from apscheduler.triggers.date import DateTrigger
    from scheduler_service import scheduler, add_reminder_to_scheduler, load_reminders, _convert_bot_time_to_server, _shift_days
    from database import get_reminders
    import pytz
    from datetime import timedelta

    fake_bot = object()
    original_offset = await get_setting("time_offset_minutes", "0")
    try:
        if not scheduler.running:
            scheduler.start(paused=True)
        scheduler.remove_all_jobs()

        await set_setting("time_offset_minutes", "120")
        await add_reminder_to_scheduler(fake_bot, 1, 1, "t", 99001, "00:30", "0", False, None, True)
        job = scheduler.get_job("rem_99001")
        if not job or not isinstance(job.trigger, CronTrigger):
            raise RuntimeError("Cron job не создался")
        trg = str(job.trigger)
        ok_shift_back = ("hour='22'" in trg and "minute='30'" in trg and ("day_of_week='sun'" in trg or "day_of_week='6'" in trg))
        print(f"├ Cron +120 мин (Пн 00:30 → Вс 22:30): {'✅' if ok_shift_back else '⚠️'}")

        scheduler.remove_all_jobs()
        await set_setting("time_offset_minutes", "-120")
        await add_reminder_to_scheduler(fake_bot, 1, 1, "t", 99002, "23:30", "0", False, None, True)
        job = scheduler.get_job("rem_99002")
        if not job or not isinstance(job.trigger, CronTrigger):
            raise RuntimeError("Cron job не создался")
        trg = str(job.trigger)
        ok_shift_fwd = ("hour='1'" in trg and "minute='30'" in trg and ("day_of_week='tue'" in trg or "day_of_week='1'" in trg))
        print(f"├ Cron -120 мин (Пн 23:30 → Вт 01:30): {'✅' if ok_shift_fwd else '⚠️'}")

        scheduler.remove_all_jobs()
        h, m, s = _convert_bot_time_to_server("00:30", 120)
        ok_helpers_1 = (h, m, s) == (22, 30, -1)
        ok_helpers_2 = _shift_days("0,2,6", -1) == "6,1,5"
        print(f"├ Сдвиг времени/дней (helpers): {'✅' if (ok_helpers_1 and ok_helpers_2) else '⚠️'}")

        scheduler.remove_all_jobs()
        await set_setting("time_offset_minutes", "180")
        future_date = (datetime.now().date() + timedelta(days=3)).strftime("%Y-%m-%d")
        await add_reminder_to_scheduler(fake_bot, 1, 1, "t", 99003, "23:59", "all", False, future_date, False)
        job = scheduler.get_job("rem_99003")
        if not job or not isinstance(job.trigger, DateTrigger):
            raise RuntimeError("Date job не создался")
        tz = scheduler.timezone
        bot_fire_time = datetime.strptime(f"{future_date} 23:59", "%Y-%m-%d %H:%M")
        server_fire_time = bot_fire_time - timedelta(minutes=180)
        if hasattr(tz, "localize"):
            server_fire_time = tz.localize(server_fire_time)
        else:
            server_fire_time = server_fire_time.replace(tzinfo=tz)
        ok_date = abs((job.trigger.run_date - server_fire_time).total_seconds()) < 1
        print(f"├ Date job (разовая дата через 3 дня): {'✅' if ok_date else '⚠️'}")

        scheduler.remove_all_jobs()
        await set_setting("time_offset_minutes", "0")
        await add_reminder_to_scheduler(fake_bot, 1, 1, "t", 99004, "10:00", "all", False, "2000-01-01", False)
        job = scheduler.get_job("rem_99004")
        ok_past_skip = (job is None)
        print(f"└ Date job в прошлом пропускается: {'✅' if ok_past_skip else '⚠️'}")

        scheduler.remove_all_jobs()
        await set_setting("time_offset_minutes", str(original_offset))
        if not group_chat_id:
            print("└ Перезагрузка из БД: пропуск (не задан `group_chat_id`)")
        else:
            await load_reminders(fake_bot)
            jobs = scheduler.get_jobs()

            tz = scheduler.timezone
            if tz is None:
                tz = pytz.timezone(TIMEZONE)
            now = datetime.now(tz)
            raw_offset = await get_setting("time_offset_minutes", "0")
            try:
                offset_minutes = int(str(raw_offset))
            except Exception:
                offset_minutes = 0

            reminders = await get_reminders()
            expected_ids: set[str] = set()
            skipped_past_date = 0
            for r in reminders:
                rid, thread_id, text, r_time, r_days, active, t_name, needs_confirm, specific_date, is_recurring = r
                if not active:
                    continue
                if specific_date:
                    bot_fire_time = datetime.strptime(f"{specific_date} {r_time}", "%Y-%m-%d %H:%M")
                    server_fire_time = bot_fire_time - timedelta(minutes=offset_minutes)
                    if hasattr(tz, "localize"):
                        server_fire_time = tz.localize(server_fire_time)
                    else:
                        server_fire_time = server_fire_time.replace(tzinfo=tz)
                    if server_fire_time < now:
                        skipped_past_date += 1
                        continue
                expected_ids.add(f"rem_{rid}")

            job_ids = {j.id for j in jobs}
            missing = sorted(expected_ids - job_ids)
            extra = sorted(job_ids - expected_ids)
            ok_db_reload = (not missing and not extra)
            if skipped_past_date:
                print(f"├ Активных разовых в прошлом пропущено: `{skipped_past_date}`")
            print(f"└ Перезагрузка из БД (активные → job'ы): {'✅' if ok_db_reload else '⚠️'}")
            if missing:
                print(f"  - Missing jobs: {missing[:5]}{'...' if len(missing) > 5 else ''}")
            if extra:
                print(f"  - Extra jobs: {extra[:5]}{'...' if len(extra) > 5 else ''}")
    except Exception as e:
        print(f"└ Планировщик: 🔴 Ошибка проверки: {e}")
    finally:
        try:
            scheduler.remove_all_jobs()
        except Exception:
            pass
        try:
            if scheduler.running:
                scheduler.shutdown(wait=False)
        except Exception:
            pass
        await set_setting("time_offset_minutes", str(original_offset))
    print("-" * 50)

    # 5. Deployment Preconditions
    print("🧰 [5/5] Преддеплой-проверки...")
    if not group_chat_id:
        print("├ ⚠️ ВАЖНО: не задан `group_chat_id`. В группе выполните `/bind`.")
    else:
        print("├ Привязка к группе: ✅")
    print("└ Рекомендация: держать бота 24/7, иначе cron-задачи не исполнятся во время.")
    print("-" * 50)
    
    print("\n🏁 ДИАГНОСТИКА ЗАВЕРШЕНА.")
    print("Если все пункты отмечены ✅ или 🟢 — бот готов к работе.")
    print("Если есть 🔴 или ⚠️ — проверьте логи выполнения.")

if __name__ == "__main__":
    asyncio.run(run_diagnostics())
