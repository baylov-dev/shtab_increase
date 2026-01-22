"""
ADMIN HANDLERS MODULE
---------------------
Contains all administrative logic, FSM flows, and UI keyboards.
Manages reminder creation, system settings, broadcasts, and topic control.
"""
import asyncio
import logging
from datetime import datetime, timedelta
import pytz

from aiogram import Router, F, Bot
from aiogram.types import Message, CallbackQuery, InlineKeyboardButton, ChatMemberUpdated
from aiogram.filters import Command, ChatMemberUpdatedFilter, IS_MEMBER
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.filters.callback_data import CallbackData

from config import ADMIN_IDS, TIMEZONE
from database import (
    get_all_topics, add_reminder, delete_reminder, get_reminders, 
    get_setting, set_setting,
    get_stats, toggle_reminder_status, log_task_completion
)
from scheduler_service import add_reminder_to_scheduler, reload_scheduler, scheduler

logger = logging.getLogger("handlers")

router = Router()

PARSE_MODE = "Markdown"


async def _get_time_offset_minutes() -> int:
    raw = await get_setting("time_offset_minutes", "0")
    try:
        return int(str(raw))
    except Exception:
        return 0


async def _now() -> datetime:
    tz = pytz.timezone(TIMEZONE)
    offset_minutes = await _get_time_offset_minutes()
    return datetime.now(tz) + timedelta(minutes=offset_minutes)


async def _safe_delete(message: Message) -> None:
    try:
        await message.delete()
    except Exception:
        return


async def _edit_or_answer(target: Message | CallbackQuery, text: str, reply_markup=None) -> None:
    message = target.message if isinstance(target, CallbackQuery) else target
    try:
        await message.edit_text(text, reply_markup=reply_markup, parse_mode=PARSE_MODE)
    except Exception:
        await message.answer(text, reply_markup=reply_markup, parse_mode=PARSE_MODE)


class AdminCB(CallbackData, prefix="adm"):
    act: str # action
    val: str = "0" # value

class ReminderForm(StatesGroup):
    text = State()
    thread_id = State()
    time = State()
    schedule_type = State() # 'periodic' or 'once'
    specific_date = State() # YYYY-MM-DD
    days = State()

class BroadcastState(StatesGroup):
    selecting_topics = State()
    waiting_for_message = State()

class SettingsForm(StatesGroup):
    welcome_text = State()
    time_now = State()


async def settings_menu_kb():
    w_on = await get_setting("welcome_enabled", "0") == "1"
    offset_minutes = await _get_time_offset_minutes()
    now = await _now()
    now_str = now.strftime("%H:%M")

    status_icon = "🔵" if w_on else "⚪️"
    text = (
        "⚙️ **Настройки**\n\n"
        f"👋 Приветствие: `{'Вкл' if w_on else 'Выкл'}`\n"
        f"🕒 Время бота: `{now_str}` (коррекция `{offset_minutes:+d} мин`)\n\n"
        "Выберите действие:\n"
        "• включение/выключение — влияет на сообщения о входе\n"
        "• текст приветствия — шаблон, можно `{name}`\n"
        "• время — нужно, если серверное время отличается\n"
        "• обслуживание — бэкап и чистка логов"
    )

    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(
            text=f" {'📴 Выключить' if w_on else '🆗 Включить'} привет",
            callback_data=AdminCB(act="t_w").pack(),
        )
    )
    builder.row(InlineKeyboardButton(text="✏️ Изменить текст приветствия", callback_data=AdminCB(act="e_w").pack()))
    builder.row(InlineKeyboardButton(text="🕒 Указать текущее время", callback_data=AdminCB(act="tm_m").pack()))
    builder.row(InlineKeyboardButton(text="🛠 Обслуживание", callback_data=AdminCB(act="maint").pack()))
    builder.row(InlineKeyboardButton(text="🔙 Назад", callback_data=AdminCB(act="main").pack()))
    return text, builder.as_markup()

# --- Keyboards & UI Helpers ---

async def main_menu_kb():
    """Generates the main administrative dashboard text and keyboard."""
    now = await _now()
    time_str = now.strftime("%H:%M")
    date_str = now.strftime("%d.%m.%Y")
    t_cnt, r_cnt = await get_stats()
    
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="⏰ Задачи", callback_data=AdminCB(act="task_m").pack()),
        InlineKeyboardButton(text="📢 Объявления", callback_data=AdminCB(act="broad_m").pack())
    )
    builder.row(
        InlineKeyboardButton(text="📁 Топики", callback_data=AdminCB(act="struct_m").pack()),
        InlineKeyboardButton(text="⚙️ Настройки", callback_data=AdminCB(act="sets").pack())
    )
    builder.row(InlineKeyboardButton(text="🛑 Закрыть", callback_data=AdminCB(act="cls").pack()))
    
    text = (
        f"💎 **Панель управления**\n\n"
        f"📅 `{date_str}`  🕒 `{time_str}`\n"
        f"⏰ Задач: `{r_cnt}`  📁 Топиков: `{t_cnt}`\n\n"
        "Как пользоваться:\n"
        "• ⏰ Задачи — напоминания по времени/дням или по дате\n"
        "• 📢 Объявления — разовая рассылка по выбранным топикам\n"
        "• 📁 Топики — список топиков, которые бот запомнил\n"
        "• ⚙️ Настройки — приветствие, время, обслуживание\n\n"
        "Выберите раздел:"
    )
    return text, builder.as_markup()

# --- Handlers ---

@router.message(Command("admin"))
async def cmd_admin(message: Message, state: FSMContext):
    """Entry point for the admin panel. Restricted to ADMIN_IDS."""
    if message.from_user.id not in ADMIN_IDS:
        logger.warning(f"Unauthorized /admin access attempt from {message.from_user.id}")
        return
    
    await state.clear()
    text, kb = await main_menu_kb()
    await message.answer(text, reply_markup=kb, parse_mode=PARSE_MODE)

@router.message(Command("bind"))
async def cmd_bind(message: Message):
    """Explicitly binds the bot to the current supergroup for reminders."""
    if message.from_user.id not in ADMIN_IDS: return
    if message.chat.type not in ["group", "supergroup"]:
        await message.answer("❌ Используйте эту команду в супергруппе с топиками!")
        return
    
    await set_setting("group_chat_id", message.chat.id)
    await message.answer(f"✅ Группа успешно привязана!\n`ID: {message.chat.id}`", parse_mode=PARSE_MODE)
    logger.info(f"Admin bound the bot to chat {message.chat.id}")

@router.callback_query(AdminCB.filter(F.act == "main"))
async def back_to_main(callback: CallbackQuery, state: FSMContext):
    """Returns the UI to the main dashboard menu."""
    await callback.answer()
    await state.clear()
    text, kb = await main_menu_kb()
    await callback.message.edit_text(text, reply_markup=kb, parse_mode=PARSE_MODE)

# --- Block 1: Tasks (⏰ Задачи) ---
@router.callback_query(AdminCB.filter(F.act == "task_m"))
async def task_m(callback: CallbackQuery):
    await callback.answer()
    text = (
        "⏰ **Задачи**\n\n"
        "• 📋 Список задач — включить/выключить, удалить, создать\n"
        "• ➕ Создать новую — мастер создания задачи\n"
        "• 🔙 Главное меню — вернуться на панель\n\n"
        "Выберите действие:"
    )
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="📋 Список задач", callback_data=AdminCB(act="rems").pack()))
    builder.row(InlineKeyboardButton(text="➕ Создать новую", callback_data=AdminCB(act="add_r").pack()))
    builder.row(InlineKeyboardButton(text="🔙 Главное меню", callback_data=AdminCB(act="main").pack()))
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode=PARSE_MODE)

# --- Block 2: Broadcasts (📢 Объявления) ---
@router.callback_query(AdminCB.filter(F.act == "broad_m"))
async def broadcast_m(callback: CallbackQuery):
    await callback.answer()
    text = (
        "📢 **Центр Объявлений**\n"
        "─── Информирование ───\n\n"
        "Создайте объявление и выберите, в какие топики отправить.\n"
        "Можно выбрать сразу все."
    )
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="🚀 Новое объявление", callback_data=AdminCB(act="broad").pack()))
    builder.row(InlineKeyboardButton(text="🔙 Главное меню", callback_data=AdminCB(act="main").pack()))
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode=PARSE_MODE)

# --- Block 3: Structure (📁 Структура) ---
@router.callback_query(AdminCB.filter(F.act == "struct_m"))
async def structure_m(callback: CallbackQuery):
    await callback.answer()
    text = (
        "📁 **Топики**\n\n"
        "• 📂 Список топиков — что бот видит/запомнил\n"
        "• 🧺 Очистка списка — убрать устаревшие из БД бота (в Telegram не удаляет)\n\n"
        "Выберите действие:"
    )
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="📂 Список топиков", callback_data=AdminCB(act="tops_m").pack()))
    builder.row(InlineKeyboardButton(text="🧺 Очистка списка", callback_data=AdminCB(act="del_t_m").pack()))
    builder.row(InlineKeyboardButton(text="🔙 Главное меню", callback_data=AdminCB(act="main").pack()))
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode=PARSE_MODE)

@router.callback_query(AdminCB.filter(F.act == "tops_m"))
async def topics_menu(callback: CallbackQuery):
    await callback.answer()
    topics = await get_all_topics()
    lines = []
    for tid, name in topics:
        lines.append(f"• `{tid}` — {name}")
    text = (
        "📂 **Список топиков**\n\n"
        "Это список топиков, которые бот сохранил в своей базе.\n\n"
        + ("\n".join(lines) if lines else "_Пусто_")
    )

    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="🔄 Обновить", callback_data=AdminCB(act="tops_m").pack()))
    builder.row(InlineKeyboardButton(text="🧺 Очистка списка", callback_data=AdminCB(act="del_t_m").pack()))
    builder.row(InlineKeyboardButton(text="🔙 Назад", callback_data=AdminCB(act="struct_m").pack()))
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode=PARSE_MODE)

@router.callback_query(AdminCB.filter(F.act == "del_t_m"))
async def del_topics_menu(callback: CallbackQuery):
    """Lists all topics for manual pruning."""
    await callback.answer()
    topics = await get_all_topics()
    builder = InlineKeyboardBuilder()
    for tid, name in topics:
        if tid == 1: continue # Don't delete General
        builder.row(InlineKeyboardButton(text=f"🗑 {name}", callback_data=AdminCB(act="c_del_t", val=str(tid)).pack()))
    builder.row(InlineKeyboardButton(text="🔙 Назад", callback_data=AdminCB(act="struct_m").pack()))
    await callback.message.edit_text(
        "🧺 **Очистка структуры**\n\n"
        "Удаление тут затрагивает только базу бота.\n"
        "В Telegram-топиках ничего не удаляется.\n\n"
        "Выберите топики для удаления из базы бота:",
        reply_markup=builder.as_markup(),
        parse_mode=PARSE_MODE,
    )

@router.callback_query(AdminCB.filter(F.act == "c_del_t"))
async def confirm_del_topic(callback: CallbackQuery, callback_data: AdminCB):
    tid = int(callback_data.val)
    from database import delete_topic
    await delete_topic(tid)
    await callback.answer("Удалено из базы")
    await del_topics_menu(callback)

# --- Block 4: System (⚙️ Система) ---
@router.callback_query(AdminCB.filter(F.act == "sys_m"))
async def system_m(callback: CallbackQuery):
    await callback.answer()
    await sets_m(callback)

# --- Reminders ---
@router.callback_query(AdminCB.filter(F.act == "rems"))
async def list_reminders_h(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.clear()
    reminders = await get_reminders()
    
    text = (
        "📋 **Список задач**\n\n"
        "Как пользоваться:\n"
        "• нажмите на задачу — включить/выключить\n"
        "• 🗑 — удалить задачу\n"
        "• ➕ Создать — добавить новую\n\n"
    )
    
    if not reminders:
        text += "_Пока пусто._\n"
    else:
        for r in reminders:
            # rid, thread_id, text, time, days, active, topic, confirm, date, recurring
            rid, thread_id, r_text, r_time, r_days, active, t_name, needs_confirm, specific_date, is_recurring = r
            status = "🔔" if active else "🔕"
            
            if specific_date:
                d_str = f"📅 {datetime.strptime(specific_date, '%Y-%m-%d').strftime('%d.%m.%Y')}"
            else:
                days_map = {"0":"Пн","1":"Вт","2":"Ср","3":"Чт","4":"Пт","5":"Сб","6":"Вс","all":"Ежедневно"}
                d_str = days_map["all"] if r_days == "all" else ", ".join([days_map.get(d, d) for d in r_days.split(",")])
            
            short_txt = (r_text[:60] + "…") if len(r_text) > 60 else r_text
            text += f"{status} **{t_name}** — `{r_time}` · `{d_str}`\n"
            text += f"_{short_txt}_\n\n"
    
    builder = InlineKeyboardBuilder()
    if reminders:
        for r in reminders:
            # rid, thread_id, text, time, days, active, topic, confirm, date, recurring
            rid, thread_id, r_text, r_time, r_days, active, t_name, needs_confirm, specific_date, is_recurring = r
            btn_text = f"{'🔔' if active else '🔕'} {t_name} {r_time}"
            builder.row(
                InlineKeyboardButton(text=btn_text, callback_data=AdminCB(act="tog_rem", val=str(rid)).pack()),
                InlineKeyboardButton(text="🗑", callback_data=AdminCB(act="c_del", val=str(rid)).pack()),
            )

    builder.row(
        InlineKeyboardButton(text="➕ Создать", callback_data=AdminCB(act="add_r").pack()),
        InlineKeyboardButton(text="🔙 Назад", callback_data=AdminCB(act="task_m").pack()),
    )
    
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode=PARSE_MODE)

@router.callback_query(AdminCB.filter(F.act == "tog_rem"))
async def tog_rem_h(callback: CallbackQuery, callback_data: AdminCB, state: FSMContext):
    rid = int(callback_data.val)
    new_status = await toggle_reminder_status(rid)
    await callback.answer(f"Статус изменен: {'ВКЛ' if new_status else 'ВЫКЛ'}")
    # Refresh list
    await list_reminders_h(callback, state)

async def get_time_picker_kb(current_time: str):
    builder = InlineKeyboardBuilder()
    # Row 1: Hours
    builder.row(
        InlineKeyboardButton(text="−1 ч", callback_data=AdminCB(act="t_adj", val="-1h").pack()),
        InlineKeyboardButton(text="🕒", callback_data="none"),
        InlineKeyboardButton(text="+1 ч", callback_data=AdminCB(act="t_adj", val="+1h").pack())
    )
    # Row 2: Minutes
    builder.row(
        InlineKeyboardButton(text="−5 м", callback_data=AdminCB(act="t_adj", val="-5m").pack()),
        InlineKeyboardButton(text="⏰", callback_data="none"),
        InlineKeyboardButton(text="+5 м", callback_data=AdminCB(act="t_adj", val="+5m").pack())
    )
    # Row 3: Presets
    builder.row(
        InlineKeyboardButton(text=":00", callback_data=AdminCB(act="t_adj", val="m00").pack()),
        InlineKeyboardButton(text=":15", callback_data=AdminCB(act="t_adj", val="m15").pack()),
        InlineKeyboardButton(text=":30", callback_data=AdminCB(act="t_adj", val="m30").pack()),
        InlineKeyboardButton(text=":45", callback_data=AdminCB(act="t_adj", val="m45").pack())
    )
    # Row 4: Confirm
    builder.row(InlineKeyboardButton(text=f"✅ Подтвердить {current_time}", callback_data=AdminCB(act="t_conf").pack()))
    builder.row(InlineKeyboardButton(text="🔙 Назад", callback_data=AdminCB(act="back_top").pack()))
    return builder.as_markup()

async def show_time_picker(message: Message | CallbackQuery, state: FSMContext, current_time: str = "12:00"):
    await state.update_data(temp_time=current_time)
    text = (
        "🕒 **Время**\n\n"
        f"Выбрано: `{current_time}`\n\n"
        "Настройте кнопками и нажмите «Подтвердить»."
    )
    kb = await get_time_picker_kb(current_time)
    await _edit_or_answer(message, text, reply_markup=kb)

@router.callback_query(AdminCB.filter(F.act == "add_r"))
async def start_add_rem(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.set_state(ReminderForm.text)
    await callback.message.edit_text(
        "📝 **Текст задачи**\n\n"
        "Напишите, что нужно сделать.\n"
        "Разметка Markdown поддерживается.\n",
        reply_markup=InlineKeyboardBuilder().row(
            InlineKeyboardButton(text="🔙 Отмена", callback_data=AdminCB(act="task_m").pack())
        ).as_markup(),
        parse_mode=PARSE_MODE,
    )

@router.message(ReminderForm.text)
async def process_rem_text(message: Message, state: FSMContext):
    await state.update_data(text=message.text)
    await _safe_delete(message)
    await show_topics_selection(message, state)

async def show_topics_selection(message: Message | CallbackQuery, state: FSMContext):
    topics = await get_all_topics()
    builder = InlineKeyboardBuilder()
    
    # Always put General (1) at the top
    builder.row(InlineKeyboardButton(text="💎 General (Общий)", callback_data=AdminCB(act="s_top", val="1").pack()))
    
    for tid, name in topics:
        if tid == 1: continue
        builder.row(InlineKeyboardButton(text=f"📁 {name}", callback_data=AdminCB(act="s_top", val=str(tid)).pack()))
    
    builder.row(InlineKeyboardButton(text="🔄 Обновить список", callback_data=AdminCB(act="ref_t_sel").pack()))
    builder.row(InlineKeyboardButton(text="🔙 Отмена", callback_data=AdminCB(act="task_m").pack()))
    
    text = (
        "📂 **Куда отправлять?**\n\n"
        "Выберите топик, куда будет приходить задача."
    )
    await _edit_or_answer(message, text, reply_markup=builder.as_markup())

@router.callback_query(AdminCB.filter(F.act == "back_top"))
async def back_to_topics(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await show_topics_selection(callback, state)

@router.callback_query(AdminCB.filter(F.act == "ref_t"))
async def refresh_t(callback: CallbackQuery, state: FSMContext):
    await callback.answer("Обновлено")
    await show_topics_selection(callback, state)

@router.callback_query(AdminCB.filter(F.act == "ref_t_sel"))
async def refresh_t_sel(callback: CallbackQuery, state: FSMContext):
    await callback.answer("Обновлено")
    await show_topics_selection(callback, state)

@router.callback_query(AdminCB.filter(F.act == "s_top"))
async def process_s_top(callback: CallbackQuery, callback_data: AdminCB, state: FSMContext):
    await callback.answer()
    await state.update_data(thread_id=int(callback_data.val))
    await show_time_picker(callback, state)

@router.callback_query(AdminCB.filter(F.act == "t_adj"))
async def adjust_time_h(callback: CallbackQuery, callback_data: AdminCB, state: FSMContext):
    data = await state.get_data()
    t_str = data.get("temp_time", "12:00")
    h, m = map(int, t_str.split(":"))
    
    adj = callback_data.val
    if adj == "+1h": h = (h + 1) % 24
    elif adj == "-1h": h = (h - 1) % 24
    elif adj == "+5m": m = (m + 5) % 60
    elif adj == "-5m": m = (m - 5) % 60
    elif adj.startswith("m"): m = int(adj[1:])
    
    new_time = f"{h:02d}:{m:02d}"
    await state.update_data(temp_time=new_time)
    await callback.answer()
    
    text = (
        "🕒 **Время**\n\n"
        f"Выбрано: `{new_time}`\n\n"
        "Нажмите «Подтвердить»."
    )
    kb = await get_time_picker_kb(new_time)
    await callback.message.edit_text(text, reply_markup=kb, parse_mode=PARSE_MODE)

@router.callback_query(AdminCB.filter(F.act == "t_conf"))
async def time_confirm_h(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    final_time = data.get("temp_time", "12:00")
    await state.update_data(time=final_time)
    await callback.answer(f"Время {final_time} установлено")
    
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="🔄 Повторять (дни недели)", callback_data=AdminCB(act="s_type", val="periodic").pack()))
    builder.row(InlineKeyboardButton(text="🗓 Один раз (дата)", callback_data=AdminCB(act="s_type", val="once").pack()))
    builder.row(InlineKeyboardButton(text="🔙 Отмена", callback_data=AdminCB(act="task_m").pack()))
    
    text = (
        "📅 **Когда отправлять?**\n\n"
        "• Повторять — по выбранным дням недели\n"
        "• Один раз — по конкретной дате\n\n"
        "Выберите тип:"
    )
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode=PARSE_MODE)

@router.callback_query(AdminCB.filter(F.act == "s_type"))
async def process_schedule_type(callback: CallbackQuery, callback_data: AdminCB, state: FSMContext):
    stype = callback_data.val
    await state.update_data(schedule_type=stype)
    
    if stype == "periodic":
        await state.update_data(days=[])
        await show_days_kb(callback.message, state)
    else:
        await state.set_state(ReminderForm.specific_date)
        
        # New: CIS Friendly Date Picker with Buttons
        builder = InlineKeyboardBuilder()
        now = await _now()
        
        today = now.strftime("%d.%m.%Y")
        tomorrow = (now + timedelta(days=1)).strftime("%d.%m.%Y")
        
        # Next Monday
        days_ahead = 7 - now.weekday()
        if days_ahead <= 0: days_ahead += 7
        monday = (now + timedelta(days=days_ahead)).strftime("%d.%m.%Y")
        
        builder.row(InlineKeyboardButton(text="📍 Сегодня", callback_data=AdminCB(act="d_sel", val=today).pack()))
        builder.row(InlineKeyboardButton(text="⏩ Завтра", callback_data=AdminCB(act="d_sel", val=tomorrow).pack()))
        builder.row(InlineKeyboardButton(text="🗓 Пн (след.)", callback_data=AdminCB(act="d_sel", val=monday).pack()))
        builder.row(InlineKeyboardButton(text="🔙 Отмена", callback_data=AdminCB(act="task_m").pack()))

        await callback.message.edit_text(
            "📅 **Дата**\n\nВыберите или введите `ДД.ММ.ГГГГ` (например `25.05.2026`):",
            reply_markup=builder.as_markup(),
            parse_mode=PARSE_MODE
        )

@router.callback_query(AdminCB.filter(F.act == "d_sel"))
async def process_date_button(callback: CallbackQuery, callback_data: AdminCB, state: FSMContext):
    date_val = callback_data.val
    # Convert from DD.MM.YYYY to ISO YYYY-MM-DD for storage
    iso_date = datetime.strptime(date_val, "%d.%m.%Y").strftime("%Y-%m-%d")
    await state.update_data(specific_date=iso_date, days="")
    await callback.answer(f"Дата {date_val} выбрана")
    await ask_audit_option(callback, state)

@router.message(ReminderForm.specific_date)
async def process_specific_date(message: Message, state: FSMContext):
    date_str = message.text.strip()
    try:
        # Support both formats for flexibility
        if "." in date_str:
            dt = datetime.strptime(date_str, "%d.%m.%Y")
        else:
            dt = datetime.strptime(date_str, "%Y-%m-%d")
        
        iso_date = dt.strftime("%Y-%m-%d")
        await state.update_data(specific_date=iso_date, days="")
        await _safe_delete(message)
        await ask_audit_option_msg(message, state)
    except ValueError:
        await message.answer(
            "❌ Неверный формат! Используйте `ДД.ММ.ГГГГ` (например, `31.12.2024`)",
            parse_mode=PARSE_MODE,
        )

async def ask_audit_option_msg(message: Message, state: FSMContext):
    # Overload for message context
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="📝 Просто сообщение", callback_data=AdminCB(act="s_aud", val="0").pack()))
    builder.row(InlineKeyboardButton(text="✅ С кнопкой «Выполнено»", callback_data=AdminCB(act="s_aud", val="1").pack()))
    builder.row(InlineKeyboardButton(text="🔙 Отмена", callback_data=AdminCB(act="task_m").pack()))
    
    text = (
        "⚙️ **Как выглядит уведомление?**\n\n"
        "• Просто сообщение — без подтверждения\n"
        "• С кнопкой «Выполнено» — фиксирует факт выполнения"
    )
    await message.answer(text, reply_markup=builder.as_markup(), parse_mode=PARSE_MODE)

async def show_days_kb(message: Message | CallbackQuery, state: FSMContext):
    data = await state.get_data()
    sel = data.get("days", [])
    days_list = [("Пн", "0"), ("Вт", "1"), ("Ср", "2"), ("Чт", "3"), ("Пт", "4"), ("Сб", "5"), ("Вс", "6")]
    builder = InlineKeyboardBuilder()
    for name, val in days_list:
        label = f"💠 {name}" if val in sel else name
        builder.add(InlineKeyboardButton(text=label, callback_data=AdminCB(act="t_day", val=val).pack()))
    builder.adjust(4)
    builder.row(InlineKeyboardButton(text="🌟 Выбрать все дни", callback_data=AdminCB(act="all_d").pack()))
    builder.row(InlineKeyboardButton(text="✅ Готово (Сохранить)", callback_data=AdminCB(act="f_rem").pack()))
    builder.row(InlineKeyboardButton(text="🔙 Отмена", callback_data=AdminCB(act="rems").pack()))
    
    text = (
        "📅 **Дни недели**\n\nВыберите дни:"
    )
    await _edit_or_answer(message, text, reply_markup=builder.as_markup())

@router.callback_query(AdminCB.filter(F.act == "t_day"))
async def t_day(callback: CallbackQuery, callback_data: AdminCB, state: FSMContext):
    data = await state.get_data()
    sel = data.get("days", [])
    day = callback_data.val
    if day in sel: sel.remove(day)
    else: sel.append(day)
    await state.update_data(days=sel)
    await callback.answer()
    
    days_list = [("Пн", "0"), ("Вт", "1"), ("Ср", "2"), ("Чт", "3"), ("Пт", "4"), ("Сб", "5"), ("Вс", "6")]
    builder = InlineKeyboardBuilder()
    for name, val in days_list:
        label = f"💠 {name}" if val in sel else name
        builder.add(InlineKeyboardButton(text=label, callback_data=AdminCB(act="t_day", val=val).pack()))
    builder.adjust(4)
    builder.row(InlineKeyboardButton(text="🌟 Выбрать все дни", callback_data=AdminCB(act="all_d").pack()))
    builder.row(InlineKeyboardButton(text="✅ Готово (Сохранить)", callback_data=AdminCB(act="f_rem").pack()))
    builder.row(InlineKeyboardButton(text="🔙 Отмена", callback_data=AdminCB(act="rems").pack()))
    
    text = (
        "📅 **Дни недели**\n\nВыберите дни:"
    )
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode=PARSE_MODE)

@router.callback_query(AdminCB.filter(F.act == "all_d"))
async def all_d(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.update_data(days="all")
    await ask_audit_option(callback, state)

@router.callback_query(AdminCB.filter(F.act == "f_rem"))
async def f_rem(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    days = data.get("days", [])
    if not days:
        await callback.answer("⚠️ Выберите хотя бы один день!", show_alert=True)
        return
    days_str = ",".join(sorted(days)) if isinstance(days, list) else days
    await state.update_data(days=days_str)
    await ask_audit_option(callback, state)

async def ask_audit_option(callback: CallbackQuery, state: FSMContext):
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="📝 Просто сообщение", callback_data=AdminCB(act="s_aud", val="0").pack()))
    builder.row(InlineKeyboardButton(text="✅ С кнопкой «Выполнено»", callback_data=AdminCB(act="s_aud", val="1").pack()))
    builder.row(InlineKeyboardButton(text="🔙 Отмена", callback_data=AdminCB(act="rems").pack()))
    
    text = (
        "⚙️ **Как выглядит уведомление?**\n\n"
        "• Просто сообщение — без подтверждения\n"
        "• С кнопкой «Выполнено» — фиксирует факт выполнения"
    )
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode=PARSE_MODE)

@router.callback_query(AdminCB.filter(F.act == "s_aud"))
async def process_audit_option(callback: CallbackQuery, callback_data: AdminCB, state: FSMContext):
    needs_confirm = int(callback_data.val)
    await state.update_data(needs_confirm=needs_confirm)
    data = await state.get_data()
    schedule_type = data.get("schedule_type")
    is_recurring = 1 if schedule_type == "periodic" else 0
    await state.update_data(is_recurring=is_recurring)
    await confirm_reminder_h(callback, state)

async def confirm_reminder_h(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    # Safety check for all required fields
    required = ['thread_id', 'text', 'time', 'days', 'needs_confirm']
    if not all(k in data for k in required):
        await callback.answer("⚠️ Ошибка: Сессия истекла. Начните создание заново.", show_alert=True)
        await state.clear()
        text, kb = await main_menu_kb()
        await callback.message.edit_text(text, reply_markup=kb, parse_mode=PARSE_MODE)
        return

    rid = await add_reminder(
        data['thread_id'], 
        data['text'], 
        data['time'], 
        data['days'], 
        data['needs_confirm'],
        data.get('specific_date'),
        bool(data.get('is_recurring', 1))
    )
    chat_id = await get_setting("group_chat_id")
    if chat_id:
        await add_reminder_to_scheduler(
            callback.bot, 
            int(chat_id), 
            data['thread_id'], 
            data['text'], 
            rid, 
            data['time'], 
            data['days'], 
            data['needs_confirm'],
            data.get('specific_date'),
            bool(data.get('is_recurring', 1))
        )
    
    await state.clear()
    confirm = await callback.message.edit_text("✨ **Данные успешно сохранены!**", parse_mode=PARSE_MODE)
    await asyncio.sleep(2)
    text, kb = await main_menu_kb()
    await confirm.edit_text(text, reply_markup=kb, parse_mode=PARSE_MODE)

@router.callback_query(AdminCB.filter(F.act == "del_rem"))
async def del_l(callback: CallbackQuery):
    await callback.answer()
    rems = await get_reminders()
    builder = InlineKeyboardBuilder()
    for r in rems:
        # rid, thread_id, text, time, days, active, topic, confirm, date, recurring
        rid, thread_id, r_text, r_time, r_days, active, t_name, needs_confirm, specific_date, is_recurring = r
        builder.row(InlineKeyboardButton(text=f"🗑 {t_name} | {r_time}", callback_data=AdminCB(act="c_del", val=str(rid)).pack()))
    builder.row(InlineKeyboardButton(text="🔙 Назад", callback_data=AdminCB(act="task_m").pack()))
    await callback.message.edit_text("Удаление:", reply_markup=builder.as_markup())

@router.callback_query(AdminCB.filter(F.act == "c_del"))
async def c_del(callback: CallbackQuery, callback_data: AdminCB, state: FSMContext):
    rid = int(callback_data.val)
    await delete_reminder(rid)
    try:
        scheduler.remove_job(f"rem_{rid}")
    except Exception:
        pass
    await callback.answer("Удалено")
    await list_reminders_h(callback, state)

@router.callback_query(AdminCB.filter(F.act == "sets"))
async def sets_m(callback: CallbackQuery):
    """Displays the settings menu with toggles for bot behavior."""
    await callback.answer()
    text, kb = await settings_menu_kb()
    await callback.message.edit_text(text, reply_markup=kb, parse_mode=PARSE_MODE)

@router.callback_query(AdminCB.filter(F.act == "t_w"))
async def tog_welcome(callback: CallbackQuery):
    """Toggles the state of the welcome message feature."""
    cur = await get_setting("welcome_enabled", "0")
    new_val = "1" if cur == "0" else "0"
    await set_setting("welcome_enabled", new_val)
    await callback.answer(f"Приветствие {'включено' if new_val=='1' else 'выключено'}")
    await sets_m(callback)
@router.callback_query(AdminCB.filter(F.act == "e_w"))
async def edit_welcome_start(callback: CallbackQuery, state: FSMContext):
    """Starts the FSM flow for editing the welcome message template."""
    await callback.answer()
    cur = await get_setting("welcome_text", "Привет, {name}! 👋")
    await state.set_state(SettingsForm.welcome_text)
    builder = InlineKeyboardBuilder().row(InlineKeyboardButton(text="🔙 Отмена", callback_data=AdminCB(act="sets").pack()))
    await callback.message.edit_text(
        f"📝 **Текущий текст:**\n`{cur}`\n\n"
        "Введите новый текст приветствия.\n"
        "Используйте `{name}` там, где должно быть имя пользователя.\n\n"
        "Для отмены нажмите кнопку ниже:",
        reply_markup=builder.as_markup(),
        parse_mode=PARSE_MODE
    )

@router.callback_query(AdminCB.filter(F.act == "tm_m"))
async def time_menu(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    if state is not None:
        await state.clear()

    offset_minutes = await _get_time_offset_minutes()
    server_now = datetime.now(pytz.timezone(TIMEZONE))
    bot_now = await _now()

    text = (
        "🕒 **Коррекция времени**\n"
        "─── Настройка ───\n\n"
        f"Сервер: `{server_now.strftime('%H:%M')}`\n"
        f"Бот: `{bot_now.strftime('%H:%M')}`\n"
        f"Коррекция: `{offset_minutes:+d} мин`\n\n"
        "Нажмите кнопку или введите текущее время (например `14:25`)."
    )

    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="−1 ч", callback_data=AdminCB(act="tm_adj", val="-60").pack()),
        InlineKeyboardButton(text="+1 ч", callback_data=AdminCB(act="tm_adj", val="+60").pack()),
    )
    builder.row(
        InlineKeyboardButton(text="−5 м", callback_data=AdminCB(act="tm_adj", val="-5").pack()),
        InlineKeyboardButton(text="+5 м", callback_data=AdminCB(act="tm_adj", val="+5").pack()),
    )
    builder.row(InlineKeyboardButton(text="✏️ Ввести время", callback_data=AdminCB(act="tm_in").pack()))
    builder.row(InlineKeyboardButton(text="🔙 Назад", callback_data=AdminCB(act="sets").pack()))
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode=PARSE_MODE)

@router.callback_query(AdminCB.filter(F.act == "tm_adj"))
async def time_adjust(callback: CallbackQuery, callback_data: AdminCB, state: FSMContext):
    delta = int(callback_data.val)
    cur = await _get_time_offset_minutes()
    new_val = cur + delta
    await set_setting("time_offset_minutes", str(new_val))
    await reload_scheduler(callback.bot)
    await callback.answer(f"✅ Коррекция: {new_val:+d} мин")
    await time_menu(callback, state)

@router.callback_query(AdminCB.filter(F.act == "tm_in"))
async def time_input_start(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.set_state(SettingsForm.time_now)
    builder = InlineKeyboardBuilder().row(InlineKeyboardButton(text="🔙 Отмена", callback_data=AdminCB(act="tm_m").pack()))
    await callback.message.edit_text(
        "✏️ **Укажите текущее время**\n\n"
        "Формат: `HH:MM`\n"
        "Пример: `14:25`",
        reply_markup=builder.as_markup(),
        parse_mode=PARSE_MODE,
    )

@router.message(SettingsForm.time_now)
async def time_input_finish(message: Message, state: FSMContext):
    raw = (message.text or "").strip()
    try:
        h_str, m_str = raw.split(":")
        h = int(h_str)
        m = int(m_str)
        if not (0 <= h <= 23 and 0 <= m <= 59):
            raise ValueError
    except Exception:
        await message.answer("❌ Неверный формат. Пример: `14:25`", parse_mode=PARSE_MODE)
        return

    tz = pytz.timezone(TIMEZONE)
    server_now = datetime.now(tz)
    desired_now = server_now.replace(hour=h, minute=m, second=server_now.second, microsecond=server_now.microsecond)
    offset_minutes = int((desired_now - server_now).total_seconds() // 60)
    await set_setting("time_offset_minutes", str(offset_minutes))
    await reload_scheduler(message.bot)
    await state.clear()
    await _safe_delete(message)
    await message.answer(f"✅ Время установлено, коррекция: `{offset_minutes:+d} мин`", parse_mode=PARSE_MODE)
    text, kb = await settings_menu_kb()
    await message.answer(text, reply_markup=kb, parse_mode=PARSE_MODE)

@router.message(SettingsForm.welcome_text)
async def process_welcome_text(message: Message, state: FSMContext):
    await set_setting("welcome_text", message.text)
    await state.clear()
    await _safe_delete(message)
    confirm = await message.answer("✨ **Шаблон приветствия обновлен!**", parse_mode=PARSE_MODE)
    await asyncio.sleep(2)
    await _safe_delete(confirm)
    text, kb = await main_menu_kb()
    await message.answer(text, reply_markup=kb, parse_mode=PARSE_MODE)

@router.callback_query(AdminCB.filter(F.act == "maint"))
async def maintenance_m(callback: CallbackQuery):
    """Maintenance sub-menu in settings."""
    await callback.answer()
    text = (
        "🛠 **Обслуживание**\n\n"
        "• 📥 Бэкап БД — отправит файл базы админам\n"
        "• 🧹 Очистить логи — удалит записи старше 30 дней\n\n"
        "Выберите действие:"
    )
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="📥 Сделать Бэкап БД", callback_data=AdminCB(act="m_back").pack()))
    builder.row(InlineKeyboardButton(text="🧹 Очистить логи (30д)", callback_data=AdminCB(act="m_clean").pack()))
    builder.row(InlineKeyboardButton(text="🔙 Назад", callback_data=AdminCB(act="sets").pack()))
    await callback.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode=PARSE_MODE)

@router.callback_query(AdminCB.filter(F.act == "m_back"))
async def manual_backup(callback: CallbackQuery):
    from scheduler_service import run_maintenance_job
    await callback.answer("Бэкап запущен...")
    await run_maintenance_job(callback.bot)
    await callback.message.answer("✅ Бэкап отправлен администраторам.")

@router.callback_query(AdminCB.filter(F.act == "m_clean"))
async def manual_clean(callback: CallbackQuery):
    from database import cleanup_old_logs
    await cleanup_old_logs(30)
    await callback.answer("Логи очищены!")

# --- Broadcast Logic ---
async def _broadcast_topics_ordered() -> list[tuple[int, str]]:
    topics = await get_all_topics()
    uniq: dict[int, str] = {}
    for tid, name in topics:
        try:
            uniq[int(tid)] = str(name)
        except Exception:
            continue
    if 1 not in uniq:
        uniq[1] = "General (Общий)"
    ordered: list[tuple[int, str]] = [(1, uniq[1])]
    rest = [(tid, name) for tid, name in uniq.items() if tid != 1]
    rest.sort(key=lambda x: (x[1].lower(), x[0]))
    ordered.extend(rest)
    return ordered

async def _render_broadcast_topics_menu(message: Message | CallbackQuery, state: FSMContext) -> None:
    topics = await _broadcast_topics_ordered()
    data = await state.get_data()
    selected_raw = data.get("b_selected_topics", None)
    selected: set[int] = set()
    if selected_raw is None:
        selected = {tid for tid, _ in topics}
        await state.update_data(b_selected_topics=sorted(selected))
    else:
        for v in selected_raw:
            try:
                selected.add(int(v))
            except Exception:
                continue

    selected_count = len(selected)
    total_count = len(topics)

    text = (
        "📢 **Объявление: куда отправлять?**\n\n"
        "Выберите топики. По умолчанию выбраны все.\n"
        f"Выбрано: `{selected_count}` из `{total_count}`\n"
    )

    builder = InlineKeyboardBuilder()
    for tid, name in topics:
        is_on = tid in selected
        label = f"{'✅' if is_on else '▫️'} {name}"
        builder.add(InlineKeyboardButton(text=label, callback_data=AdminCB(act="b_tog", val=str(tid)).pack()))
    builder.adjust(2)

    builder.row(
        InlineKeyboardButton(text="🌐 Выбрать все", callback_data=AdminCB(act="b_all").pack()),
        InlineKeyboardButton(text="🧹 Снять все", callback_data=AdminCB(act="b_clr").pack()),
    )
    builder.row(InlineKeyboardButton(text="✅ Дальше (текст)", callback_data=AdminCB(act="b_next").pack()))
    builder.row(InlineKeyboardButton(text="🔙 Отмена", callback_data=AdminCB(act="broad_m").pack()))

    await _edit_or_answer(message, text, reply_markup=builder.as_markup())

@router.callback_query(AdminCB.filter(F.act == "broad"))
async def start_broadcast(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    chat_id = await get_setting("group_chat_id")
    if not chat_id:
        await callback.message.edit_text(
            "❌ Группа не привязана.\n\n"
            "Сделайте так:\n"
            "1) зайдите в нужную супергруппу\n"
            "2) выполните команду `/bind`\n"
            "3) вернитесь в `/admin`",
            parse_mode=PARSE_MODE,
            reply_markup=InlineKeyboardBuilder().row(
                InlineKeyboardButton(text="🔙 Назад", callback_data=AdminCB(act="broad_m").pack())
            ).as_markup(),
        )
        await state.clear()
        return

    await state.set_state(BroadcastState.selecting_topics)
    await state.update_data(b_selected_topics=None)
    await _render_broadcast_topics_menu(callback, state)

@router.callback_query(AdminCB.filter(F.act == "b_tog"))
async def broadcast_toggle_topic(callback: CallbackQuery, callback_data: AdminCB, state: FSMContext):
    await callback.answer()
    topics = await _broadcast_topics_ordered()
    allowed = {tid for tid, _ in topics}

    data = await state.get_data()
    selected_raw = data.get("b_selected_topics", None)
    selected: set[int] = set()
    if selected_raw is None:
        selected = set(allowed)
    else:
        for v in selected_raw:
            try:
                selected.add(int(v))
            except Exception:
                continue

    try:
        tid = int(callback_data.val)
    except Exception:
        return
    if tid not in allowed:
        return

    if tid in selected:
        selected.remove(tid)
    else:
        selected.add(tid)

    await state.update_data(b_selected_topics=sorted(selected))
    await _render_broadcast_topics_menu(callback, state)

@router.callback_query(AdminCB.filter(F.act == "b_all"))
async def broadcast_select_all(callback: CallbackQuery, state: FSMContext):
    await callback.answer("Выбраны все")
    topics = await _broadcast_topics_ordered()
    selected = sorted({tid for tid, _ in topics})
    await state.update_data(b_selected_topics=selected)
    await _render_broadcast_topics_menu(callback, state)

@router.callback_query(AdminCB.filter(F.act == "b_clr"))
async def broadcast_clear_all(callback: CallbackQuery, state: FSMContext):
    await callback.answer("Снято")
    await state.update_data(b_selected_topics=[])
    await _render_broadcast_topics_menu(callback, state)

@router.callback_query(AdminCB.filter(F.act == "b_next"))
async def broadcast_next(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    topics = await _broadcast_topics_ordered()
    allowed = {tid for tid, _ in topics}

    data = await state.get_data()
    selected_raw = data.get("b_selected_topics", None)
    selected: set[int] = set()
    if selected_raw is None:
        selected = set(allowed)
    else:
        for v in selected_raw:
            try:
                selected.add(int(v))
            except Exception:
                continue

    if not selected:
        await callback.answer("Выберите хотя бы один топик", show_alert=True)
        return

    selected &= allowed
    if not selected:
        await callback.answer("Выберите хотя бы один топик", show_alert=True)
        return

    await state.update_data(b_selected_topics=sorted(selected))
    await state.set_state(BroadcastState.waiting_for_message)

    builder = InlineKeyboardBuilder().row(
        InlineKeyboardButton(text="🔙 Назад", callback_data=AdminCB(act="broad").pack())
    )
    await callback.message.edit_text(
        "📝 **Текст объявления**\n\n"
        "Введите текст сообщения.\n"
        "Разметка Markdown поддерживается.",
        reply_markup=builder.as_markup(),
        parse_mode=PARSE_MODE,
    )

@router.message(BroadcastState.waiting_for_message)
async def process_broadcast(message: Message, state: FSMContext, bot: Bot):
    topics = await _broadcast_topics_ordered()
    chat_id = await get_setting("group_chat_id")
    
    if not chat_id:
        await message.answer("❌ Группа не привязана! Используйте `/bind` в группе.")
        await state.clear()
        return

    data = await state.get_data()
    selected_raw = data.get("b_selected_topics", None)
    selected: set[int] = set()
    if selected_raw is None:
        selected = {tid for tid, _ in topics}
    else:
        for v in selected_raw:
            try:
                selected.add(int(v))
            except Exception:
                continue

    success_cnt = 0
    fail_cnt = 0
    
    msg_wait = await message.answer("⏳ Рассылка запущена...")
    
    for tid, name in topics:
        if tid not in selected:
            continue
        try:
            await bot.send_message(
                chat_id=int(chat_id),
                message_thread_id=None if tid == 1 else tid,
                text=f"📢 **ВАЖНОЕ ОБЪЯВЛЕНИЕ**\n\n{message.text}",
                parse_mode=PARSE_MODE
            )
            success_cnt += 1
        except Exception as e:
            logger.warning(f"Failed broadcast to topic {tid}: {e}")
            fail_cnt += 1
            
    await state.clear()
    await msg_wait.edit_text(
        f"🏁 **Рассылка завершена!**\n\n✅ Успешно: `{success_cnt}`\n❌ Ошибок: `{fail_cnt}`",
        parse_mode=PARSE_MODE,
    )
    await asyncio.sleep(3)
    await _safe_delete(msg_wait)
    await broadcast_m_overload(message)

async def broadcast_m_overload(message: Message):
    text = (
        "📢 **Центр Объявлений**\n"
        "─── Информирование ───\n\n"
        "Создайте объявление и выберите, в какие топики отправить.\n"
        "Можно выбрать сразу все."
    )
    builder = InlineKeyboardBuilder()
    builder.row(InlineKeyboardButton(text="🚀 Новое объявление", callback_data=AdminCB(act="broad").pack()))
    builder.row(InlineKeyboardButton(text="🔙 Главное меню", callback_data=AdminCB(act="main").pack()))
    await message.answer(text, reply_markup=builder.as_markup(), parse_mode=PARSE_MODE)

@router.callback_query(AdminCB.filter(F.act == "cls"))
async def cls_h(callback: CallbackQuery):
    """Closes the admin panel by deleting the message."""
    await callback.answer()
    await _safe_delete(callback.message)

@router.callback_query(F.data.startswith("task_done:"))
async def task_done_callback(callback: CallbackQuery):
    rid = int(callback.data.split(":")[1])
    user = callback.from_user
    u_name = user.full_name or user.username or f"ID {user.id}"
    
    await log_task_completion(rid, user.id, u_name)
    await callback.answer("✅ Отмечено!")
    
    try:
        new_text = callback.message.text + f"\n\n✅ **Выполнено:** {u_name}"
        await callback.message.edit_text(new_text, reply_markup=None, parse_mode=PARSE_MODE)
    except Exception as e:
        logger.error(f"Error updating task message: {e}")


# --- Welcome Message Handler ---
@router.chat_member(ChatMemberUpdatedFilter(member_status_changed=IS_MEMBER))
async def on_user_join(event: ChatMemberUpdated, bot: Bot):
    """Detects new members joining the group and sends a welcome message if enabled."""
    if event.old_chat_member.status in ("member", "creator", "administrator"):
        return

    enabled = await get_setting("welcome_enabled", "0") == "1"
    if not enabled:
        return

    chat_id = event.chat.id
    user = event.new_chat_member.user
    full_name = user.full_name or user.first_name or "Участник"
    
    template = await get_setting("welcome_text", "Привет, {name}! 👋")
    text = template.replace("{name}", full_name)
    
    try:
        thread_id_raw = await get_setting("welcome_thread_id", "1")
        try:
            thread_id = int(thread_id_raw)
        except Exception:
            thread_id = 1
            
        await bot.send_message(
            chat_id=chat_id,
            text=text,
            message_thread_id=None if thread_id == 1 else thread_id,
            parse_mode=PARSE_MODE
        )
        logger.info(f"Sent welcome message to {user.id} in chat {chat_id}")
    except Exception as e:
        logger.error(f"Failed to send welcome message: {e}")

# --- Catch-all (Must be last) ---
@router.callback_query()
async def unhandled_callback(callback: CallbackQuery):
    logger.warning(f"UNHANDLED: {callback.data}")
    await callback.answer("⚠️ Кнопка не актуальна. Используйте /admin", show_alert=True)
