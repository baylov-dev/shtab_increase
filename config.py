import os
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")

# Parse multiple ADMIN_IDS from a comma-separated string (e.g. 123,456)
admin_ids_raw = os.getenv("ADMIN_IDS", os.getenv("ADMIN_ID", "0"))
ADMIN_IDS = [int(i.strip()) for i in admin_ids_raw.split(",") if i.strip().isdigit()]

TIMEZONE = "Asia/Almaty"

if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN is not set in .env file")

if not ADMIN_IDS or ADMIN_IDS == [0]:
    print("WARNING: ADMIN_IDS is not set. Admin features will be inaccessible.")
