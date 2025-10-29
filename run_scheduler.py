# run_scheduler.py — versão BlockingScheduler
import os, time
from datetime import datetime
from pytz import timezone
from apscheduler.schedulers.blocking import BlockingScheduler
import requests

TZ = os.getenv("LOCAL_TZ", "Europe/Lisbon")
HOUR = int(os.getenv("SYNC_HOUR", "7"))
MINUTE = int(os.getenv("SYNC_MINUTE", "15"))
LIMIT = int(os.getenv("SYNC_LIMIT", "500"))
PREFER_MINOR = os.getenv("SYNC_PREFER_MINOR_UNITS", "0")
GATEWAY = os.getenv("LOCAL_GATEWAY", "http://localhost:5000")

def do_daily_sync():
    url = f"{GATEWAY}/suprides/offers/sync-batch?limit={LIMIT}&prefer_minor_units={PREFER_MINOR}"
    try:
        r = requests.post(url, timeout=900)
        print(f"[{datetime.now()}] sync-batch {r.status_code} -> {r.text[:400]}")
    except Exception as e:
        print(f"[{datetime.now()}] sync-batch falhou: {e}")

if __name__ == "__main__":
    sched = BlockingScheduler(timezone=timezone(TZ))
    sched.add_job(do_daily_sync, "cron", hour=HOUR, minute=MINUTE, id="daily_sync")
    print(f"Scheduler ativo: executará daily_sync às {HOUR:02d}:{MINUTE:02d} {TZ} com LIMIT={LIMIT}")
    try:
        sched.start()
    except (KeyboardInterrupt, SystemExit):
        print("Scheduler terminado.")
