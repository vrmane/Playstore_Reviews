import hashlib
import time
import random
import os
import json
import logging
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

from google_play_scraper import reviews, Sort
from google.cloud import bigquery


open("monitor.log", "a").close()

if not os.path.exists("checkpoint.json"):
    with open("checkpoint.json", "w") as f:
        f.write("{}")


# =========================================================
print("🧪 FINAL SCRIPT | BigQuery Backend | Parallel | Crash-Resume | Monitoring", flush=True)

# ================= CONFIG =================

PROJECT_ID = "valid-cedar-485813-v7"
DATASET = "reviews"

RAW_TABLE = f"{PROJECT_ID}.{DATASET}.raw_reviews"
STATS_TABLE = f"{PROJECT_ID}.{DATASET}.daily_review_stats"

CHECKPOINT_FILE = "checkpoint.json"

IST = timezone(timedelta(hours=5, minutes=30))
BACKFILL_START_UTC = datetime(2025, 12, 1, tzinfo=timezone.utc)

APPS = [
    {"name": "MoneyView", "id": "com.whizdm.moneyview.loans"},
    {"name": "KreditBee", "id": "com.kreditbee.android"},
    {"name": "Navi", "id": "com.naviapp"},
    {"name": "Fibe", "id": "com.earlysalary.android"},
    {"name": "Kissht", "id": "com.fastbanking"}
]

# ================= MONITOR =================
logging.basicConfig(
    filename="monitor.log",
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

def monitor(msg):
    print(msg, flush=True)
    logging.info(msg)

def monitor_error(msg):
    print("❌", msg, flush=True)
    logging.error(msg)

# ================= CHECKPOINT =================
def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE) as f:
            return json.load(f)
    return {}

def save_checkpoint(data):
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(data, f)

# ================= BIGQUERY =================
def get_bq():
    return bigquery.Client(project=PROJECT_ID)

def insert_rows(client, table, rows):
    if not rows:
        return
    errors = client.insert_rows_json(table, rows)
    if errors:
        raise RuntimeError(f"BigQuery insert error: {errors}")

def load_existing_ids(client):
    query = f"SELECT review_id FROM `{RAW_TABLE}`"
    return set(r.review_id for r in client.query(query))

# ================= HELPERS =================
def generate_review_id(app_id, text, date_ist):
    raw = f"{app_id}|{text.strip()}|{date_ist}"
    return hashlib.sha1(raw.encode()).hexdigest()

# ================= FETCH =================
def fetch_app(app, existing_ids, start_utc, end_utc, resume_token):
    token = resume_token
    rows = []
    seen_tokens = set()

    monitor(f"Fetching {app['name']}")

    while True:
        if token in seen_tokens:
            monitor_error(f"{app['name']} token loop detected")
            break
        seen_tokens.add(token)

        batch, token = reviews(
            app["id"],
            lang="en",
            country="in",
            sort=Sort.NEWEST,
            count=200,
            continuation_token=token
        )

        for r in batch:
            rd = r["at"]
            if rd.tzinfo is None:
                rd = rd.replace(tzinfo=timezone.utc)
            rd_utc = rd.astimezone(timezone.utc)

            if rd_utc > end_utc:
                continue
            if rd_utc < start_utc:
                return rows, None

            text = (r.get("content") or "").strip()
            date_ist = rd_utc.astimezone(IST)

            rid = generate_review_id(
                app["id"],
                text,
                date_ist.strftime("%Y-%m-%d %H:%M:%S")
            )

            if rid in existing_ids:
                continue

            rows.append({
                "review_id": rid,
                "app_name": app["name"],
                "review_date": date_ist.isoformat(),
                "rating": r["score"],
                "inserted_on": datetime.now(IST).isoformat(),
                "review_text": text
            })

            existing_ids.add(rid)

        if not token:
            break

        time.sleep(random.uniform(1, 3))

    return rows, token

# ================= MAIN =================
def main():
    client = get_bq()
    existing_ids = load_existing_ids(client)
    checkpoint = load_checkpoint()

    now_ist = datetime.now(IST)
    yesterday_ist = (now_ist - timedelta(days=1)).replace(
        hour=23, minute=59, second=59, microsecond=0
    )
    end_utc = yesterday_ist.astimezone(timezone.utc)

    today = now_ist.date()
    run_ts = now_ist.isoformat()

    monitor(f"Loaded {len(existing_ids)} existing reviews")
    monitor(f"Window {BACKFILL_START_UTC.date()} → {end_utc.date()}")

    results = {}

    with ThreadPoolExecutor(max_workers=len(APPS)) as executor:
        futures = {
            executor.submit(
                fetch_app,
                app,
                existing_ids,
                BACKFILL_START_UTC,
                end_utc,
                checkpoint.get(app["id"])
            ): app for app in APPS
        }

        for future in as_completed(futures):
            app = futures[future]
            rows, last_token = future.result()
            results[app["id"]] = (app["name"], rows, last_token)

    for app_id, (app_name, rows, last_token) in results.items():

        insert_rows(client, RAW_TABLE, rows)

        insert_rows(client, STATS_TABLE, [{
            "date": str(today),
            "app_name": app_name,
            "reviews_added": len(rows),
            "run_timestamp": run_ts
        }])

        monitor(f"{app_name} added {len(rows)} reviews")

        if last_token:
            checkpoint[app_id] = last_token
        else:
            checkpoint.pop(app_id, None)

    save_checkpoint(checkpoint)
    monitor("JOB FINISHED SUCCESSFULLY")

# ================= SAFE RUN =================
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        monitor_error(f"CRASH: {e}")
        raise
