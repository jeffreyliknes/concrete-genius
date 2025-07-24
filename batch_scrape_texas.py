# batch_scrape_texas.py
import os
import time
import requests
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

TOKEN = os.getenv("APIFY_TOKEN")
MAPS_ACTOR_ID = os.getenv("APIFY_MAPS_ACTOR_ID")  # e.g. Z1m8HE2JfTNU9ZBfx
if not TOKEN:
    raise SystemExit("APIFY_TOKEN missing from environment (.env not loaded).")
if not MAPS_ACTOR_ID:
    raise SystemExit("Set APIFY_MAPS_ACTOR_ID in .env to the actor ID from Apify console (looks like Z1m8HE2JfTNU9ZBfx)")

def start_run(city):
    body = {
        "memory": 1024,
        "searchStringsArray": [f"ready mix concrete {city} TX"],
        "locationQuery": f"{city}, TX",
        "maxCrawledPlacesPerSearch": 80,
        "includeWebResults": True,
        "scrapePlaceDetailPage": True,
        "skipClosedPlaces": True
    }
    url = f"https://api.apify.com/v2/acts/{MAPS_ACTOR_ID}/runs?token={TOKEN}"
    # print("DEBUG URL:", url)
    # print("DEBUG body:", body)
    r = requests.post(url, json=body, timeout=60)
    if r.status_code >= 400:
        print("---- START_RUN ERROR ----", r.status_code, r.text)
    r.raise_for_status()
    return r.json()["data"]["id"]

CITIES = [
    "Austin", "Dallas", "Houston", "San Antonio", "El Paso",
    "Fort Worth", "Corpus Christi", "Lubbock", "McAllen", "Abilene"
]

engine = create_engine("sqlite:///leads.db")

def print_log(run_id: str, lines: int = 25):
    """Fetch and print first N lines of run log for debugging failures."""
    try:
        resp = requests.get(f"https://api.apify.com/v2/logs/{run_id}?token={TOKEN}", timeout=30)
        if resp.ok:
            snippet = "\n".join(resp.text.splitlines()[:lines])
            print("---- RUN LOG ----")
            print(snippet)
    except Exception as e:
        print(f"Could not fetch log for {run_id}: {e}")

def wait_for_dataset(run_id: str) -> pd.DataFrame:
    """Poll run until finished; return DataFrame or empty on failure."""
    while True:
        run = requests.get(
            f"https://api.apify.com/v2/actor-runs/{run_id}?token={TOKEN}",
            timeout=30
        )
        run.raise_for_status()
        data = run.json()["data"]
        status = data["status"]
        if status == "SUCCEEDED":
            ds = data["defaultDatasetId"]
            csv_url = f"https://api.apify.com/v2/datasets/{ds}/items?format=csv&token={TOKEN}"
            return pd.read_csv(csv_url)
        if status in ("FAILED", "ABORTED", "TIMED-OUT"):
            print(f"Run {run_id} ended with {status}")
            print_log(run_id)
            return pd.DataFrame()
        time.sleep(8)

def main():
    frames = []
    for city in CITIES:
        print(f"Starting scrape: {city}")
        try:
            run_id = start_run(city)
        except Exception as e:
            print(f"{city}: failed to start run -> {e}")
            continue

        df = wait_for_dataset(run_id)
        if df.empty:
            print(f"{city}: no data")
            continue
        df["source_city"] = city
        frames.append(df)
        print(f"{city}: {len(df)} rows scraped.")

    if not frames:
        raise SystemExit("No data scraped. Exiting.")

    df_all = pd.concat(frames, ignore_index=True)

    # Normalize → only companies with a website
    name_col = "title" if "title" in df_all.columns else "businessName"
    df_all = df_all[[name_col, "website", "source_city"]].rename(
        columns={name_col: "company_name", "website": "url"}
    )
    df_all = df_all[df_all["url"].notna() & df_all["url"].str.strip().ne("")]
    df_all = df_all.drop_duplicates("url")
    df_all["scraped_at"] = datetime.utcnow()

    print("Total unique with website:", len(df_all))
    df_all.to_sql("prospects_raw", engine, if_exists="append", index=False)
    print("✅ inserted prospects into DB")

if __name__ == "__main__":
    main()