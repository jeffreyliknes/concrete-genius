import csv
import requests
import os
import time
from pathlib import Path
from dotenv import load_dotenv
from bs4 import BeautifulSoup

# Always load .env from script directory regardless of current working directory
load_dotenv(Path(__file__).parent / ".env")

APIFY_TOKEN = os.getenv("APIFY_TOKEN")
ACTOR_TASK_ID = os.getenv("APIFY_TASK_ID")  # For task + polling mode
ACTOR_ID = os.getenv("APIFY_ACTOR_ID")      # Optional: direct actor run-sync mode

if not APIFY_TOKEN:
    print("Error: APIFY_TOKEN not set in .env")
    exit(1)

# Mode selection: prefer task if provided, else actor
MODE = "task" if ACTOR_TASK_ID else ("actor" if ACTOR_ID else None)
if MODE is None:
    print("Error: Provide either APIFY_TASK_ID (for task mode) or APIFY_ACTOR_ID (for direct actor mode) in .env")
    exit(1)

print(f"Using mode: {MODE}")

# Create a sample prospects_raw.csv if it doesn't exist
if not os.path.exists("prospects_raw.csv"):
    with open("prospects_raw.csv", "w", newline="", encoding="utf-8") as f:
        f.write("company_name,website\nConcrete Co,https://www.concreteco.com\nEZ Mix,https://www.ezmixinc.com\n")
    print("Sample prospects_raw.csv created with demo data. Edit this file with real domains and re-run.")
    exit(0)

def load_domains(csv_path: str):
    """
    Reads prospects_raw.csv and returns a list of {"url": cleaned_url}.
    Accepts columns named 'url' or 'website', or any cell that starts with http/https.
    """
    import re
    url_pattern = re.compile(r'^https?://', re.I)

    domains = []
    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Preferred explicit columns
            url = (row.get("url") or row.get("website") or "").strip()

            # Fallback: scan all cells for the first URL-looking value
            if not url:
                for val in row.values():
                    val = str(val).strip()
                    if url_pattern.match(val):
                        url = val
                        break

            if url:
                # Normalize angle brackets or stray characters
                url = url.strip("<> \t\r\n")
                domains.append({"url": url})

    print(f"Loaded {len(domains)} domains.")
    if domains:
        print("Sample URLs:", [d["url"] for d in domains[:3]])
    return domains

def run_apify_crawler(domains):
    """
    If MODE == 'task': create a run for the saved task (requires polling).
    If MODE == 'actor': call the actor directly with run-sync-get-dataset-items and return data immediately.
    """
    # Domains may have been collected as list of {"url": "..."}
    flat_urls = [d["url"] for d in domains if d.get("url")]
    # Debug: show raw URLs with repr to catch hidden whitespace/newlines
    print("RAW URLs:", [repr(u) for u in flat_urls])
    if not flat_urls:
        raise SystemExit("No domains found in prospects_raw.csv")

    # Apify Website Content Crawler expects an array of objects: [{"url": "..."}]
    # Also strip whitespace to avoid hidden chars causing "not valid URL" errors.
    start_url_objs = [{"url": u.strip()} for u in flat_urls if u.strip()]
    print("Normalized start URLs:", [o["url"] for o in start_url_objs])
    payload = {"startUrls": start_url_objs}
    print("DEBUG payload:", payload)

    if MODE == "task":
        url = f"https://api.apify.com/v2/actor-tasks/{ACTOR_TASK_ID}/runs?token={APIFY_TOKEN}"
        res = requests.post(url, json=payload)
        if res.status_code >= 400:
            print("---- ERROR STATUS ----", res.status_code)
            print("---- ERROR BODY ----", res.text)
            res.raise_for_status()
        run_id = res.json()["data"]["id"]
        print("Created task run:", run_id)
        return run_id
    else:  # actor mode
        url = f"https://api.apify.com/v2/acts/{ACTOR_ID}/runs/run-sync-get-dataset-items?token={APIFY_TOKEN}"
        res = requests.post(url, json=payload)
        if res.status_code >= 400:
            print("---- ERROR STATUS ----", res.status_code)
            print("---- ERROR BODY ----", res.text)
            res.raise_for_status()
        items = res.json()
        if not items:
            print("Actor returned no items.")
        else:
            write_items_csv(items, "crawled_websites.csv")
            print(f"Wrote {len(items)} items to crawled_websites.csv")
        return None

def write_items_csv(items, output_path):
    keys = sorted({k for item in items for k in item.keys()})
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(items)

def wait_for_run_completion(run_id):
    url = f"https://api.apify.com/v2/actor-runs/{run_id}?token={APIFY_TOKEN}"
    while True:
        res = requests.get(url)
        res.raise_for_status()
        run_data = res.json()["data"]
        status = run_data["status"]
        if status == "SUCCEEDED":
            dataset_id = run_data["defaultDatasetId"]
            return dataset_id
        elif status in ["FAILED", "ABORTED", "TIMED-OUT"]:
            # Fetch log for diagnostics
            log_url = f"https://api.apify.com/v2/actor-runs/{run_id}/log?token={APIFY_TOKEN}&format=text"
            try:
                log_res = requests.get(log_url)
                log_text = log_res.text[:5000]  # limit output
                print("---- RUN ERROR LOG (truncated) ----")
                print(log_text)
            except Exception as e:
                print(f"Could not fetch log: {e}")
            err_msg = run_data.get("statusMessage") or run_data.get("errorMessage") or "No error message provided."
            raise Exception(f"Run {run_id} finished with status {status}. Message: {err_msg}")
        else:
            print(f"Run status: {status}")
            time.sleep(5)

def run_actor_direct(domains):
    actor_id = ACTOR_ID
    flat_urls = [d["url"].strip() for d in domains if d.get("url")]
    if not flat_urls:
        raise SystemExit("No domains for actor direct mode.")
    payload = {
        "startUrls": [{"url": u} for u in flat_urls],
        "crawlerType": "got-scraping",
        "cleanHtml": True,
        "respectsRobotsTxt": False,
        "maxCrawledPages": len(flat_urls),
        "maxCrawledPagesPerDomain": 1
    }
    print("ACTOR DIRECT payload:", payload)
    # Proper run-sync call
    url = f"https://api.apify.com/v2/acts/{actor_id}/runs/run-sync?token={APIFY_TOKEN}"
    res = requests.post(url, json=payload)
    if res.status_code == 404:
        print("Actor endpoint 404. Falling back to simple HTTP fetch.")
        simple_http_fallback(domains)
        return
    if res.status_code >= 400:
        print("---- ACTOR DIRECT ERROR ----", res.status_code, res.text)
        print("Falling back to simple HTTP fetch.")
        simple_http_fallback(domains)
        return
    run_data = res.json().get("data") or {}
    dataset_id = run_data.get("defaultDatasetId")
    if not dataset_id:
        print("No datasetId returned. Falling back.")
        simple_http_fallback(domains)
        return
    # Download dataset items
    ds_url = f"https://api.apify.com/v2/datasets/{dataset_id}/items?format=json&token={APIFY_TOKEN}"
    ds_res = requests.get(ds_url)
    if ds_res.status_code >= 400:
        print("Dataset fetch error, falling back:", ds_res.text[:300])
        simple_http_fallback(domains)
        return
    items = ds_res.json()
    if items:
        write_items_csv(items, "crawled_websites.csv")
        print(f"Actor direct wrote {len(items)} items to crawled_websites.csv")
    else:
        print("Actor direct returned no items, falling back.")
        simple_http_fallback(domains)

def simple_http_fallback(domains, output_path="crawled_websites.csv"):
    """
    Fallback scraper: plain HTTP GET + BeautifulSoup to extract visible text.
    No JS rendering, but fast and avoids Apify issues.
    """
    rows = []
    headers = {"User-Agent": "Mozilla/5.0 (pipeline-fallback)"}
    for d in domains:
        url = d.get("url")
        if not url:
            continue
        try:
            r = requests.get(url, headers=headers, timeout=20)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")
            for tag in soup(["script", "style", "noscript"]):
                tag.decompose()
            text = " ".join(soup.get_text(separator=" ").split())
            rows.append({"url": url, "textContent": text[:50000]})
            print(f"[fallback] Fetched {url} ({len(text)} chars raw, truncated)")
        except Exception as e:
            print(f"[fallback] Error fetching {url}: {e}")
    if rows:
        keys = sorted({k for row in rows for k in row.keys()})
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(rows)
        print(f"[fallback] Wrote {len(rows)} rows to {output_path}")
    else:
        print("[fallback] No rows written.")

if __name__ == "__main__":
    domains = load_domains("prospects_raw.csv")
    if MODE == "actor":
        print("Attempting actor mode; will fallback automatically if it fails.")
        run_actor_direct(domains)
        exit(0)
    run_or_none = run_apify_crawler(domains)
    if MODE == "task":
        dataset_id = wait_for_run_completion(run_or_none)
        download_dataset(dataset_id, "crawled_websites.csv")
        print("Done.")