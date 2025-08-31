#!/usr/bin/env python3
# hunter_enrich.py
import os, time, sqlite3, csv, argparse, json, pathlib
import pandas as pd
import requests
from urllib.parse import urlparse

from proxycurl import Proxycurl
from zerobounce import ZeroBounce
from hunterio import HunterClient

HUNTER_KEY = os.getenv("HUNTER_API_KEY")
SLEEP = float(os.getenv("HUNTER_SLEEP_SECONDS", 1))
MAX_SEARCH = int(os.getenv("HUNTER_MAX_SEARCHES", 25))
MAX_VERIFY = int(os.getenv("HUNTER_MAX_VERIFICATIONS", 50))

# ------------------------------------------------------------------
# External enrichment clients (Proxycurl → ZeroBounce → Hunter)
PROXYCURL_KEY = os.getenv("PROXYCURL_KEY")
ZEROBOUNCE_KEY = os.getenv("ZEROBOUNCE_KEY")

pc = Proxycurl(api_key=PROXYCURL_KEY)
zb = ZeroBounce(ZEROBOUNCE_KEY)
hc = HunterClient(HUNTER_KEY)

CACHE_DIR = pathlib.Path("data/cache")
CACHE_DIR.mkdir(parents=True, exist_ok=True)
# ------------------------------------------------------------------

PREFERRED_POSITIONS = ["owner", "ceo", "president", "vp", "operations", "gm", "manager", "sales"]

def domain_from_url(url: str) -> str:
    if not url:
        return ""
    netloc = urlparse(url.strip()).netloc
    return netloc.replace("www.", "").lower()

def enrich_row(row: dict) -> dict:
    """
    Apply the stage‑gated enrichment flow to a single prospect dict.

    1. skip if product_fit is False or an email is already present
    2. pull company data from Proxycurl (cached)
    3. attempt Hunter domain search for one email, verify via ZeroBounce
    """
    if not row.get("product_fit") or row.get("email_final"):
        return row  # nothing to do

    dom = row["domain"]
    cache_file = CACHE_DIR / f"{dom}.json"
    if cache_file.exists():
        pdata = json.loads(cache_file.read_text())
    else:
        pdata = pc.company(domain=dom)
        cache_file.write_text(json.dumps(pdata))
        time.sleep(0.35)  # be polite to the API

    # minimal fields we care about
    row["linkedin_url"] = pdata.get("linkedin", "")
    row["employee_count"] = pdata.get("employee_count")

    # only now hit Hunter and ZeroBounce
    emails = hc.domain_search(dom, limit=1)["data"]["emails"]
    if emails:
        candidate = emails[0]["value"]
        if zb.validate(candidate)["status"] in ("valid", "catch-all"):
            row["email_final"] = candidate
            row["verification_status"] = "deliverable"

    return row

def pick_best_email(emails):
    # choose best by position/seniority, else first
    for e in emails:
        pos = (e.get("position") or "").lower()
        if any(p in pos for p in PREFERRED_POSITIONS):
            return e
    return emails[0] if emails else None

def hunter_domain_search(domain):
    r = requests.get(
        "https://api.hunter.io/v2/domain-search",
        params={"domain": domain, "api_key": HUNTER_KEY, "limit": 10},
        timeout=30,
    )
    r.raise_for_status()
    return r.json().get("data", {})

def hunter_verify(email):
    r = requests.get(
        "https://api.hunter.io/v2/email-verifier",
        params={"email": email, "api_key": HUNTER_KEY},
        timeout=30,
    )
    r.raise_for_status()
    return r.json().get("data", {})

def load_prospects():
    if USE_SQLITE:
        con = sqlite3.connect(DB_PATH)
        df = pd.read_sql("SELECT url, company_name FROM prospects_raw", con)
        con.close()
    else:
        df = pd.read_csv(INPUT_CSV)
    df["domain"] = df["url"].apply(domain_from_url)
    df = df[df["domain"].str.len() > 0]
    return df

def save_to_sqlite(rows):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS hunter_hits(
            domain TEXT PRIMARY KEY,
            email TEXT,
            first_name TEXT,
            last_name TEXT,
            position TEXT,
            confidence INTEGER,
            verification_status TEXT,
            raw_json TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS contacts(
            url TEXT,
            company_name TEXT,
            email TEXT,
            phone TEXT,
            reason TEXT,
            qualified INTEGER,
            source TEXT
        )
    """)
    for r in rows:
        cur.execute("""INSERT OR REPLACE INTO hunter_hits
            (domain,email,first_name,last_name,position,confidence,verification_status,raw_json)
            VALUES (?,?,?,?,?,?,?,?)""",
            (r["domain"], r["email"], r["first_name"], r["last_name"],
             r["position"], r["confidence"], r["verification_status"], r["raw_json"])
        )
    con.commit()
    con.close()

def main():
    if not HUNTER_KEY:
        raise SystemExit("Set HUNTER_API_KEY in .env")

    df = load_prospects()
    rows = df.to_dict(orient="records")

    enriched_rows = [enrich_row(r) for r in rows]
    found_rows = [
        {
            "domain": r["domain"],
            "email": r.get("email_final"),
            "first_name": r.get("first_name", ""),
            "last_name": r.get("last_name", ""),
            "position": r.get("position", ""),
            "confidence": r.get("confidence", ""),
            "verification_status": r.get("verification_status", ""),
            "raw_json": "",
        }
        for r in enriched_rows
        if r.get("email_final")
    ]

    if not found_rows:
        print("No emails found.")
        return

    # Save
    if USE_SQLITE:
        save_to_sqlite(found_rows)

    pd.DataFrame(found_rows).to_csv(OUTPUT_CSV, index=False)
    print(f"✅ Saved {len(found_rows)} rows to {OUTPUT_CSV}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--sqlite", action="store_true", help="Use SQLite (default)")
    args = parser.parse_args()
    if args.sqlite:
        USE_SQLITE = True
    main()