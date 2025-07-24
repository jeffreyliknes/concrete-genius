#!/usr/bin/env python3
# hunter_enrich.py
import os, time, sqlite3, csv, argparse
import pandas as pd
import requests
from urllib.parse import urlparse

HUNTER_KEY = os.getenv("HUNTER_API_KEY")
SLEEP = float(os.getenv("HUNTER_SLEEP_SECONDS", 1))
MAX_SEARCH = int(os.getenv("HUNTER_MAX_SEARCHES", 25))
MAX_VERIFY = int(os.getenv("HUNTER_MAX_VERIFICATIONS", 50))

USE_SQLITE = True
DB_PATH = "leads.db"
INPUT_CSV = "contacts_clients.csv"        # fallback if not using DB
OUTPUT_CSV = "emails_enriched.csv"

PREFERRED_POSITIONS = ["owner", "ceo", "president", "vp", "operations", "gm", "manager", "sales"]

def domain_from_url(url: str) -> str:
    if not url:
        return ""
    netloc = urlparse(url.strip()).netloc
    return netloc.replace("www.", "").lower()

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
    domains = df["domain"].drop_duplicates().tolist()

    found_rows = []
    searched = 0
    verified = 0

    for d in domains:
        if searched >= MAX_SEARCH:
            print("Reached search limit.")
            break
        try:
            data = hunter_domain_search(d)
            emails = data.get("emails", [])
            if not emails:
                print(f"[NO EMAILS] {d}")
                searched += 1
                time.sleep(SLEEP)
                continue

            picked = pick_best_email(emails)
            if picked:
                email = picked.get("value")
                verification_status = ""
                if email and verified < MAX_VERIFY:
                    v = hunter_verify(email)
                    verification_status = v.get("result", "")
                    verified += 1
                    time.sleep(SLEEP)

                found_rows.append({
                    "domain": d,
                    "email": email,
                    "first_name": picked.get("first_name"),
                    "last_name": picked.get("last_name"),
                    "position": picked.get("position"),
                    "confidence": picked.get("confidence"),
                    "verification_status": verification_status,
                    "raw_json": str(picked)
                })
                print(f"[OK] {d} -> {email} ({verification_status})")

            searched += 1
            time.sleep(SLEEP)
        except requests.HTTPError as e:
            print(f"[HTTP {e.response.status_code}] {d}: {e.response.text[:120]}")
        except Exception as e:
            print(f"[ERR] {d}: {e}")

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