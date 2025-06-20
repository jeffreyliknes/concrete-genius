#!/usr/bin/env python3
"""
news_hunter.py
-------------------------------------------
Scan Google News for each company / domain and flag
articles that indicate NEW PLANTS, EXPANSIONS, or other
cap‑ex triggers for concrete‑equipment purchases.

Outputs: news_triggers.csv
Columns : company, domain, trigger_date, headline, article_url, reason
-------------------------------------------
Environment (.env or shell)

OPENAI_API_KEY   = ...
NEWS_WINDOW_DAYS = 365            # optional, default inside script
DELAY_SECONDS    = 3              # polite pause between Google calls
-------------------------------------------
"""

import os
import time
import csv
import re
from datetime import datetime, timedelta, timezone

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import openai
import pandas as pd
from dateutil import parser as dateparse   # NEW – forgiving parser

load_dotenv()

# ---- Config ----------------------------------------------------------- #
INPUT_FILE       = "scored_prospects.csv"
OUTPUT_FILE      = "news_triggers.csv"
NEWS_WINDOW_DAYS = int(os.getenv("NEWS_WINDOW_DAYS", 120))
DELAY_SECONDS    = float(os.getenv("DELAY_SECONDS", 3))

openai.api_key   = os.getenv("OPENAI_API_KEY")
if not openai.api_key:
    raise SystemExit("❌  Set OPENAI_API_KEY in .env or environment")

USER_AGENT       = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.4 Safari/605.1.15"
)

GPT_MODEL        = "gpt-4o-mini"           # ~$0.003 / 1k tokens
TRIGGER_PROMPT   = (
    "You are a B2B prospecting analyst. "
    "Given the news blurb below, say ONLY 'yes' or 'no' followed by a pipe "
    "and one‑sentence reason whether the company is expanding concrete "
    "production capacity (new plant, plant upgrade, large fleet purchase, etc.).\n\n"
    "NEWS:\n\n{blurb}"
)

rss_template = "https://news.google.com/rss/search?q={query}"

# ---------------------------------------------------------------------- #

def google_news_rss(query: str) -> list[dict]:
    """Return list of dicts: title, link, pub_date, description."""
    url = rss_template.format(query=requests.utils.quote(query))
    xml = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=10).text
    soup = BeautifulSoup(xml, "xml")
    articles = []
    for item in soup.find_all("item"):
        date_str = item.pubDate.text
        try:
            item_date = dateparse.parse(date_str)
            # Ensure datetime is naive UTC for consistent comparison
            if item_date.tzinfo is not None:
                item_date = item_date.astimezone(timezone.utc).replace(tzinfo=None)
        except (ValueError, TypeError):
            print(f"⚠️  Could not parse date '{date_str}' – skipping item")
            continue
        articles.append(
            {
                "title"      : item.title.text,
                "link"       : item.link.text,
                "pub_date"   : item_date,
                "description": item.description.text,
            }
        )
    return articles


def is_recent(d: datetime) -> bool:
    return d >= datetime.utcnow() - timedelta(days=NEWS_WINDOW_DAYS)


def gpt_relevant(blurb: str) -> tuple[bool, str]:
    """Return (True/False, reason_string)."""
    prompt = TRIGGER_PROMPT.format(blurb=blurb[:1500])  # keep tokens small
    try:
        resp = openai.chat.completions.create(
            model=GPT_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2,
        )
        answer = resp.choices[0].message.content.lower()
        if answer.startswith("yes"):
            reason = answer.split("|", 1)[-1].strip() if "|" in answer else ""
            return True, reason
    except Exception as e:
        print(f"⚠ GPT error: {e}")
    return False, ""

# ---------------------------------------------------------------------- #

def main():
    df = pd.read_csv(INPUT_FILE)
    if "gpt_score" in df.columns:
        df = df[df["gpt_score"] == "YES"]

    triggers = []

    for _, row in df.iterrows():
        company = str(row.get("company", "")).strip()
        domain  = str(row.get("domain", "")).strip()

        if not company and not domain:
            continue

        query = f'"{company}" OR "{domain}" (plant OR facility OR expansion OR opens OR new site OR upgrade)'
        print(f"🔎  {company[:40]:40} | searching news …")
        try:
            for art in google_news_rss(query)[:10]:   # look at first 10 hits
                if not is_recent(art["pub_date"]):
                    continue
                blurb = f"{art['title']}. {BeautifulSoup(art['description'], 'lxml').text}"
                ok, why = gpt_relevant(blurb)
                if ok:
                    triggers.append(
                        {
                            "company"      : company,
                            "domain"       : domain,
                            "trigger_date" : art["pub_date"].strftime("%Y-%m-%d"),
                            "headline"     : art["title"],
                            "article_url"  : art["link"],
                            "reason"       : why,
                        }
                    )
                    # one good hit is enough
                    break
        except Exception as e:
            print(f"⚠ News check error for {company}: {e}")

        time.sleep(DELAY_SECONDS)

    if triggers:
        pd.DataFrame(triggers).to_csv(OUTPUT_FILE, index=False)
        print(f"\n✅ Saved {len(triggers)} trigger rows → {OUTPUT_FILE}")
    else:
        print("\n⚠  No triggers found in the current window.")

# ---------------------------------------------------------------------- #
if __name__ == "__main__":
    main()