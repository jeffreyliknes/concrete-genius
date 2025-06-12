#!/usr/bin/env python3
"""
enrichment_agent.py
Pulls contact email for companies using Apollo.io with fallback to page content.
"""

import pandas as pd
import time
import requests
import re
from bs4 import BeautifulSoup

# --------------------------------------------------------------------------- #
# 0  Configuration
# --------------------------------------------------------------------------- #
INPUT_FILE = "scored_prospects.csv"
ENRICHED_FILE = "enriched_prospects.csv"
OUTPUT_FILE = "enriched_contacts.csv"

APOLLO_API_KEY = "QOyUCmG5NCK9rc5A9awmbA"
APOLLO_SEARCH_ENDPOINT = "https://api.apollo.io/v1/contacts/search"

# --------------------------------------------------------------------------- #
# 1  Load scored prospects and enriched page‐content
# --------------------------------------------------------------------------- #
df_score = pd.read_csv(INPUT_FILE)
df_enrich = pd.read_csv(ENRICHED_FILE)

# Keep only GPT-approved leads, merge in your scraped HTML/text
df = (
    df_score[df_score["gpt_score"] == "YES"]
    .merge(df_enrich[["domain", "content"]], on="domain", how="left")
)
df["content"] = df["content"].fillna("").astype(str)

# --------------------------------------------------------------------------- #
# 2  Prepare results container
# --------------------------------------------------------------------------- #
results = []

# --------------------------------------------------------------------------- #
# 3  For each domain: try Apollo, then fallback to mailto→regex
# --------------------------------------------------------------------------- #
for _, row in df.iterrows():
    domain = row["domain"]
    content = row["content"]
    contact_email = None
    source = None

    # --- 3A: Apollo.io Search API ---
    try:
        # Some Apollo plans require API key in headers instead of params:
        headers = {"Authorization": f"Bearer {APOLLO_API_KEY}"}
        params = {"email_domain": domain}
        resp = requests.get(APOLLO_SEARCH_ENDPOINT, headers=headers, params=params, timeout=10)

        if resp.status_code == 200 and resp.text.strip():
            data = resp.json()
            contacts = data.get("contacts", [])
            if contacts:
                contact_email = contacts[0].get("email")
                source = "apollo"
        else:
            print(f"⚠ Apollo returned {resp.status_code} / empty for {domain}")
    except Exception as e:
        print(f"⚠ Apollo.io Search failed for {domain}: {e}")

    # --- 3B: Content‐based fallback ---
    if not contact_email and content:
        #  B1: Look for mailto: links
        soup = BeautifulSoup(content, "html.parser")
        mailto = soup.select_one('a[href^="mailto:"]')
        if mailto:
            contact_email = mailto["href"].split(":", 1)[1]
            source = "content_mailto"
        else:
            #  B2: General email regex
            found = re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", content)
            if found:
                contact_email = found[0]
                source = "content_fallback"

    # Log and store
    print(f"🔎 {domain} → {contact_email or 'no email'} ({source})")
    results.append({
        "company": row["company"],
        "domain" : domain,
        "contact_email": contact_email,
        "source": source
    })

    # be kind to the API
    time.sleep(2)

# --------------------------------------------------------------------------- #
# 4  Save enriched contacts to CSV
# --------------------------------------------------------------------------- #
df_out = pd.DataFrame(results)
df_out.to_csv(OUTPUT_FILE, index=False)
print(f"\n✅ Saved enriched contacts to '{OUTPUT_FILE}'")