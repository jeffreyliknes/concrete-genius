"""
enrich_contacts.py
Pulls contact data for companies from Clearbit or Hunter.io.
"""

import pandas as pd
import time
import requests
import re

# --------------------------------------------------------------------------- #
# 2  Enrichment setup (Apollo.io)
# --------------------------------------------------------------------------- #
APOLLO_API_KEY = "QOyUCmG5NCK9rc5A9awmbA"
APOLLO_ENDPOINT = "https://api.apollo.io/v1/people/search"

INPUT_FILE = "scored_prospects.csv"
OUTPUT_FILE = "enriched_contacts.csv"

# --------------------------------------------------------------------------- #
# 1  Load the scored prospects where GPT classified as YES
# --------------------------------------------------------------------------- #
df = pd.read_csv(INPUT_FILE)
df = df[df["gpt_score"] == "YES"]  # Only keep good prospects

results = []

# --------------------------------------------------------------------------- #
# 3  Query Apollo.io for each domain
# --------------------------------------------------------------------------- #
for index, row in df.iterrows():
    domain = row['domain']
    print(f"🔎 Searching contacts for {domain} via Apollo...")

    try:
        headers = {
            "Authorization": f"Bearer {APOLLO_API_KEY}"
        }
        params = {
            "q_organization_domains[]": domain,
            "per_page": 10
        }
        response = requests.get(APOLLO_ENDPOINT, params=params, headers=headers)
        data = response.json()
        contacts = data.get("people", [])
        if contacts:
            for contact in contacts:
                results.append({
                    "company": row['company'],
                    "domain": domain,
                    "contact_email": contact.get("email"),
                    "first_name": contact.get("first_name"),
                    "last_name": contact.get("last_name"),
                    "position": contact.get("title"),
                    "confidence": contact.get("confidence_score"),
                    "contact_phone": contact.get("phone"),
                    "content": contact.get("bio") if contact.get("bio") else "",
                    "source": "apollo"
                })
        else:
            results.append({
                "company": row['company'],
                "domain": domain,
                "contact_email": None,
                "first_name": None,
                "last_name": None,
                "position": None,
                "confidence": None,
                "contact_phone": None,
                "content": "",
                "source": "apollo"
            })

    except Exception as e:
        print(f"⚠ Failed for {domain}: {e}")
    
    time.sleep(2)  # Respectful delay

# --------------------------------------------------------------------------- #
# 3D Fallbacks: extract email or phone from scraped content
# --------------------------------------------------------------------------- #
for item in results:
    content = item.get("content", "") or ""
    # content-based email fallback
    if not item.get("contact_email"):
        emails = re.findall(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+", content)
        if emails:
            item["contact_email"] = emails[0]
            item["source"] = "content_fallback"
    # content-based phone fallback
    if not item.get("contact_phone"):
        phones = re.findall(r"\+?\d[\d\-\s\(\)]{7,}\d", content)
        if phones:
            item["contact_phone"] = phones[0]
            item["source"] = "phone_fallback"

# --------------------------------------------------------------------------- #
# 4  Save enriched contacts to file
# --------------------------------------------------------------------------- #
df_out = pd.DataFrame(results, columns=[
    "company","domain","contact_email","first_name","last_name","position","confidence","contact_phone","source"
])
df_out.to_csv(OUTPUT_FILE, index=False)
print(f"\n✅ Saved enriched contacts to '{OUTPUT_FILE}'")
