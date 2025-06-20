#!/usr/bin/env python3
"""
enrichment_agent.py
Pulls contact email for companies using Apollo.io with fallback to page content and People Data Labs (PDL).
"""

import pandas as pd
import time
import requests
import re
from bs4 import BeautifulSoup
import json
from dotenv import load_dotenv
load_dotenv()  # ↳ loads variables from .env into environment
import os

# --------------------------------------------------------------------------- #
# 0  Configuration
# --------------------------------------------------------------------------- #
INPUT_FILE = "scored_prospects.csv"
ENRICHED_FILE = "enriched_prospects.csv"
OUTPUT_FILE = "enriched_contacts.csv"

APOLLO_API_KEY = os.getenv("APOLLO_API_KEY")
if not APOLLO_API_KEY:
    raise SystemExit("❌  Set APOLLO_API_KEY as an environment variable or in a .env file")
APOLLO_SEARCH_ENDPOINT = "https://api.apollo.io/v1/contacts/search"

# People Data Labs (PDL)
PDL_API_KEY = os.getenv("PDL_API_KEY")
if not PDL_API_KEY:
    raise SystemExit("❌  Set PDL_API_KEY as an environment variable or in a .env file")
PDL_PERSON_ENDPOINT = "https://api.peopledatalabs.com/v5/person/search"

# PDL company enrichment
PDL_COMPANY_ENDPOINT = "https://api.peopledatalabs.com/v5/company/enrich"

TARGET_ROLES = [
    "operations manager",
    "plant manager",
    "procurement",
    "owner",
    "principal",
]

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
no_contact = []

EMAIL_REGEX = r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"

# --------------------------------------------------------------------------- #
# 3  For each domain: try Apollo, then fallback to mailto→regex
# --------------------------------------------------------------------------- #
for _, row in df.iterrows():
    domain   = row["domain"]
    content  = row["content"]

    # placeholders
    contact_name = contact_title = contact_email = phone = None
    industry = employee_count = country = None
    source = None

    # --- 3A • Apollo.io -------------------------------------------------- #
    try:
        headers = {"Authorization": f"Bearer {APOLLO_API_KEY}"}
        resp = requests.get(
            APOLLO_SEARCH_ENDPOINT,
            headers=headers,
            params={"email_domain": domain},
            timeout=10,
        )
        if resp.status_code == 200 and resp.text.strip():
            for c in resp.json().get("contacts", []):
                title = (c.get("title") or "").lower()
                if any(r in title for r in TARGET_ROLES):
                    contact_name  = f"{c.get('first_name','')} {c.get('last_name','')}".strip()
                    contact_title = c.get("title")
                    contact_email = c.get("email")
                    phone         = (c.get("phone_numbers") or [None])[0]
                    org           = c.get("organization") or {}
                    industry      = org.get("industry")
                    employee_count= org.get("estimated_num_employees")
                    country       = org.get("country")
                    source        = "apollo"
                    break
    except Exception as e:
        print(f"⚠ Apollo error for {domain}: {e}")

    # --- 3B • People Data Labs (PDL) ------------------------------------ #
    if not contact_email:
        try:
            titles_query = " OR ".join([f'title:\"{r}\"' for r in TARGET_ROLES])
            params = {
                "api_key": PDL_API_KEY,
                "query": f"domain:{domain} AND ({titles_query})",
                "size": 1,
                "titlecase": "true",
            }
            resp = requests.get(PDL_PERSON_ENDPOINT, params=params, timeout=10)
            if resp.status_code == 200:
                for p in resp.json().get("data", []):
                    contact_name   = f"{p.get('first_name','')} {p.get('last_name','')}".strip()
                    contact_title  = p.get("job_title")
                    contact_email  = p.get("work_email") or p.get("email")
                    phone          = (p.get("phone_numbers") or [None])[0]
                    org            = p.get("job_company") or {}
                    industry       = industry or org.get("industry")
                    employee_count = employee_count or org.get("size")
                    country        = country or p.get("location_country")
                    source         = "pdl"
                    break
        except Exception as e:
            print(f"⚠ PDL error for {domain}: {e}")

    # --- 3C • PDL company enrichment (firmographics only) -------------- #
    if not industry or not employee_count or not country:
        try:
            resp = requests.get(
                PDL_COMPANY_ENDPOINT,
                params={
                    "api_key": PDL_API_KEY,
                    "website": domain,
                    "size": 1
                },
                timeout=10,
            )
            if resp.status_code == 200:
                comp = resp.json()
                industry       = industry or comp.get("industry")
                employee_count = employee_count or comp.get("size")
                country        = country or (comp.get("location") or {}).get("country")
        except Exception as e:
            print(f"⚠ PDL company error for {domain}: {e}")

    # --- 3D • Content fallback ------------------------------------------ #
    if not contact_email and content:
        soup = BeautifulSoup(content, "html.parser")
        link = soup.select_one('a[href^="mailto:"]')
        if link:
            contact_email = link["href"].split(":", 1)[1]
            source = "content_mailto"
        else:
            found = re.findall(EMAIL_REGEX, content)
            if found:
                contact_email = found[0]
                source = "content_regex"

    # --- 3E • Collect results ------------------------------------------- #
    if contact_email:
        results.append(
            {
                "company"        : row["company"],
                "domain"         : domain,
                "contact_name"   : contact_name,
                "contact_title"  : contact_title,
                "contact_email"  : contact_email,
                "phone"          : phone,
                "industry"       : industry,
                "employee_count" : employee_count,
                "country"        : country,
                "source"         : source,
            }
        )
    else:
        no_contact.append({"company": row["company"], "domain": domain})

    # be kind to rate‑limits
    time.sleep(2)

# --------------------------------------------------------------------------- #
# 4  Save enriched contacts to CSV
# --------------------------------------------------------------------------- #
df_out = pd.DataFrame(results)
df_out.to_csv(OUTPUT_FILE, index=False)
print(f"\n✅ Saved enriched contacts to '{OUTPUT_FILE}'")

# Save misses for manual follow‑up
if no_contact:
    pd.DataFrame(no_contact).to_csv("no_contacts.csv", index=False)
    print("⚠ Saved domains with no contacts to 'no_contacts.csv'")