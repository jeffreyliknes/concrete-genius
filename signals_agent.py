"""
signals_agent.py
Fetches external buying signals (news mentions, RFPs, job postings) for each scored prospect.
"""

import pandas as pd
import time
from serpapi import GoogleSearch

INPUT_FILE = "scored_prospects.csv"
OUTPUT_FILE = "prospects_with_signals.csv"

# 1. Load your top prospects
df = pd.read_csv(INPUT_FILE)
df = df[df["gpt_score"] == "YES"].reset_index(drop=True)

SERPAPI_KEY = "f5b2b6bb88e241fa85760004556a92fa492298fbfd596a4d0af4441e0462cbf8"

def run_search(query, engine="google", num=5):
    search = GoogleSearch({
        "engine": engine,
        "q": query,
        "api_key": SERPAPI_KEY,
        "num": num
    })
    return search.get_dict()

news_counts = []
job_counts = []

for _, row in df.iterrows():
    name = row["company"]

    # News signals
    news_query = f"{name} new plant expansion OR RFP OR bidding"
    news_data = run_search(news_query, engine="google", num=5)
    news_results = news_data.get("news_results", [])
    news_counts.append(len(news_results))
    
    # Job signals
    job_query = f"{name} hiring OR \"plant operator\" OR \"operations manager\""
    job_data = run_search(job_query, engine="google", num=5)
    job_results = job_data.get("organic_results", [])
    job_counts.append(len(job_results))
    
    print(f"🔎 {name}: {len(news_results)} news, {len(job_results)} jobs")
    time.sleep(1.5)

df["news_signals"] = news_counts
df["job_signals"] = job_counts
df.to_csv(OUTPUT_FILE, index=False)
print(f"\n✅ Saved prospects with signals to '{OUTPUT_FILE}'")