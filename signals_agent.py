"""
signals_agent.py
Fetches external buying signals (news mentions, RFPs, job postings) for each scored prospect.
"""

import os
from dotenv import load_dotenv
import pandas as pd
import time
import requests


load_dotenv()

INPUT_FILE = "scored_prospects.csv"
OUTPUT_FILE = "prospects_with_signals.csv"

# 1. Load your top prospects
df = pd.read_csv(INPUT_FILE)
df = df[df["gpt_score"] == "YES"].reset_index(drop=True)

def run_search(query, search_type="news", num=5):
    params = {
        "api_key": os.getenv("SERPWOW_API_KEY"),
        "q": query,
        "search_type": search_type,
        "num": num,
        "output": "json"
    }
    response = requests.get("https://api.serpwow.com/search", params=params)
    return response.json()

news_counts = []
job_counts = []

for _, row in df.iterrows():
    name = row["company"]

    # News signals
    news_query = f"{name} new plant expansion OR RFP OR bidding"
    news_data = run_search(news_query, search_type="news", num=5)
    news_results = news_data.get("news_results", [])
    news_counts.append(len(news_results))
    
    # Job signals
    job_query = f"{name} hiring OR \"plant operator\" OR \"operations manager\""
    job_data = run_search(job_query, search_type="search", num=5)
    job_results = job_data.get("organic_results", [])
    job_counts.append(len(job_results))
    
    print(f"🔎 {name}: {len(news_results)} news, {len(job_results)} jobs")
    time.sleep(1.5)

df["news_signals"] = news_counts
df["job_signals"] = job_counts
df.to_csv(OUTPUT_FILE, index=False)
print(f"\n✅ Saved prospects with signals to '{OUTPUT_FILE}'")