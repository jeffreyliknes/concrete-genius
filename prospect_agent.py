import pandas as pd
import serpapi
import time
from contacts import companies
from collections import Counter

# --------------------------------------------------------------------------- #
# 1  Extract keywords from your master company list
# --------------------------------------------------------------------------- #
keywords = []
for name in companies:
    for word in name.lower().replace('-', ' ').split():
        if len(word) > 2:
            keywords.append(word)

keyword_counts = Counter(keywords)
top_keywords = [word for word, count in keyword_counts.most_common(5)]

# --------------------------------------------------------------------------- #
# 2  Build stronger, fixed search queries
# --------------------------------------------------------------------------- #
queries = [
    "ready mix concrete companies Canada",
    "concrete batch plant operators Canada",
    "precast concrete manufacturers Canada",
    "civil contractors concrete Canada",
    "concrete recycling companies Canada",
    "ready mix concrete companies USA",
    "concrete batch plant operators USA",
    "precast concrete manufacturers USA",
    "civil contractors concrete USA",
    "concrete recycling companies USA"
]

# --------------------------------------------------------------------------- #
# 3  Initialize SerpAPI (insert your real API key here)
# --------------------------------------------------------------------------- #
SERPAPI_KEY = 'f5b2b6bb88e241fa85760004556a92fa492298fbfd596a4d0af4441e0462cbf8'

results = []

for query in queries:
    params = {
        "engine": "google",
        "q": query,
        "api_key": SERPAPI_KEY,
        "num": 10
    }

    search = serpapi.GoogleSearch(params)
    data = search.get_dict()

    for res in data.get("organic_results", []):
        title = res.get("title")
        link = res.get("link")
        results.append({"query": query, "company": title, "url": link})

    time.sleep(2)  # polite delay for rate limits

# --------------------------------------------------------------------------- #
# 4  Save new prospects file
# --------------------------------------------------------------------------- #
df = pd.DataFrame(results)
df.to_csv("new_prospects.csv", index=False)
print(f"\n✅ Saved {len(df)} new prospects to 'new_prospects.csv'")