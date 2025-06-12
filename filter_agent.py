import pandas as pd
import re
from urllib.parse import urlparse

RAW_FILE = "new_prospects.csv"
OUTPUT_FILE = "filtered_prospects.csv"

# --------------------------------------------------------------------------- #
# 1  Load the raw prospect list
# --------------------------------------------------------------------------- #
df = pd.read_csv(RAW_FILE)

# Guard – drop empty URLs first
df = df[df["url"].notna()]
df = df.drop_duplicates(subset=["url"]).reset_index(drop=True)

# --------------------------------------------------------------------------- #
# 2  Filter out bad patterns
# --------------------------------------------------------------------------- #
BAD_PATTERNS = [
    r"linkedin\.com",
    r"/jobs?",
    r"/careers?",
    r"\.pdf$",
    r"news",
    r"article",
    r"blog",
    r"press",
    r"directory"
]

def is_valid_url(url: str) -> bool:
    for pattern in BAD_PATTERNS:
        if re.search(pattern, url, flags=re.IGNORECASE):
            return False
    return True

df = df[df["url"].apply(is_valid_url)]

# --------------------------------------------------------------------------- #
# 3  Only keep domains that look like companies
# --------------------------------------------------------------------------- #
df = df[df["url"].str.contains(r"\.(ca|com|net)", case=False, regex=True)]

# --------------------------------------------------------------------------- #
# 4  Extract domain for enrichment step
# --------------------------------------------------------------------------- #
def get_domain(url: str) -> str:
    try:
        netloc = urlparse(url).netloc
        return netloc.replace("www.", "")
    except Exception:
        return ""

df["domain"] = df["url"].apply(get_domain)

# --------------------------------------------------------------------------- #
# 5  Save the filtered file
# --------------------------------------------------------------------------- #
df.to_csv(OUTPUT_FILE, index=False)
print(f"✅ Saved {len(df)} filtered leads to '{OUTPUT_FILE}'")