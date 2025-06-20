import pandas as pd
import re
from urllib.parse import urlparse

# -------------------------- Fit / Exclusion Rules -------------------------- #
ALLOWED_COUNTRIES = {"Canada", "United States", "USA"}
ALLOWED_NAICS = {"327320", "327331", "238110"}  # ready‑mix, precast, foundations
EXCLUDED_NAICS = {"333120"}                     # machinery manufacturers
EMPLOYEE_MIN = 20
EMPLOYEE_MAX = 1000

# Known large competitors – drop outright
COMPETITOR_DOMAINS = {
    "lafarge.ca", "lafarge.com", "cemex.com", "holcim.com", "vulcanmaterials.com"
}

RAW_FILE = "enriched_prospects.csv"   # output of your enrichment step
OUTPUT_FILE = "filtered_prospects.csv"

# --------------------------------------------------------------------------- #
# 1  Load the raw prospect list
# --------------------------------------------------------------------------- #

df = pd.read_csv(RAW_FILE)

# Identify which column contains the website URL; handle different heading names
URL_COL = next(
    (col for col in df.columns if col.lower() in ("url", "website", "homepage", "domain_url", "source_url")),
    None
)
if URL_COL is None:
    raise ValueError(
        "Input file is missing a URL / website column. "
        "Add a column named 'url' (or 'website', 'homepage', 'domain_url')."
    )
# Standardise column name to `url` so the rest of the script can stay unchanged
if URL_COL != "url":
    df = df.rename(columns={URL_COL: "url"})


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
# 2b  Firmographic filtering (country, NAICS, employee size, competitors)
# --------------------------------------------------------------------------- #

def parse_naics_codes(val: str) -> list[str]:
    """Extract 6‑digit NAICS codes from arbitrary string/list formats."""
    if pd.isna(val):
        return []
    if isinstance(val, (list, tuple, set)):
        return [str(code) for code in val]
    return re.findall(r"\d{6}", str(val))

def has_allowed_naics(val: str) -> bool:
    codes = parse_naics_codes(val)
    return (
        any(code in ALLOWED_NAICS for code in codes)
        and not any(code in EXCLUDED_NAICS for code in codes)
    )

# Apply filters only if the requisite columns are present
if "country" in df.columns:
    df = df[df["country"].isin(ALLOWED_COUNTRIES)]

if "naics_codes" in df.columns:
    df = df[df["naics_codes"].apply(has_allowed_naics)]

if "employee_count" in df.columns:
    df = df[df["employee_count"].between(EMPLOYEE_MIN, EMPLOYEE_MAX, inclusive="both")]

if "domain" in df.columns:
    df = df[~df["domain"].isin(COMPETITOR_DOMAINS)]

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
# 4b  Optional “maybe” bucket – companies that failed NAICS filter but show
#     recent intent signals (e.g. news or job postings) so you can eyeball them
# --------------------------------------------------------------------------- #
if "naics_codes" in df.columns:
    maybe_mask = ~df["naics_codes"].apply(has_allowed_naics)
else:
    maybe_mask = pd.Series(False, index=df.index)
signal_cols = [c for c in ("news_signals", "job_signals") if c in df.columns]
if signal_cols:
    intent_mask = df[signal_cols].sum(axis=1) > 0
    maybe_df = df[maybe_mask & intent_mask].copy()
    maybe_df.to_csv("maybe_prospects.csv", index=False)

# --------------------------------------------------------------------------- #
# 5  Save the filtered file
# --------------------------------------------------------------------------- #
df.to_csv(OUTPUT_FILE, index=False)
print(f"✅ Saved {len(df)} qualified leads to '{OUTPUT_FILE}'.  "
      f"📋 See 'maybe_prospects.csv' for {len(maybe_df) if 'maybe_df' in locals() else 0} borderline leads.")