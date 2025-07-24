import csv, re, pathlib, pandas as pd

EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')
PHONE_RE = re.compile(r'\+?\d[\d\s().-]{7,}\d')

# Reasons tied to positive keywords
KEYWORDS = {
    "ready mix":   "Ready-mix supplier likely needs higher output consistency",
    "batch plant": "Operates batch plant → may upgrade to volumetric",
    "on-site":     "On-site pours → volumetric mixer saves travel",
    "short load":  "Handles short loads → reduce waste with volumetric"
}

# Positive/negative filters for qualification
POSITIVE_KWS = ["ready mix", "batch plant", "volumetric", "concrete supply"]

NEGATIVE_KWS = [
    # Generic non-target trades
    "roof", "landscap", "window", "fence", "carpet", "tile", "floor",
    "painting", "plumbing", "lawn", "clean", "pressure wash",
    "home builder", "real estate", "hotel", "restaurant",
    # Big-box & retail
    "home depot", "walmart", "lowe", "ace hardware", "garden center",
    "department store", "big box", "hardware store", "winn-dixie",
    # Misc service
    "pawn", "smoke shop", "body scan", "jewelry", "dentist", "clinic"
]

def detect_reason(text: str):
    """Return (reason, qualified_bool) based on keyword filters."""
    text_l = text.lower()

    # Hard disqualify first
    for bad in NEGATIVE_KWS:
        if bad in text_l:
            return "Non-target industry", False

    # Positive mapping
    for kw, reason in KEYWORDS.items():
        if kw in text_l:
            return reason, True

    # If positive keyword appears anywhere else
    for kw in POSITIVE_KWS:
        if kw in text_l:
            return "Concrete supplier (generic)", True

    return "General concrete supplier", False

def run():
    if not pathlib.Path("crawled_websites.csv").exists():
        raise SystemExit("Run website_content_agent.py first.")

    # Extract + write main CSV
    with open("crawled_websites.csv", newline='', encoding='utf-8') as inp, \
         open("contacts_extracted.csv", "w", newline='', encoding='utf-8') as out:
        rdr = csv.DictReader(inp)
        fieldnames = ["url", "email", "phone", "reason", "qualified"]
        writer = csv.DictWriter(out, fieldnames=fieldnames)
        writer.writeheader()

        for row in rdr:
            text = row.get("textContent", "")
            emails = EMAIL_RE.findall(text)
            phones = PHONE_RE.findall(text)
            reason, qual = detect_reason(text)

            writer.writerow({
                "url": row["url"],
                "email": emails[0] if emails else "",
                "phone": phones[0] if phones else "",
                "reason": reason,
                "qualified": qual
            })

    # Split into qualified / non-qualified files
    df = pd.read_csv("contacts_extracted.csv")
    df[df["qualified"]].to_csv("contacts_clients.csv", index=False)
    df[~df["qualified"]].to_csv("contacts_non_clients.csv", index=False)
    print("contacts_extracted.csv, contacts_clients.csv, contacts_non_clients.csv written.")

if __name__ == "__main__":
    run()