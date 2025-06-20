import csv
from contacts import companies
import tldextract

"""
seed_analyzer.py

Role:
- Reads your initial list of target companies from contacts.companies.
- Filters to North American targets (US and Canada TLDs).
- Removes duplicate domains.
- Writes a clean seed list to 'seed_list.csv'.
"""

seen_domains = set()
seed_list = []

for entry in companies:
    name = entry if isinstance(entry, str) else entry.get('name') or entry.get('company') or ''
    domain = ''
    if isinstance(entry, dict):
        domain = entry.get('domain') or ''
    elif isinstance(entry, str):
        domain = entry
    # Skip entries without a domain
    if not domain:
        continue
    # Extract the top-level domain (TLD)
    ext = tldextract.extract(domain)
    tld = ext.suffix.lower()
    # Only include US (.com or .us) and Canadian (.ca) domains
    if tld in ('com', 'us', 'ca'):
        # Deduplicate by domain
        if domain not in seen_domains:
            seen_domains.add(domain)
            seed_list.append({'name': name, 'domain': domain})

# Save to CSV
with open('seed_list.csv', 'w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=['name', 'domain'])
    writer.writeheader()
    writer.writerows(seed_list)

print(f"✅ Saved {len(seed_list)} unique North American seeds to 'seed_list.csv'")