#!/usr/bin/env python3
# === Concrete Genius Lead Runner ===
# 1) Load prospects_raw.csv (company_name, url)
# 2) Resolve final domain (follow redirects, normalize)
# 3) Scrape phones/emails (regex + deobfuscation + JSON-LD)
# 4) MX presence check (cheap validation)
# 5) Write Merged_Final_Leads_Master.csv with confidence + source_url

import csv
import json
import re
import sys
import time
import random
import argparse
from dataclasses import dataclass, asdict
from typing import List, Dict, Set, Optional, Tuple
from urllib.parse import urlparse, urljoin, unquote
from concurrent.futures import ThreadPoolExecutor, as_completed

import httpx
from selectolax.parser import HTMLParser
import tldextract
import phonenumbers

# Optional: MX presence check via dnspython
try:
    import dns.resolver
except Exception:
    dns = None

# ---------------------- Config ----------------------
USER_AGENT = "Mozilla/5.0 (compatible; CG-LeadsRunner/1.0; +https://example.com/bot)"
REQUEST_TIMEOUT = 15
SLEEP_BETWEEN_SITES = (0.5, 1.2)   # jitter between sites
REGION_DEFAULT = "US"
CANDIDATE_PATHS = ["", "contact", "contact-us", "about", "team", "privacy", "impressum", "terms", "sitemap.xml"]
ROLE_EMAILS = {"info", "sales", "office", "contact", "admin", "support", "hello", "enquiries", "service"}
MAX_EMAILS_PER_DOMAIN = 3  # for cold email focus; collect a few options

DEFAULT_PAGE_CONCURRENCY = 4
DEFAULT_PROGRESS_EVERY = 25

# Added constants for chunking, concurrency, append/resume
DEFAULT_CHUNK_SIZE = 100
DEFAULT_SITE_CONCURRENCY = 1
APPEND_MODE_DEFAULT = True
RESUME_DEFAULT = True

INPUT_CSV = "prospects_raw.csv"
OUTPUT_CSV = "Merged_Final_Leads_Master.csv"

# ---------------------- Utils ----------------------

def normalize_url(u: str) -> str:
    u = u.strip()
    if not u:
        return ""
    if not u.startswith(("http://", "https://")):
        u = "https://" + u
    return u

def final_url_and_domain(start_url: str) -> Tuple[str, str]:
    url = normalize_url(start_url)
    try:
        with httpx.Client(follow_redirects=True, timeout=REQUEST_TIMEOUT, headers={"User-Agent": USER_AGENT}) as client:
            r = client.get(url)
            final = str(r.url)
            extracted = tldextract.extract(final)
            domain = ".".join(p for p in [extracted.domain, extracted.suffix] if p)
            return final, domain
    except Exception:
        # Fall back to parsing
        parsed = urlparse(url)
        extracted = tldextract.extract(parsed.netloc or "")
        domain = ".".join(p for p in [extracted.domain, extracted.suffix] if p)
        return url, domain

EMAIL_RAW = re.compile(r'[\w\.-]+@[\w\.-]+\.\w+', re.I)
EMAIL_OBF = [
    (re.compile(r'\s*\[\s*at\s*\]\s*', re.I), '@'),
    (re.compile(r'\s*\(\s*at\s*\)\s*', re.I), '@'),
    (re.compile(r'\s+AT\s+', re.I), '@'),
    (re.compile(r'\s*\[\s*dot\s*\]\s*', re.I), '.'),
    (re.compile(r'\s*\(\s*dot\s*\)\s*', re.I), '.'),
    (re.compile(r'\s+DOT\s+', re.I), '.'),
]

PHONE_RAW = re.compile(r'\+?\d[\d\-\.\s\(\)]{7,}')

def deobfuscate(text: str) -> str:
    s = unquote(text).replace('&#64;', '@').replace('\\u0040', '@')
    for pat, repl in EMAIL_OBF:
        s = pat.sub(repl, s)
    return s

def extract_emails(html: str) -> Set[str]:
    text = deobfuscate(html)
    emails = set(EMAIL_RAW.findall(text))
    # mailto:
    try:
        doc = HTMLParser(html)
        for a in doc.css('a[href^="mailto:"]'):
            href = a.attributes.get('href', '')
            e = deobfuscate(href)[7:]
            e = e.split('?')[0]
            if re.search(EMAIL_RAW, e):
                emails.add(e)
    except Exception:
        pass
    return {e.lower() for e in emails}

def extract_phones(html: str, region: str = REGION_DEFAULT) -> Set[str]:
    phones = set()
    for raw in PHONE_RAW.findall(html):
        try:
            num = phonenumbers.parse(raw, region)
            if phonenumbers.is_possible_number(num):
                phones.add(phonenumbers.format_number(num, phonenumbers.PhoneNumberFormat.E164))
        except Exception:
            pass
    # JSON-LD
    try:
        doc = HTMLParser(html)
        for node in doc.css('script[type="application/ld+json"]'):
            try:
                data = json.loads(node.text())
                blobs = data if isinstance(data, list) else [data]
                for b in blobs:
                    t = b.get('telephone')
                    if t:
                        try:
                            num = phonenumbers.parse(str(t), region)
                            if phonenumbers.is_possible_number(num):
                                phones.add(phonenumbers.format_number(num, phonenumbers.PhoneNumberFormat.E164))
                        except Exception:
                            pass
            except Exception:
                pass
    except Exception:
        pass
    return phones

def fetch(url: str) -> Optional[str]:
    try:
        with httpx.Client(follow_redirects=True, timeout=REQUEST_TIMEOUT, headers={"User-Agent": USER_AGENT}) as client:
            r = client.get(url)
            if r.status_code < 400 and "text/html" in r.headers.get("content-type", ""):
                return r.text
    except Exception:
        return None
    return None

def fetch_many(urls: List[str], concurrency: int) -> Dict[str, Optional[str]]:
    results: Dict[str, Optional[str]] = {u: None for u in urls}
    with ThreadPoolExecutor(max_workers=max(1, concurrency)) as ex:
        future_map = {ex.submit(fetch, u): u for u in urls}
        for fut in as_completed(future_map):
            u = future_map[fut]
            try:
                results[u] = fut.result()
            except Exception:
                results[u] = None
    return results

def candidate_urls(base_url: str) -> List[str]:
    base = normalize_url(base_url)
    urls = []
    for p in CANDIDATE_PATHS:
        if p and not p.startswith('/'):
            urls.append(urljoin(base, '/' + p))
        else:
            urls.append(urljoin(base, p or '/'))
    return urls

def mx_present(domain: str) -> str:
    """Return 'mx_present', 'no_mx', or 'unknown'."""
    if not domain:
        return "unknown"
    if dns is None:
        return "unknown"
    try:
        resolver = dns.resolver.Resolver()
        resolver.lifetime = 3.0
        resolver.timeout = 2.0
        answers = resolver.resolve(domain, 'MX')
        return "mx_present" if answers else "no_mx"
    except Exception:
        return "no_mx"

def pick_best_emails(domain: str, emails: Set[str]) -> List[str]:
    """Prioritize named emails over role-based; keep same-domain first."""
    domain_emails = [e for e in emails if e.endswith("@" + domain)]
    ext_emails = [e for e in emails if e not in domain_emails]

    def score(e: str):
        local = e.split("@")[0]
        role = 1 if local in ROLE_EMAILS else 0
        return (0 if role == 0 else 1, len(local))  # named first, then longer local part

    domain_emails.sort(key=score)
    ext_emails.sort(key=score)
    prioritized = domain_emails + ext_emails
    return prioritized[:MAX_EMAILS_PER_DOMAIN]

from dataclasses import dataclass

@dataclass
class RowOut:
    company_name: str
    url: str
    domain: str
    email_final: str
    email_source: str
    verification_status: str
    phone: str
    source_url: str
    reason: str
    qualified: str
    product_fit: str
    score: int

def scrape_site(company: str, start_url: str, page_concurrency: int = DEFAULT_PAGE_CONCURRENCY) -> List[RowOut]:
    final, domain = final_url_and_domain(start_url)
    pages = candidate_urls(final)
    collected_emails: Dict[str, str] = {}   # email -> source_url
    collected_phones: Set[str] = set()

    page_html_map = fetch_many(pages, page_concurrency)
    for p, html in page_html_map.items():
        if not html:
            continue
        emails = extract_emails(html)
        phones = extract_phones(html)
        for e in emails:
            if e not in collected_emails:
                collected_emails[e] = p
        collected_phones |= phones

    if not collected_emails and not collected_phones:
        return [RowOut(
            company_name=company, url=final, domain=domain,
            email_final="", email_source="",
            verification_status="unknown",
            phone="", source_url="",
            reason="no_contacts_found", qualified="no",
            product_fit="", score=0
        )]

    best_emails = pick_best_emails(domain, set(collected_emails.keys()))
    mx_status = mx_present(domain)

    rows: List[RowOut] = []
    if best_emails:
        for e in best_emails:
            local = e.split("@")[0]
            role = (local in ROLE_EMAILS)
            conf = 90 if not role else 70  # named > role
            rows.append(RowOut(
                company_name=company, url=final, domain=domain,
                email_final=e, email_source="mailto/raw/decoded",
                verification_status=mx_status,
                phone=";".join(sorted(collected_phones)),
                source_url=collected_emails.get(e, final),
                reason="regex_scrape",
                qualified="yes" if not role else "maybe",
                product_fit="",  # downstream tagger can update
                score=conf
            ))
    else:
        rows.append(RowOut(
            company_name=company, url=final, domain=domain,
            email_final="", email_source="",
            verification_status=mx_status,
            phone=";".join(sorted(collected_phones)),
            source_url=final,
            reason="phone_only",
            qualified="maybe",
            product_fit="",
            score=60
        ))
    return rows

def run(
    input_csv: str = INPUT_CSV,
    output_csv: str = OUTPUT_CSV,
    offset: int = 0,
    limit: Optional[int] = None,
    page_concurrency: int = DEFAULT_PAGE_CONCURRENCY,
    sleep_min: float = SLEEP_BETWEEN_SITES[0],
    sleep_max: float = SLEEP_BETWEEN_SITES[1],
    progress_every: int = DEFAULT_PROGRESS_EVERY,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    site_concurrency: int = DEFAULT_SITE_CONCURRENCY,
    append: bool = APPEND_MODE_DEFAULT,
    resume: bool = RESUME_DEFAULT,
):
    # Read prospects
    prospects: List[Tuple[str, str]] = []
    with open(input_csv, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            company = (row.get("company_name") or "").strip()
            url = (row.get("url") or "").strip()
            if not url:
                continue
            prospects.append((company, url))

    # Resume logic: filter out already processed (company_name, url) pairs
    processed_keys = set()
    if resume:
        try:
            with open(output_csv, newline='', encoding='utf-8') as f:
                rd = csv.DictReader(f)
                for row in rd:
                    processed_keys.add((row.get('company_name','').strip(), row.get('url','').strip()))
        except FileNotFoundError:
            pass

    if offset:
        prospects = prospects[offset:]
    if limit is not None:
        prospects = prospects[:limit]

    if processed_keys:
        prospects = [(c,u) for (c,u) in prospects if (c,u) not in processed_keys]

    out_fieldnames = ["company_name","url","domain","email_final","email_source","verification_status",
                      "phone","source_url","reason","qualified","product_fit","score"]

    def write_rows(rows: List[RowOut], header_written: bool) -> bool:
        mode = 'a' if append else 'w'
        # If not appending or file doesn't exist, we need header
        write_header = not header_written
        try:
            with open(output_csv, mode, newline='', encoding='utf-8') as f:
                w = csv.DictWriter(f, fieldnames=out_fieldnames)
                if write_header:
                    w.writeheader()
                for r in rows:
                    w.writerow(asdict(r))
            return True
        except Exception:
            return False

    header_written = False
    # If appending and file exists, assume header already present
    if append:
        try:
            with open(output_csv, 'r', encoding='utf-8') as _:
                header_written = True
        except FileNotFoundError:
            header_written = False

    total_remaining = len(prospects)
    processed_count = 0

    for start in range(0, len(prospects), max(1, chunk_size)):
        batch = prospects[start:start+chunk_size]
        batch_rows: List[RowOut] = []
        if site_concurrency and site_concurrency > 1:
            with ThreadPoolExecutor(max_workers=site_concurrency) as ex:
                futures = [ex.submit(scrape_site, c, u, page_concurrency) for (c,u) in batch]
                for idx, fut in enumerate(as_completed(futures), 1):
                    try:
                        batch_rows.extend(fut.result())
                    except Exception:
                        pass
                    if progress_every and ((processed_count + idx) % progress_every == 0):
                        print(f"Processed {processed_count + idx} / {total_remaining} …")
        else:
            for idx, (company, url) in enumerate(batch, 1):
                try:
                    rows = scrape_site(company, url, page_concurrency=page_concurrency)
                    batch_rows.extend(rows)
                except Exception:
                    batch_rows.append(RowOut(company_name=company, url=url, domain="", email_final="", email_source="", verification_status="unknown", phone="", source_url="", reason="error:site", qualified="no", product_fit="", score=0))
                time.sleep(random.uniform(sleep_min, sleep_max))
                if progress_every and ((processed_count + idx) % progress_every == 0):
                    print(f"Processed {processed_count + idx} / {total_remaining} …")

        # Dedup within batch
        seen = set()
        deduped = []
        for r in batch_rows:
            key = (r.company_name, r.email_final, r.phone)
            if key in seen:
                continue
            seen.add(key)
            deduped.append(r)

        # Write batch (append or overwrite depending on flags)
        if deduped:
            ok = write_rows(deduped, header_written)
            if ok:
                header_written = True

        processed_count += len(batch)
    print(f"Done. Wrote/updated: {output_csv}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Concrete Genius Leads Runner")
    parser.add_argument("input", nargs="?", default=INPUT_CSV, help="Input CSV (prospects_raw.csv)")
    parser.add_argument("output", nargs="?", default=OUTPUT_CSV, help="Output CSV (Merged_Final_Leads_Master.csv)")
    parser.add_argument("--offset", type=int, default=0, help="Start index in prospects")
    parser.add_argument("--limit", type=int, default=None, help="Max number of prospects to process")
    parser.add_argument("--page-concurrency", type=int, default=DEFAULT_PAGE_CONCURRENCY, help="Parallel fetches per site")
    parser.add_argument("--sleep-min", type=float, default=SLEEP_BETWEEN_SITES[0], help="Min sleep between sites")
    parser.add_argument("--sleep-max", type=float, default=SLEEP_BETWEEN_SITES[1], help="Max sleep between sites")
    parser.add_argument("--progress-every", type=int, default=DEFAULT_PROGRESS_EVERY, help="Print progress every N sites (0=off)")
    parser.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE, help="Batch size for incremental writes")
    parser.add_argument("--site-concurrency", type=int, default=DEFAULT_SITE_CONCURRENCY, help="Parallel sites to process")
    parser.add_argument("--no-append", action="store_true", help="Do not append; overwrite output file")
    parser.add_argument("--no-resume", action="store_true", help="Do not resume from existing output")
    args = parser.parse_args()

    run(
        input_csv=args.input,
        output_csv=args.output,
        offset=args.offset,
        limit=args.limit,
        page_concurrency=args.page_concurrency,
        sleep_min=args.sleep_min,
        sleep_max=args.sleep_max,
        progress_every=args.progress_every,
        chunk_size=args.chunk_size,
        site_concurrency=args.site_concurrency,
        append=(not args.no_append),
        resume=(not args.no_resume),
    )
