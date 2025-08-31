import argparse
import asyncio
import csv
import json
import re
from collections import Counter, defaultdict
from urllib.parse import urlparse, urljoin

import httpx
from selectolax.parser import HTMLParser


KEY_PAGES = ['/', '/about', '/services', '/contact', '/locations', '/plants', '/ready-mix']

BUSINESS_TYPE_KEYWORDS = {
    'producer_plant': ['batch plant', 'ready mix plant', 'volumetric', 'ready mix', 'batch plant', 'batching plant'],
    'producer_corporate': ['group', 'corporation', 'inc.', 'llc', 'ltd', 'company', 'co.'],
    'contractor': ['general contractor', 'construction services', 'construction company', 'contractor'],
    'marketplace': ['brands', 'products', 'marketplace', 'distributor', 'dealer', 'reseller'],
    'supplier': ['aggregate supplier', 'aggregate supply', 'supplier', 'material supplier', 'material supply'],
}

SERVICE_KEYWORDS = [
    'ready mix', 'volumetric', 'precast', 'batch plant', 'aggregate supply', 'concrete', 'construction',
    'plants', 'materials', 'cement', 'asphalt', 'aggregate', 'delivery', 'mixing', 'sand', 'gravel'
]

LOCATION_KEYS = ['address', 'addressLocality', 'addressRegion', 'postalCode', 'addressCountry', 'streetAddress']


def parse_args():
    parser = argparse.ArgumentParser(description="Site Profiler for concrete-related businesses")
    parser.add_argument('--in', dest='input_file', required=True, help='Input CSV file')
    parser.add_argument('--out', dest='output_file', required=True, help='Output CSV file')
    parser.add_argument('--site-concurrency', type=int, default=3, help='Max concurrent site requests')
    parser.add_argument('--timeout', type=int, default=12, help='HTTP request timeout in seconds')
    return parser.parse_args()


def normalize_domain(url_or_domain):
    if not url_or_domain:
        return None
    if not url_or_domain.startswith('http'):
        url_or_domain = 'http://' + url_or_domain
    try:
        parsed = urlparse(url_or_domain)
        if parsed.netloc:
            return parsed.netloc.lower()
        return parsed.path.lower()
    except Exception:
        return None


async def fetch_page(client, base_url, path, timeout):
    url = urljoin(f'http://{base_url}', path)
    try:
        resp = await client.get(url, timeout=timeout)
        if resp.status_code == 200 and resp.text:
            return resp.text
    except Exception:
        pass
    return None


def extract_jsonld(html):
    tree = HTMLParser(html)
    scripts = tree.css('script[type="application/ld+json"]')
    data = []
    for s in scripts:
        try:
            text = s.text()
            if not text:
                continue
            parsed = json.loads(text)
            # JSON-LD can be a list or dict
            if isinstance(parsed, list):
                data.extend(parsed)
            else:
                data.append(parsed)
        except Exception:
            continue
    return data


def extract_location_from_jsonld(jsonld_list):
    for item in jsonld_list:
        if not isinstance(item, dict):
            continue
        addr = item.get('address')
        if isinstance(addr, dict):
            parts = []
            for key in LOCATION_KEYS:
                val = addr.get(key)
                if val:
                    parts.append(str(val))
            if parts:
                return ', '.join(parts)
        # Sometimes address is nested deeper or differently structured
        # Try to find any address-like string in JSON-LD
        for key in LOCATION_KEYS:
            val = item.get(key)
            if val and isinstance(val, str):
                return val
    return None


def extract_text_elements(html):
    tree = HTMLParser(html)
    texts = []
    # title
    title = tree.css_first('title')
    if title:
        texts.append(title.text().lower())
    # meta description
    meta_desc = tree.css_first('meta[name="description"]')
    if meta_desc and meta_desc.attributes.get('content'):
        texts.append(meta_desc.attributes['content'].lower())
    # h1 and h2
    for tag in ['h1', 'h2']:
        for elem in tree.css(tag):
            if elem.text():
                texts.append(elem.text().lower())
    return texts


def count_keyword_matches(texts, keywords):
    count = 0
    for text in texts:
        for kw in keywords:
            if kw in text:
                count += 1
    return count


def classify_business_type(texts, jsonld_data, pages_fetched):
    text_blob = ' '.join(texts)
    text_blob_lower = text_blob.lower()

    # Check for marketplace first (multiple brands/products)
    marketplace_terms = BUSINESS_TYPE_KEYWORDS['marketplace']
    marketplace_found = any(term in text_blob_lower for term in marketplace_terms)

    # Check for producer_plant
    producer_plant_terms = BUSINESS_TYPE_KEYWORDS['producer_plant']
    producer_plant_found = any(term in text_blob_lower for term in producer_plant_terms)

    # Check for producer_corporate
    producer_corporate_terms = BUSINESS_TYPE_KEYWORDS['producer_corporate']
    producer_corporate_found = any(term in text_blob_lower for term in producer_corporate_terms)

    # Check for contractor
    contractor_terms = BUSINESS_TYPE_KEYWORDS['contractor']
    contractor_found = any(term in text_blob_lower for term in contractor_terms)

    # Check for supplier
    supplier_terms = BUSINESS_TYPE_KEYWORDS['supplier']
    supplier_found = any(term in text_blob_lower for term in supplier_terms)

    # Heuristics:
    if producer_plant_found:
        return 'producer_plant'
    if marketplace_found:
        return 'marketplace'
    if contractor_found:
        return 'contractor'
    if supplier_found:
        return 'supplier'
    if producer_corporate_found:
        # If no location pages fetched, more likely corporate
        location_pages = {'/locations', '/plants'}
        if not any(p in pages_fetched for p in location_pages):
            return 'producer_corporate'

    return 'unknown'


def extract_service_keywords(texts):
    found = set()
    for text in texts:
        for kw in SERVICE_KEYWORDS:
            if kw in text:
                found.add(kw)
    return sorted(found)


def calculate_profile_confidence(texts, business_type, service_keywords, location_detected):
    # Basic heuristic: more keyword hits and location detected => higher confidence
    text_blob = ' '.join(texts)
    count_keywords = 0
    for kw_list in BUSINESS_TYPE_KEYWORDS.values():
        for kw in kw_list:
            if kw in text_blob:
                count_keywords += 1
    count_services = len(service_keywords)
    confidence = 0
    confidence += min(count_keywords * 10, 50)  # max 50 points from business type keywords
    confidence += min(count_services * 7, 35)   # max 35 points from service keywords
    if location_detected and location_detected != 'unknown':
        confidence += 15
    confidence = min(confidence, 100)
    return confidence


def extract_signals(texts, jsonld_data):
    signals = []
    # Add business type keywords found
    text_blob = ' '.join(texts)
    for btype, kws in BUSINESS_TYPE_KEYWORDS.items():
        for kw in kws:
            if kw in text_blob:
                signals.append(f'{btype}:{kw}')
    # Add presence of JSON-LD
    if jsonld_data:
        signals.append('jsonld_found')
    return ';'.join(signals) if signals else 'none'


async def profile_domain(client, domain, timeout):
    pages_fetched = set()
    all_texts = []
    all_jsonld = []
    for path in KEY_PAGES:
        html = await fetch_page(client, domain, path, timeout)
        if html:
            pages_fetched.add(path)
            texts = extract_text_elements(html)
            all_texts.extend(texts)
            jsonld = extract_jsonld(html)
            if jsonld:
                all_jsonld.extend(jsonld)
    location = extract_location_from_jsonld(all_jsonld) or 'unknown'
    business_type = classify_business_type(all_texts, all_jsonld, pages_fetched)
    service_keywords = extract_service_keywords(all_texts)
    profile_confidence = calculate_profile_confidence(all_texts, business_type, service_keywords, location)
    signals = extract_signals(all_texts, all_jsonld)

    return {
        'business_type': business_type,
        'service_keywords': ', '.join(service_keywords) if service_keywords else 'unknown',
        'location_detected': location,
        'profile_confidence': profile_confidence,
        'signals': signals,
    }


async def main():
    args = parse_args()

    input_rows = []
    with open(args.input_file, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            input_rows.append(row)

    # Extract domains from input
    domains = []
    for row in input_rows:
        domain = row.get('final_domain') or row.get('website') or ''
        domain = normalize_domain(domain)
        if domain:
            domains.append(domain)
        else:
            domains.append(None)

    timeout = httpx.Timeout(args.timeout)
    limits = httpx.Limits(max_keepalive_connections=args.site_concurrency, max_connections=args.site_concurrency)

    async with httpx.AsyncClient(limits=limits, timeout=timeout, follow_redirects=True) as client:

        semaphore = asyncio.Semaphore(args.site_concurrency)

        async def sem_profile(domain):
            if not domain:
                # Return unknowns for missing domain
                return {
                    'business_type': 'unknown',
                    'service_keywords': 'unknown',
                    'location_detected': 'unknown',
                    'profile_confidence': 0,
                    'signals': 'none',
                }
            async with semaphore:
                return await profile_domain(client, domain, args.timeout)

        tasks = [sem_profile(domain) for domain in domains]
        results = await asyncio.gather(*tasks)

    # Add profiling columns to rows
    fieldnames = None
    if input_rows:
        fieldnames = list(input_rows[0].keys())
    else:
        # If empty input, create minimal header
        fieldnames = ['final_domain', 'business_type', 'service_keywords', 'location_detected', 'profile_confidence', 'signals']

    new_fields = ['business_type', 'service_keywords', 'location_detected', 'profile_confidence', 'signals']
    for nf in new_fields:
        if nf not in fieldnames:
            fieldnames.append(nf)

    with open(args.output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row, prof in zip(input_rows, results):
            row.update(prof)
            writer.writerow(row)

    print(f"Profiled {len(domains)} domains -> {args.output_file}")


if __name__ == '__main__':
    asyncio.run(main())
