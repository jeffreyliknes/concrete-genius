#!/usr/bin/env python3
"""
tag_product_fit.py
------------------
Add / update a Boolean `product_fit` column on an existing leads CSV,
using the same regex gate as batch_scrape_texas.py but **also** filtering
out Lowe's and Home Depot.

Can also use fields from site_profiler.py (business_type, service_keywords, signals)
for improved detection.

Usage example (from repo root, venv active):

    python tag_product_fit.py \
        --in  data/outputs/prospects_profiled.csv \
        --out data/outputs/prospects_tagged.csv

The --in file can be prospects_profiled.csv or prospects_enriched.csv.
"""
import argparse
import re
import pandas as pd
from pathlib import Path
import glob
import os

DEFAULT_OUT = Path("data/outputs/prospects_tagged.csv")
DEFAULT_IN  = Path("data/outputs/prospects_enriched.csv")


def _find_latest_enriched() -> Path | None:
    """Return Path to the most recently modified enriched CSV in data/outputs.
    Supports multiple naming schemes used in this repo."""
    folder = Path("data/outputs")
    patterns = [
        "*prospects_enriched.csv",
        "*final_leads_enriched.csv",
        "*enriched.csv",
        "Merged_Final_Leads_Master.csv",
        "prospects_enriched.csv",
    ]
    candidates: list[Path] = []
    for pat in patterns:
        candidates.extend(folder.glob(pat))
    if not candidates:
        return None
    return max(candidates, key=lambda p: p.stat().st_mtime)

READY_MIX_RX = re.compile(
    r"(ready[\s\-]?mix(?:ed)?|"                 # ready-mix / readymixed
    r"ready\s?mix\s?concrete|"                 # “ready mix concrete”
    r"redi[\s\-]?mix|"                         # redi-mix
    r"volumetric|volumetric[\s\-]?(mixer|truck)|"  # volumetric mixer / truck
    r"mobile\s?mix|central[\s\-]?mix|"         # mobile mix, central mix
    r"batch\s?plant|batching\s?plant|concrete\s?plant|"
    r"concrete[\s\-]?delivery(?:\sservice)?|"  # concrete delivery / service
    r"on[\-\s]?site pours)",
    re.I,
)
NEGATIVE_RX = re.compile(
    r"(hardware|garden\scenter|roofing|foundation\srepair|asphalt\s+only|precast|masonry\ssupply)"
    r"|(?:homedepot|home\sdepot|lowe'?s|walmart|acehardware|tractor\s?supply)",
    re.I,
)
MARKETPLACE_RX = re.compile(r"(yelp|angi|houzz|homeadvisor|thumbtack|facebook\.com)", re.I)


def product_fit(row) -> bool:
    """Return True if this looks like a ready‑mix/producer (plant or corp) or strongly matches producer keywords.
    Uses site_profiler fields when available; otherwise falls back to regex over text fields.
    """
    def g(key: str) -> str:
        v = row.get(key, "")
        return "" if v is None else str(v)

    # 1) Strong signal: business_type from site_profiler
    btype = g("business_type").lower()
    if btype in {"producer_plant", "producer_corporate"}:
        blob = " ".join([g("company_name"), g("service_keywords"), g("signals"), g("reason"), g("url"), g("domain")])
        if NEGATIVE_RX.search(blob) or MARKETPLACE_RX.search(blob):
            return False
        return True
    if btype in {"contractor", "supplier", "marketplace"}:
        # allow only if we still find strong producer keywords
        pass

    # 2) Fallback to keyword heuristic across fields
    txt = " ".join([
        g("company_name"), g("reason"), g("service_keywords"), g("signals"), g("url"), g("domain"), g("source_url")
    ])
    if NEGATIVE_RX.search(txt) or MARKETPLACE_RX.search(txt):
        return False
    return bool(READY_MIX_RX.search(txt))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--in", dest="src", default=None,
                        help="input CSV path. If omitted, auto-detects latest *_final_leads_enriched.csv")
    parser.add_argument("--out", dest="dst", default=DEFAULT_OUT,
                        help="output CSV path (defaults to final_leads_tagged.csv)")
    args = parser.parse_args()

    # Auto-detect latest enriched file if --in not provided
    if args.src is None:
        auto_path = _find_latest_enriched()
        if auto_path is None:
            raise SystemExit("No enriched CSV found in data/outputs/. Specify --in <file>")
        src = auto_path
        print(f"[i] Auto-detected input: {src}")
    else:
        src = Path(args.src)

    dst = Path(args.dst or DEFAULT_OUT)
    if not src.exists():
        raise SystemExit(f"Input file not found: {src}")

    df = pd.read_csv(src)

    for col in ["company_name","reason","service_keywords","signals","business_type","url","domain","source_url"]:
        if col not in df.columns:
            df[col] = ""

    df["product_fit"] = df.apply(product_fit, axis=1)

    # sync the “qualified” flag with our improved signal
    df["qualified"] = df["product_fit"]

    # further remove generic contractors or hardware-only businesses
    DROP_RX = re.compile(r"(contractor|hardware|repair)", re.I)
    df = df[~df["company_name"].astype(str).str.contains(DROP_RX, na=False)]
    if "reason" in df.columns:
        df = df[~df["reason"].fillna("").str.contains(DROP_RX)]

    # Optionally hard‑drop Lowe's & Home Depot rows entirely:
    if "domain" in df.columns:
        df = df[~df["domain"].str.contains(r"(homedepot\.com|lowes\.com|walmart\.com)", na=False)]

    total = len(df)
    fit_count = int(df["product_fit"].sum()) if "product_fit" in df.columns else 0
    dst.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(dst, index=False)

    # keep a daily snapshot for rollback/audit
    snapshot = dst.with_stem(f"{dst.stem}_{pd.Timestamp.today():%Y%m%d}")
    df.to_csv(snapshot, index=False)
    print(f"[✓] product_fit updated → {dst}  (rows: {total}, fit=True: {fit_count})")
    print(f"[✓] snapshot written → {snapshot}")


if __name__ == "__main__":
    main()