#!/usr/bin/env python3
"""
lead_scoring.py
---------------
Compute a 0–10 lead-quality score and Tier (A / B / C) for your leads.

Signals used (gracefully optional):
- product_fit (bool/str) → core relevance
- contact_quality (named_email | role_email | phone_only) if present
  • If missing, we infer from email presence and whether it looks role-based
- verification_status (valid/verified/mx_present/accept_all/catch_all/unknown/invalid)
- linkedin_url (optional)
- business_type (producer_plant / producer_corporate / contractor / supplier / marketplace / unknown)
- profile_confidence (0–100, from site_profiler.py) if present

Usage:
    python lead_scoring.py \
        --in  data/outputs/Cleaned_Leads.csv \
        --out data/outputs/Scored_Leads.csv
"""

from __future__ import annotations
import argparse
from pathlib import Path
import pandas as pd
import re

ROLE_EMAILS = {"info","sales","office","contact","admin","support","hello","enquiries","service","orders","jobs","hr","careers","noreply"}

GOOD_VERIF_2 = {"valid","verified","deliverable"}
GOOD_VERIF_1 = {"mx_present","accept_all","catch_all","risky","unknown","ok"}
BAD_VERIF_0  = {"invalid","undeliverable","disposable","bad","rejected"}

# ---------------------- helpers ----------------------

def as_bool(v) -> bool:
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    return s in {"1","true","yes","y"}


def is_role_email(addr: str) -> bool:
    if not addr or "@" not in addr:
        return False
    local = addr.split("@",1)[0].lower()
    return local in ROLE_EMAILS or local.startswith("info") or local.startswith("sales") or local.endswith("support")


def infer_contact_quality(row) -> str:
    # prefer explicit contact_quality if present
    cq = str(row.get("contact_quality",""))
    if cq in {"named_email","role_email","phone_only"}:
        return cq
    email = str(row.get("email") or row.get("email_final") or "").strip()
    phone = str(row.get("phone") or "").strip()
    if email:
        return "role_email" if is_role_email(email) else "named_email"
    return "phone_only" if phone else ""


def verif_points(status: str) -> int:
    s = str(status or "").strip().lower()
    if s in GOOD_VERIF_2:
        return 2
    if s in GOOD_VERIF_1:
        return 1
    if s in BAD_VERIF_0:
        return 0
    return 0


def biztype_bonus(bt: str) -> int:
    s = str(bt or "").lower()
    if s == "producer_plant":
        return 1
    # producer_corporate is neutral; contractor/supplier/marketplace are 0 (we rely on product_fit upstream)
    return 0


# ---------------------- scoring ----------------------

def compute_score(row) -> int:
    score = 0
    # 0) strong relevance
    if as_bool(row.get("product_fit")):
        score += 4

    # 1) contact quality
    cq = infer_contact_quality(row)
    if cq == "named_email":
        score += 3
    elif cq == "role_email":
        score += 2
    elif cq == "phone_only":
        score += 1

    # 2) verification status
    score += verif_points(row.get("verification_status"))

    # 3) linkedin presence (light bonus)
    linkedin_val = str(row.get("linkedin_url", "") or "").strip()
    has_li = bool(linkedin_val)
    if has_li:
        score += 1

    # 4) business type bonus (tiny nudge)
    score += biztype_bonus(row.get("business_type"))

    # 5) profile confidence (site_profiler): +1 if strong
    try:
        pc = int(float(row.get("profile_confidence", 0)))
        if pc >= 80:
            score += 1
    except Exception:
        pass

    # cap to 10
    return min(score, 10)


def tier(score: int) -> str:
    return "A" if score >= 8 else ("B" if score >= 5 else "C")


# ---------------------- cli ----------------------

def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--in",  dest="src", required=True, help="Input CSV path")
    p.add_argument("--out", dest="dst", required=True, help="Output CSV path")
    args = p.parse_args()

    src, dst = Path(args.src), Path(args.dst)
    dst.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(src)

    # make sure expected columns exist so .get() won't KeyError on Series
    for col in ["product_fit","email","email_final","phone","verification_status","linkedin_url","business_type","profile_confidence","contact_quality"]:
        if col not in df.columns:
            df[col] = ""

    df["score"] = df.apply(compute_score, axis=1)
    df["tier"]  = df["score"].apply(tier)

    df.to_csv(dst, index=False)
    print(f"[✓] Scored file written → {dst}   (rows: {len(df)})")
    try:
        print(df["tier"].value_counts().sort_index())
    except Exception:
        pass

if __name__ == "__main__":
    main()