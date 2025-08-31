"""
snov_enrich.py — enrich only the rows worth spending credits on.

Recommended position in pipeline:
  cg_runner.py → site_profiler.py → tag_product_fit.py → cg_cleaner.py → lead_scoring.py → snov_enrich.py (top slice) → lead_scoring.py (re-score)

Default behavior:
  • Enrich rows where email_final is blank
  • If --only-fit is set and product_fit column exists, require product_fit=True
  • If --min-score N is set and score column exists, require score ≥ N
  • Skip rows with blocked_domain=True when that column exists
  • Prefer named emails, then better verification status

CLI examples:
  python archive/snov_enrich.py \
      --in  data/outputs/prospects_tagged.csv \
      --out data/outputs/prospects_enriched.csv \
      --only-fit --min-score 70 --limit-per-domain 5

  # also verify (uses additional verification credits)
  python archive/snov_enrich.py --verify --only-fit --min-score 80
"""

import os
import time
import json
import pathlib
import argparse
from typing import Dict, List, Any
import re

import requests
import pandas as pd

EMAIL_RE = re.compile(r"^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$", re.I)
PLATFORM_DOMAINS = ("facebook.com","squarespace.com","yelp.com","angi.com","houzz.com","homeadvisor.com")
JUNK_LOCALPARTS = {"bootstrap","react","react-dom","lodash","wght","chunk","flags","noreply","no-reply","do-not-reply"}

AUTH_URL    = "https://api.snov.io/v1/oauth/access_token"
DOMAIN_URL  = "https://api.snov.io/v2/domain-emails-with-info"
VERIFY_URL  = "https://api.snov.io/v1/get-emails-verification-status"

CACHE_DIR = pathlib.Path("data/cache/snov"); CACHE_DIR.mkdir(parents=True, exist_ok=True)

SNOV_ID     = os.getenv("SNOV_CLIENT_ID")
SNOV_SECRET = os.getenv("SNOV_CLIENT_SECRET")

ROLE_EMAILS = {"info","sales","office","contact","admin","support","hello","enquiries","service","orders","jobs","hr","careers","noreply"}

DEFAULT_IN  = pathlib.Path("data/outputs/prospects_tagged.csv")
DEFAULT_OUT = pathlib.Path("data/outputs/prospects_enriched.csv")

# --------------------------- HTTP helpers --------------------------- #

def get_access_token() -> str:
    if not SNOV_ID or not SNOV_SECRET:
        raise RuntimeError("Missing SNOV_CLIENT_ID / SNOV_CLIENT_SECRET in environment")
    resp = requests.post(AUTH_URL, data={
        "grant_type": "client_credentials",
        "client_id": SNOV_ID,
        "client_secret": SNOV_SECRET
    }, timeout=20)
    resp.raise_for_status()
    return resp.json()["access_token"]


def snov_session() -> requests.Session:
    s = requests.Session()
    token = get_access_token()
    s.headers.update({"Authorization": f"Bearer {token}", "User-Agent":"CG-SnovEnrich/1.0"})
    return s


def cached_json(path: pathlib.Path, fetch_func):
    if path.exists():
        try:
            return json.loads(path.read_text())
        except Exception:
            pass
    data = fetch_func()
    path.write_text(json.dumps(data))
    return data

# --------------------------- Logic --------------------------- #

def pick_best_email(domain_payload: Dict[str, Any], limit_per_domain: int = 5) -> List[Dict[str, Any]]:
    """Select up to N emails, prefer named over role, prefer verified/valid status."""
    emails = domain_payload.get("emails", []) or []

    def is_role(local: str) -> bool:
        L = local.lower()
        return (L in ROLE_EMAILS) or L.startswith("info") or L.endswith("support") or L.startswith("sales")

    def status_rank(st: str) -> int:
        s = (st or "").lower()
        if s in ("valid","verified"): return 0
        if s in ("accept_all","unknown"): return 1
        if s in ("invalid","disposable","undeliverable"): return 3
        return 2

    scored = []
    for e in emails:
        addr = e.get("email") or ""
        if "@" not in addr:
            continue
        local = addr.split("@",1)[0]
        st = e.get("email_status") or e.get("status") or "unknown"
        scored.append((is_role(local), status_rank(st), -len(local), e))

    scored.sort(key=lambda t: (t[0], t[1], t[2]))  # named first, then better status, then longer localpart
    return [t[3] for t in scored[:max(1, int(limit_per_domain))]]


def enrich_with_snov(df: pd.DataFrame, session: requests.Session, limit_per_domain: int) -> pd.DataFrame:
    new_rows = 0
    for idx, row in df.iterrows():
        dom = str(row.get("final_domain") or row.get("domain") or "").strip()
        if not dom:
            continue
        cache_path = CACHE_DIR / f"{dom}.json"
        try:
            payload = cached_json(cache_path, lambda: session.get(DOMAIN_URL, params={
                "domain": dom,
                "type": "all",
                "limit": limit_per_domain
            }, timeout=25).json())
            best = pick_best_email(payload, limit_per_domain=limit_per_domain)
            if not best:
                continue
            top = best[0]
            addr = top.get("email","")
            status = top.get("email_status") or top.get("status") or "unknown"
            others = ";".join([b.get("email","" ) for b in best[1:]])

            existing = str(row.get("email_final",""))
            if (not existing) or looks_junk_email(existing):
                df.at[idx, "email_final"] = addr
                df.at[idx, "verification_status"] = status
                df.at[idx, "email_source"] = "snov"
                if others:
                    df.at[idx, "email_alt_candidates"] = others
                new_rows += 1
        except requests.HTTPError as ex:
            print(f"[HTTP] {dom}: {ex}")
        except Exception as ex:
            print(f"[ERR] {dom}: {ex}")
        time.sleep(0.25)
    print(f"[snov] wrote {new_rows} new emails from Snov")
    return df


def verify_new_emails(df: pd.DataFrame, session: requests.Session) -> pd.DataFrame:
    # Verify only emails we just set from Snov or where status is unknown/blank
    src_is_snov = df.get("email_source", "").astype(str).str.lower().eq("snov") if "email_source" in df.columns else pd.Series(False, index=df.index)
    status_unknown = df.get("verification_status", "").astype(str).str.lower().isin(["", "unknown"]) if "verification_status" in df.columns else pd.Series(False, index=df.index)
    emails = df.loc[(src_is_snov | status_unknown), "email_final"].dropna().unique()
    emails = [e for e in emails if isinstance(e, str) and "@" in e]
    if not emails:
        return df
    try:
        resp = session.post(VERIFY_URL, json={"emails": emails}, timeout=25)
        resp.raise_for_status()
        data = resp.json() or {}
        statuses = {item.get("email"): item.get("status") for item in data.get("data", [])}
        updated = 0
        for i, r in df[df["email_final"].isin(statuses.keys())].iterrows():
            st = statuses.get(r["email_final"]) or r.get("verification_status")
            if st:
                df.at[i, "verification_status"] = st
                updated += 1
        print(f"[verify] updated {updated} verification statuses")
    except Exception as ex:
        print(f"[verify] error: {ex}")
    return df


def looks_junk_email(e: str) -> bool:
    e = str(e or "").strip()
    if not e or "@" not in e:
        return True
    if not EMAIL_RE.match(e):
        return True
    local = e.split("@",1)[0].lower()
    return local in JUNK_LOCALPARTS

def is_platform_domain(d: str) -> bool:
    d = (d or "").lower()
    return any(p in d for p in PLATFORM_DOMAINS)


def main():
    ap = argparse.ArgumentParser(description="Enrich emails with Snov.io for high-priority rows only")
    ap.add_argument("--in", dest="src", default=str(DEFAULT_IN), help="Input CSV (tagged/profiled)")
    ap.add_argument("--out", dest="dst", default=str(DEFAULT_OUT), help="Output CSV path")
    ap.add_argument("--only-fit", action="store_true", help="Only enrich rows with product_fit=True (if column exists)")
    ap.add_argument("--min-score", type=int, default=None, help="Only enrich rows with score >= this value (if column exists)")
    ap.add_argument("--limit-per-domain", type=int, default=5, help="Max emails to request per domain")
    ap.add_argument("--verify", action="store_true", help="Verify emails via Snov (uses verification credits)")
    args = ap.parse_args()

    src = pathlib.Path(args.src)
    dst = pathlib.Path(args.dst)

    if not src.exists():
        raise SystemExit(f"Input not found: {src}")

    df = pd.read_csv(src)

    # Normalize expected columns
    if "email_final" not in df.columns:
        df["email_final"] = ""
    if "verification_status" not in df.columns:
        df["verification_status"] = ""
    if "email_source" not in df.columns:
        df["email_source"] = ""

    # choose a domain to work with for platform filtering
    df["_work_domain"] = df.get("final_domain", df.get("domain",""))
    df["_work_domain"] = df["_work_domain"].astype(str)

    ef = df["email_final"].astype(str).fillna("").str.strip()
    missing_or_junk = ef.eq("") | ef.map(looks_junk_email)

    mask = missing_or_junk
    if args.only_fit and "product_fit" in df.columns:
        mask = mask & (df["product_fit"].astype(str).str.lower().isin(["true","1","yes"]))
    if args.min_score is not None and "score" in df.columns:
        mask = mask & (pd.to_numeric(df["score"], errors="coerce").fillna(0) >= args.min_score)
    if "blocked_domain" in df.columns:
        mask = mask & (df["blocked_domain"].astype(str).str.lower().isin(["false","0","","no"]))
    # never enrich platform/marketplace domains
    mask = mask & (~df["_work_domain"].map(is_platform_domain))

    work = df[mask].copy()
    print(f"[snov] candidates to enrich: {len(work)} of {len(df)} total")

    if work.empty:
        print("Nothing to enrich.")
        dst.parent.mkdir(parents=True, exist_ok=True)
        if "_work_domain" in df.columns:
            df = df.drop(columns=["_work_domain"])
        df.to_csv(dst, index=False)
        print(f"Wrote passthrough → {dst}")
        return

    with snov_session() as sess:
        df = enrich_with_snov(df, sess, args.limit_per_domain)
        if args.verify:
            df = verify_new_emails(df, sess)

    if "_work_domain" in df.columns:
        df = df.drop(columns=["_work_domain"])
    dst.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(dst, index=False)
    print(f"Saved enriched CSV → {dst}")


if __name__ == "__main__":
    main()