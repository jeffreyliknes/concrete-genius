#!/usr/bin/env python3
"""
email_stub_generator.py
-----------------------
Generates short, Smartlead-ready cold emails using the best leads from your
pipeline (A/B tier, product_fit=True, valid emails), with minimal manual touch.

Default input:  data/outputs/Scored_Leads_Rescored.csv
Default output: data/outputs/Smartlead_Import.csv

It will:
  • Filter to product_fit=True and tier in {A,B}
  • Drop platform/free-mail/junk emails and (by default) role inboxes
  • Build a concise context + inferred pain point from profiler signals
  • Ask the model for a ≤120-word email (no links), practical tone + clear CTA
  • Produce Smartlead-friendly columns: email, first_name, last_name, company,
    website, phone_primary, subject, email_body, plus a few custom fields

Run:
  python email_stub_generator.py \
    --in  data/outputs/Scored_Leads_Rescored.csv \
    --out data/outputs/Smartlead_Import.csv \
    --allow-role   # (optional) include role emails if no named exists

Env:
  OPENAI_API_KEY must be set.
"""

import os
import re
import csv
import textwrap
import argparse
from typing import Dict, Any

import pandas as pd
from dotenv import load_dotenv

try:
    import openai
except Exception:
    openai = None

load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()

EMAIL_RE = re.compile(r"^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$", re.I)
ROLE_LOCALPARTS = {
    "info","sales","office","contact","admin","support","hello","enquiries",
    "service","orders","jobs","hr","careers","noreply","no-reply","do-not-reply"
}
PLATFORMS = ("facebook.com","squarespace.com","yelp.com","angi.com","houzz.com","homeadvisor.com")
FREEMAIL = ("gmail.com","yahoo.com","aol.com","hotmail.com","outlook.com","proton.me")

DEFAULT_IN  = "data/outputs/Scored_Leads_Rescored.csv"
DEFAULT_OUT = "data/outputs/Smartlead_Import.csv"

TEMPLATE = textwrap.dedent("""
Write a concise cold email in 110-120 words to {first_name} at {company_name}.
Context: {context}
Suspected pain point: {pain_point}
Value props: consistent mixes, reduced waste, remote plant/fleet monitoring (CloudOps), faster dispatch decisions.
Tone: practical, respectful, no fluff.
Rules: no links, no emojis, 1 short paragraph + a single clear CTA to schedule a 15-minute call (no URL), use plain text, personalize lightly with the company/service.
Return ONLY the email body, nothing else.
""")

SUBJECT_TMPL = "Quick idea for {company_name}’s ready-mix ops"

# ----------------- helpers -----------------

def is_platform_domain(d: str) -> bool:
    d = (d or "").lower()
    return any(p in d for p in PLATFORMS)


def is_valid_email(e: str) -> bool:
    e = (e or "").strip()
    return bool(e and EMAIL_RE.match(e))


def is_role_email(e: str) -> bool:
    if not is_valid_email(e):
        return False
    local = e.split("@",1)[0].lower()
    if local in ROLE_LOCALPARTS: return True
    return local.startswith("info") or local.startswith("sales") or local.endswith("support")


def split_name_from_email(e: str) -> Dict[str, str]:
    """Derive first/last from localpart if missing: john.doe -> John / Doe."""
    first_name, last_name = "", ""
    try:
        local = e.split("@",1)[0]
        parts = re.split(r"[._-]+", local)
        if parts:
            first_name = parts[0].strip().title()
            if len(parts) > 1:
                last_name = parts[-1].strip().title()
    except Exception:
        pass
    return {"first_name": first_name, "last_name": last_name}


def infer_pain_point(row: pd.Series) -> str:
    signals = str(row.get("signals", "")).lower()
    svc = str(row.get("service_keywords", "")).lower()
    reason = str(row.get("reason", "")).lower()
    ctx = []
    if "dispatch" in signals or "delivery" in signals:
        ctx.append("dispatch coordination / on-time delivery")
    if "quality" in signals or "inconsistent" in reason or "mix" in svc:
        ctx.append("consistent mix quality with fewer redeliveries")
    if "downtime" in signals or "maintenance" in signals:
        ctx.append("reducing unplanned plant downtime")
    if "hiring" in signals or "jobs" in signals:
        ctx.append("doing more with leaner teams")
    if not ctx:
        # generic but relevant to RMX ops
        ctx = ["visibility across batching and fleet to cut waste and delays"]
    return ", ".join(ctx)


def build_context(row: pd.Series) -> str:
    parts = []
    bt = str(row.get("business_type","")) or ""
    if bt:
        parts.append(f"type: {bt}")
    svc = str(row.get("service_keywords",""))
    if svc:
        parts.append(f"services: {svc}")
    loc = str(row.get("location_detected",""))
    if loc and loc.lower() != "unknown":
        parts.append(f"location: {loc}")
    reason = str(row.get("reason",""))
    if reason:
        parts.append(f"reason: {reason}")
    return "; ".join(parts) or "ready-mix operations and batching/dispatch context"


def model_email(first_name: str, company_name: str, context: str, pain_point: str) -> str:
    if not OPENAI_API_KEY:
        raise SystemExit("OPENAI_API_KEY not set")
    if openai is None:
        raise SystemExit("openai package not installed in this environment")

    openai.api_key = OPENAI_API_KEY
    prompt = TEMPLATE.format(first_name=first_name or "there", company_name=company_name, context=context, pain_point=pain_point)
    try:
        rsp = openai.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=220,
            temperature=0.5,
        )
        text = (rsp.choices[0].message.content or "").strip()
    except Exception as ex:
        text = f"Hi {first_name or 'there'}, quick idea to improve ready-mix ops at {company_name}. We help plants keep mixes consistent, reduce waste, and give dispatch live visibility without adding headcount. If this is relevant, open to a 15-minute call to compare notes?"
    # hard cap ~120 words
    words = text.split()
    if len(words) > 120:
        text = " ".join(words[:120])
    return text

# ----------------- main -----------------

def run(src: str, dst: str, allow_role: bool = False) -> None:
    df = pd.read_csv(src)

    # normalize columns used downstream
    for col in [
        "company_name","domain","final_domain","email_final","contact_quality",
        "product_fit","tier","signals","service_keywords","location_detected",
        "business_type","reason","phone","website","url","first_name","last_name"
    ]:
        if col not in df.columns:
            df[col] = ""

    # base filters
    is_fit = df["product_fit"].astype(str).str.lower().isin(["true","1","yes"])
    is_tier = df["tier"].astype(str).str.upper().isin(["A","B"])  # keep A/B

    # email sanity
    emails = df["email_final"].astype(str).str.strip()
    valid = emails.map(is_valid_email)
    is_role = emails.map(is_role_email)

    # platform skip
    domain = df["final_domain"].where(df["final_domain"].astype(str).str.len() > 0, df["domain"]).astype(str)
    not_platform = ~domain.map(is_platform_domain)

    mask = is_fit & is_tier & valid & not_platform
    if not allow_role:
        mask = mask & (~is_role)

    work = df[mask].copy()

    if work.empty:
        raise SystemExit("No eligible rows after filtering (need product_fit=True, tier A/B, valid non-role email). Try --allow-role.")

    rows = []
    for _, r in work.iterrows():
        company = str(r.get("company_name")) or domain.loc[_] or str(r.get("url") or r.get("website") or "").split("//")[-1]
        email = str(r.get("email_final"))
        first = str(r.get("first_name",""))
        last = str(r.get("last_name",""))
        if not first:
            n = split_name_from_email(email)
            first = first or n["first_name"]
            last = last or n["last_name"]

        context = build_context(r)
        pain = infer_pain_point(r)
        subject = SUBJECT_TMPL.format(company_name=company)
        body = model_email(first, company, context, pain)

        # Smartlead-friendly columns
        rows.append({
            "email": email,
            "first_name": first,
            "last_name": last,
            "company": company,
            "website": str(r.get("website") or r.get("url") or ""),
            "phone_primary": str(r.get("phone") or "").split(";")[0],
            "subject": subject,
            "email_body": body,
            # extras for mapping / debugging
            "domain": domain.loc[_],
            "tier": str(r.get("tier")),
            "verification_status": str(r.get("verification_status","")),
            "contact_quality": str(r.get("contact_quality","")),
            "signals": str(r.get("signals","")),
        })

    out_cols = [
        "email","first_name","last_name","company","website","phone_primary","subject","email_body",
        "domain","tier","verification_status","contact_quality","signals"
    ]
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    with open(dst, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=out_cols)
        w.writeheader()
        w.writerows(rows)
    print(f"[✓] Wrote {len(rows)} Smartlead-ready rows → {dst}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="src", default=DEFAULT_IN)
    ap.add_argument("--out", dest="dst", default=DEFAULT_OUT)
    ap.add_argument("--allow-role", action="store_true", help="Include role emails if named not available")
    args = ap.parse_args()

    run(args.src, args.dst, allow_role=args.allow_role)


if __name__ == "__main__":
    main()