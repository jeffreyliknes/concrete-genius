import argparse
import csv
import re
import pandas as pd
from urllib.parse import urlparse
from pathlib import Path

KEYWORDS_POSITIVE = [
    "ready-mix","ready mix","readymix","on-site mix","onsite mix","mobile mix","volumetric",
    "concrete delivery","batch plant","redi mix","redimix","rmx"
]

ROLE_EMAIL_PATTERNS = [
    r"^(admin|support|info|sales|contact|help|office|webmaster|marketing|noreply|no-reply|security|postmaster|abuse|billing|customerservice|customersupport|donotreply|do-not-reply|enquiries|finance|hr|jobs|media|news|press|privacy|recruitment|service|subscribe|unsubscribe|team|tech|techsupport|twitter|twitterfeed|user|users|customers|customerservice|customersupport|customersupport|customerservice)@",
]

BLOCKED_DOMAINS = {
    "mailinator.com",
    "10minutemail.com",
    "guerrillamail.com",
    "trashmail.com",
    "tempmail.com",
    "dispostable.com",
    "fakeinbox.com",
    "maildrop.cc",
    "yopmail.com",
    "mailcatch.com",
    "spamgourmet.com",
    "facebook.com",
    "marketplace.facebook.com",
}

FACEBOOK_DOMAINS = {
    "facebook.com",
    "marketplace.facebook.com",
}

def is_role_email(email):
    email = email.lower()
    for pattern in ROLE_EMAIL_PATTERNS:
        if re.match(pattern, email):
            return True
    return False

def domain_from_email(email):
    return email.split("@")[-1].lower()

def domain_from_url(url):
    try:
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        if domain.startswith("www."):
            domain = domain[4:]
        return domain
    except Exception:
        return ""

def is_blocked_domain(domain, allow_facebook=False):
    if allow_facebook:
        blocked = BLOCKED_DOMAINS - FACEBOOK_DOMAINS
    else:
        blocked = BLOCKED_DOMAINS
    return domain in blocked

def infer_product_fit(row):
    # Prefer explicit product_fit column if present
    pf = row.get("product_fit", None)
    if isinstance(pf, bool):
        return pf
    if isinstance(pf, str) and pf.strip().lower() in {"true","yes","1"}:
        return True
    if isinstance(pf, str) and pf.strip().lower() in {"false","no","0"}:
        return False
    # Heuristic: company/url/domain contains positive keywords
    blob = " ".join([
        str(row.get("company_name","")),
        str(row.get("url","")),
        str(row.get("website","")),
        str(row.get("final_domain","")),
        str(row.get("domain","")),
        str(row.get("source_url","")),
    ]).lower()
    return any(k in blob for k in KEYWORDS_POSITIVE)

def parse_args():
    parser = argparse.ArgumentParser(
        description="Clean enriched leads CSV. New flags: --max-contacts-per-domain N to keep up to N contacts per domain (preferring named > role > phone-only), --email-only to exclude phone-only, single-output workflow (no longer splits by default). --out-call is kept for backward compatibility."
    )
    parser.add_argument("input_csv", help="Input enriched CSV file from cg_runner.py")
    parser.add_argument("--keep-roles", action="store_true", help="Keep role-based emails")
    parser.add_argument("--require-fit", action="store_true", help="Require product fit")
    parser.add_argument("--allow-facebook", action="store_true", help="Allow Facebook/Marketplace domains")
    parser.add_argument("--out-clean", default="Cleaned_Leads.csv", help="Output cleaned leads CSV filename")
    parser.add_argument("--out-call", default="Call_List.csv", help="Output call list CSV filename")
    parser.add_argument("--max-contacts-per-domain", type=int, default=2, help="Keep up to N contacts per domain (named > role > phone-only)")
    parser.add_argument("--email-only", action="store_true", help="Only include rows with an email (exclude phone-only)")
    return parser.parse_args()

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # email
    if "email" not in df.columns:
        if "email_final" in df.columns:
            df["email"] = df["email_final"].astype(str)
        elif "email_address" in df.columns:
            df["email"] = df["email_address"].astype(str)
        else:
            df["email"] = ""
    # phone
    if "phone" not in df.columns:
        cand = None
        for c in ("phones","phone_number","telephone"):
            if c in df.columns:
                cand = c; break
        df["phone"] = df[cand].astype(str) if cand else ""
    # website/url
    if "website" not in df.columns:
        if "url" in df.columns:
            df["website"] = df["url"].astype(str)
        else:
            df["website"] = ""
    # final_domain
    if "final_domain" not in df.columns:
        if "domain" in df.columns:
            df["final_domain"] = df["domain"].astype(str)
        else:
            # derive from website if possible
            df["final_domain"] = df["website"].apply(domain_from_url)
    # strip whitespace
    for c in ("email","phone","website","final_domain"):
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()
    return df

def drop_blocked_domains(df, allow_facebook):
    def domain_allowed(email, website, final_domain):
        # If both empty, drop
        if (not email) and (not website) and (not final_domain):
            return False
        # check email domain
        if email:
            d = domain_from_email(email)
            if is_blocked_domain(d, allow_facebook):
                return False
        # check final_domain first, then website
        d2 = (final_domain or domain_from_url(website) or "").lower()
        if d2 and is_blocked_domain(d2, allow_facebook):
            return False
        return True
    return df[df.apply(lambda r: domain_allowed(r.get("email",""), r.get("website",""), r.get("final_domain","")), axis=1)]

def require_contact_method(df):
    # Require at least one contact method: email or phone (non-empty)
    mask = (df["email"].astype(str).str.strip() != "") | (df["phone"].astype(str).str.strip() != "")
    return df[mask]

# Contact quality classification helper
def classify_contact(row):
    e = str(row.get("email",""))
    p = str(row.get("phone",""))
    if e:
        return "named_email" if not is_role_email(e) else "role_email"
    return "phone_only" if p else "none"

def filter_product_fit(df):
    mask = df.apply(infer_product_fit, axis=1)
    return df[mask]

def select_contacts(df, keep_roles: bool, max_per_domain: int, email_only: bool):
    df = df.copy()
    # annotate qualities
    df["contact_quality"] = df.apply(classify_contact, axis=1)
    # optionally drop phone-only
    if email_only:
        df = df[df["contact_quality"].isin(["named_email","role_email"])]
    # if not keeping roles, drop role_email when a named exists within domain
    if not keep_roles:
        has_named = df.groupby("final_domain")["contact_quality"].apply(lambda s: (s=="named_email").any())
        has_named = has_named.to_dict()
        df = df[~((df["contact_quality"]=="role_email") & df["final_domain"].map(lambda d: has_named.get(d, False)))]
    # priority for selection: named_email (0) < role_email (1) < phone_only (2)
    prio_map = {"named_email":0, "role_email":1, "phone_only":2}
    df["_prio"] = df["contact_quality"].map(prio_map).fillna(99)
    # small tiebreakers: prefer MX present, then longer localpart (proxy for named), then source url length
    mx_bonus = df.get("verification_status","unknown").astype(str).str.contains("mx_present", case=False, na=False).astype(int)
    df["_mx"] = 1 - mx_bonus  # 0 if mx_present, 1 otherwise (so present sorts first)
    def local_len(e):
        e = str(e or "")
        return len(e.split("@")[0]) if "@" in e else 0
    df["_loclen"] = df["email"].apply(local_len)
    df = df.sort_values(by=["final_domain","_prio","_mx","_loclen"], ascending=[True, True, True, False])
    # take up to N per domain
    df["_rank"] = df.groupby("final_domain").cumcount()+1
    selected = df[df["_rank"] <= max_per_domain].copy()
    selected["preferred_contact"] = selected["email"].apply(lambda x: "email" if str(x).strip() else "phone")
    # drop helper cols
    return selected.drop(columns=[c for c in ["_prio","_mx","_loclen","_rank"] if c in selected.columns])

def main():
    args = parse_args()
    df = pd.read_csv(args.input_csv)
    df = normalize_columns(df)
    df = drop_blocked_domains(df, args.allow_facebook)
    df = require_contact_method(df)
    if args.require_fit:
        df = filter_product_fit(df)
    # Single-output contact selection workflow
    df = select_contacts(df, keep_roles=args.keep_roles, max_per_domain=args.max_contacts_per_domain, email_only=args.email_only)
    # single output
    cleaned = df
    cleaned.to_csv(args.out_clean, index=False, quoting=csv.QUOTE_ALL)
    # maintain backward compatibility for --out-call: write phone-only subset if file name provided and not email-only
    if args.out_call and not args.email_only:
        phone_only = cleaned[cleaned["contact_quality"] == "phone_only"]
        phone_only.to_csv(args.out_call, index=False, quoting=csv.QUOTE_ALL)
    print(f"Wrote {len(cleaned)} rows -> {args.out_clean}")
    if args.out_call and not args.email_only:
        print(f"Also wrote {len(phone_only)} phone-only rows -> {args.out_call}")

if __name__ == "__main__":
    main()
