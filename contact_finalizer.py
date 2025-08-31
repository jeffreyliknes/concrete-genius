#!/usr/bin/env python3
import argparse, pandas as pd, re

FREE_DOMAINS = {"gmail.com","yahoo.com","hotmail.com","outlook.com","live.com","aol.com","icloud.com","gmx.com","proton.me","protonmail.com"}
PHONE_DIGITS_RE = re.compile(r"\d{7,}")

ROLE_MAILS = {"info","sales","contact","support","hello","service","office","admin","team","hr","jobs","careers"}
GOOD_VERIF = {"verified","mx_present"}  # keep this strict so bounces drop

def is_valid_phone(p: str) -> bool:
    if not p:
        return False
    return bool(PHONE_DIGITS_RE.search(str(p)))

def is_named_email(e: str) -> bool:
    if not e or "@" not in e: return False
    local = e.split("@",1)[0].lower()
    # named if it contains a dot or hyphen (john.smith) and isn't a known role
    return not any(local == r or local.startswith(r + "+") for r in ROLE_MAILS) and bool(re.search(r"[.\-]", local))

def pick_best_email(row):
    # candidates from primary and alternates
    cands = []
    if str(row.get("email_final","")).strip():
        cands.append(("email_final", str(row["email_final"]).strip(), str(row.get("verification_status","")).strip(), str(row.get("email_source","")).strip()))
    for alt in str(row.get("email_alt_candidates","")).split(";"):
        alt = alt.strip()
        if alt:
            cands.append(("email_alt", alt, str(row.get("verification_status","")).strip(), str(row.get("email_source","")).strip()))

    if not cands:
        return "", "", "", ""

    # score: named > role; verified > mx_present > other; snov > mailto/raw > other
    def score(t):
        _, em, ver, src = t
        s = 0
        s += 100 if is_named_email(em) else 0
        s += 60 if ver == "verified" else 30 if ver == "mx_present" else 0
        s += 20 if src == "snov" else 10 if "mailto" in src or "mx" in ver else 0
        # light penalty for catchall-y roles
        local = em.split("@",1)[0].lower()
        # penalize free webmail domains
        try:
            domain = em.split("@",1)[1].lower()
            if domain in FREE_DOMAINS:
                s -= 30
        except Exception:
            pass
        if local in ROLE_MAILS: s -= 15
        return s

    cands.sort(key=score, reverse=True)
    top = cands[0]
    return top[1], top[2], top[3], ";".join(sorted({em for _,em,_,_ in cands if em}))

def classify_lead(phone_primary:str, email_primary:str):
    has_p = is_valid_phone(phone_primary)
    has_e = bool(str(email_primary).strip())
    if has_p and has_e: return "phone+email"
    if has_p: return "phone_only"
    if has_e: return "email_only"
    return "no_contact"

def main():
    ap = argparse.ArgumentParser(description="Merge phones + emails into a final sales-ready list.")
    ap.add_argument("--outreach", required=True, help="Original outreach CSV (has emails/score/etc.)")
    ap.add_argument("--call", required=True, help="Phone-cleaned CSV (Call_Ready.csv)")
    ap.add_argument("--output", required=True, help="Final merged CSV")
    ap.add_argument("--key", default="domain,company_name", help="Join keys, comma-separated")
    args = ap.parse_args()

    keys = [k.strip() for k in args.key.split(",") if k.strip()]

    O = pd.read_csv(args.outreach, dtype=str, keep_default_na=False)
    C = pd.read_csv(args.call, dtype=str, keep_default_na=False)

    # --- ensure phone columns exist / derive if missing ---
    for _col in ["phone_primary", "phone_all", "phone_count"]:
        if _col not in C.columns:
            C[_col] = ""

    # try to derive from any available phone-ish source
    _phone_src = None
    for _candidate in ["phone", "phones", "phone_numbers", "contact_phone", "phone_all"]:
        if _candidate in C.columns:
            _phone_src = _candidate
            break

    def _normalize_phone(_s: str) -> str:
        _s = str(_s or "").strip()
        if not _s or _s.lower() == "nan":
            return ""
        _digits = re.sub(r"\D", "", _s)
        if not _digits:
            return ""
        if len(_digits) == 11 and _digits.startswith("1"):
            return "+1" + _digits[1:]
        if len(_digits) == 10:
            return "+1" + _digits
        return "+" + _digits

    if _phone_src is not None:
        def _split_phones(_val):
            _s = str(_val or "").replace(",", ";")
            _parts = [p.strip() for p in _s.split(";") if p.strip() and p.strip().lower() != "nan"]
            _cleaned = [_normalize_phone(p) for p in _parts]
            _cleaned = [c for c in _cleaned if c]
            # de-duplicate preserving order
            _seen = set(); _uniq = []
            for c in _cleaned:
                if c not in _seen:
                    _seen.add(c); _uniq.append(c)
            _primary = _uniq[0] if _uniq else ""
            _all_str = ";".join(_uniq)
            _count = len(_uniq)
            return pd.Series([_primary, _all_str, _count])

        _need_fill = C["phone_primary"].astype(str).str.strip().eq("") & C[_phone_src].astype(str).str.strip().ne("")
        C.loc[_need_fill, ["phone_primary", "phone_all", "phone_count"]] = C.loc[_need_fill, _phone_src].apply(_split_phones)

    # Ensure keys present
    for k in keys:
        if k not in O.columns or k not in C.columns:
            raise SystemExit(f"Join key '{k}' missing in one of the files")

    # Left join to keep ALL outreach rows; bring in phone data from Call_Ready
    merged = O.merge(
        C[keys + ["phone_primary","phone_all","phone_count"]],
        on=keys, how="left", suffixes=("","")
    )

    # Backfill phone_primary from the first phone in phone_all if missing
    def _first_phone_from_all(v: str) -> str:
        if not v:
            return ""
        # phone_all expected as semicolon-separated list
        return str(v).split(";")[0].strip()

    merged["phone_primary"] = merged.apply(
        lambda r: r.get("phone_primary","") or _first_phone_from_all(r.get("phone_all","")), axis=1
    )
    merged["phone_valid"] = merged["phone_primary"].apply(is_valid_phone)

    # Pick best email, aggregate all emails if needed
    best_emails = merged.apply(pick_best_email, axis=1, result_type="expand")
    best_emails.columns = ["email_primary","email_verification","email_source_best","email_all"]
    merged = pd.concat([merged, best_emails], axis=1)
    merged["email_is_good"] = merged["email_verification"].isin(GOOD_VERIF)

    # Lead type + preferred contact
    merged["lead_type"] = merged.apply(lambda r: classify_lead(r.get("phone_primary",""), r.get("email_primary","")), axis=1)
    merged["preferred_contact"] = merged["lead_type"].map({
        "phone+email":"phone_first",
        "phone_only":"phone",
        "email_only":"email",
        "no_contact":"research"
    })

    # Filter out truly unusable rows (no phone and no email)
    merged = merged[merged["lead_type"] != "no_contact"].copy()

    # Useful column order
    front = [c for c in [
        "company_name","domain","url",
        "phone_primary","phone_valid","phone_all","phone_count",
        "email_primary","email_verification","email_source_best","email_all",
        "email_is_good",
        "lead_type","preferred_contact",
        "score","qualified","product_fit","reason"
    ] if c in merged.columns]
    rest = [c for c in merged.columns if c not in front]
    merged = merged[front + rest]

    merged.to_csv(args.output, index=False)
    print(f"Wrote {len(merged)} rows → {args.output}")
    print(merged["lead_type"].value_counts(dropna=False).to_string())

if __name__ == "__main__":
    main()