#!/usr/bin/env python3
import argparse, re, sys
from collections import Counter
import pandas as pd

DIGIT_RE = re.compile(r"[+\d().xXext;,\s-]+")

# Common junk patterns you showed (placeholders / regex defaults / obvious fakes)
JUNK_SUBSTRINGS = {
    "0000000000", "1111111111", "2222222222", "3333333333", "4444444444",
    "5555555555", "6666666666", "7777777777", "8888888888", "9999999999"
}
# If any of these exact numbers slip through after normalization, drop them
JUNK_EXACT = {
    "+10000000000", "+11111111111", "+12222222222", "+13333333333",
    "+14444444444", "+15555555555", "+16666666666", "+17777777777",
    "+18888888888", "+19999999999"
}

def only_digits(s: str) -> str:
    return re.sub(r"\D", "", s or "")

def normalize_us_ca(raw: str):
    """Return (+1XXXXXXXXXX) or None."""
    d = only_digits(raw)
    if len(d) == 11 and d.startswith("1"):
        d = d[1:]
    if len(d) != 10:
        return None
    # NANP sanity checks (NXX-NXX-XXXX, no leading 0/1 in NPA/NXX)
    if d[0] in "01" or d[3] in "01":
        return None
    # filter obvious junk (e.g., all same digit)
    if len(set(d)) <= 2 or d in JUNK_SUBSTRINGS:
        return None
    e164 = f"+1{d}"
    if e164 in JUNK_EXACT:
        return None
    return e164

def normalize_e164_any(raw: str):
    """Permissive E.164 (8–15 digits, leading +). Used only if --keep-intl."""
    d = only_digits(raw)
    # Allow leading + in original
    has_plus = str(raw).strip().startswith("+")
    if not has_plus:
        return None
    if not (8 <= len(d) <= 15):
        return None
    # kill sequences of a single digit
    if len(set(d)) <= 2:
        return None
    return "+" + d

def extract_candidates(cell: str):
    if pd.isna(cell):
        return []
    # Split rudely on common separators, then also scan for digit-y runs
    parts = re.split(r"[;,/|]", str(cell))
    out = []
    for p in parts:
        # quick reject if it has no digits
        if not re.search(r"\d", p):
            continue
        out.append(p.strip())
    return out

def clean_row_numbers(cell: str, keep_intl: bool, max_per_row: int):
    cands = extract_candidates(cell)
    found = []
    seen = set()
    for c in cands:
        n = normalize_us_ca(c)
        if n is None and keep_intl:
            n = normalize_e164_any(c)
        if n and n not in seen:
            seen.add(n)
            found.append(n)
        if len(found) >= max_per_row:
            break
    return found

def main():
    ap = argparse.ArgumentParser(description="Clean and normalize phone numbers in a CSV.")
    ap.add_argument("--input", required=True, help="Path to input CSV")
    ap.add_argument("--output", required=True, help="Path to write cleaned CSV")
    ap.add_argument("--phone-col", default="phone", help="Column name containing phones")
    ap.add_argument("--keep-intl", action="store_true", help="Keep non-US/CA E.164 numbers too")
    ap.add_argument("--keep-empty", action="store_true", help="Do not drop rows with no valid phone")
    ap.add_argument("--max-per-row", type=int, default=3, help="Max phones to keep per row in phone_all")
    ap.add_argument("--dedupe-key", default="domain,company_name",
                    help="Comma-separated columns to define uniqueness for cross-row phone dedupe")
    args = ap.parse_args()

    try:
        df = pd.read_csv(args.input, dtype=str, keep_default_na=False)
    except Exception as e:
        print(f"ERROR: could not read {args.input}: {e}", file=sys.stderr)
        sys.exit(1)

    if args.phone_col not in df.columns:
        print(f"ERROR: '{args.phone_col}' column not found.", file=sys.stderr)
        sys.exit(1)

    # Clean per-row
    cleaned = df[args.phone_col].apply(lambda s: clean_row_numbers(s, args.keep_intl, args.max_per_row))
    df["phone_all"] = cleaned.apply(lambda lst: ";".join(lst))
    df["phone_count"] = cleaned.apply(len)
    df["phone_primary"] = cleaned.apply(lambda lst: lst[0] if lst else "")

    # Drop rows without phones unless user wants to keep them
    before_rows = len(df)
    if not args.keep_empty:
        df = df[df["phone_count"] > 0].copy()
    after_rows = len(df)

    # Optional cross-row dedupe of identical numbers for the same (domain, company)
    dedupe_cols = [c.strip() for c in args.dedupe_key.split(",") if c.strip()]
    for col in dedupe_cols:
        if col not in df.columns:
            print(f"WARNING: dedupe key column '{col}' not in dataframe; skipping cross-row phone dedupe.",
                  file=sys.stderr)
            dedupe_cols = []
            break

    drop_count_cross = 0
    if dedupe_cols:
        # For each group, ensure each unique phone appears once (favor the row with higher 'score' if present)
        if "score" in df.columns:
            df["__sort"] = pd.to_numeric(df["score"], errors="coerce").fillna(0)
        else:
            df["__sort"] = 0

        df.sort_values(by=dedupe_cols + ["__sort"], ascending=[True]*len(dedupe_cols) + [False], inplace=True)

        keep_idx = []
        seen_in_group = {}
        for idx, row in df.iterrows():
            key = tuple(row[c] for c in dedupe_cols)
            phones = [p for p in str(row.get("phone_all", "")).split(";") if p]
            if not phones:
                keep_idx.append(idx)
                continue
            already = seen_in_group.get(key, set())
            new_ones = [p for p in phones if p not in already]
            if new_ones:
                keep_idx.append(idx)
                seen_in_group[key] = already.union(new_ones)
            else:
                drop_count_cross += 1
        df = df.loc[keep_idx].copy()
        df.drop(columns=["__sort"], inplace=True, errors="ignore")

    # Reorder helpful columns near the front if present
    front = [c for c in ["domain","company_name","url","phone_primary","phone_all","phone_count","email_final","score","reason","qualified","product_fit"] if c in df.columns]
    rest = [c for c in df.columns if c not in front]
    df = df[front + rest]

    # Write
    try:
        df.to_csv(args.output, index=False)
    except Exception as e:
        print(f"ERROR: could not write {args.output}: {e}", file=sys.stderr)
        sys.exit(1)

    # Stats
    kept_rows = after_rows
    dropped_rows = before_rows - after_rows
    total_numbers = sum(df["phone_count"])
    unique_numbers = len(set(p for row in df["phone_all"] for p in row.split(";") if p))
    print(f"Wrote {len(df)} rows → {args.output}")
    print(f"Dropped {dropped_rows} rows with no valid phone (use --keep-empty to retain).")
    print(f"Cross-row duplicate drops within groups: {drop_count_cross}")
    print(f"Total dialable numbers: {total_numbers} (unique: {unique_numbers})")

if __name__ == "__main__":
    main()