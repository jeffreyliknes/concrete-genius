#!/usr/bin/env python3
"""
pipeline_runner.py
------------------
Runs the full Concrete Genius lead-gen pipeline end-to-end with checkpoints.

Steps:
  1  cg_runner.py                          → Merged_Final_Leads_Master.csv
  2  site_profiler.py                      → prospects_profiled.csv
  3  tag_product_fit.py                    → prospects_tagged.csv
  4  cg_cleaner.py                         → Cleaned_Leads.csv
  5  lead_scoring.py                       → Scored_Leads.csv
  6  archive/snov_enrich.py   (optional)   → Scored_Leads_Enriched.csv
  7  lead_scoring.py         (re-score)    → Scored_Leads_Rescored.csv
  8  contact_finalizer.py                  → Outreach_Ready.csv
  9  email_stub_generator.py  (optional)   → Smartlead_Import.csv

Usage examples:
  python pipeline_runner.py
  python pipeline_runner.py --from 3 --to 7
  python pipeline_runner.py --skip-snov
  python pipeline_runner.py --min-score 9 --limit-per-domain 2
  python pipeline_runner.py --site-concurrency 3 --page-concurrency 4 --chunk-size 100

Env (only needed if those steps are enabled):
  SNOV_CLIENT_ID / SNOV_CLIENT_SECRET  (step 6)
  OPENAI_API_KEY                       (step 9)
"""

from __future__ import annotations
import argparse
import os
import sys
import time
import shutil
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent

# Default paths
IN_DIR   = ROOT / "data" / "inputs"
OUT_DIR  = ROOT / "data" / "outputs"

RAW           = IN_DIR / "prospects_raw.csv"
MERGED        = OUT_DIR / "Merged_Final_Leads_Master.csv"
PROFILED      = OUT_DIR / "prospects_profiled.csv"
TAGGED        = OUT_DIR / "prospects_tagged.csv"
CLEANED       = OUT_DIR / "Cleaned_Leads.csv"
SCORED        = OUT_DIR / "Scored_Leads.csv"
ENRICHED      = OUT_DIR / "Scored_Leads_Enriched.csv"
RESCORED      = OUT_DIR / "Scored_Leads_Rescored.csv"
OUTREACH      = OUT_DIR / "Outreach_Ready.csv"
SMARTLEAD_CSV = OUT_DIR / "Smartlead_Import.csv"

PY = shutil.which("python") or shutil.which("python3") or sys.executable

def run(cmd: list[str], check: bool=True) -> int:
    print("›", " ".join(cmd))
    start = time.time()
    proc = subprocess.run(cmd)
    dur = time.time() - start
    print(f"↳ exit {proc.returncode} in {dur:.1f}s\n")
    if check and proc.returncode != 0:
        raise SystemExit(proc.returncode)
    return proc.returncode

def require(path: Path, hint: str=""):
    if not path.exists():
        msg = f"Missing file: {path}"
        if hint:
            msg += f"\n  ↳ {hint}"
        raise SystemExit(msg)

def main():
    ap = argparse.ArgumentParser(description="Concrete Genius pipeline runner")
    ap.add_argument("--from", dest="from_step", type=int, default=1, help="First step to run (1-9)")
    ap.add_argument("--to", dest="to_step", type=int, default=9, help="Last step to run (1-9)")
    ap.add_argument("--skip-snov", action="store_true", help="Skip step 6 (Snov enrichment)")
    ap.add_argument("--skip-stub", action="store_true", help="Skip step 9 (Smartlead stub generation)")
    ap.add_argument("--snov-verify", action="store_true", help="Call Snov verify endpoint in step 6")
    ap.add_argument("--min-score", type=int, default=8, help="Minimum score for Snov enrichment")
    ap.add_argument("--limit-per-domain", type=int, default=3, help="Snov max emails per domain")

    # Runner tuning for cg_runner
    ap.add_argument("--site-concurrency", type=int, default=3)
    ap.add_argument("--page-concurrency", type=int, default=4)
    ap.add_argument("--chunk-size", type=int, default=100)

    args = ap.parse_args()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    IN_DIR.mkdir(parents=True, exist_ok=True)

    # Step toggles
    do_snov = not args.skip_snov
    do_stub = not args.skip_stub

    # Guardrails on step window
    start = max(1, min(9, args.from_step))
    end   = max(1, min(9, args.to_step))
    if start > end:
        raise SystemExit("--from must be <= --to")

    # 1) cg_runner.py
    if 1 >= start and 1 <= end:
        require(RAW, "Put your seed list at data/inputs/prospects_raw.csv")
        run([
            PY, str(ROOT / "cg_runner.py"),
            str(RAW), str(MERGED),
            "--site-concurrency", str(args.site_concurrency),
            "--page-concurrency", str(args.page_concurrency),
            "--chunk-size", str(args.chunk_size),
        ])

    # 2) site_profiler.py
    if 2 >= start and 2 <= end:
        require(MERGED, "Step 1 should have produced this file.")
        run([
            PY, str(ROOT / "site_profiler.py"),
            "--in",  str(MERGED),
            "--out", str(PROFILED),
            "--site-concurrency", str(args.site_concurrency),
        ])

    # 3) tag_product_fit.py
    if 3 >= start and 3 <= end:
        require(PROFILED, "Step 2 should have produced this file.")
        run([
            PY, str(ROOT / "tag_product_fit.py"),
            "--in",  str(PROFILED),
            "--out", str(TAGGED),
        ])

    # 4) cg_cleaner.py
    if 4 >= start and 4 <= end:
        require(TAGGED, "Step 3 should have produced this file.")
        run([
            PY, str(ROOT / "cg_cleaner.py"),
            str(TAGGED),
            "--out-clean", str(CLEANED),
            "--max-contacts-per-domain", "2",
            "--require-fit",
        ])

    # 5) lead_scoring.py
    if 5 >= start and 5 <= end:
        require(CLEANED, "Step 4 should have produced this file.")
        run([
            PY, str(ROOT / "lead_scoring.py"),
            "--in",  str(CLEANED),
            "--out", str(SCORED),
        ])

    # 6) archive/snov_enrich.py  (optional credit spend)
    if 6 >= start and 6 <= end:
        if do_snov:
            require(SCORED, "Step 5 should have produced this file.")
            # Ensure creds are present
            cid = os.getenv("SNOV_CLIENT_ID", "").strip()
            csc = os.getenv("SNOV_CLIENT_SECRET", "").strip()
            if not cid or not csc:
                print("[!] SNOV credentials missing; skipping step 6.")
            else:
                cmd = [
                    PY, str(ROOT / "archive" / "snov_enrich.py"),
                    "--in",  str(SCORED),
                    "--out", str(ENRICHED),
                    "--only-fit",
                    "--min-score", str(args.min_score),
                    "--limit-per-domain", str(args.limit_per_domain),
                ]
                if args.snov_verify:
                    cmd.append("--verify")
                run(cmd)
        else:
            print("[-] Skipping step 6 (Snov enrichment) by flag.")
            # If skipping and rescoring is requested later, pass-through
            if not ENRICHED.exists() and SCORED.exists():
                ENRICHED.write_bytes(SCORED.read_bytes())

    # 7) lead_scoring.py (re-score)
    if 7 >= start and 7 <= end:
        require(ENRICHED, "Step 6 should have produced this file (or use --skip-snov).")
        run([
            PY, str(ROOT / "lead_scoring.py"),
            "--in",  str(ENRICHED),
            "--out", str(RESCORED),
        ])

    # 8) contact_finalizer.py
    if 8 >= start and 8 <= end:
        require(RESCORED, "Step 7 should have produced this file.")
        # Use Cleaned_Leads as a phone source unless you maintain a separate call list
        call_csv = OUT_DIR / "Call_Ready.csv"
        call_src = call_csv if call_csv.exists() else CLEANED
        run([
            PY, str(ROOT / "contact_finalizer.py"),
            "--outreach", str(RESCORED),
            "--call",     str(call_src),
            "--output",   str(OUTREACH),
        ])

    # 9) email_stub_generator.py (optional Smartlead)
    if 9 >= start and 9 <= end:
        if do_stub:
            require(RESCORED, "Step 7 should have produced this file.")
            if not os.getenv("OPENAI_API_KEY", "").strip():
                print("[!] OPENAI_API_KEY missing; skipping step 9 (email stubs).")
            else:
                run([
                    PY, str(ROOT / "email_stub_generator.py"),
                    "--in",  str(RESCORED),
                    "--out", str(SMARTLEAD_CSV),
                ])
        else:
            print("[-] Skipping step 9 (stub generator) by flag.")

    print("\n✅ Pipeline finished.")
    print(f"   Outreach CSV : {OUTREACH if OUTREACH.exists() else '—'}")
    print(f"   Smartlead CSV: {SMARTLEAD_CSV if SMARTLEAD_CSV.exists() else '—'}")

if __name__ == "__main__":
    main()