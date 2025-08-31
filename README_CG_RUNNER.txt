
# CG Leads Runner

One-file pipeline to go from `prospects_raw.csv` -> `Merged_Final_Leads_Master.csv`.

## What it does
1. Resolves final URL + domain (follows redirects).
2. Scrapes candidate pages for emails (raw, mailto, de-obfuscated) and phones (including JSON-LD).
3. Performs a cheap MX presence check (`mx_present` / `no_mx` / `unknown`).
4. Outputs ready-to-use CSV with `email_source`, `source_url`, and a simple `score` to prioritize cold email sends.

## Install
```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

## Run
```bash
python cg_runner.py prospects_raw.csv Merged_Final_Leads_Master.csv
```
(Args are optional; defaults to those filenames if omitted.)

## Notes
- Role-based emails (info@, sales@, etc.) are kept but deprioritized; named emails are favored.
- For catch-all domains, `verification_status` is `mx_present` (SMTP checks are intentionally not performed here to stay cheap).
- Cold email tactic: send to named contacts first; follow-up via phone 3 days later if no reply.
