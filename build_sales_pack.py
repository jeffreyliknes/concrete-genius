#!/usr/bin/env python3
import pandas as pd, sqlite3, pathlib, datetime as dt, sys
from urllib.parse import urlparse

DB = "leads.db"
TODAY = dt.date.today().isoformat()
OUT_DIR = pathlib.Path("data/outputs"); OUT_DIR.mkdir(parents=True, exist_ok=True)

def domain(u):
    try:
        return urlparse(u).netloc.replace("www.","").lower()
    except:
        return ""

# --- Load core tables/files ---
con = sqlite3.connect(DB)
pros = pd.read_sql("SELECT url, company_name, source_city, scraped_at FROM prospects_raw", con)
con.close()
pros["domain"] = pros["url"].apply(domain)

# contacts_* (from extractor)
frames = []
for f in ("contacts_clients.csv","contacts_non_clients.csv"):
    p = pathlib.Path(f)
    if p.exists():
        frames.append(pd.read_csv(p))
contacts = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
for col in ["url","email","phone","reason","qualified"]:
    if col not in contacts.columns:
        contacts[col] = ""

# hunter emails
hun = pathlib.Path("emails_enriched.csv")
hunter = pd.read_csv(hun).drop_duplicates("domain") if hun.exists() else pd.DataFrame(columns=["domain","email","verification_status"])

# email drafts
draft_path = pathlib.Path("lead_sheet.csv")
drafts = pd.read_csv(draft_path)[["url","email_draft"]] if draft_path.exists() else pd.DataFrame(columns=["url","email_draft"])

# --- Merge ---
final = pros.merge(contacts[["url","email","phone","reason","qualified"]], on="url", how="left")
final = final.merge(hunter[["domain","email","verification_status"]].rename(columns={"email":"email_hunter"}), on="domain", how="left")
final = final.merge(drafts, on="url", how="left")

# pick best email
final["email_final"] = final["email"].fillna("").replace("", pd.NA)
final["email_final"] = final["email_final"].fillna(final["email_hunter"])
final["email_source"] = final.apply(
    lambda r: "extractor" if pd.notna(r["email"]) and r["email"] != "" 
              else ("hunter" if pd.notna(r["email_hunter"]) else ""),
    axis=1
)

final["qualified"] = final["qualified"].fillna(False).astype(bool)

cols = [
    "company_name","url","domain","email_final","email_source","verification_status",
    "phone","reason","qualified","email_draft","source_city","scraped_at"
]
final = final.reindex(columns=cols).sort_values(["qualified","company_name"], ascending=[False, True])

# Decide Excel engine (fallback if xlsxwriter missing)
try:
    import xlsxwriter  # noqa: F401
    _excel_engine = "xlsxwriter"
except ModuleNotFoundError:
    _excel_engine = None

# write CSV
csv_path = OUT_DIR / f"{TODAY}_final_leads.csv"
final.to_csv(csv_path, index=False)

# write nice Excel with tabs (optional if engine available)
xlsx_path = OUT_DIR / f"{TODAY}_final_leads.xlsx"
if _excel_engine:
    with pd.ExcelWriter(xlsx_path, engine=_excel_engine) as xl:
        final.to_excel(xl, sheet_name="ALL", index=False)
        final[final["qualified"]].to_excel(xl, sheet_name="Qualified", index=False)
        final[final["email_final"].isna()].to_excel(xl, sheet_name="No Email", index=False)
    print(f"✅ XLSX -> {xlsx_path}")
else:
    print("⚠️ xlsxwriter not installed, skipped Excel export. Run `pip install xlsxwriter` to enable it.")

print(f"✅ CSV  -> {csv_path}")