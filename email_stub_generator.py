import csv, os, openai, textwrap
from dotenv import load_dotenv

load_dotenv()
openai.api_key = os.getenv("OPENAI_API_KEY")

TEMPLATE = textwrap.dedent("""\
    Write a concise (≤120 words) cold email to a {company} decision-maker
    about upgrading to a high-output volumetric concrete mixer.
    Reason for interest: {reason}.
    Mention a tangible benefit (e.g., consistent mix, reduced waste, CloudOps remote monitoring).
    Tone: practical, no fluff, no exclamation marks.
    CTA: short 15-minute call.
""")

# -------- File paths --------
INPUT_FILE = "contacts_clients.csv"       # <- read only the qualified leads
OUTPUT_FILE = "lead_sheet.csv"

def draft_email(company, reason):
    prompt = TEMPLATE.format(company=company, reason=reason)
    rsp = openai.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=180,
        temperature=0.6,
    )
    return rsp.choices[0].message.content.strip()

def run():
    with open(INPUT_FILE, newline='', encoding='utf-8') as inp, \
         open(OUTPUT_FILE, "w", newline='', encoding='utf-8') as out:
        rdr = csv.DictReader(inp)
        fieldnames = rdr.fieldnames + ["email_draft"]
        w = csv.DictWriter(out, fieldnames=fieldnames)
        w.writeheader()
        for row in rdr:
            company = row["url"].split("//")[-1].split("/")[0]
            draft = draft_email(company, row["reason"])
            row["email_draft"] = draft
            w.writerow(row)
    print(f"{OUTPUT_FILE} ready for the sales team.")

if __name__ == "__main__":
    run()