import pandas as pd
import time
import openai
import os
from dotenv import load_dotenv

load_dotenv()

INPUT_FILE = "enriched_prospects.csv"
OUTPUT_FILE = "scored_prospects.csv"

# --------------------------------------------------------------------------- #
# 1  Load enriched prospects
# --------------------------------------------------------------------------- #
df = pd.read_csv(INPUT_FILE)

# --------------------------------------------------------------------------- #
# 2  GPT configuration
# --------------------------------------------------------------------------- #
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
client = openai.OpenAI(api_key=OPENAI_API_KEY)
model = "gpt-4o"

# --------------------------------------------------------------------------- #
# 3  Scoring function
# --------------------------------------------------------------------------- #
def score_content(company, website_text):
    prompt = f"""
You are an expert B2B outbound agent for heavy concrete equipment (OMNI Mixer, Batch Pro).

The target ideal customers are companies involved in: 
- Ready-Mix Concrete Production
- Concrete Batch Plants
- Precast Concrete Manufacturing
- Civil Contractors who pour or produce concrete
- Aggregates, Recycling, or Cement Contracting

Given the following company's website content, answer:

COMPANY NAME: {company}
WEBSITE CONTENT:
\"\"\"
{website_text}
\"\"\"

Is this company a likely target for outbound sales? 
Answer with a single word: YES or NO.
"""
    try:
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": "You are a B2B sales qualification assistant."},
                {"role": "user", "content": prompt}
            ],
            temperature=0
        )
        answer = response.choices[0].message.content.strip().upper()
        if "YES" in answer:
            return "YES"
        elif "NO" in answer:
            return "NO"
        else:
            return "UNKNOWN"
    except Exception as e:
        print(f"⚠ GPT failed for {company} — {e}")
        return "ERROR"

# --------------------------------------------------------------------------- #
# 4  Apply scoring to each row
# --------------------------------------------------------------------------- #
results = []

for index, row in df.iterrows():
    company = row['company']
    website_text = row['content']
    score = score_content(company, website_text)
    
    print(f"Scored {company}: {score}")

    results.append({
        "company": company,
        "domain": row['domain'],
        "source_url": row['source_url'],
        "gpt_score": score
    })

    time.sleep(1.5)  # Respectful delay to stay within API rate limits

# --------------------------------------------------------------------------- #
# 5  Save results
# --------------------------------------------------------------------------- #
df_out = pd.DataFrame(results)
df_out.to_csv(OUTPUT_FILE, index=False)
print(f"\n✅ Saved scored results to '{OUTPUT_FILE}'")