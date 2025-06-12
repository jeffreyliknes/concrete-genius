import pandas as pd

# Load your scored prospects
df = pd.read_csv("scored_prospects.csv")

# Select key columns and rename the score column as "ICP_Match"
df_display = df[['company', 'domain', 'source_url', 'gpt_score']].copy()
df_display.rename(columns={'gpt_score': 'ICP_Match (YES/NO)'}, inplace=True)

# Print the table to console
print(df_display.to_string(index=False))

# Save to CSV for manual review
df_display.to_csv("review_prospects.csv", index=False)
print("\n✅ Saved review table to 'review_prospects.csv'")