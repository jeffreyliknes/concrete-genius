import subprocess

print("🚀 Starting full prospecting pipeline...\n")

# Step 1 - Run Google Prospect Search
print("🔎 Running prospect_agent.py ...")
subprocess.run(["python3", "prospect_agent.py"])

# Step 2 - Filter obvious junk
print("\n🧹 Running filter_agent.py ...")
subprocess.run(["python3", "filter_agent.py"])

# Step 3 - Enrich with website scraping
print("\n🌐 Running enrichment_agent.py ...")
subprocess.run(["python3", "enrichment_agent.py"])

# Step 4 - Score using GPT
print("\n🤖 Running scoring_agent.py ...")
subprocess.run(["python3", "scoring_agent.py"])

# Step 5 - Enrich contacts using Hunter.io
print("\n📧 Running enrich_contacts.py ...")
subprocess.run(["python3", "enrich_contacts.py"])

# Step 6 - Gather buying signals
print("\n📊 Running signals_agent.py ...")
subprocess.run(["python3", "signals_agent.py"])

print("\n🎯 Full pipeline with contact enrichment completed successfully!")