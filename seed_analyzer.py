from contacts import companies
from collections import Counter

# Extract keywords
keywords = []
for name in companies:
    for word in name.lower().replace('-', ' ').split():
        if len(word) > 2:
            keywords.append(word)

# Count and display most common keywords
keyword_counts = Counter(keywords)
print(keyword_counts.most_common(15))