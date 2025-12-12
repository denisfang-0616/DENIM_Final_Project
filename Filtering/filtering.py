import pandas as pd
import psycopg2
from dotenv import load_dotenv
import os
from collections import Counter
import re

load_dotenv()

# Connect to database
conn = psycopg2.connect(
    host=os.getenv('DB_HOST'),
    port=int(os.getenv('DB_PORT')),
    database=os.getenv('DB_NAME'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD')
)

print("Loading data from PostgreSQL...")
query = "SELECT * FROM forum_posts"
df = pd.read_sql(query, conn)
print(f"Total posts: {len(df):,}\n")

# ============================================================
# DIAGNOSTIC 1: Thread Title Analysis
# ============================================================
print("="*70)
print("DIAGNOSTIC 1: THREAD TITLE ANALYSIS")
print("="*70)

# Define potential admissions keywords
admissions_threads = [
    'profile', 'evaluation', 'eval', 'result', 
    'admitted', 'accepted', 'chances', 'background', 
    'sweat', 'application', 'decision', 'admit'
]

# Split into groups
df['has_admissions_title'] = df['thread_title'].str.contains(
    '|'.join(admissions_threads), case=False, na=False
)

kept_threads = df[df['has_admissions_title']]
filtered_threads = df[~df['has_admissions_title']]

print(f"\nWould KEEP: {len(kept_threads):,} posts ({len(kept_threads)/len(df)*100:.1f}%)")
print(f"Would FILTER: {len(filtered_threads):,} posts ({len(filtered_threads)/len(df)*100:.1f}%)")

# Analyze what we're filtering out
print("\n--- Top 30 Thread Titles Being FILTERED OUT ---")
filtered_title_counts = filtered_threads['thread_title'].value_counts().head(30)
for i, (title, count) in enumerate(filtered_title_counts.items(), 1):
    print(f"{i:2d}. [{count:4d} posts] {title[:80]}")

print("\n--- Sample Posts from FILTERED Threads (for manual review) ---")
sample_filtered = filtered_threads.sample(min(10, len(filtered_threads)))
for idx, row in sample_filtered.iterrows():
    print(f"\nThread: {row['thread_title'][:60]}")
    print(f"Post preview: {row['post_content'][:200]}...")
    print("-" * 70)

# ============================================================
# DIAGNOSTIC 2: Keyword Frequency Analysis
# ============================================================
print("\n" + "="*70)
print("DIAGNOSTIC 2: KEYWORD FREQUENCY ANALYSIS")
print("="*70)

admissions_keywords = [
    'gpa', 'gre', 'quant', 'verbal', 'toefl', 'ielts',
    'undergrad', 'graduate', 'university', 'college',
    'accept', 'reject', 'admit', 'waitlist', 'rejection',
    'research', 'publication', 'paper', 'letter', 'recommendation',
    'math', 'econ', 'statistics', 'calculus', 'analysis',
    'mit', 'harvard', 'stanford', 'berkeley', 'chicago',
    'masters', 'phd', 'doctoral', 'degree'
]

def extract_all_words(text):
    """Extract all words from text"""
    text_lower = str(text).lower()
    words = re.findall(r'\b[a-z]{3,}\b', text_lower)  # Words 3+ chars
    return words

# Count keyword frequencies in KEPT vs FILTERED posts
print("\n--- Keyword Frequencies: KEPT Posts ---")
kept_words = []
for text in kept_threads['post_content']:
    kept_words.extend(extract_all_words(text))
kept_word_freq = Counter(kept_words).most_common(50)

print("Top 50 words in KEPT posts:")
for word, count in kept_word_freq:
    print(f"  {word:20s}: {count:,}")

print("\n--- Keyword Frequencies: FILTERED Posts ---")
filtered_words = []
for text in filtered_threads['post_content']:
    filtered_words.extend(extract_all_words(text))
filtered_word_freq = Counter(filtered_words).most_common(50)

print("Top 50 words in FILTERED posts:")
for word, count in filtered_word_freq:
    print(f"  {word:20s}: {count:,}")

# Check if important admissions keywords appear in filtered posts
print("\n--- Admissions Keywords Found in FILTERED Posts ---")
for keyword in admissions_keywords:
    count = sum(1 for text in filtered_threads['post_content'] 
                if keyword in str(text).lower())
    if count > 0:
        pct = count / len(filtered_threads) * 100
        print(f"  '{keyword}': {count:,} posts ({pct:.1f}% of filtered)")

# ============================================================
# DIAGNOSTIC 3: Post Length Distribution
# ============================================================
print("\n" + "="*70)
print("DIAGNOSTIC 3: POST LENGTH ANALYSIS")
print("="*70)

df['post_length'] = df['post_content'].str.len()

print("\n--- Length Distribution (All Posts) ---")
print(df['post_length'].describe())

# Analyze short posts
short_threshold = 50
short_posts = df[df['post_length'] < short_threshold]
print(f"\n--- Posts Under {short_threshold} Characters ---")
print(f"Count: {len(short_posts):,} posts ({len(short_posts)/len(df)*100:.1f}%)")
print("\nSample short posts:")
for idx, row in short_posts.sample(min(20, len(short_posts))).iterrows():
    print(f"  [{len(row['post_content']):3d} chars] {row['post_content']}")

# Check if short posts have admissions keywords
short_with_keywords = short_posts[
    short_posts['post_content'].str.contains(
        '|'.join(admissions_keywords), case=False, na=False
    )
]
print(f"\nShort posts WITH admissions keywords: {len(short_with_keywords):,}")
print("Sample:")
for idx, row in short_with_keywords.head(10).iterrows():
    print(f"  {row['post_content']}")

# ============================================================
# DIAGNOSTIC 4: Keyword Density Analysis
# ============================================================
print("\n" + "="*70)
print("DIAGNOSTIC 4: KEYWORD DENSITY ANALYSIS")
print("="*70)

def count_keywords(text):
    text_lower = str(text).lower()
    return sum(1 for keyword in admissions_keywords if keyword in text_lower)

df['keyword_count'] = df['post_content'].apply(count_keywords)

print("\n--- Keyword Count Distribution ---")
print(df['keyword_count'].value_counts().sort_index())

# Analyze posts with 0-1 keywords
low_keyword_posts = df[df['keyword_count'] < 2]
print(f"\n--- Posts with <2 Keywords (would be filtered) ---")
print(f"Count: {len(low_keyword_posts):,} posts ({len(low_keyword_posts)/len(df)*100:.1f}%)")

print("\nSample posts with 0 keywords:")
zero_keyword = df[df['keyword_count'] == 0].sample(min(10, len(df[df['keyword_count'] == 0])))
for idx, row in zero_keyword.iterrows():
    print(f"\nThread: {row['thread_title'][:60]}")
    print(f"Post: {row['post_content'][:200]}...")
    print("-" * 70)

print("\nSample posts with 1 keyword:")
one_keyword = df[df['keyword_count'] == 1].sample(min(10, len(df[df['keyword_count'] == 1])))
for idx, row in one_keyword.iterrows():
    print(f"\nThread: {row['thread_title'][:60]}")
    print(f"Post: {row['post_content'][:200]}...")
    print("-" * 70)

# ============================================================
# DIAGNOSTIC 5: Author Activity Analysis
# ============================================================
print("\n" + "="*70)
print("DIAGNOSTIC 5: AUTHOR ACTIVITY ANALYSIS")
print("="*70)

author_counts = df['author'].value_counts()
print(f"\nTotal unique authors: {len(author_counts):,}")
print(f"Average posts per author: {author_counts.mean():.1f}")

single_post_authors = author_counts[author_counts == 1]
print(f"\nAuthors with only 1 post: {len(single_post_authors):,} ({len(single_post_authors)/len(author_counts)*100:.1f}%)")
print(f"Posts from single-post authors: {single_post_authors.sum():,} ({single_post_authors.sum()/len(df)*100:.1f}%)")

# Sample posts from single-post authors
single_post_df = df[df['author'].isin(single_post_authors.index)]
print("\nSample posts from single-post authors:")
for idx, row in single_post_df.sample(min(15, len(single_post_df))).iterrows():
    print(f"\nAuthor: {row['author']}")
    print(f"Thread: {row['thread_title'][:60]}")
    print(f"Post: {row['post_content'][:150]}...")
    print("-" * 70)

# ============================================================
# DIAGNOSTIC 6: Duplicate Analysis
# ============================================================
print("\n" + "="*70)
print("DIAGNOSTIC 6: DUPLICATE CONTENT ANALYSIS")
print("="*70)

duplicates = df[df.duplicated(subset=['post_content'], keep=False)]
print(f"\nTotal duplicate posts: {len(duplicates):,} ({len(duplicates)/len(df)*100:.1f}%)")

if len(duplicates) > 0:
    print("\nSample duplicate content:")
    dup_groups = duplicates.groupby('post_content')
    for content, group in list(dup_groups)[:5]:
        print(f"\nDuplicated {len(group)} times:")
        print(f"Content: {content[:200]}...")
        print(f"Authors: {', '.join(group['author'].unique())}")
        print("-" * 70)

# ============================================================
# DIAGNOSTIC 7: Off-Topic Thread Analysis
# ============================================================
print("\n" + "="*70)
print("DIAGNOSTIC 7: OFF-TOPIC THREAD ANALYSIS")
print("="*70)

exclude_keywords = [
    'toefl', 'ielts', 'visa', 'loan', 
    'housing', 'apartment', 'roommate',
    'tutoring', 'for sale', 'buying', 'selling'
]

def is_offtopic(title):
    title_lower = str(title).lower()
    return any(keyword in title_lower for keyword in exclude_keywords)

df['is_offtopic'] = df['thread_title'].apply(is_offtopic)
offtopic_posts = df[df['is_offtopic']]

print(f"\nPosts in off-topic threads: {len(offtopic_posts):,} ({len(offtopic_posts)/len(df)*100:.1f}%)")

print("\nOff-topic threads:")
offtopic_titles = offtopic_posts['thread_title'].value_counts().head(20)
for title, count in offtopic_titles.items():
    print(f"  [{count:4d} posts] {title}")

# Check if any have admissions keywords
offtopic_with_keywords = offtopic_posts[
    offtopic_posts['post_content'].str.contains(
        '|'.join(['gpa', 'gre', 'accept', 'reject', 'admit']), 
        case=False, na=False
    )
]
print(f"\nOff-topic posts WITH admissions keywords: {len(offtopic_with_keywords):,}")
if len(offtopic_with_keywords) > 0:
    print("\nSample:")
    for idx, row in offtopic_with_keywords.head(10).iterrows():
        print(f"\nThread: {row['thread_title']}")
        print(f"Post: {row['post_content'][:200]}...")
        print("-" * 70)

# ============================================================
# DIAGNOSTIC 8: Combined Filter Impact
# ============================================================
print("\n" + "="*70)
print("DIAGNOSTIC 8: COMBINED FILTER IMPACT")
print("="*70)

# Apply all filters step by step
df_analysis = df.copy()

print(f"\nOriginal posts: {len(df_analysis):,}")

# Filter 1: Thread title
df_analysis = df_analysis[df_analysis['has_admissions_title']]
print(f"After thread filter: {len(df_analysis):,} ({len(df_analysis)/len(df)*100:.1f}%)")

# Filter 2: Length
df_analysis = df_analysis[df_analysis['post_length'] >= 50]
print(f"After length filter: {len(df_analysis):,} ({len(df_analysis)/len(df)*100:.1f}%)")

# Filter 3: Keywords
df_analysis = df_analysis[df_analysis['keyword_count'] >= 2]
print(f"After keyword filter: {len(df_analysis):,} ({len(df_analysis)/len(df)*100:.1f}%)")

# Filter 4: Duplicates
df_analysis = df_analysis.drop_duplicates(subset=['post_content'], keep='first')
print(f"After duplicate removal: {len(df_analysis):,} ({len(df_analysis)/len(df)*100:.1f}%)")

# Filter 5: Off-topic
df_analysis = df_analysis[~df_analysis['is_offtopic']]
print(f"After off-topic filter: {len(df_analysis):,} ({len(df_analysis)/len(df)*100:.1f}%)")

# ============================================================
# DIAGNOSTIC 9: False Negative Check
# ============================================================
print("\n" + "="*70)
print("DIAGNOSTIC 9: FALSE NEGATIVE CHECK")
print("="*70)

# Posts that would be FILTERED but contain strong admissions signals
strong_signals = ['gpa', 'gre', 'admitted', 'accepted', 'rejected']

filtered_out = df[~df.index.isin(df_analysis.index)]
false_negatives = filtered_out[
    filtered_out['post_content'].str.contains(
        '|'.join(strong_signals), case=False, na=False
    )
]

print(f"\nPotential false negatives: {len(false_negatives):,} posts")
print(f"(Posts that were filtered but contain: {', '.join(strong_signals)})")

if len(false_negatives) > 0:
    print("\nSample potential false negatives:")
    for idx, row in false_negatives.sample(min(15, len(false_negatives))).iterrows():
        print(f"\nThread: {row['thread_title'][:60]}")
        print(f"Post length: {row['post_length']}")
        print(f"Keywords found: {row['keyword_count']}")
        print(f"Post: {row['post_content'][:250]}...")
        print("-" * 70)

# ============================================================
# SUMMARY
# ============================================================
print("\n" + "="*70)
print("SUMMARY")
print("="*70)

print(f"""
Original posts:           {len(df):,}
After all filters:        {len(df_analysis):,}
Posts filtered out:       {len(df) - len(df_analysis):,}
Reduction:                {(len(df) - len(df_analysis))/len(df)*100:.1f}%

Potential false negatives: {len(false_negatives):,} ({len(false_negatives)/len(filtered_out)*100:.1f}% of filtered)

Estimated GPT cost (filtered): ${len(df_analysis) * 333 * 0.125 / 1000000 + len(df_analysis) * 175 * 1.000 / 1000000:.2f}
Estimated GPT cost (no filter): ${len(df) * 333 * 0.125 / 1000000 + len(df) * 175 * 1.000 / 1000000:.2f}
Savings: ${(len(df) - len(df_analysis)) * 333 * 0.125 / 1000000 + (len(df) - len(df_analysis)) * 175 * 1.000 / 1000000:.2f}
""")

print("="*70)
print("RECOMMENDATIONS:")
print("="*70)
print("""
1. Review the "Top Thread Titles Being FILTERED OUT" section
   - Are any admissions-related threads being excluded?
   
2. Check "False Negative" samples
   - Do these look like valid admissions posts?
   
3. Adjust filters if needed:
   - Add more thread title keywords
   - Lower keyword threshold (2 → 1)
   - Reduce length requirement (50 → 30)
   
4. Save this output and review with your team before running GPT extraction
""")

conn.close()

# Save detailed report
print("\nSaving detailed diagnostic report...")
with open('filter_diagnostics_report.txt', 'w', encoding='utf-8') as f:
    f.write("FILTER DIAGNOSTICS REPORT\n")
    f.write("="*70 + "\n\n")
    # You can redirect print statements here for a full report
    
print("✅ Diagnostic analysis complete!")
print("Review the output above and 'filter_diagnostics_report.txt'")