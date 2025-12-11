# import pandas as pd
# import psycopg2
# from psycopg2.extras import execute_batch
# from dotenv import load_dotenv
# import os

# load_dotenv()

# conn = psycopg2.connect(
#     host=os.getenv('DB_HOST'),
#     port=int(os.getenv('DB_PORT')),
#     database=os.getenv('DB_NAME'),
#     user=os.getenv('DB_USER'),
#     password=os.getenv('DB_PASSWORD')
# )

# print("Loading data from PostgreSQL...")
# df = pd.read_sql("SELECT * FROM forum_posts", conn)
# print(f"Initial posts: {len(df):,}\n")

# # ===== FILTER 1: THREAD TITLE =====
# print("=== Filter 1: Thread Title ===")
# admissions_threads = [
#     # Core admissions keywords
#     'profile', 'evaluation', 'eval', 'result', 
#     'admitted', 'accepted', 'chances', 'background', 
#     'sweat', 'application', 'decision', 'admit',
#     'admission', 'rejection', 'waiting', 'applicant',
    
#     # Time-based
#     'fall', 'spring', 'cycle',
#     '2010', '2011', '2012', '2013', '2014', '2015',
#     '2016', '2017', '2018', '2019', '2020', '2021',
#     '2022', '2023', '2024', '2025',
    
#     # Program types
#     'phd', 'doctoral', 'masters', 'mba', 'ma ', 'ms ',
    
#     # Fields
#     'econ', 'economics', 'finance', 'business', 'are',
    
#     # Help/Q&A
#     'ask', 'question', 'advice', 'help', 'consultant'
# ]

# df = df[df['thread_title'].str.contains('|'.join(admissions_threads), case=False, na=False)]
# print(f"After thread filter: {len(df):,}")

# # ===== FILTER 2: POST LENGTH =====
# print("\n=== Filter 2: Post Length ===")
# df['post_length'] = df['post_content'].str.len()
# df = df[df['post_length'] >= 50]
# print(f"After length filter: {len(df):,}")

# # ===== FILTER 3: SMART KEYWORDS =====
# print("\n=== Filter 3: Smart Keyword Filter ===")
# def has_strong_signal(text):
#     text_lower = str(text).lower()
    
#     # Strong signals - any ONE is enough
#     strong_keywords = [
#         'gpa', 'gre', 'admitted', 'accepted', 'rejected', 
#         'waitlist', 'admit', 'rejection', 'acceptance'
#     ]
#     if any(keyword in text_lower for keyword in strong_keywords):
#         return True
    
#     # General keywords - need 2+
#     general_keywords = [
#         'undergrad', 'graduate', 'university', 'publication', 
#         'letter', 'recommendation', 'research', 'phd', 'math', 
#         'econ', 'statistics', 'masters', 'doctoral',
#         'quant', 'verbal', 'toefl', 'ielts'
#     ]
#     return sum(1 for keyword in general_keywords if keyword in text_lower) >= 2

# df['has_signal'] = df['post_content'].apply(has_strong_signal)
# df = df[df['has_signal']]
# print(f"After keyword filter: {len(df):,}")

# # ===== FILTER 4: DUPLICATES =====
# print("\n=== Filter 4: Duplicates ===")
# df = df.drop_duplicates(subset=['post_content'], keep='first')
# print(f"After duplicates: {len(df):,}")

# # ===== FILTER 5: OFF-TOPIC =====
# print("\n=== Filter 5: Off-Topic Threads ===")
# exclude_keywords = [
#     'toefl only', 'ielts only', 'visa only',
#     'housing', 'apartment', 'roommate',
#     'for sale', 'naplex', 'fpgee', 'pharmacy',
#     'tutoring'
# ]

# def is_offtopic(title):
#     title_lower = str(title).lower()
#     return any(keyword in title_lower for keyword in exclude_keywords)

# df = df[~df['thread_title'].apply(is_offtopic)]
# print(f"After off-topic filter: {len(df):,}")

# # Clean data
# df_clean = df[['id', 'thread_title', 'thread_url', 'author', 'page', 'post_content', 'scraped_at']]

# # ===== SAVE TO POSTGRESQL =====
# print("\n" + "="*60)
# print("SAVING TO POSTGRESQL")
# print("="*60)

# cursor = conn.cursor()

# # Create new table for filtered posts
# print("\nCreating filtered_posts table...")
# cursor.execute("""
#     DROP TABLE IF EXISTS filtered_posts;
    
#     CREATE TABLE filtered_posts (
#         id INTEGER PRIMARY KEY,
#         thread_title TEXT,
#         thread_url TEXT,
#         author VARCHAR(255),
#         page INTEGER,
#         post_content TEXT,
#         scraped_at TIMESTAMP,
#         filtered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
#     );
    
#     CREATE INDEX idx_filtered_thread ON filtered_posts(thread_title);
#     CREATE INDEX idx_filtered_author ON filtered_posts(author);
# """)
# conn.commit()
# print("✅ Table created")

# # Insert filtered data
# print(f"\nInserting {len(df_clean):,} filtered posts...")
# insert_query = """
#     INSERT INTO filtered_posts (id, thread_title, thread_url, author, page, post_content, scraped_at)
#     VALUES (%s, %s, %s, %s, %s, %s, %s)
# """

# data = [
#     (
#         int(row['id']),
#         str(row['thread_title']),
#         str(row['thread_url']),
#         str(row['author']),
#         int(row['page']),
#         str(row['post_content']),
#         row['scraped_at']
#     )
#     for _, row in df_clean.iterrows()
# ]

# execute_batch(cursor, insert_query, data, page_size=1000)
# conn.commit()
# print("✅ Data inserted")

# # Verify
# cursor.execute("SELECT COUNT(*) FROM filtered_posts")
# count = cursor.fetchone()[0]
# print(f"\n✅ Verified: {count:,} posts in filtered_posts table")

# # Also save to CSV as backup
# print("\nSaving backup CSV...")
# df_clean.to_csv('filtered_posts_for_gpt.csv', index=False)
# print("✅ CSV saved")

# # Summary
# print("\n" + "="*60)
# print("SUMMARY")
# print("="*60)
# print(f"Original posts (forum_posts table): 134,524")
# print(f"Filtered posts (filtered_posts table): {count:,}")
# print(f"Reduction: {((134524 - count) / 134524 * 100):.1f}%")
# print(f"Estimated GPT cost: ${(count * 333 * 0.125 / 1000000 + count * 175 * 1.000 / 1000000):.2f}")
# print("="*60)

# cursor.close()
# conn.close()

# print("\n✅ Complete! You now have:")
# print("  1. forum_posts table (raw data - 134k posts)")
# print("  2. filtered_posts table (filtered data - ready for GPT)")
# print("  3. filtered_posts_for_gpt.csv (backup)")

import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv
import os
import re

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv('DB_HOST'),
    port=int(os.getenv('DB_PORT')),
    database=os.getenv('DB_NAME'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD')
)

print("="*70)
print("COMPREHENSIVE FILTERING - forum_posts → filtered_posts")
print("="*70)

print("\nLoading RAW data from forum_posts...")
df = pd.read_sql("SELECT * FROM forum_posts", conn)
print(f"Initial posts: {len(df):,}\n")

# ===== FILTER 1: POST LENGTH =====
print("=== Filter 1: Post Length ===")
df['post_length'] = df['post_content'].str.len()

# Between 100 and 5000 characters
df = df[(df['post_length'] >= 100) & (df['post_length'] <= 5000)]
print(f"After length filter (100-5000 chars): {len(df):,}")

# ===== FILTER 2: REQUIRE MULTIPLE STRONG SIGNALS =====
print("\n=== Filter 2: Require 2+ Strong Signals ===")

def count_strong_signals(text):
    """Count number of strong admissions signals"""
    text_lower = str(text).lower()
    
    signals = {
        'has_gpa': any(x in text_lower for x in ['gpa', 'grade point']),
        'has_gre': any(x in text_lower for x in ['gre', 'quant', 'verbal']),
        'has_admission': any(x in text_lower for x in ['admitted', 'accepted', 'rejected', 'waitlist']),
        'has_schools': any(x in text_lower for x in ['university', 'college', 'harvard', 'mit', 'stanford', 'yale', 'princeton', 'chicago', 'berkeley', 'columbia', 'nyu', 'penn']),
        'has_test_scores': any(x in text_lower for x in ['toefl', 'ielts']),
        'has_numbers': bool(re.search(r'\d+\.\d+', text))  # Decimal numbers (GPA scores)
    }
    
    return sum(signals.values())

df['signal_count'] = df['post_content'].apply(count_strong_signals)

# Require at least 2 strong signals
df = df[df['signal_count'] >= 2]
print(f"After 2+ signals: {len(df):,}")

# ===== FILTER 3: QUALITY SCORE =====
print("\n=== Filter 3: Quality Score (Keep High-Quality Posts) ===")

def calculate_quality_score(text):
    """Calculate quality score based on content richness"""
    text_lower = str(text).lower()
    score = 0
    
    # Length score (sweet spot is 200-2000 chars)
    length = len(text)
    if 200 <= length <= 2000:
        score += 3
    elif 100 <= length <= 200:
        score += 1
    
    # Strong keyword diversity
    strong_keywords = ['gpa', 'gre', 'admitted', 'accepted', 'rejected', 'toefl', 'ielts', 'waitlist']
    score += sum(1 for kw in strong_keywords if kw in text_lower)
    
    # Has specific numerical data
    if re.search(r'\d+\.\d+', text):  # GPA-like numbers
        score += 2
    if re.search(r'\d{3}', text):  # GRE-like numbers
        score += 2
    
    # Has school names
    top_schools = ['harvard', 'mit', 'stanford', 'yale', 'princeton', 'chicago', 
                   'berkeley', 'columbia', 'nyu', 'penn', 'northwestern', 'duke',
                   'michigan', 'ucla', 'ucsd', 'wisconsin', 'minnesota', 'texas']
    score += sum(1 for school in top_schools if school in text_lower)
    
    # Has structured information
    structure_words = ['undergrad', 'graduate', 'masters', 'research', 'publication', 
                       'letter', 'recommendation', 'major', 'minor', 'degree']
    score += sum(1 for word in structure_words if word in text_lower)
    
    return score

df['quality_score'] = df['post_content'].apply(calculate_quality_score)

# Keep posts with quality score >= 5
df = df[df['quality_score'] >= 5]
print(f"After quality filter (score >= 5): {len(df):,}")

# ===== FILTER 4: REMOVE QUESTION-ONLY POSTS =====
print("\n=== Filter 4: Remove Question-Only Posts ===")

def is_question_only(text):
    """Identify posts that are only asking questions (not sharing profiles)"""
    text_lower = str(text).lower()
    
    # Count question markers
    question_count = text.count('?')
    
    # Short posts with many questions are likely not profile posts
    if len(text) < 300 and question_count >= 2:
        return True
    
    # Posts starting with question words and having few strong keywords
    question_starts = ['what', 'how', 'where', 'when', 'who', 'which', 'should i', 'can i', 'is it', 'does anyone', 'do you']
    if any(text_lower.strip().startswith(q) for q in question_starts):
        # If starts with question AND has low keyword count, likely just a question
        keyword_count = sum(1 for kw in ['gpa', 'gre', 'admitted', 'accepted', 'rejected'] if kw in text_lower)
        if keyword_count < 2:
            return True
    
    return False

df = df[~df['post_content'].apply(is_question_only)]
print(f"After removing question-only: {len(df):,}")

# ===== FILTER 5: REMOVE GENERIC SHORT RESPONSES =====
print("\n=== Filter 5: Remove Generic Responses ===")

def is_generic_response(text):
    """Remove posts that are just generic thanks/congrats"""
    text_lower = str(text).lower().strip()
    
    if len(text) < 200:
        generic_patterns = [
            'thanks', 'thank you', 'congrats', 'congratulations',
            'good luck', 'best of luck', 'cool', 'nice',
            'awesome', 'great', 'wow', 'lol', 'haha'
        ]
        
        words = text_lower.split()
        if len(words) == 0:
            return True
            
        generic_count = sum(1 for word in words if any(pattern in word for pattern in generic_patterns))
        
        # If more than 30% of words are generic
        if generic_count / len(words) > 0.3:
            return True
    
    return False

df = df[~df['post_content'].apply(is_generic_response)]
print(f"After removing generic responses: {len(df):,}")

# ===== FILTER 6: OFF-TOPIC THREADS =====
print("\n=== Filter 6: Off-Topic Threads ===")

exclude_keywords = [
    'toefl only', 'ielts only', 'visa only',
    'housing', 'apartment', 'roommate',
    'for sale', 'naplex', 'fpgee', 'pharmacy',
    'tutoring', 'internship only', 'job posting',
    'selling', 'buying', 'rent', 'sublet'
]

def is_offtopic(title):
    title_lower = str(title).lower()
    return any(keyword in title_lower for keyword in exclude_keywords)

df = df[~df['thread_title'].apply(is_offtopic)]
print(f"After off-topic removal: {len(df):,}")

# ===== FILTER 7: DUPLICATES =====
print("\n=== Filter 7: Remove Duplicates ===")
df = df.drop_duplicates(subset=['post_content'], keep='first')
print(f"After duplicate removal: {len(df):,}")

# ===== FILTER 8: REMOVE NEAR-DUPLICATES =====
print("\n=== Filter 8: Remove Near-Duplicates ===")

def get_signature(text):
    """Get signature for detecting near-duplicates"""
    text_lower = str(text).lower()
    
    # Extract numbers and key schools
    numbers = sorted(re.findall(r'\b\d+\.?\d*\b', text_lower))
    
    schools = []
    school_list = ['harvard', 'mit', 'stanford', 'yale', 'princeton', 'chicago', 
                   'berkeley', 'columbia', 'nyu', 'penn']
    for school in school_list:
        if school in text_lower:
            schools.append(school)
    
    return '|'.join(numbers[:10]) + '||' + '|'.join(sorted(schools))

df['signature'] = df['post_content'].apply(get_signature)
df = df.drop_duplicates(subset=['signature'], keep='first')
print(f"After near-duplicate removal: {len(df):,}")

# ===== OPTIONAL: CAP AT 30K IF NEEDED =====
target_max = 30000

if len(df) > target_max:
    print(f"\n=== Optional: Sampling to {target_max:,} posts ===")
    print(f"Current: {len(df):,} posts")
    # Take highest quality posts
    df = df.nlargest(target_max, 'quality_score')
    print(f"After sampling (keeping highest quality): {len(df):,}")

# Clean data
df_clean = df[['id', 'thread_title', 'thread_url', 'author', 'page', 'post_content', 'scraped_at']].copy()

# ===== CREATE/OVERWRITE filtered_posts TABLE =====
print("\n" + "="*70)
print("CREATING/OVERWRITING filtered_posts TABLE")
print("="*70)

cursor = conn.cursor()

print("\nDropping existing filtered_posts table if it exists...")
cursor.execute("DROP TABLE IF EXISTS filtered_posts;")

print("Creating new filtered_posts table...")
cursor.execute("""
    CREATE TABLE filtered_posts (
        id INTEGER PRIMARY KEY,
        thread_title TEXT,
        thread_url TEXT,
        author VARCHAR(255),
        page INTEGER,
        post_content TEXT,
        scraped_at TIMESTAMP,
        filtered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE INDEX idx_filtered_thread ON filtered_posts(thread_title);
    CREATE INDEX idx_filtered_author ON filtered_posts(author);
    CREATE INDEX idx_filtered_content ON filtered_posts USING gin(to_tsvector('english', post_content));
""")
conn.commit()
print("✅ Table created")

# Insert data
print(f"\nInserting {len(df_clean):,} filtered posts...")
insert_query = """
    INSERT INTO filtered_posts (id, thread_title, thread_url, author, page, post_content, scraped_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
"""

data = [
    (int(row['id']), str(row['thread_title']), str(row['thread_url']),
     str(row['author']), int(row['page']), str(row['post_content']), row['scraped_at'])
    for _, row in df_clean.iterrows()
]

execute_batch(cursor, insert_query, data, page_size=1000)
conn.commit()
print("✅ Data inserted")

# Verify
cursor.execute("SELECT COUNT(*) FROM filtered_posts")
count = cursor.fetchone()[0]
print(f"\n✅ Verified: {count:,} posts in filtered_posts table")

# OVERWRITE CSV
print("\nOverwriting filtered_posts_for_gpt.csv...")
df_clean.to_csv('filtered_posts_for_gpt.csv', index=False)
print("✅ CSV saved")

# Cost and time estimates
print("\n" + "="*70)
print("SUMMARY")
print("="*70)
print(f"Original posts (forum_posts): 134,524")
print(f"Filtered posts (filtered_posts): {count:,}")
print(f"Reduction: {((134524 - count) / 134524 * 100):.1f}%")

# GPT cost calculation
avg_input_tokens = 333
avg_output_tokens = 200
input_cost = count * avg_input_tokens * 0.15 / 1_000_000
output_cost = count * avg_output_tokens * 0.60 / 1_000_000
total_cost = input_cost + output_cost

print(f"\nEstimated GPT extraction cost:")
print(f"  Input: ${input_cost:.2f}")
print(f"  Output: ${output_cost:.2f}")
print(f"  TOTAL: ${total_cost:.2f}")

# Time estimate with 5 workers
posts_per_minute = 150
time_minutes = count / posts_per_minute
time_hours = time_minutes / 60

print(f"\nEstimated processing time (5 workers):")
print(f"  ~{time_minutes:.0f} minutes (~{time_hours:.1f} hours)")

print("\n" + "="*70)
print("FILTERS APPLIED")
print("="*70)
print("✅ Post length: 100-5000 characters")
print("✅ Require 2+ strong signals (GPA, GRE, schools, etc.)")
print("✅ Quality score >= 5")
print("✅ Remove question-only posts")
print("✅ Remove generic responses")
print("✅ Remove off-topic threads")
print("✅ Remove exact duplicates")
print("✅ Remove near-duplicates")
print("✅ Optional: Cap at 30k (keep highest quality)")
print("="*70)

cursor.close()
conn.close()

print(f"\n✅ COMPLETE! Clean workflow:")
print(f"   forum_posts (134k) → filtered_posts ({count:,})")
print(f"\n📁 Output:")
print(f"   1. filtered_posts table ({count:,} rows)")
print(f"   2. filtered_posts_for_gpt.csv")
print(f"\n💰 Ready for GPT:")
print(f"   Cost: ${total_cost:.2f}")
print(f"   Time: ~{time_hours:.1f} hours")