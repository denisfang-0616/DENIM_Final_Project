import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv
import os

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv('DB_HOST'),
    port=int(os.getenv('DB_PORT')),
    database=os.getenv('DB_NAME'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD')
)

print("Loading data from PostgreSQL...")
df = pd.read_sql("SELECT * FROM forum_posts", conn)
print(f"Initial posts: {len(df):,}\n")

# ===== FILTER 1: THREAD TITLE =====
print("=== Filter 1: Thread Title ===")
admissions_threads = [
    # Core admissions keywords
    'profile', 'evaluation', 'eval', 'result', 
    'admitted', 'accepted', 'chances', 'background', 
    'sweat', 'application', 'decision', 'admit',
    'admission', 'rejection', 'waiting', 'applicant',
    
    # Time-based
    'fall', 'spring', 'cycle',
    '2010', '2011', '2012', '2013', '2014', '2015',
    '2016', '2017', '2018', '2019', '2020', '2021',
    '2022', '2023', '2024', '2025',
    
    # Program types
    'phd', 'doctoral', 'masters', 'mba', 'ma ', 'ms ',
    
    # Fields
    'econ', 'economics', 'finance', 'business', 'are',
    
    # Help/Q&A
    'ask', 'question', 'advice', 'help', 'consultant'
]

df = df[df['thread_title'].str.contains('|'.join(admissions_threads), case=False, na=False)]
print(f"After thread filter: {len(df):,}")

# ===== FILTER 2: POST LENGTH =====
print("\n=== Filter 2: Post Length ===")
df['post_length'] = df['post_content'].str.len()
df = df[df['post_length'] >= 50]
print(f"After length filter: {len(df):,}")

# ===== FILTER 3: SMART KEYWORDS =====
print("\n=== Filter 3: Smart Keyword Filter ===")
def has_strong_signal(text):
    text_lower = str(text).lower()
    
    # Strong signals - any ONE is enough
    strong_keywords = [
        'gpa', 'gre', 'admitted', 'accepted', 'rejected', 
        'waitlist', 'admit', 'rejection', 'acceptance'
    ]
    if any(keyword in text_lower for keyword in strong_keywords):
        return True
    
    # General keywords - need 2+
    general_keywords = [
        'undergrad', 'graduate', 'university', 'publication', 
        'letter', 'recommendation', 'research', 'phd', 'math', 
        'econ', 'statistics', 'masters', 'doctoral',
        'quant', 'verbal', 'toefl', 'ielts'
    ]
    return sum(1 for keyword in general_keywords if keyword in text_lower) >= 2

df['has_signal'] = df['post_content'].apply(has_strong_signal)
df = df[df['has_signal']]
print(f"After keyword filter: {len(df):,}")

# ===== FILTER 4: DUPLICATES =====
print("\n=== Filter 4: Duplicates ===")
df = df.drop_duplicates(subset=['post_content'], keep='first')
print(f"After duplicates: {len(df):,}")

# ===== FILTER 5: OFF-TOPIC =====
print("\n=== Filter 5: Off-Topic Threads ===")
exclude_keywords = [
    'toefl only', 'ielts only', 'visa only',
    'housing', 'apartment', 'roommate',
    'for sale', 'naplex', 'fpgee', 'pharmacy',
    'tutoring'
]

def is_offtopic(title):
    title_lower = str(title).lower()
    return any(keyword in title_lower for keyword in exclude_keywords)

df = df[~df['thread_title'].apply(is_offtopic)]
print(f"After off-topic filter: {len(df):,}")

# Clean data
df_clean = df[['id', 'thread_title', 'thread_url', 'author', 'page', 'post_content', 'scraped_at']]

# ===== SAVE TO POSTGRESQL =====
print("\n" + "="*60)
print("SAVING TO POSTGRESQL")
print("="*60)

cursor = conn.cursor()

# Create new table for filtered posts
print("\nCreating filtered_posts table...")
cursor.execute("""
    DROP TABLE IF EXISTS filtered_posts;
    
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
""")
conn.commit()
print("✅ Table created")

# Insert filtered data
print(f"\nInserting {len(df_clean):,} filtered posts...")
insert_query = """
    INSERT INTO filtered_posts (id, thread_title, thread_url, author, page, post_content, scraped_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
"""

data = [
    (
        int(row['id']),
        str(row['thread_title']),
        str(row['thread_url']),
        str(row['author']),
        int(row['page']),
        str(row['post_content']),
        row['scraped_at']
    )
    for _, row in df_clean.iterrows()
]

execute_batch(cursor, insert_query, data, page_size=1000)
conn.commit()
print("✅ Data inserted")

# Verify
cursor.execute("SELECT COUNT(*) FROM filtered_posts")
count = cursor.fetchone()[0]
print(f"\n✅ Verified: {count:,} posts in filtered_posts table")

# Also save to CSV as backup
print("\nSaving backup CSV...")
df_clean.to_csv('filtered_posts_for_gpt.csv', index=False)
print("✅ CSV saved")

# Summary
print("\n" + "="*60)
print("SUMMARY")
print("="*60)
print(f"Original posts (forum_posts table): 134,524")
print(f"Filtered posts (filtered_posts table): {count:,}")
print(f"Reduction: {((134524 - count) / 134524 * 100):.1f}%")
print(f"Estimated GPT cost: ${(count * 333 * 0.125 / 1000000 + count * 175 * 1.000 / 1000000):.2f}")
print("="*60)

cursor.close()
conn.close()

print("\n✅ Complete! You now have:")
print("  1. forum_posts table (raw data - 134k posts)")
print("  2. filtered_posts table (filtered data - ready for GPT)")
print("  3. filtered_posts_for_gpt.csv (backup)")
