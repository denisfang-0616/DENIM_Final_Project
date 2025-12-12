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
df = pd.read_sql("SELECT * FROM forum_posts", conn)

df['post_length'] = df['post_content'].str.len()
df = df[(df['post_length'] >= 100) & (df['post_length'] <= 5000)] #discarding very small and very large posts


def count_strong_signals(text):
    """Counting number of strong admissions signals"""
    text_lower = str(text).lower()
    
    signals = {
        'has_gpa': any(x in text_lower for x in ['gpa', 'grade point']),
        'has_gre': any(x in text_lower for x in ['gre', 'quant', 'verbal']),
        'has_admission': any(x in text_lower for x in ['admitted', 'accepted', 'rejected', 'waitlist']),
        'has_schools': any(x in text_lower for x in ['university', 'college', 'harvard', 'mit', 'stanford', 'yale', 'princeton', 'chicago', 'berkeley', 'columbia', 'nyu', 'penn']),
        'has_test_scores': any(x in text_lower for x in ['toefl', 'ielts']),
        'has_numbers': bool(re.search(r'\d+\.\d+', text))  
    }
    
    return sum(signals.values())

df['signal_count'] = df['post_content'].apply(count_strong_signals) # Require 2 strong signals
df = df[df['signal_count'] >= 2]

def calculate_quality_score(text):
    """Calculating post quality to drop low quality posts"""
    text_lower = str(text).lower()
    score = 0
    
    length = len(text)
    if 200 <= length <= 2000:
        score += 3
    elif 100 <= length <= 200:
        score += 1
    
    strong_keywords = ['gpa', 'gre', 'admitted', 'accepted', 'rejected', 'toefl', 'ielts', 'waitlist']
    score += sum(1 for kw in strong_keywords if kw in text_lower)
    
    if re.search(r'\d+\.\d+', text):  
        score += 2
    if re.search(r'\d{3}', text):  
        score += 2
    
    top_schools = ['harvard', 'mit', 'stanford', 'yale', 'princeton', 'chicago', 
                   'berkeley', 'columbia', 'nyu', 'penn', 'northwestern', 'duke',
                   'michigan', 'ucla', 'ucsd', 'wisconsin', 'minnesota', 'texas']
    score += sum(1 for school in top_schools if school in text_lower)

    structure_words = ['undergrad', 'graduate', 'masters', 'research', 'publication', 
                       'letter', 'recommendation', 'major', 'minor', 'degree']
    score += sum(1 for word in structure_words if word in text_lower)
    
    return score

df['quality_score'] = df['post_content'].apply(calculate_quality_score)
df = df[df['quality_score'] >= 5] # quality filter

def is_question_only(text):
    """Removing posts that are just questions"""
    text_lower = str(text).lower()
    question_count = text.count('?')
    
    if len(text) < 300 and question_count >= 2:
        return True
    
    question_starts = ['what', 'how', 'where', 'when', 'who', 'which', 'should i', 'can i', 'is it', 'does anyone', 'do you']
    if any(text_lower.strip().startswith(q) for q in question_starts):
        keyword_count = sum(1 for kw in ['gpa', 'gre', 'admitted', 'accepted', 'rejected'] if kw in text_lower)
        if keyword_count < 2:
            return True
    
    return False

df = df[~df['post_content'].apply(is_question_only)]

def is_generic_response(text):
    """Filtering out generic posts"""
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
        
        if generic_count / len(words) > 0.3:
            return True
    
    return False

df = df[~df['post_content'].apply(is_generic_response)]

exclude_keywords = [
    'toefl only', 'ielts only', 'visa only',
    'housing', 'apartment', 'roommate',
    'for sale', 'naplex', 'fpgee', 'pharmacy',
    'tutoring', 'internship only', 'job posting',
    'selling', 'buying', 'rent', 'sublet'
]

def is_offtopic(title):
    """Removing off-topic threads"""
    title_lower = str(title).lower()
    return any(keyword in title_lower for keyword in exclude_keywords)

df = df[~df['thread_title'].apply(is_offtopic)]

df = df.drop_duplicates(subset=['post_content'], keep='first')

def get_signature(text):
    """Finding and removing duplicate posts"""
    text_lower = str(text).lower()
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

target_max = 30000
if len(df) > target_max:
    df = df.nlargest(target_max, 'quality_score')

df_clean = df[['id', 'thread_title', 'thread_url', 'author', 'page', 'post_content', 'scraped_at']].copy()

cursor = conn.cursor()

cursor.execute("DROP TABLE IF EXISTS filtered_posts;")

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


df_clean.to_csv('filtered_posts_for_gpt.csv', index=False)


cursor.close()
conn.close()

