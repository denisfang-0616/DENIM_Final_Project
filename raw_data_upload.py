import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
import os
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'port': int(os.getenv('DB_PORT')),
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

def upload_csv_to_postgres(csv_file):
    if not os.path.exists(csv_file):
        return
    
    try:
        df = pd.read_csv(csv_file, encoding='utf-8')
    except Exception:
        return
    
    conn = None
    cursor = None
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        insert_query = """
            INSERT INTO forum_posts (thread_title, thread_url, author, page, post_content)
            VALUES (%s, %s, %s, %s, %s)
        """
        
        data = [
            (
                str(row['thread_title']),
                str(row['thread_url']),
                str(row['author']),
                int(row['page']) if pd.notna(row['page']) else 1,
                str(row['post'])
            )
            for _, row in df.iterrows()
        ]
        
        batch_size = 1000
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            execute_batch(cursor, insert_query, batch, page_size=500)
            conn.commit()
            
    except Exception:
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    csv_file = "urch_forum_pages_1_to_717.csv"
