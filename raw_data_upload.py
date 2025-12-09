import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database configuration from .env
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'port': int(os.getenv('DB_PORT')),
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

def upload_csv_to_postgres(csv_file):
    """
    Upload CSV file to PostgreSQL database.
    This script is fully reproducible - anyone can run it with their own .env file.
    """
    print(f"\n{'='*70}")
    print(f"CSV TO POSTGRESQL UPLOAD")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*70}\n")
    
    # Validate environment variables
    if not all([DB_CONFIG['host'], DB_CONFIG['database'], DB_CONFIG['user'], DB_CONFIG['password']]):
        print("❌ Error: Missing database configuration in .env file")
        print("\nMake sure .env file exists with:")
        print("  DB_HOST=your_host")
        print("  DB_PORT=5432")
        print("  DB_NAME=urch_forum")
        print("  DB_USER=postgres")
        print("  DB_PASSWORD=your_password")
        return False
    
    # Check if CSV file exists
    if not os.path.exists(csv_file):
        print(f"❌ Error: File '{csv_file}' not found!")
        print(f"   Current directory: {os.getcwd()}")
        return False
    
    # Get file info
    file_size_mb = os.path.getsize(csv_file) / (1024 * 1024)
    print(f"📁 File: {csv_file}")
    print(f"📊 Size: {file_size_mb:.2f} MB")
    
    # Load CSV
    print("\n⏳ Loading CSV file...")
    try:
        df = pd.read_csv(csv_file, encoding='utf-8')
        print(f"✅ Loaded {len(df):,} rows with {len(df.columns)} columns")
        print(f"   Columns: {list(df.columns)}")
        
        # Verify expected columns
        expected_cols = ['thread_title', 'thread_url', 'author', 'page', 'post']
        if not all(col in df.columns for col in expected_cols):
            print(f"⚠️  Warning: CSV columns don't match expected format")
            print(f"   Expected: {expected_cols}")
            print(f"   Got: {list(df.columns)}")
        
        # Show sample
        print(f"\n   Sample row:")
        print(f"   Thread: {df.iloc[0]['thread_title'][:60]}...")
        print(f"   Author: {df.iloc[0]['author']}")
        print(f"   Post length: {len(str(df.iloc[0]['post']))} characters")
        
    except Exception as e:
        print(f"❌ Error loading CSV: {e}")
        return False
    
    # Connect to database
    print("\n⏳ Connecting to PostgreSQL database...")
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        print(f"✅ Connected to database at {DB_CONFIG['host']}")
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        print("\nTroubleshooting:")
        print("  1. Check .env file has correct credentials")
        print("  2. Verify instance is running in GCP")
        print("  3. Ensure 0.0.0.0/0 is in authorized networks")
        print("  4. Confirm you're not on VPN")
        return False
    
    # Check if table exists
    print("\n⏳ Checking if forum_posts table exists...")
    try:
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'forum_posts'
            );
        """)
        table_exists = cursor.fetchone()[0]
        
        if not table_exists:
            print("❌ Table 'forum_posts' does not exist!")
            print("   Run the CREATE TABLE statement in DBeaver first.")
            cursor.close()
            conn.close()
            return False
        
        # Check current row count
        cursor.execute("SELECT COUNT(*) FROM forum_posts")
        existing_rows = cursor.fetchone()[0]
        print(f"✅ Table exists with {existing_rows:,} existing rows")
        
    except Exception as e:
        print(f"❌ Error checking table: {e}")
        cursor.close()
        conn.close()
        return False
    
    # Upload to database
    print("\n⏳ Uploading data to PostgreSQL...")
    print("   (This may take several minutes for large files...)")
    
    start_upload = datetime.now()
    
    try:
        # Prepare data for batch insert
        insert_query = """
            INSERT INTO forum_posts (thread_title, thread_url, author, page, post_content)
            VALUES (%s, %s, %s, %s, %s)
        """
        
        # Convert dataframe to list of tuples
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
        
        # Batch insert for better performance
        batch_size = 1000
        total_batches = (len(data) // batch_size) + 1
        
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            execute_batch(cursor, insert_query, batch, page_size=500)
            
            # Commit after each batch
            conn.commit()
            
            # Progress update
            progress = min(((i + batch_size) / len(data)) * 100, 100)
            rows_done = min(i + batch_size, len(data))
            elapsed = (datetime.now() - start_upload).total_seconds()
            rate = rows_done / elapsed if elapsed > 0 else 0
            
            print(f"   Progress: {progress:.1f}% ({rows_done:,}/{len(data):,} rows) "
                  f"[{rate:.0f} rows/sec]", end='\r')
        
        upload_duration = (datetime.now() - start_upload).total_seconds()
        print(f"\n✅ Successfully uploaded {len(data):,} rows in {upload_duration:.1f} seconds")
        
    except Exception as e:
        print(f"\n❌ Upload failed: {e}")
        conn.rollback()
        cursor.close()
        conn.close()
        return False
    
    # Verify upload
    print("\n⏳ Verifying upload...")
    try:
        cursor.execute("SELECT COUNT(*) FROM forum_posts")
        total_count = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT thread_title, author, LEFT(post_content, 50) 
            FROM forum_posts 
            ORDER BY id DESC
            LIMIT 5
        """)
        sample = cursor.fetchall()
        
        print(f"✅ Verification complete")
        print(f"   Total rows in database: {total_count:,}")
        print(f"   New rows added: {total_count - existing_rows:,}")
        print(f"\n   Latest entries:")
        for i, (title, author, content) in enumerate(sample, 1):
            print(f"   {i}. {title[:50]}... (by {author})")
        
    except Exception as e:
        print(f"⚠️  Verification failed: {e}")
    
    # Cleanup
    cursor.close()
    conn.close()
    
    print(f"\n{'='*70}")
    print(f"UPLOAD COMPLETED SUCCESSFULLY")
    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*70}\n")
    
    return True

if __name__ == "__main__":
    # Configuration
    csv_file = "urch_forum_pages_1_to_717.csv"
    
    # Run upload
    success = upload_csv_to_postgres(csv_file)
    
    if success:
        print("✅ Ready for next steps:")
        print("   1. Run GPT extraction to create cleaned dataset")
        print("   2. Build Streamlit dashboard")
        print("   3. Train ML models")
    else:
        print("❌ Upload failed. Fix errors above and try again.")