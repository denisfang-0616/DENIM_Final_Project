import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from openai import AsyncOpenAI
from dotenv import load_dotenv
import json
from datetime import datetime
import asyncio
from typing import List, Dict
import time

# Load environment variables
load_dotenv()

# Initialize async OpenAI client
client = AsyncOpenAI(api_key=os.getenv('OPENAI_API_KEY'))

# Database connection params
db_params = {
    'host': os.getenv('DB_HOST'),
    'port': int(os.getenv('DB_PORT')),
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD')
}

SYSTEM_PROMPT = """You are an expert at extracting structured admissions data from PhD economics forum posts.
Extract the following fields from each post. If a field is not mentioned, use null. Try your best to place most relevant information in fields, the goal is to fill as much as possible while being correct.
Return ONLY valid JSON with this exact structure:
{
  "undergrad_gpa": float or null,
  "undergrad_gpa_out_of": float or null,
  "grad_gpa": float or null,
  "grad_gpa_out_of": float or null,
  "gre_quant": int (130-170) or null,
  "gre_verbal": int (130-170) or null,
  "gre_writing": float (0-6) or null,
  "undergrad_institution": string or null,
  "grad_institution": string or null,
  "undergrad_major": string or null,
  "grad_major": string or null,
  "math_courses": array of strings or [],
  "phd_course_taken": boolean,
  "research_experience": boolean or null,
  "publications": int or null,
  "work_experience_years": int or null,
  "letters_of_rec": string or null,
  "schools_applied": array of strings or [],
  "schools_accepted": array of strings or [],
  "schools_rejected": array of strings or [],
  "schools_waitlisted": array of strings or [],
  "funding_status": string or null
}
Rules:
- GPA can be on any scale, you can use your judgement here. Always include the "undergrad_gpa_out_of"/"grad_gpa_out_of" when including "undergrad_gpa"/"grad_gpa". If the gpa is less than 4 it is likely on the 4.0 scale and if above it is likely on another scale. Again use best context clues.
- GRE Quant/Verbal must be between 130-170.
- Parse school names carefully (MIT, Harvard, Stanford, etc.). Make sure to have consistent naming for convenience. Example: Harvard and Harvard University are the same school and should be classified the same. But Boston University and Boston College are two different universities.
- Distinguish between acceptance with/without funding
- If multiple GPAs mentioned, put each one in relevant fields.
- Extract math course names from mentions like "took Real Analysis", "took Linear Algebra", "Took Calculus 1/2/3". Be lenient and try to extract wherever you think it does fit.
- Count publications if mentioned
- Return ONLY the JSON object, no other text"""


async def extract_single_post(post_id: int, post_content: str, semaphore: asyncio.Semaphore, retry_count: int = 0) -> Dict:
    """Extract data from a single post with rate limiting and retry logic"""
    async with semaphore:
        try:
            response = await client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": f"Extract admissions data from this post:\n\n{post_content}"}
                ],
                temperature=0,
                max_completion_tokens=800
            )
            
            extracted_text = response.choices[0].message.content.strip()
            
            # Strip Markdown
            if extracted_text.startswith("```json"):
                extracted_text = extracted_text.replace("```json", "").replace("```", "").strip()
            elif extracted_text.startswith("```"):
                extracted_text = extracted_text.strip("`").strip()
            
            # Handle incomplete JSON - find last complete brace
            if not extracted_text.endswith('}'):
                last_brace = extracted_text.rfind('}')
                if last_brace > 0:
                    extracted_text = extracted_text[:last_brace+1]
            
            # Parse JSON
            extracted_data = json.loads(extracted_text)
            extracted_data["original_post_id"] = post_id
            
            # Cost calculation
            prompt_tokens = response.usage.prompt_tokens
            completion_tokens = response.usage.completion_tokens
            cost = (prompt_tokens * 0.15 / 1_000_000) + (completion_tokens * 0.60 / 1_000_000)
            
            return {
                "success": True,
                "data": extracted_data,
                "cost": cost
            }
            
        except json.JSONDecodeError:
            return {
                "success": False,
                "post_id": post_id,
                "error": "JSON_PARSE_FAIL",
                "raw_response": extracted_text[:300] if 'extracted_text' in locals() else 'N/A'
            }
        except Exception as e:
            error_str = str(e)
            
            # Handle rate limiting with exponential backoff
            if '429' in error_str and retry_count < 3:
                wait_time = (2 ** retry_count) * 3  # 3s, 6s, 12s
                print(f"    ⏳ Rate limit - waiting {wait_time}s (retry {retry_count+1}/3)...")
                await asyncio.sleep(wait_time)
                return await extract_single_post(post_id, post_content, semaphore, retry_count + 1)
            
            return {
                "success": False,
                "post_id": post_id,
                "error": str(e)
            }


async def process_batch(posts_df: pd.DataFrame, max_concurrent: int = 7) -> tuple:
    """Process batch with controlled concurrency (7 workers - ultra-safe)"""
    semaphore = asyncio.Semaphore(max_concurrent)
    
    tasks = [
        extract_single_post(row["id"], row["post_content"], semaphore)
        for _, row in posts_df.iterrows()
    ]
    
    results = await asyncio.gather(*tasks)
    
    successes = [r["data"] for r in results if r["success"]]
    errors = [{"post_id": r["post_id"], "error": r["error"]} for r in results if not r["success"]]
    total_cost = sum(r.get("cost", 0) for r in results if r["success"])
    
    return successes, errors, total_cost


def save_to_database(results: List[Dict], conn):
    """Save results to database"""
    if not results:
        return
        
    cursor = conn.cursor()
    insert_query = """
        INSERT INTO admissions_data (
            original_post_id, undergrad_gpa, undergrad_gpa_out_of, grad_gpa, grad_gpa_out_of,
            gre_quant, gre_verbal, gre_writing,
            undergrad_institution, grad_institution, undergrad_major, grad_major,
            math_courses, phd_course_taken, research_experience, publications,
            work_experience_years, letters_of_rec,
            schools_applied, schools_accepted, schools_rejected, schools_waitlisted,
            funding_status
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
    """
    data = [
        (
            r.get("original_post_id"),
            r.get("undergrad_gpa"),
            r.get("undergrad_gpa_out_of"),
            r.get("grad_gpa"),
            r.get("grad_gpa_out_of"),
            r.get("gre_quant"),
            r.get("gre_verbal"),
            r.get("gre_writing"),
            r.get("undergrad_institution"),
            r.get("grad_institution"),
            r.get("undergrad_major"),
            r.get("grad_major"),
            r.get("math_courses", []),
            r.get("phd_course_taken"),
            r.get("research_experience"),
            r.get("publications"),
            r.get("work_experience_years"),
            r.get("letters_of_rec"),
            r.get("schools_applied", []),
            r.get("schools_accepted", []),
            r.get("schools_rejected", []),
            r.get("schools_waitlisted", []),
            r.get("funding_status")
        )
        for r in results
    ]
    execute_batch(cursor, insert_query, data, page_size=200)
    conn.commit()
    cursor.close()


async def main():
    start_time = time.time()
    
    print("="*70)
    print("GPT EXTRACTION - 7 CONCURRENT WORKERS (ULTRA-SAFE)")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)
    
    # Load posts
    print("\nLoading posts from filtered_posts...")
    conn = psycopg2.connect(**db_params)
    df_all_posts = pd.read_sql("SELECT id, post_content FROM filtered_posts", conn)
    print(f"✅ Loaded {len(df_all_posts):,} posts\n")
    
    # Create table
    print("Creating admissions_data table...")
    cursor = conn.cursor()
    cursor.execute("""
        DROP TABLE IF EXISTS admissions_data;
        CREATE TABLE admissions_data (
            id SERIAL PRIMARY KEY,
            original_post_id INTEGER,
            undergrad_gpa DECIMAL(4,2),
            undergrad_gpa_out_of DECIMAL(4,1),
            grad_gpa DECIMAL(4,2),
            grad_gpa_out_of DECIMAL(4,1),
            gre_quant INTEGER,
            gre_verbal INTEGER,
            gre_writing DECIMAL(2,1),
            undergrad_institution TEXT,
            grad_institution TEXT,
            undergrad_major TEXT,
            grad_major TEXT,
            math_courses TEXT[],
            phd_course_taken BOOLEAN,
            research_experience BOOLEAN,
            publications INTEGER,
            work_experience_years INTEGER,
            letters_of_rec TEXT,
            schools_applied TEXT[],
            schools_accepted TEXT[],
            schools_rejected TEXT[],
            schools_waitlisted TEXT[],
            funding_status TEXT,
            extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX idx_post_id ON admissions_data(original_post_id);
    """)
    conn.commit()
    cursor.close()
    print("✅ Table created\n")
    
    print(f"Processing {len(df_all_posts):,} posts with 7 concurrent workers...")
    print(f"Estimated time: ~{(len(df_all_posts) / 100):.0f} minutes (~{(len(df_all_posts) / 100 / 60):.1f} hours)")
    print(f"Estimated cost: ~${0.0003 * len(df_all_posts):.2f}\n")
    
    chunk_size = 100
    total_chunks = (len(df_all_posts) + chunk_size - 1) // chunk_size
    
    all_results = []
    all_errors = []
    total_cost = 0
    
    for i in range(0, len(df_all_posts), chunk_size):
        chunk = df_all_posts.iloc[i:i+chunk_size]
        chunk_num = i // chunk_size + 1
        
        chunk_start = time.time()
        print(f"[{chunk_num}/{total_chunks}] Processing posts {i+1}–{i+len(chunk)}...")
        
        results, errors, cost = await process_batch(chunk, max_concurrent=7)
        
        chunk_time = time.time() - chunk_start
        total_cost += cost
        
        if results:
            save_to_database(results, conn)
        
        all_results.extend(results)
        all_errors.extend(errors)
        
        # Calculate ETA
        elapsed = time.time() - start_time
        posts_processed = i + len(chunk)
        posts_remaining = len(df_all_posts) - posts_processed
        if posts_processed > 0:
            rate = posts_processed / elapsed
            eta_seconds = posts_remaining / rate if rate > 0 else 0
            eta_minutes = eta_seconds / 60
        else:
            eta_minutes = 0
        
        progress = (posts_processed / len(df_all_posts)) * 100
        print(f"  ✅ {progress:.1f}% | Errors: {len(errors)} | Cost: ${total_cost:.2f} | ETA: {eta_minutes:.0f}m")
        
        # Longer delay between chunks to avoid rate limits
        if chunk_num < total_chunks:
            await asyncio.sleep(3)
        print()
    
    # Final results
    elapsed_total = time.time() - start_time
    
    print("\n" + "="*70)
    print("EXTRACTION COMPLETE - SAVING RESULTS")
    print("="*70)
    
    df_results = pd.read_sql("SELECT * FROM admissions_data ORDER BY id", conn)
    
    # Save CSV
    csv_filename = f"admissions_data_extracted_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    df_results.to_csv(csv_filename, index=False)
    print(f"✅ CSV saved: {csv_filename}")
    
    # Save errors
    if all_errors:
        error_file = f"extraction_errors_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        pd.DataFrame(all_errors).to_csv(error_file, index=False)
        print(f"⚠️  Errors saved: {error_file}")
    
    # Quality metrics
    print("\n" + "="*70)
    print("EXTRACTION STATS")
    print("="*70)
    print(f"Total time: {elapsed_total/60:.1f} minutes ({elapsed_total/3600:.2f} hours)")
    print(f"Total cost: ${total_cost:.2f}")
    print(f"Posts processed: {len(df_results):,}")
    print(f"Successful: {len(all_results):,}")
    print(f"Errors: {len(all_errors):,}")
    print(f"Success rate: {(len(all_results)/len(df_all_posts))*100:.1f}%")
    print(f"Avg rate: {len(df_all_posts)/(elapsed_total/60):.0f} posts/minute")
    
    print(f"\nField completeness:")
    print(f"  Undergrad GPA: {df_results['undergrad_gpa'].notna().sum():,} ({df_results['undergrad_gpa'].notna().sum()/len(df_results)*100:.1f}%)")
    print(f"  Grad GPA: {df_results['grad_gpa'].notna().sum():,} ({df_results['grad_gpa'].notna().sum()/len(df_results)*100:.1f}%)")
    print(f"  GRE Quant: {df_results['gre_quant'].notna().sum():,} ({df_results['gre_quant'].notna().sum()/len(df_results)*100:.1f}%)")
    print(f"  GRE Verbal: {df_results['gre_verbal'].notna().sum():,} ({df_results['gre_verbal'].notna().sum()/len(df_results)*100:.1f}%)")
    print(f"  Undergrad institution: {df_results['undergrad_institution'].notna().sum():,} ({df_results['undergrad_institution'].notna().sum()/len(df_results)*100:.1f}%)")
    print(f"  Math courses: {df_results['math_courses'].apply(lambda x: len(x) if x else 0).sum():,} total")
    print(f"  Schools accepted: {df_results['schools_accepted'].apply(lambda x: len(x) if x else 0).sum():,} total")
    print(f"  Schools rejected: {df_results['schools_rejected'].apply(lambda x: len(x) if x else 0).sum():,} total")
    
    # Sample data
    print("\n" + "="*70)
    print("SAMPLE EXTRACTED DATA (First 5 with GPA)")
    print("="*70)
    sample = df_results[df_results['undergrad_gpa'].notna()].head(5)
    if len(sample) > 0:
        print(sample[['original_post_id', 'undergrad_gpa', 'undergrad_gpa_out_of', 
                      'gre_quant', 'gre_verbal', 'undergrad_institution']])
    
    print("\n" + "="*70)
    print("NEXT STEPS")
    print("="*70)
    print("1. Review extraction quality in DBeaver:")
    print("   SELECT * FROM admissions_data LIMIT 100;")
    print("\n2. Filter to posts with actual data:")
    print("   SELECT * FROM admissions_data")
    print("   WHERE undergrad_gpa IS NOT NULL")
    print("      OR gre_quant IS NOT NULL")
    print("      OR array_length(schools_accepted, 1) > 0;")
    print("\n3. Build Streamlit dashboard for analysis")
    print("4. Train ML models for admission prediction")
    print("="*70)
    
    conn.close()
    print(f"\n✅ Complete! {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    asyncio.run(main())