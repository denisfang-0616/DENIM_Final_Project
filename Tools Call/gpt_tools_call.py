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
            
            # CLAMP gre_writing to valid range (0-6)
            if extracted_data.get("gre_writing") is not None:
                try:
                    gre_writing = float(extracted_data["gre_writing"])
                    if gre_writing > 6.0:
                        extracted_data["gre_writing"] = 6.0
                    elif gre_writing < 0:
                        extracted_data["gre_writing"] = None
                except (ValueError, TypeError):
                    extracted_data["gre_writing"] = None
            
            # CLAMP GPAs to reasonable range (0-100)
            for gpa_field in ['undergrad_gpa', 'grad_gpa']:
                if extracted_data.get(gpa_field) is not None:
                    try:
                        gpa_val = float(extracted_data[gpa_field])
                        if gpa_val > 100.0 or gpa_val < 0:
                            extracted_data[gpa_field] = None
                    except (ValueError, TypeError):
                        extracted_data[gpa_field] = None
            
            # CLAMP GPA out_of fields
            for gpa_out_field in ['undergrad_gpa_out_of', 'grad_gpa_out_of']:
                if extracted_data.get(gpa_out_field) is not None:
                    try:
                        gpa_out = float(extracted_data[gpa_out_field])
                        if gpa_out > 100.0 or gpa_out < 0:
                            extracted_data[gpa_out_field] = None
                    except (ValueError, TypeError):
                        extracted_data[gpa_out_field] = None
            
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
                await asyncio.sleep(wait_time)
                return await extract_single_post(post_id, post_content, semaphore, retry_count + 1)
            
            return {
                "success": False,
                "post_id": post_id,
                "error": str(e)
            }


async def process_batch(posts_df: pd.DataFrame, max_concurrent: int = 10) -> tuple:
    """Process batch with controlled concurrency"""
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


def sanitize_value(value, field_name):
    """Convert unexpected types to database-safe values"""
    # Arrays are fine for array fields
    if field_name in ['math_courses', 'schools_applied', 'schools_accepted', 'schools_rejected', 'schools_waitlisted']:
        if isinstance(value, list):
            # Ensure all items in list are strings, not dicts
            clean_list = []
            for item in value:
                if isinstance(item, dict):
                    if 'name' in item:
                        clean_list.append(str(item['name']))
                    elif item:
                        clean_list.append(str(list(item.values())[0]))
                else:
                    clean_list.append(str(item))
            return clean_list
        elif value is None:
            return []
        elif isinstance(value, dict):
            if 'name' in value:
                return [str(value['name'])]
            elif value:
                return [str(list(value.values())[0])]
            else:
                return []
        else:
            return [str(value)]
    
    # For other fields, flatten dicts/lists to strings
    if isinstance(value, dict):
        if 'name' in value:
            return str(value['name'])
        elif 'value' in value:
            return str(value['value'])
        elif value:
            return str(list(value.values())[0])
        else:
            return None
    elif isinstance(value, list) and value:
        str_items = []
        for item in value:
            if isinstance(item, dict):
                if 'name' in item:
                    str_items.append(str(item['name']))
                elif item:
                    str_items.append(str(list(item.values())[0]))
            else:
                str_items.append(str(item))
        return ', '.join(str_items) if str_items else None
    elif isinstance(value, list) and not value:
        return None
    else:
        return value


def save_to_database(results: List[Dict], conn):
    """Save results to database with sanitization"""
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
            sanitize_value(r.get("original_post_id"), "original_post_id"),
            sanitize_value(r.get("undergrad_gpa"), "undergrad_gpa"),
            sanitize_value(r.get("undergrad_gpa_out_of"), "undergrad_gpa_out_of"),
            sanitize_value(r.get("grad_gpa"), "grad_gpa"),
            sanitize_value(r.get("grad_gpa_out_of"), "grad_gpa_out_of"),
            sanitize_value(r.get("gre_quant"), "gre_quant"),
            sanitize_value(r.get("gre_verbal"), "gre_verbal"),
            sanitize_value(r.get("gre_writing"), "gre_writing"),
            sanitize_value(r.get("undergrad_institution"), "undergrad_institution"),
            sanitize_value(r.get("grad_institution"), "grad_institution"),
            sanitize_value(r.get("undergrad_major"), "undergrad_major"),
            sanitize_value(r.get("grad_major"), "grad_major"),
            sanitize_value(r.get("math_courses", []), "math_courses"),
            sanitize_value(r.get("phd_course_taken"), "phd_course_taken"),
            sanitize_value(r.get("research_experience"), "research_experience"),
            sanitize_value(r.get("publications"), "publications"),
            sanitize_value(r.get("work_experience_years"), "work_experience_years"),
            sanitize_value(r.get("letters_of_rec"), "letters_of_rec"),
            sanitize_value(r.get("schools_applied", []), "schools_applied"),
            sanitize_value(r.get("schools_accepted", []), "schools_accepted"),
            sanitize_value(r.get("schools_rejected", []), "schools_rejected"),
            sanitize_value(r.get("schools_waitlisted", []), "schools_waitlisted"),
            sanitize_value(r.get("funding_status"), "funding_status")
        )
        for r in results
    ]
    execute_batch(cursor, insert_query, data, page_size=200)
    conn.commit()
    cursor.close()


async def main():
    start_time = time.time()
    
    # Load posts
    conn = psycopg2.connect(**db_params)
    df_all_posts = pd.read_sql("SELECT id, post_content FROM filtered_posts", conn)
    
    # Create table with generous field sizes to prevent overflow
    cursor = conn.cursor()
    cursor.execute("""
        DROP TABLE IF EXISTS admissions_data;
        CREATE TABLE admissions_data (
            id SERIAL PRIMARY KEY,
            original_post_id INTEGER,
            undergrad_gpa DECIMAL(5,2),
            undergrad_gpa_out_of DECIMAL(5,1),
            grad_gpa DECIMAL(5,2),
            grad_gpa_out_of DECIMAL(5,1),
            gre_quant INTEGER,
            gre_verbal INTEGER,
            gre_writing DECIMAL(3,1),
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
    
    
    chunk_size = 100
    total_chunks = (len(df_all_posts) + chunk_size - 1) // chunk_size
    
    all_results = []
    all_errors = []
    total_cost = 0
    
    for i in range(0, len(df_all_posts), chunk_size):
        chunk = df_all_posts.iloc[i:i+chunk_size]
        chunk_num = i // chunk_size + 1
        
        chunk_start = time.time()
        
        results, errors, cost = await process_batch(chunk, max_concurrent=10)
        
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
        
        # Small delay between chunks
        if chunk_num < total_chunks:
            await asyncio.sleep(2)
    
    # Final results
    elapsed_total = time.time() - start_time
    
    
    df_results = pd.read_sql("SELECT * FROM admissions_data ORDER BY id", conn)
    
    # Save CSV with consistent naming
    csv_filename = "admissions_data_final.csv"
    df_results.to_csv(csv_filename, index=False)
    print(f"CSV saved: {csv_filename}")
    
    # Save errors if any
    if all_errors:
        error_file = "extraction_errors.csv"
        pd.DataFrame(all_errors).to_csv(error_file, index=False)
    
    
    print(f"\nField completeness:")
    print(f"  Undergrad GPA: {df_results['undergrad_gpa'].notna().sum():,} ({df_results['undergrad_gpa'].notna().sum()/len(df_results)*100:.1f}%)")
    print(f"  Grad GPA: {df_results['grad_gpa'].notna().sum():,} ({df_results['grad_gpa'].notna().sum()/len(df_results)*100:.1f}%)")
    print(f"  GRE Quant: {df_results['gre_quant'].notna().sum():,} ({df_results['gre_quant'].notna().sum()/len(df_results)*100:.1f}%)")
    print(f"  GRE Verbal: {df_results['gre_verbal'].notna().sum():,} ({df_results['gre_verbal'].notna().sum()/len(df_results)*100:.1f}%)")
    print(f"  Undergrad institution: {df_results['undergrad_institution'].notna().sum():,} ({df_results['undergrad_institution'].notna().sum()/len(df_results)*100:.1f}%)")
    print(f"  Math courses: {df_results['math_courses'].apply(lambda x: len(x) if x else 0).sum():,} total")
    print(f"  Schools accepted: {df_results['schools_accepted'].apply(lambda x: len(x) if x else 0).sum():,} total")
    print(f"  Schools rejected: {df_results['schools_rejected'].apply(lambda x: len(x) if x else 0).sum():,} total")
    
    
    conn.close()

if __name__ == "__main__":
    asyncio.run(main())