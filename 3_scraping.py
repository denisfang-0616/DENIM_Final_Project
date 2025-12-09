# import requests
# from bs4 import BeautifulSoup
# import time
# import csv
# import os
# from datetime import datetime

# FORUM_BASE = "https://www.urch.com"
# FORUM_URL = "https://www.urch.com/forums/?forumId=104"
# HEADERS = {
#     "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
# }

# def get_soup(url):
#     r = requests.get(url, headers=HEADERS, timeout=20)
#     r.raise_for_status()
#     return BeautifulSoup(r.text, "html.parser")

# def extract_threads_from_forum_page(page_url):
#     soup = get_soup(page_url)
#     threads = soup.select("li.ipsDataItem[data-rowid]")
#     results = []
#     for t in threads:
#         title_tag = t.select_one("h4 a[href*='topic']")
#         if not title_tag:
#             continue
#         thread_title = title_tag.get_text(strip=True)
#         thread_url = title_tag["href"]
#         if thread_url.startswith("/"):
#             thread_url = FORUM_BASE + thread_url
#         results.append((thread_title, thread_url))
    
#     # Find the next page URL
#     next_link = soup.select_one("a[rel='next']")
#     next_url = None
#     if next_link and next_link.get("href"):
#         next_url = next_link["href"]
#         if next_url.startswith("/"):
#             next_url = FORUM_BASE + next_url
    
#     return results, next_url

# def safe_print(text):
#     """Safely print text that might contain unicode characters."""
#     try:
#         print(text)
#     except UnicodeEncodeError:
#         # Replace problematic characters with ?
#         print(text.encode('ascii', 'replace').decode('ascii'))

# def extract_posts_from_thread(thread_url, csv_writer, thread_title, max_pages=None):
#     """
#     Extract posts from a thread and write them immediately to CSV.
#     """
#     posts_count = 0
#     page = 1
#     seen_posts = set()  # Track unique posts to avoid duplicates
    
#     while True:
#         if max_pages and page > max_pages:
#             safe_print(f"    Reached max pages ({max_pages}), moving to next thread")
#             break
            
#         # Build paginated URL
#         if page == 1:
#             url = thread_url
#         else:
#             url = thread_url.rstrip("/") + f"/page/{page}/"
        
#         safe_print(f"    Scraping thread page {page}...")
        
#         try:
#             soup = get_soup(url)
#         except Exception as e:
#             safe_print(f"    Error fetching page {page}: {e}")
#             break
            
#         post_items = soup.select("article.ipsComment")
        
#         if not post_items:
#             safe_print(f"    No more posts found on page {page}")
#             break
        
#         # Check if we're seeing duplicate posts (sign we've hit the end)
#         new_posts_on_page = 0
#         for post in post_items:
#             # Create a unique identifier for each post
#             post_id = post.get("data-comment-id") or post.get("id")
#             if not post_id:
#                 # If no ID, use content as identifier
#                 content_tag = post.select_one(".ipsComment_content")
#                 post_id = hash(content_tag.get_text(strip=True)[:100]) if content_tag else None
            
#             # Skip if we've seen this post before
#             if post_id and post_id in seen_posts:
#                 continue
            
#             if post_id:
#                 seen_posts.add(post_id)
            
#             author_tag = post.select_one(".ipsComment_author a")
#             content_tag = post.select_one(".ipsComment_content")
#             author = author_tag.get_text(strip=True) if author_tag else "Unknown"
#             content = content_tag.get_text(" ", strip=True) if content_tag else ""
            
#             # Write immediately to CSV
#             csv_writer.writerow([thread_title, thread_url, author, page, content])
#             posts_count += 1
#             new_posts_on_page += 1
        
#         safe_print(f"    Found {new_posts_on_page} new posts on page {page}")
        
#         # If no new posts were found, we've reached the end
#         if new_posts_on_page == 0:
#             safe_print(f"    All posts on page {page} were duplicates - stopping pagination")
#             break
        
#         page += 1
#         time.sleep(1)
    
#     return posts_count

# def scrape_forum(start_page=1, end_page=None, max_thread_pages=None, output_file=None):
#     """
#     Scrape the forum from a specific page range.
#     """
#     # Record start time
#     start_time = datetime.now()
#     print(f"\n{'='*60}")
#     print(f"SCRAPING STARTED: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
#     print(f"{'='*60}\n")
    
#     # Auto-generate filename based on page range
#     if output_file is None:
#         if end_page:
#             output_file = f"urch_forum_pages_{start_page}_to_{end_page}.csv"
#         else:
#             output_file = f"urch_forum_pages_{start_page}_onwards.csv"
    
#     total_posts = 0
    
#     # Build starting URL
#     if start_page == 1:
#         current_url = FORUM_URL
#     else:
#         current_url = f"https://www.urch.com/forums/?page={start_page}"
    
#     with open(output_file, "w", newline="", encoding="utf-8") as f:
#         writer = csv.writer(f)
#         writer.writerow(["thread_title", "thread_url", "author", "page", "post"])
        
#         forum_page_count = start_page - 1
        
#         while current_url:
#             forum_page_count += 1
            
#             # Check if we've reached the end page
#             if end_page and forum_page_count > end_page:
#                 print(f"\nReached end page ({end_page})")
#                 break
                
#             print(f"\n{'='*60}")
#             print(f"Scraping forum page {forum_page_count}")
#             print(f"URL: {current_url}")
#             print(f"{'='*60}")
            
#             try:
#                 threads, next_url = extract_threads_from_forum_page(current_url)
#             except Exception as e:
#                 print(f"Error scraping forum page {forum_page_count}: {e}")
#                 break
                
#             if not threads:
#                 print("No threads found on this page. Stopping.")
#                 break
            
#             print(f"Found {len(threads)} threads on forum page {forum_page_count}\n")
            
#             for idx, (title, url) in enumerate(threads, 1):
#                 safe_print(f"  [{idx}/{len(threads)}] Thread: {title}")
#                 try:
#                     posts = extract_posts_from_thread(url, writer, title, max_pages=max_thread_pages)
#                     total_posts += posts
#                     safe_print(f"    ✓ Extracted {posts} posts (Total so far: {total_posts})")
#                     f.flush()
#                 except Exception as e:
#                     safe_print(f"    ✗ Error scraping thread '{title}': {e}")
#                     continue
            
#             # Move to next page
#             current_url = next_url
#             if current_url:
#                 print(f"\nNext page URL: {current_url}")
#                 time.sleep(1)
#             else:
#                 print("\nNo more pages found. Finished!")
    
#     # Record end time and calculate duration
#     end_time = datetime.now()
#     duration = end_time - start_time
#     hours, remainder = divmod(duration.total_seconds(), 3600)
#     minutes, seconds = divmod(remainder, 60)
    
#     print(f"\n{'='*60}")
#     print(f"SCRAPING COMPLETED!")
#     print(f"{'='*60}")
#     print(f"Start time:    {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
#     print(f"End time:      {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
#     print(f"Total duration: {int(hours)}h {int(minutes)}m {int(seconds)}s")
#     print(f"{'='*60}")
#     print(f"Saved {total_posts} posts to {output_file}")
#     print(f"Scraped forum pages {start_page} to {forum_page_count}")
#     print(f"{'='*60}\n")

# if __name__ == "__main__":
#     # Full scrape: all 717 pages, up to 100 pages per thread
#     scrape_forum(start_page=672, end_page=717, max_thread_pages=100)


import requests
from bs4 import BeautifulSoup
import time
import csv
import os
from datetime import datetime

FORUM_BASE = "https://www.urch.com"
FORUM_URL = "https://www.urch.com/forums/?forumId=104"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

def get_soup(url):
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser")

def extract_threads_from_forum_page(page_url):
    soup = get_soup(page_url)
    threads = soup.select("li.ipsDataItem[data-rowid]")
    results = []
    for t in threads:
        title_tag = t.select_one("h4 a[href*='topic']")
        if not title_tag:
            continue
        thread_title = title_tag.get_text(strip=True)
        thread_url = title_tag["href"]
        if thread_url.startswith("/"):
            thread_url = FORUM_BASE + thread_url
        results.append((thread_title, thread_url))
    
    # Find the next page URL
    next_link = soup.select_one("a[rel='next']")
    next_url = None
    if next_link and next_link.get("href"):
        next_url = next_link["href"]
        if next_url.startswith("/"):
            next_url = FORUM_BASE + next_url
    
    return results, next_url

def safe_print(text):
    """Safely print text that might contain unicode characters."""
    try:
        print(text)
    except UnicodeEncodeError:
        # Replace problematic characters with ?
        print(text.encode('ascii', 'replace').decode('ascii'))

def extract_posts_from_thread(thread_url, csv_writer, thread_title, max_pages=None):
    """
    Extract posts from a thread and write them immediately to CSV.
    """
    posts_count = 0
    page = 1
    seen_posts = set()  # Track unique posts to avoid duplicates
    
    while True:
        if max_pages and page > max_pages:
            safe_print(f"    Reached max pages ({max_pages}), moving to next thread")
            break
            
        # Build paginated URL
        if page == 1:
            url = thread_url
        else:
            url = thread_url.rstrip("/") + f"/page/{page}/"
        
        safe_print(f"    Scraping thread page {page}...")
        
        try:
            soup = get_soup(url)
        except Exception as e:
            safe_print(f"    Error fetching page {page}: {e}")
            break
            
        post_items = soup.select("article.ipsComment")
        
        if not post_items:
            safe_print(f"    No more posts found on page {page}")
            break
        
        # Check if we're seeing duplicate posts (sign we've hit the end)
        new_posts_on_page = 0
        for post in post_items:
            # Create a unique identifier for each post
            post_id = post.get("data-comment-id") or post.get("id")
            if not post_id:
                # If no ID, use content as identifier
                content_tag = post.select_one(".ipsComment_content")
                post_id = hash(content_tag.get_text(strip=True)[:100]) if content_tag else None
            
            # Skip if we've seen this post before
            if post_id and post_id in seen_posts:
                continue
            
            if post_id:
                seen_posts.add(post_id)
            
            author_tag = post.select_one(".ipsComment_author a")
            content_tag = post.select_one(".ipsComment_content")
            author = author_tag.get_text(strip=True) if author_tag else "Unknown"
            content = content_tag.get_text(" ", strip=True) if content_tag else ""
            
            # Write immediately to CSV
            csv_writer.writerow([thread_title, thread_url, author, page, content])
            posts_count += 1
            new_posts_on_page += 1
        
        safe_print(f"    Found {new_posts_on_page} new posts on page {page}")
        
        # If no new posts were found, we've reached the end
        if new_posts_on_page == 0:
            safe_print(f"    All posts on page {page} were duplicates - stopping pagination")
            break
        
        page += 1
        time.sleep(1)
    
    return posts_count

def scrape_forum(start_page=1, end_page=None, max_thread_pages=None, output_file=None):
    """
    Scrape the forum from a specific page range.
    If output_file exists, new data will be appended. If not, a new file will be created.
    """
    # Record start time
    start_time = datetime.now()
    print(f"\n{'='*60}")
    print(f"SCRAPING STARTED: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")
    
    # Auto-generate filename based on page range
    if output_file is None:
        if end_page:
            output_file = f"urch_forum_pages_{start_page}_to_{end_page}.csv"
        else:
            output_file = f"urch_forum_pages_{start_page}_onwards.csv"
    
    total_posts = 0
    
    # Build starting URL
    if start_page == 1:
        current_url = FORUM_URL
    else:
        current_url = f"https://www.urch.com/forums/?page={start_page}"
    
    # Check if file exists to determine if we should write header
    file_exists = os.path.exists(output_file)
    
    # Use 'a' (append) mode if file exists, otherwise 'w' (write) mode
    mode = 'a' if file_exists else 'w'
    
    with open(output_file, mode, newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        
        # Only write header if file doesn't exist
        if not file_exists:
            writer.writerow(["thread_title", "thread_url", "author", "page", "post"])
            print(f"Created new file: {output_file}")
        else:
            print(f"Appending to existing file: {output_file}")
        
        forum_page_count = start_page - 1
        
        while current_url:
            forum_page_count += 1
            
            # Check if we've reached the end page
            if end_page and forum_page_count > end_page:
                print(f"\nReached end page ({end_page})")
                break
                
            print(f"\n{'='*60}")
            print(f"Scraping forum page {forum_page_count}")
            print(f"URL: {current_url}")
            print(f"{'='*60}")
            
            try:
                threads, next_url = extract_threads_from_forum_page(current_url)
            except Exception as e:
                print(f"Error scraping forum page {forum_page_count}: {e}")
                break
                
            if not threads:
                print("No threads found on this page. Stopping.")
                break
            
            print(f"Found {len(threads)} threads on forum page {forum_page_count}\n")
            
            for idx, (title, url) in enumerate(threads, 1):
                safe_print(f"  [{idx}/{len(threads)}] Thread: {title}")
                try:
                    posts = extract_posts_from_thread(url, writer, title, max_pages=max_thread_pages)
                    total_posts += posts
                    safe_print(f"    ✓ Extracted {posts} posts (Total so far: {total_posts})")
                    f.flush()
                except Exception as e:
                    safe_print(f"    ✗ Error scraping thread '{title}': {e}")
                    continue
            
            # Move to next page
            current_url = next_url
            if current_url:
                print(f"\nNext page URL: {current_url}")
                time.sleep(1)
            else:
                print("\nNo more pages found. Finished!")
    
    # Record end time and calculate duration
    end_time = datetime.now()
    duration = end_time - start_time
    hours, remainder = divmod(duration.total_seconds(), 3600)
    minutes, seconds = divmod(remainder, 60)
    
    print(f"\n{'='*60}")
    print(f"SCRAPING COMPLETED!")
    print(f"{'='*60}")
    print(f"Start time:    {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"End time:      {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Total duration: {int(hours)}h {int(minutes)}m {int(seconds)}s")
    print(f"{'='*60}")
    print(f"Saved {total_posts} posts to {output_file}")
    print(f"Scraped forum pages {start_page} to {forum_page_count}")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    # OPTION 1: Full scrape from scratch
    # scrape_forum(start_page=1, end_page=717, max_thread_pages=100)
    
    # OPTION 2: Resume from page 672 (append to existing file)
    # IMPORTANT: Specify the SAME filename to append
    scrape_forum(start_page=1, end_page=717, max_thread_pages=100, 
                 output_file="urch_forum_pages_1_to_717.csv")
    
    # OPTION 3: Test with just a few pages
    # scrape_forum(start_page=1, end_page=5, max_thread_pages=10)