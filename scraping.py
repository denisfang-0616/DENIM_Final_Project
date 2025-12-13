import requests
from bs4 import BeautifulSoup
import time
import csv
import os

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
    
    next_link = soup.select_one("a[rel='next']")
    next_url = None
    if next_link and next_link.get("href"):
        next_url = next_link["href"]
        if next_url.startswith("/"):
            next_url = FORUM_BASE + next_url
    
    return results, next_url


def extract_posts_from_thread(thread_url, csv_writer, thread_title, max_pages=None):
    posts_count = 0
    page = 1
    seen_posts = set()  
    
    while True:
        if max_pages and page > max_pages:
            break
            
        if page == 1:
            url = thread_url
        else:
            url = thread_url.rstrip("/") + f"/page/{page}/"
        
        try:
            soup = get_soup(url)
        except Exception:
            break
            
        post_items = soup.select("article.ipsComment")
        
        if not post_items:
            break
        
        new_posts_on_page = 0
        for post in post_items:
            post_id = post.get("data-comment-id") or post.get("id")
            if not post_id:
                content_tag = post.select_one(".ipsComment_content")
                post_id = hash(content_tag.get_text(strip=True)[:100]) if content_tag else None
            
            if post_id and post_id in seen_posts:
                continue
            
            if post_id:
                seen_posts.add(post_id)
            
            author_tag = post.select_one(".ipsComment_author a")
            content_tag = post.select_one(".ipsComment_content")
            author = author_tag.get_text(strip=True) if author_tag else "Unknown"
            content = content_tag.get_text(" ", strip=True) if content_tag else ""
            
            csv_writer.writerow([thread_title, thread_url, author, page, content])
            posts_count += 1
            new_posts_on_page += 1
        
        
        if new_posts_on_page == 0:
            break
        
        page += 1
        time.sleep(1)
    
    return posts_count

def scrape_forum(start_page=1, end_page=None, max_thread_pages=None, output_file=None):
    if output_file is None:
        if end_page:
            output_file = f"urch_forum_pages_{start_page}_to_{end_page}.csv"
        else:
            output_file = f"urch_forum_pages_{start_page}_onwards.csv"
    
    if start_page == 1:
        current_url = FORUM_URL
    else:
        current_url = f"https://www.urch.com/forums/?page={start_page}"
    

    file_exists = os.path.exists(output_file)
    
    mode = 'a' if file_exists else 'w'
    
    with open(output_file, mode, newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        
        if not file_exists:
            writer.writerow(["thread_title", "thread_url", "author", "page", "post"])
        
        forum_page_count = start_page - 1
        
        while current_url:
            forum_page_count += 1
            
            if end_page and forum_page_count > end_page:
                break
                
            try:
                threads, next_url = extract_threads_from_forum_page(current_url)
            except Exception as e:
                break
                
            if not threads:
                break
            
            for title, url in threads:
                try:
                    extract_posts_from_thread(url, writer, title, max_pages=max_thread_pages)
                    f.flush()
                except Exception:
                    continue
            
            current_url = next_url
            if current_url:
                time.sleep(1)
    

    


if __name__ == "__main__":
    scrape_forum(start_page=1, end_page=717, max_thread_pages=100, 
                 output_file="urch_forum_pages_1_to_717.csv")
    