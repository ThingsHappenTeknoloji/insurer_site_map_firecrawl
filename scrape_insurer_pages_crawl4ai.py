import os
import psycopg2
from psycopg2 import sql
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, BFSDeepCrawlStrategy
from dotenv import load_dotenv
import logging
import asyncio
import re
from typing import List, Optional

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Regex to check for a basic valid domain structure and not N/A or similar
URL_PATTERN = re.compile(
    r'^(?:(?:https?|ftp)://)?' # Optional scheme
    r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?))' # domain...
    r'(?:/?|[/?]\S+)$' # Optional path
    , re.IGNORECASE)


# Basic configuration
strategy = BFSDeepCrawlStrategy(
    max_depth=10,              # Crawl initial page + 2 levels deep
    include_external=False,    # Stay within the same domain
    max_pages=5000,            # Maximum number of pages to crawl (optional)
    score_threshold=-float('inf'),       # Minimum score for URLs to be crawled (optional)
)

def is_valid_url_format(url: str) -> bool:
    """Check if the URL string seems like a valid format and not 'N/A' or empty."""
    if not url or url.strip().upper() == 'N/A' or not URL_PATTERN.match(url.strip()):
        return False
    return True

def create_insurer_pages_table(conn: psycopg2.extensions.connection) -> None:
    """Creates the th.insurer_pages_c4ai table if it doesn't exist."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE SCHEMA IF NOT EXISTS th;
                CREATE TABLE IF NOT EXISTS th.insurer_pages_c4ai (
                    id SERIAL PRIMARY KEY,
                    insurer_name VARCHAR(255) NOT NULL,
                    page_url TEXT NOT NULL,
                    page_type VARCHAR(50),
                    page_title TEXT,
                    page_content TEXT,
                    crawl_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT uq_insurer_page_c4ai UNIQUE (insurer_name, page_url)
                );
            """)
            conn.commit()
            logging.info("Table 'th.insurer_pages_c4ai' checked/created successfully.")
    except Exception as e:
        logging.error(f"Error creating table 'th.insurer_pages_c4ai': {e}")
        conn.rollback()
        raise

def get_insurers(conn: psycopg2.extensions.connection) -> List[tuple]:
    """Fetches insurer names and root URLs from the th.insurer table."""
    insurers = []
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT name, root_url FROM th.insurer;")
            insurers = cur.fetchall()
            logging.info(f"Fetched {len(insurers)} insurers from 'th.insurer'.")
    except psycopg2.Error as e:
        logging.error(f"Error fetching insurers: {e}")
        raise
    return insurers

def insert_page_data(conn: psycopg2.extensions.connection, 
                    insurer_name: str, 
                    page_url: str, 
                    page_type: str,
                    page_title: Optional[str] = None,
                    page_content: Optional[str] = None) -> None:
    """Inserts page data into the th.insurer_pages_c4ai table."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("""
                    INSERT INTO th.insurer_pages_c4ai 
                    (insurer_name, page_url, page_type, page_title, page_content) 
                    VALUES (%s, %s, %s, %s, %s) 
                    ON CONFLICT (insurer_name, page_url) 
                    DO UPDATE SET 
                        page_type = EXCLUDED.page_type,
                        page_title = EXCLUDED.page_title,
                        page_content = EXCLUDED.page_content,
                        crawl_date = CURRENT_TIMESTAMP;
                """),
                (insurer_name, page_url, page_type, page_title, page_content)
            )
    except Exception as e:
        logging.error(f"Error inserting page data for {insurer_name} at {page_url}: {e}")
        conn.rollback()

async def crawl_insurer(crawler: AsyncWebCrawler, 
                       insurer_name: str, 
                       root_url: str) -> List[dict]:
    """Crawl an insurer's website and return the results."""
    try:
        # Configure the crawler run
        browser_config = BrowserConfig(
            headless=True,
            viewport_width=1920,
            viewport_height=1080,
            wait_for_images=True  # Wait for images to load
        )
        
        crawler_config = CrawlerRunConfig(
            max_depth=3,  # Limit crawl depth
            respect_robots_txt=True,
            follow_subdomains=True,
            cache_mode="BYPASS"  # Don't use cache
        )

        # Run the crawler
        result = await crawler.arun(
            url=root_url,
            config=crawler_config
        )

        # Process the results
        pages = []
        if hasattr(result, 'urls'):
            for url in result.urls:
                page_type = 'pdf' if url.lower().endswith('.pdf') else 'html'
                pages.append({
                    'url': url,
                    'type': page_type,
                    'title': getattr(result, 'title', None),
                    'content': getattr(result, 'markdown', None)
                })
        return pages

    except Exception as e:
        logging.error(f"Error during async crawl for {insurer_name}: {e}")
        return []

async def async_main():
    """Main async function to orchestrate the crawling process."""
    load_dotenv()

    # Database configuration
    db_host = os.getenv("PGHOST")
    db_user = os.getenv("PGUSER")
    db_password = os.getenv("PGPASSWORD")
    db_name = os.getenv("PGDATABASE")

    if not all([db_host, db_user, db_password, db_name]):
        logging.error("Database credentials are missing in .env file.")
        return

    conn = None
    try:
        # Connect to database
        conn = psycopg2.connect(
            host=db_host,
            user=db_user,
            password=db_password,
            dbname=db_name
        )
        logging.info("Successfully connected to PostgreSQL database.")

        # Create table if it doesn't exist
        create_insurer_pages_table(conn)
        
        # Get insurers to process
        insurers = get_insurers(conn)
        if not insurers:
            logging.info("No insurers found to process.")
            return

        # Initialize crawler with context manager
        async with AsyncWebCrawler() as crawler:
            for insurer_name, root_url in insurers:
                if not root_url:
                    logging.warning(f"Skipping {insurer_name} due to missing root_url.")
                    continue

                root_url = root_url.strip()
                if not is_valid_url_format(root_url):
                    logging.warning(f"Skipping {insurer_name} due to invalid root_url format: '{root_url}'.")
                    continue

                # Ensure URL has scheme
                if not root_url.startswith(('http://', 'https://')):
                    root_url = 'https://' + root_url
                    logging.info(f"Prepended https:// to root_url for {insurer_name}, now: {root_url}")

                logging.info(f"Processing {insurer_name} ({root_url})...")
                
                # Crawl the insurer's website
                pages = await crawl_insurer(crawler, insurer_name, root_url)
                
                if pages:
                    logging.info(f"Found {len(pages)} pages for {insurer_name}.")
                    for page in pages:
                        insert_page_data(
                            conn=conn,
                            insurer_name=insurer_name,
                            page_url=page['url'],
                            page_type=page['type'],
                            page_title=page.get('title'),
                            page_content=page.get('content')
                        )
                    conn.commit()
                    logging.info(f"Successfully inserted data for {insurer_name}.")
                else:
                    logging.warning(f"No pages found for {insurer_name} ({root_url}).")

                # Add delay between insurers
                logging.info("Waiting for 5 seconds before next insurer...")
                await asyncio.sleep(5)

    except psycopg2.Error as db_err:
        logging.error(f"Database connection error: {db_err}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
    finally:
        if conn:
            conn.close()
            logging.info("PostgreSQL connection closed.")

if __name__ == "__main__":
    asyncio.run(async_main()) 