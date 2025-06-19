import os
import psycopg2
from psycopg2 import sql
from crawl4ai import AsyncWebCrawler, BFSDeepCrawlStrategy, CrawlerRunConfig, HTTPCrawlerConfig, LXMLWebScrapingStrategy
from dotenv import load_dotenv
import logging
import time
import re
import asyncio

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Regex to check for a basic valid domain structure and not N/A or similar
URL_PATTERN = re.compile(
    r'^(?:(?:https?|ftp)://)?' # Optional scheme
    r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?))' # domain...
    r'(?:/?|[/?]\S+)$' # Optional path
    , re.IGNORECASE)

def is_valid_url_format(url):
    """Check if the URL string seems like a valid format and not 'N/A' or empty."""
    if not url or url.strip().upper() == 'N/A' or not URL_PATTERN.match(url.strip()):
        return False
    return True

def create_insurer_pages_table(conn):
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
                    markdown TEXT,
                    CONSTRAINT uq_insurer_page_c4ai UNIQUE (insurer_name, page_url)
                );
            """)
            conn.commit()
            logging.info("Table 'th.insurer_pages_c4ai' checked/created successfully.")
    except Exception as e:
        logging.error(f"Error creating table 'th.insurer_pages_c4ai': {e}")
        conn.rollback()
        raise

def get_insurers(conn):
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

def insert_page_url(conn, insurer_name, page_url, page_type, markdown):
    """Inserts a page URL and its type for an insurer into the th.insurer_pages_c4ai table."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("INSERT INTO th.insurer_pages_c4ai (insurer_name, page_url, page_type, markdown) VALUES (%s, %s, %s, %s) ON CONFLICT (insurer_name, page_url) DO NOTHING;"),
                (insurer_name, page_url, page_type, markdown)
            )
    except Exception as e:
        logging.error(f"Error inserting page URL {page_url} for {insurer_name}: {e}")
        conn.rollback()

async def crawl_insurer(crawler, insurer_name, root_url, crawl_options):
    try:
        config = CrawlerRunConfig(
            deep_crawl_strategy=BFSDeepCrawlStrategy(
                max_depth=2, 
                include_external=False
            ),
            scraping_strategy=LXMLWebScrapingStrategy(),
            verbose=True
        )
        result = await crawler.arun(root_url, config=config)
        urls = []
        if hasattr(result, 'links'):
            urls = result.links["internal"]
        elif isinstance(result, list):
            for item in result:
                if item.success:
                    if hasattr(item, 'links'):
                        try:
                            page = {
                                'url': item.url,
                                'links': item.links["internal"],
                                'markdown': item.markdown
                            }
                            urls.append(page)
                        except:
                            logging.warning(f"Skipping {insurer_name} page {item.url} due to failed crawl: ")
                else:
                    logging.warning(f"Skipping {insurer_name} page {item.url} due to failed crawl: ")


        return urls
    except Exception as e:
        logging.error(f"Error during async crawl for {insurer_name}: {e}")
        return []

async def async_main():
    load_dotenv()

    db_host = os.getenv("PGHOST")
    db_user = os.getenv("PGUSER")
    db_password = os.getenv("PGPASSWORD")
    db_name = os.getenv("PGDATABASE")
    # crawl4ai does not require an API key

    if not all([db_host, db_user, db_password, db_name]):
        logging.error("Database credentials are missing in .env file.")
        return

    conn = None
    try:
        conn = psycopg2.connect(host=db_host, user=db_user, password=db_password, dbname=db_name)
        logging.info("Successfully connected to PostgreSQL database.")

        create_insurer_pages_table(conn)
        
        insurers = get_insurers(conn)
        if not insurers:
            logging.info("No insurers found to process.")
            return

        crawler = AsyncWebCrawler()
        
        for insurer_name, root_url in insurers:
            original_root_url = root_url
            if not root_url:
                logging.warning(f"Skipping {insurer_name} due to missing root_url (was empty or None).")
                continue

            root_url = root_url.strip()
            if not is_valid_url_format(root_url):
                logging.warning(f"Skipping {insurer_name} due to invalid root_url format: '{original_root_url}'.")
                continue

            if not root_url.startswith(('http://', 'https://')):
                root_url = 'https://' + root_url
                logging.info(f"Prepended https:// to root_url for {insurer_name}, now: {root_url}")

            logging.info(f"Processing {insurer_name} ({root_url})...")
            crawl_options = {
                "max_pages": 1000,
                "follow_subdomains": True,
                "respect_robots_txt": True,
                "crawl_delay": 1,
            }
            pages = await crawl_insurer(crawler, insurer_name, root_url, crawl_options)
            if pages:
                logging.info(f"Found {len(pages)} URLs for {insurer_name}.")
                for page in pages:
                    page_type = 'unknown'
                    if page["url"] and isinstance(page["url"], str):
                        if page["url"].lower().endswith('.pdf'):
                            page_type = 'pdf'
                        else:
                            page_type = 'html'
                    else:
                        logging.warning(f"Invalid page_url format for {insurer_name}: {page["url"]}. Setting page_type to 'unknown'.")
                    insert_page_url(conn, insurer_name, page["url"], page_type, page["markdown"])
                    # insert_page_content(conn, insurer_name, page_url[0]['href'], page_url[1], page_type)
                conn.commit()
                logging.info(f"Successfully inserted URLs for {insurer_name}.")
            else:
                logging.warning(f"No URLs found by crawl4ai for {insurer_name} ({root_url}). Response might have been empty or in an unexpected format.")
            logging.info("Waiting for 10 miliseconds before next insurer...")
            await asyncio.sleep(0.1)

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