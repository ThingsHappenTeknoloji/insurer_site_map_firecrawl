import os
import psycopg2
from psycopg2 import sql, extras
from firecrawl import AsyncFirecrawlApp
from dotenv import load_dotenv # Re-enabled dotenv
import logging
import asyncio
import json # For handling JSONB

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

async def create_page_content_table(conn):
    """Creates the th.insurer_page_content table if it doesn't exist."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE SCHEMA IF NOT EXISTS th;
                CREATE TABLE IF NOT EXISTS th.insurer_page_content (
                    id SERIAL PRIMARY KEY,
                    page_url TEXT NOT NULL UNIQUE,
                    markdown TEXT,
                    full_content JSONB 
                );
            """)
            conn.commit()
            logging.info("Table 'th.insurer_page_content' checked/created successfully.")
    except Exception as e:
        logging.error(f"Error creating table 'th.insurer_page_content': {e}")
        conn.rollback()
        raise

async def get_insurers_and_pages_to_scrape(conn):
    """Fetches all insurers and up to 5 unscraped, non-PDF page URLs for each."""
    insurers_pages = {}
    try:
        with conn.cursor() as cur:
            # Get all insurer names first
            cur.execute("SELECT DISTINCT insurer_name FROM th.insurer_pages ORDER BY insurer_name;")
            insurers = [row[0] for row in cur.fetchall()]
            logging.info(f"Found {len(insurers)} distinct insurers to process.")

            for insurer_name in insurers:
                # For each insurer, get up to 5 pages that are not yet in insurer_page_content
                # and are not PDFs (if page_type column exists and is reliable)
                # Simplified query: no page_type check, as per earlier user request to skip it.
                cur.execute("""
                    SELECT ip.page_url 
                    FROM th.insurer_pages ip
                    LEFT JOIN th.insurer_page_content ipc ON ip.page_url = ipc.page_url
                    WHERE ip.insurer_name = %s 
                      AND ipc.id IS NULL  -- Only select pages not yet scraped
                    ORDER BY ip.id -- or some other consistent ordering to get the same first 5
                    LIMIT 5;
                """, (insurer_name,))
                pages = [row[0] for row in cur.fetchall()]
                if pages:
                    insurers_pages[insurer_name] = pages
                    logging.info(f"Found {len(pages)} pages to scrape for {insurer_name}.")
                else:
                    logging.info(f"No new pages to scrape for {insurer_name} (or all are already processed/PDFs if filtered).")
            
    except psycopg2.Error as e:
        logging.error(f"Error fetching insurers and pages to scrape: {e}")
        # Depending on desired behavior, could raise e or return partially filled dict
    return insurers_pages

async def insert_scraped_content(conn, page_url, markdown_content, full_response_json):
    """Inserts scraped content into the th.insurer_page_content table."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("INSERT INTO th.insurer_page_content (page_url, markdown, full_content) VALUES (%s, %s, %s) ON CONFLICT (page_url) DO UPDATE SET markdown = EXCLUDED.markdown, full_content = EXCLUDED.full_content;"),
                (page_url, markdown_content, json.dumps(full_response_json))
            )
        # Commit will be done after each insurer's batch of 5 pages
    except Exception as e:
        logging.error(f"Error inserting scraped content for {page_url}: {e}")
        conn.rollback() # Rollback this specific insertion error

async def scrape_and_store(firecrawl_app, conn, insurer_name, page_url):
    """Scrapes a single URL and stores its content."""
    logging.info(f"Preparing to scrape URL: {page_url} for {insurer_name}.")
    try:
        scrape_formats = ["markdown", "links"]
        scrape_only_main_content = True

        if not page_url or not page_url.startswith(("http://", "https://")):
            logging.warning(f"Skipping invalid or non-HTTP/S URL: {page_url} for {insurer_name}")
            return

        logging.info(f"Attempting to scrape URL: {page_url} for {insurer_name}...")
        response = await firecrawl_app.scrape_url(
            url=page_url,
            formats=scrape_formats,
            only_main_content=scrape_only_main_content
            # No timeout parameter here as it caused issues with the test key
        )
        logging.info(f"Scrape call for {page_url} (Insurer: {insurer_name}) completed. Response received: {bool(response)}")
        
        if response:
            markdown_content = None
            if hasattr(response, 'markdown'):
                markdown_content = response.markdown
            else:
                logging.warning(f"Response object for {page_url} does not have a 'markdown' attribute.")

            response_data_for_json = {}
            expected_fields = [
                'markdown', 'html', 'metadata', 'links', 'og_description', 
                'og_image', 'og_title', 'title', 'url', 'content', 'h1', 
                'h2', 'text_content', 'status_code', 'success', 
                'provider_response_time', 'llm_extraction', 'source_url'
            ]
            for field_name in expected_fields:
                if hasattr(response, field_name):
                    value = getattr(response, field_name)
                    if not callable(value):
                        response_data_for_json[field_name] = value
                    else:
                        logging.warning(f"Skipping field '{field_name}' for JSON storage as it is callable.")
            
            if not response_data_for_json and markdown_content is None:
                 logging.warning(f"Could not extract any serializable fields or markdown from response for {page_url}")
                 response_data_for_json = {"error": "No serializable content found in response"}

            await insert_scraped_content(conn, page_url, markdown_content, response_data_for_json)
            logging.info(f"Attempted to store extracted content for {page_url}.")

        else:
            logging.warning(f"No response (None or empty) from Firecrawl for {page_url} (Insurer: {insurer_name}).")

    except asyncio.TimeoutError: # Specific catch for timeout, though we removed the explicit timeout param
        logging.error(f"Timeout error scraping {page_url} for {insurer_name}.")
    except Exception as e:
        logging.error(f"Error scraping {page_url} for {insurer_name}: {e}")
    finally:
        # Add a delay after each scrape attempt to respect rate limits
        logging.debug(f"Waiting for 5 seconds after attempt for {page_url}...")
        await asyncio.sleep(5)

async def main():
    load_dotenv() # Re-enabled dotenv

    db_host = os.getenv("PGHOST")
    db_user = os.getenv("PGUSER")
    db_password = os.getenv("PGPASSWORD")
    db_name = os.getenv("PGDATABASE")
    firecrawl_api_key = os.getenv("FC_APIKEY") # Read from .env again

    if not all([db_host, db_user, db_password, db_name, firecrawl_api_key]):
        logging.error("Database credentials or Firecrawl API key are missing in .env file.")
        return

    conn = None
    try:
        conn = psycopg2.connect(host=db_host, user=db_user, password=db_password, dbname=db_name)
        psycopg2.extras.register_json(conn)
        logging.info("Successfully connected to PostgreSQL database for scraping.")

        await create_page_content_table(conn)
        
        insurers_with_pages = await get_insurers_and_pages_to_scrape(conn)
        if not insurers_with_pages:
            logging.info("No insurers or pages to scrape. Stopping.")
            return

        firecrawl_app = AsyncFirecrawlApp(api_key=firecrawl_api_key)
        logging.info(f"FirecrawlApp initialized with API key from .env: {firecrawl_api_key[:10]}...")
        
        for insurer_name, page_urls in insurers_with_pages.items():
            logging.info(f"Processing insurer: {insurer_name} with {len(page_urls)} page(s).")
            tasks = []
            for page_url in page_urls:
                if page_url and (page_url.startswith("http://") or page_url.startswith("https://")):
                    tasks.append(scrape_and_store(firecrawl_app, conn, insurer_name, page_url))
                else:
                    logging.warning(f"Skipping invalid URL for scraping: {page_url} (Insurer: {insurer_name})")
            
            if tasks:
                await asyncio.gather(*tasks) # This will run scrapes for one insurer concurrently
                conn.commit() 
                logging.info(f"Finished scraping and committed for {insurer_name}.")
            else:
                logging.info(f"No valid pages to scrape for {insurer_name} after URL validation.")
            
            logging.debug(f"Waiting for 1 second before processing next insurer...")
            await asyncio.sleep(1) 

    except psycopg2.Error as db_err:
        logging.error(f"Database connection error during scraping: {db_err}")
        if conn:
            conn.rollback()
    except Exception as e:
        logging.error(f"An unexpected error occurred during scraping: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()
            logging.info("PostgreSQL connection closed after scraping.")

if __name__ == "__main__":
    asyncio.run(main()) 