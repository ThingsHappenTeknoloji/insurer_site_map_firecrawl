import os
import psycopg2
from psycopg2 import sql
from firecrawl import FirecrawlApp
from dotenv import load_dotenv
import logging
import time # Added for sleep
import re # Added for URL validation

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Regex to check for a basic valid domain structure and not N/A or similar
# This is a basic check, not a full IETF-compliant URL validator.
# It checks for something that looks like a domain name, not just "N/A" or empty.
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
    """Creates the th.insurer_pages table if it doesn't exist."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE SCHEMA IF NOT EXISTS th;
                CREATE TABLE IF NOT EXISTS th.insurer_pages (
                    id SERIAL PRIMARY KEY,
                    insurer_name VARCHAR(255) NOT NULL,
                    page_url TEXT NOT NULL,
                    page_type VARCHAR(50),
                    CONSTRAINT uq_insurer_page UNIQUE (insurer_name, page_url)
                );
            """)
            conn.commit()
            logging.info("Table 'th.insurer_pages' checked/created successfully.")
    except Exception as e:
        logging.error(f"Error creating table 'th.insurer_pages': {e}")
        conn.rollback()
        raise

def get_insurers(conn):
    """Fetches insurer names and root URLs from the th.insurer table."""
    insurers = []
    try:
        with conn.cursor() as cur:
            # Assuming your table is indeed th.insurer and columns are name, root_url
            cur.execute("SELECT name, root_url FROM th.insurer;")
            insurers = cur.fetchall()
            logging.info(f"Fetched {len(insurers)} insurers from 'th.insurer'.")
    except psycopg2.Error as e:
        logging.error(f"Error fetching insurers: {e}")
        # Decide if you want to raise or return empty list / handle differently
        # For now, let's re-raise to stop execution if we can't get insurers
        raise
    return insurers

def insert_page_url(conn, insurer_name, page_url, page_type):
    """Inserts a page URL and its type for an insurer into the th.insurer_pages table."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("INSERT INTO th.insurer_pages (insurer_name, page_url, page_type) VALUES (%s, %s, %s) ON CONFLICT (insurer_name, page_url) DO NOTHING;"),
                (insurer_name, page_url, page_type)
            )
        # Commit can be done in batches or at the end for performance
    except Exception as e:
        logging.error(f"Error inserting page URL {page_url} for {insurer_name}: {e}")
        conn.rollback() # Rollback this specific insertion error
        # Optionally re-raise if this error should stop the process

def main():
    load_dotenv()

    db_host = os.getenv("PGHOST")
    db_user = os.getenv("PGUSER")
    db_password = os.getenv("PGPASSWORD")
    db_name = os.getenv("PGDATABASE")
    firecrawl_api_key = os.getenv("FC_APIKEY")

    if not all([db_host, db_user, db_password, db_name, firecrawl_api_key]):
        logging.error("Database credentials or Firecrawl API key are missing in .env file.")
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

        firecrawl_app = FirecrawlApp(api_key=firecrawl_api_key)
        
        for insurer_name, root_url in insurers:
            original_root_url = root_url # Keep original for logging if invalid
            if not root_url:
                logging.warning(f"Skipping {insurer_name} due to missing root_url (was empty or None).")
                continue

            root_url = root_url.strip()
            if not is_valid_url_format(root_url):
                logging.warning(f"Skipping {insurer_name} due to invalid root_url format: '{original_root_url}'.")
                continue

            # Ensure root_url has a scheme
            if not root_url.startswith(('http://', 'https://')):
                root_url = 'http://' + root_url # Default to http, or try https first
                logging.info(f"Prepended http:// to root_url for {insurer_name}, now: {root_url}")

            logging.info(f"Processing {insurer_name} ({root_url})...")
            try:
                map_params = {
                    "ignore_sitemap": True,
                    "include_subdomains": True,
                }
                
                logging.info(f"Calling Firecrawl map_url for {root_url} with params: {map_params}")
                # Assuming firecrawl_app.map_url returns a MapResponse object or raises an exception on error.
                response_object = firecrawl_app.map_url(url=root_url, **map_params)

                if response_object: # If map_url returned an object (not None and didn't raise an exception)
                    mapped_urls = []
                    if hasattr(response_object, 'links') and response_object.links is not None:
                        mapped_urls = response_object.links
                    elif isinstance(response_object, list):
                        # Fallback if it directly returns a list (less likely given MapResponse error)
                        mapped_urls = response_object
                    else:
                        logging.warning(f"Firecrawl map_url for {insurer_name} returned an object but no 'links' attribute or it was None. Object type: {type(response_object)}, Object: {str(response_object)[:200]}")

                    if mapped_urls:
                        logging.info(f"Found {len(mapped_urls)} URLs for {insurer_name}.")
                        for page_url in mapped_urls:
                            # Determine page type
                            page_type = 'unknown'
                            if page_url and isinstance(page_url, str):
                                if page_url.lower().endswith('.pdf'):
                                    page_type = 'pdf'
                                else:
                                    page_type = 'html'
                            else:
                                logging.warning(f"Invalid page_url format for {insurer_name}: {page_url}. Setting page_type to 'unknown'.")
                            
                            insert_page_url(conn, insurer_name, page_url, page_type)
                        conn.commit() 
                        logging.info(f"Successfully inserted URLs for {insurer_name}.")
                    else:
                        # This case now also covers when .links was present but empty
                        logging.info(f"No URLs found by Firecrawl for {insurer_name} ({root_url}). Response might have been empty or an unexpected format.")
                
                else: # map_url returned None
                    logging.info(f"No response (None) from Firecrawl map_url for {insurer_name} ({root_url}). This might indicate an issue not raised as an exception.")

            except Exception as e: # Catches Firecrawl API errors (429, 502), network issues, etc.
                logging.error(f"Error processing {insurer_name} with Firecrawl: {e}")
                if conn:
                    conn.rollback() 
            finally:
                # Add a delay after each Firecrawl attempt (success or failure) to respect rate limits
                logging.info(f"Waiting for 5 seconds before next insurer...")
                time.sleep(5)

    except psycopg2.Error as db_err:
        logging.error(f"Database connection error: {db_err}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
    finally:
        if conn:
            conn.close()
            logging.info("PostgreSQL connection closed.")

if __name__ == "__main__":
    main() 