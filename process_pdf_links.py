import os
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
import logging
import asyncio
import aiohttp
import fitz  # PyMuPDF

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_pdf_links(conn):
    """Fetches PDF links from the th.insurer_pages_c4ai table that need processing."""
    pdf_links = []
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, page_url FROM th.insurer_pages_c4ai
                WHERE page_url ILIKE '%.pdf' AND (markdown IS NULL OR length(markdown) < 10);
            """)
            pdf_links = cur.fetchall()
            logging.info(f"Found {len(pdf_links)} PDF links to process.")
    except psycopg2.Error as e:
        logging.error(f"Error fetching PDF links: {e}")
        raise
    return pdf_links

async def download_and_extract_text(pdf_url):
    """Downloads a PDF from a URL and extracts its text content."""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(pdf_url, timeout=60) as response:
                if response.status == 200:
                    pdf_bytes = await response.read()
                    loop = asyncio.get_event_loop()
                    text = await loop.run_in_executor(None, extract_text_from_pdf_bytes, pdf_bytes)
                    return text
                else:
                    logging.warning(f"Failed to download PDF {pdf_url}, status: {response.status}")
                    return None
    except Exception as e:
        logging.error(f"Error processing PDF URL {pdf_url}: {e}")
        return None

def extract_text_from_pdf_bytes(pdf_bytes):
    """Extracts text from PDF bytes using PyMuPDF."""
    try:
        with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
            text = "".join(page.get_text() for page in doc)
            return text
    except Exception as e:
        logging.error(f"Failed to extract text from PDF bytes: {e}")
        return None

def update_markdown(conn, page_id, markdown_text):
    """Updates the markdown field for a given page ID."""
    try:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("UPDATE th.insurer_pages_c4ai SET markdown = %s WHERE id = %s;"),
                (markdown_text, page_id)
            )
        logging.info(f"Updated markdown for page_id: {page_id}")
    except Exception as e:
        logging.error(f"Error updating markdown for page_id {page_id}: {e}")
        conn.rollback()

async def main():
    load_dotenv()

    db_host = os.getenv("PGHOST")
    db_user = os.getenv("PGUSER")
    db_password = os.getenv("PGPASSWORD")
    db_name = os.getenv("PGDATABASE")

    if not all([db_host, db_user, db_password, db_name]):
        logging.error("Database credentials are missing in .env file.")
        return

    conn = None
    try:
        conn = psycopg2.connect(host=db_host, user=db_user, password=db_password, dbname=db_name)
        logging.info("Successfully connected to PostgreSQL database.")

        pdf_links = get_pdf_links(conn)
        if not pdf_links:
            logging.info("No new PDF links to process.")
            return

        for page_id, page_url in pdf_links:
            try:
                logging.info(f"Processing {page_url}...")
                markdown = await download_and_extract_text(page_url)
                if markdown:
                    update_markdown(conn, page_id, markdown)
                    conn.commit()
                else:
                    logging.warning(f"Skipping update for {page_url} due to processing error or empty content.")
            except Exception as e:
                logging.error(f"An error occurred while processing {page_url}. Skipping. Error: {e}")
                if conn:
                    conn.rollback()  # Ensure the connection is in a good state for the next item.
            
            await asyncio.sleep(1) # Small delay to be polite to servers

    except psycopg2.Error as db_err:
        logging.error(f"Database connection error: {db_err}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
    finally:
        if conn:
            conn.close()
            logging.info("PostgreSQL connection closed.")

if __name__ == "__main__":
    asyncio.run(main()) 