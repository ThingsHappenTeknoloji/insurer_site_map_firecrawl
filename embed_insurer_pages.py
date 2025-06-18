import os
import psycopg2
from psycopg2 import sql, extras
from dotenv import load_dotenv
import logging
import asyncio
import aiohttp
import json
from qdrant_client import QdrantClient
from qdrant_client.http import models
import numpy as np
from typing import List, Dict, Any
import hashlib
from langchain.text_splitter import RecursiveCharacterTextSplitter
import tiktoken
import datetime


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Constants
CHUNK_SIZE = 8000  # bytes
CHUNK_OVERLAP = 2000  # bytes
BATCH_SIZE = int(os.getenv("BATCH_SIZE",1))  # Number of documents to process in one batch
COLLECTION_NAME = "insurer_pages_c4ai"
EMBEDDING_DIM = 3072  # Dimension for Azure text-embedding-3-large model
C4AI = True

# Rate limiting constants for Azure API
MAX_CONCURRENT_REQUESTS = int(os.getenv("MAX_CONCURRENT_REQUESTS", 5))  # Max concurrent API calls
REQUEST_DELAY = float(os.getenv("REQUEST_DELAY", 0.1))  # Delay between requests in seconds

class DynamicRateLimiter:
    """Dynamic rate limiter that adjusts based on Azure API responses."""
    
    def __init__(self, initial_limit: int = MAX_CONCURRENT_REQUESTS):
        self.semaphore = asyncio.Semaphore(initial_limit)
        self.current_limit = initial_limit
        self.min_limit = 1
        self.max_limit = 20
        self.rate_limit_count = 0
        self.success_count = 0
        
    async def acquire(self):
        """Acquire a semaphore permit."""
        await self.semaphore.acquire()
        
    def release(self):
        """Release a semaphore permit."""
        self.semaphore.release()
        
    def adjust_for_rate_limit(self, retry_after: int):
        """Adjust the rate limiter when we hit a rate limit."""
        self.rate_limit_count += 1
        
        # Reduce concurrency when we hit rate limits
        new_limit = max(self.min_limit, self.current_limit // 2)
        if new_limit != self.current_limit:
            logging.warning(f"Rate limit hit. Reducing concurrent requests from {self.current_limit} to {new_limit}")
            self.current_limit = new_limit
            # Create new semaphore with adjusted limit
            self.semaphore = asyncio.Semaphore(self.current_limit)
            
    def adjust_for_success(self):
        """Gradually increase concurrency when requests are successful."""
        self.success_count += 1
        
        # Increase concurrency every 10 successful requests
        if self.success_count % 10 == 0 and self.current_limit < self.max_limit:
            new_limit = min(self.max_limit, self.current_limit + 1)
            if new_limit != self.current_limit:
                logging.info(f"Requests successful. Increasing concurrent requests from {self.current_limit} to {new_limit}")
                self.current_limit = new_limit
                # Create new semaphore with adjusted limit
                self.semaphore = asyncio.Semaphore(self.current_limit)

def get_db_connection():
    """Create and return a database connection."""
    db_host = os.getenv("PGHOST")
    db_user = os.getenv("PGUSER")
    db_password = os.getenv("PGPASSWORD")
    db_name = os.getenv("PGDATABASE")
    
    if not all([db_host, db_user, db_password, db_name]):
        raise ValueError("Database credentials are missing in .env file.")
    
    return psycopg2.connect(
        host=db_host,
        user=db_user,
        password=db_password,
        dbname=db_name
    )

def get_qdrant_client():
    """Create and return a Qdrant client."""
    qdrant_url = os.getenv("QDRANT_URL")
    qdrant_apikey = os.getenv("QDRANT_API_KEY")
    return QdrantClient(url=qdrant_url, api_key=qdrant_apikey)


def update_documents_in_qdrant_collection(client: QdrantClient, cursor: psycopg2.extensions.cursor, db_conn: psycopg2.extensions.connection):
    """Update documents in database to mark them as embedded based on Qdrant collection."""
    collections = client.get_collections().collections
    collection_names = [collection.name for collection in collections]
    
    if COLLECTION_NAME in collection_names:
        # Get all document IDs from Qdrant collection
        list = client.scroll(collection_name=COLLECTION_NAME, with_payload=["doc_id"], with_vectors=False, limit=1000000)
        
        if list[0]:  # Check if there are any documents
            # Extract all doc_ids
            doc_ids = [item.payload["doc_id"] for item in list[0]]
            
            # Use batch update with IN clause for better performance
            if C4AI:
                cursor.execute("""
                    UPDATE th.insurer_pages_c4ai
                    SET embedded = true
                    WHERE id = ANY(%s)
                """, (doc_ids,))
            else:
                cursor.execute("""
                    UPDATE th.insurer_page_content
                    SET embedded = true
                    WHERE id = ANY(%s)
                """, (doc_ids,))
            
            # Single commit for all updates
            db_conn.commit()
            logging.info(f"Updated {len(doc_ids)} documents in collection {COLLECTION_NAME} from Qdrant")
        else:
            logging.info(f"No documents found in collection {COLLECTION_NAME}")

def create_qdrant_collection(client: QdrantClient):
    """Create Qdrant collection if it doesn't exist."""
    collections = client.get_collections().collections
    collection_names = [collection.name for collection in collections]
    
    if COLLECTION_NAME not in collection_names:
        client.create_collection(
            collection_name=COLLECTION_NAME,
            vectors_config=models.VectorParams(
                size=EMBEDDING_DIM,
                distance=models.Distance.COSINE
            )
        )
        logging.info(f"Created collection {COLLECTION_NAME} in Qdrant")
    else:
        logging.info(f"Collection {COLLECTION_NAME} already exists in Qdrant")

def generateChunks(text: str):
  # Initialize tiktoken encoder for the embedding model
  encoder = tiktoken.encoding_for_model("text-embedding-3-large");

  textSplitter = RecursiveCharacterTextSplitter(
    chunk_size= CHUNK_SIZE, # Maximum tokens per chunk
    chunk_overlap= CHUNK_OVERLAP, # 20% overlap to preserve context
    separators= ["\n\n", "\n", " ", ""], # Logical split points
    length_function= lambda text: len(encoder.encode(text))
  )
  
  return textSplitter.split_text(text)


def generate_chunk_id(page_url: str, chunk_index: int) -> str:
    """Generate a unique ID for a chunk."""
    return hashlib.md5(f"{page_url}_{chunk_index}".encode()).hexdigest()

async def get_embedding(text: str, session: aiohttp.ClientSession, rate_limiter: DynamicRateLimiter = None) -> List[float]:
    """Get embedding from Azure OpenAI API with dynamic rate limiting."""
    endpoint = os.getenv("AZURE_EMBEDDING_ENDPOINT")
    api_key = os.getenv("AZURE_API_KEY")
    
    if not endpoint or not api_key:
        raise ValueError("Azure OpenAI API credentials are missing in .env file")
    
    headers = {
        "Content-Type": "application/json",
        "api-key": api_key
    }
    
    data = {
        "input": text,
        "model": "text-embedding-3-large"  # This might be redundant as it's in the endpoint
    }
    
    try:
        startTime = datetime.datetime.now()
        logging.info(f"Sending request to Azure API")
        async with session.post(endpoint, headers=headers, json=data) as response:
            if response.status == 429:  # Rate limit exceeded
                # Read retry-after header for dynamic delay
                retry_after = response.headers.get('retry-after', '1')
                try:
                    delay_seconds = int(retry_after)
                except ValueError:
                    delay_seconds = 1  # Default to 1 second if header is invalid
                
                # Adjust rate limiter if provided
                if rate_limiter:
                    rate_limiter.adjust_for_rate_limit(delay_seconds)
                
                logging.warning(f"Rate limit exceeded. Waiting {delay_seconds} seconds as per retry-after header")
                await asyncio.sleep(delay_seconds)
                raise Exception("Rate limit exceeded - retry after delay")
            
            elif response.status != 200:
                error_text = await response.text()
                raise Exception(f"Azure API error: {error_text}")

            result = await response.json()
            endTime = datetime.datetime.now()
            logging.info(f"Time taken: {endTime - startTime} seconds")
            
            # Adjust rate limiter for successful requests
            if rate_limiter:
                rate_limiter.adjust_for_success()
                
            return result["data"][0]["embedding"]
    except Exception as e:
        if "Rate limit exceeded" in str(e):
            # This exception is raised by our own code after handling 429
            raise
        elif "429" in str(e):
            logging.warning(f"Rate limit exceeded for Azure API")
            await asyncio.sleep(1)
        else:
            logging.error(f"Error getting embedding: {e}")
        raise

async def process_chunk_with_rate_limit(chunk_data: tuple, session: aiohttp.ClientSession, rate_limiter: DynamicRateLimiter) -> models.PointStruct:
    """Process a single chunk with dynamic rate limiting and retry logic."""
    doc_id, insurer_name, page_url, chunk_idx, chunk, total_chunks = chunk_data
    
    async with rate_limiter.semaphore:  # This ensures we don't exceed current dynamic limit
        max_retries = 3
        base_delay = 1
        
        for attempt in range(max_retries):
            try:
                # Get embedding from Azure API with dynamic rate limiting
                embedding = await get_embedding(chunk, session, rate_limiter)
                
                # Create point for Qdrant
                point = models.PointStruct(
                    id=generate_chunk_id(page_url, chunk_idx),
                    vector=embedding,
                    payload={
                        "doc_id": doc_id,
                        "page_url": page_url,
                        "chunk_index": chunk_idx,
                        "chunk_text": chunk,
                        "total_chunks": total_chunks,
                        "insurer_name": insurer_name
                    }
                )
                
                # Small delay to respect rate limits (only if not rate limited)
                await asyncio.sleep(REQUEST_DELAY)
                return point
                
            except Exception as e:
                if "Rate limit exceeded" in str(e):
                    if attempt < max_retries - 1:
                        # Exponential backoff: 1s, 2s, 4s
                        delay = base_delay * (2 ** attempt)
                        logging.warning(f"Rate limit hit for chunk {chunk_idx}, retrying in {delay}s (attempt {attempt + 1}/{max_retries})")
                        await asyncio.sleep(delay)
                        continue
                    else:
                        logging.error(f"Max retries exceeded for chunk {chunk_idx} due to rate limiting")
                        return None
                else:
                    logging.error(f"Error processing chunk {chunk_idx} for {page_url}: {e}")
                    return None
        
        return None

async def process_documents():
    """Main function to process documents and store embeddings."""
    load_dotenv()
    
    # Initialize clients
    db_conn = None
    qdrant_client = None
    http_session = None
    
    try:
        db_conn = get_db_connection()
        qdrant_client = get_qdrant_client()
        http_session = aiohttp.ClientSession()
        
        # Create dynamic rate limiter for adaptive concurrency control
        rate_limiter = DynamicRateLimiter(MAX_CONCURRENT_REQUESTS)
        
        create_qdrant_collection(qdrant_client)
        
        with db_conn.cursor() as cur:
            update_documents_in_qdrant_collection(qdrant_client, cur, db_conn)

            # Get all documents that haven't been processed yet
            if C4AI:
                cur.execute("""
                    SELECT id, insurer_name, page_url, markdown 
                    FROM th.insurer_pages_c4ai 
                    WHERE markdown IS NOT NULL and embedded = false
                    ORDER BY id;
                """)
            else:
                cur.execute("""
                    SELECT id, insurer_name, page_url, markdown 
                    FROM th.insurer_page_content 
                    WHERE markdown IS NOT NULL
                    ORDER BY id;
                """)
            
            while True:
                batch = cur.fetchmany(BATCH_SIZE)
                if not batch:
                    break
                
                # Prepare all chunks for rate-limited concurrent processing
                chunk_tasks = []
                
                for doc_id, insurer_name, page_url, markdown in batch:
                    if not markdown:
                        continue
                    
                    # Split document into chunks if needed
                    chunks = generateChunks(markdown)
                    
                    for chunk_idx, chunk in enumerate(chunks):
                        chunk_data = (doc_id, insurer_name, page_url, chunk_idx, chunk, len(chunks))
                        task = process_chunk_with_rate_limit(chunk_data, http_session, rate_limiter)
                        chunk_tasks.append(task)
                
                if chunk_tasks:
                    # Process all chunks with dynamic rate limiting
                    logging.info(f"Processing {len(chunk_tasks)} chunks with dynamic rate limiting (current limit: {rate_limiter.current_limit})")
                    results = await asyncio.gather(*chunk_tasks, return_exceptions=True)
                    
                    # Filter out None results (failed chunks) and exceptions
                    points_to_upsert = []
                    for result in results:
                        if isinstance(result, Exception):
                            logging.error(f"Chunk processing failed: {result}")
                        elif result is not None:
                            points_to_upsert.append(result)
                    
                    if points_to_upsert:
                        try:
                            # Upsert points to Qdrant with retry logic
                            max_retries = 3
                            for attempt in range(max_retries):
                                try:
                                    qdrant_client.upsert(
                                        collection_name=COLLECTION_NAME,
                                        points=points_to_upsert
                                    )
                                    logging.info(f"Upserted {len(points_to_upsert)} chunks to Qdrant")
                                    break
                                except Exception as e:
                                    if attempt == max_retries - 1:
                                        raise
                                    logging.warning(f"Retry {attempt + 1}/{max_retries} after error: {e}")
                                    await asyncio.sleep(1)  # Wait before retry
                        except Exception as e:
                            logging.error(f"Failed to upsert batch after {max_retries} attempts: {e}")
                            raise
    
    except Exception as e:
        logging.error(f"Error processing documents: {e}")
        raise
    finally:
        # Cleanup connections
        if db_conn:
            try:
                db_conn.close()
                logging.info("Database connection closed")
            except Exception as e:
                logging.warning(f"Error closing database connection: {e}")
        
        if qdrant_client:
            try:
                qdrant_client.close()
                logging.info("Qdrant client connection closed")
            except Exception as e:
                logging.warning(f"Error closing Qdrant client connection: {e}")
        
        if http_session:
            try:
                await http_session.close()
                logging.info("HTTP session closed")
            except Exception as e:
                logging.warning(f"Error closing HTTP session: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(process_documents())
    except KeyboardInterrupt:
        logging.info("Process interrupted by user")
    except Exception as e:
        logging.error(f"Process failed: {e}")
        raise