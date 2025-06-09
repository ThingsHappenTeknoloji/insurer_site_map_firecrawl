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


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Constants
CHUNK_SIZE = 8000  # bytes
CHUNK_OVERLAP = 2000  # bytes
BATCH_SIZE = 32  # Number of documents to process in one batch
COLLECTION_NAME = "insurer_pages_c4ai"
EMBEDDING_DIM = 3072  # Dimension for Azure text-embedding-3-large model
C4AI = True

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

def delete_qdrant_collection(client: QdrantClient):
    """Delete Qdrant collection if it exists."""
    collections = client.get_collections().collections
    collection_names = [collection.name for collection in collections]
    
    if COLLECTION_NAME in collection_names:
        client.delete_collection(collection_name=COLLECTION_NAME)
        logging.info(f"Deleted collection {COLLECTION_NAME} from Qdrant")

def create_qdrant_collection(client: QdrantClient):
    """Create Qdrant collection if it doesn't exist."""
    collections = client.get_collections().collections
    collection_names = [collection.name for collection in collections]
    
    if COLLECTION_NAME in collection_names:
        delete_qdrant_collection(client)
    
    client.create_collection(
        collection_name=COLLECTION_NAME,
        vectors_config=models.VectorParams(
            size=EMBEDDING_DIM,
            distance=models.Distance.COSINE
        )
    )
    logging.info(f"Created collection {COLLECTION_NAME} in Qdrant")


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

async def get_embedding(text: str, session: aiohttp.ClientSession) -> List[float]:
    """Get embedding from Azure OpenAI API."""
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
        await asyncio.sleep(1)
        async with session.post(endpoint, headers=headers, json=data) as response:
            if response.status != 200:
                error_text = await response.text()
                raise Exception(f"Azure API error: {error_text}")

            result = await response.json()
            return result["data"][0]["embedding"]
    except Exception as e:
        if "429" in str(e):
            logging.warning(f"Rate limit exceeded for Azure API")
        else:
            logging.error(f"Error getting embedding: {e}")
        raise
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
        
        create_qdrant_collection(qdrant_client)
        
        with db_conn.cursor() as cur:
            # Get all documents that haven't been processed yet
            if C4AI:
                cur.execute("""
                    SELECT id, insurer_name, page_url, markdown 
                    FROM th.insurer_pages_c4ai 
                    WHERE markdown IS NOT NULL
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
                
                points_to_upsert = []
                
                for doc_id, insurer_name, page_url, markdown in batch:
                    if not markdown:
                        continue
                    
                    # Split document into chunks if needed
                    chunks = generateChunks(markdown)
                    
                    for chunk_idx, chunk in enumerate(chunks):
                        try:
                            # Get embedding from Azure API
                            embedding = await get_embedding(chunk, http_session)
                            
                            # Create point for Qdrant
                            point = models.PointStruct(
                                id=generate_chunk_id(page_url, chunk_idx),
                                vector=embedding,
                                payload={
                                    "doc_id": doc_id,
                                    "page_url": page_url,
                                    "chunk_index": chunk_idx,
                                    "chunk_text": chunk,
                                    "total_chunks": len(chunks),
                                    "insurer_name": insurer_name
                                }
                            )
                            points_to_upsert.append(point)
                        except Exception as e:
                            logging.error(f"Error processing chunk {chunk_idx} for {page_url}: {e}")
                            continue
                
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
                
                # Small delay between batches to respect API rate limits
                await asyncio.sleep(0.5)
    
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