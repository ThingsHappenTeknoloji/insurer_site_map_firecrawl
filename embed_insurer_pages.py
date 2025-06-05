import os
import psycopg2
from psycopg2 import sql, extras
from dotenv import load_dotenv
import logging
import asyncio
from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient
from qdrant_client.http import models
import numpy as np
from typing import List, Dict, Any
import hashlib

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Constants
CHUNK_SIZE = 8000  # bytes
CHUNK_OVERLAP = 2000  # bytes
BATCH_SIZE = 32  # Number of documents to process in one batch
COLLECTION_NAME = "insurer_pages"
EMBEDDING_DIM = 768  # Dimension for all-MiniLM-L6-v2 model

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
    qdrant_host = os.getenv("QDRANT_HOST", "localhost")
    qdrant_port = int(os.getenv("QDRANT_PORT", "6333"))
    return QdrantClient(host=qdrant_host, port=qdrant_port)

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

def chunk_text(text: str) -> List[str]:
    """Split text into overlapping chunks of specified size."""
    if not text:
        return []
    
    # Convert to bytes for accurate size measurement
    text_bytes = text.encode('utf-8')
    chunks = []
    
    if len(text_bytes) <= CHUNK_SIZE:
        return [text]
    
    start = 0
    while start < len(text_bytes):
        # Get chunk of CHUNK_SIZE bytes
        chunk_bytes = text_bytes[start:start + CHUNK_SIZE]
        
        # Convert back to string, ensuring we don't cut in the middle of a character
        chunk = chunk_bytes.decode('utf-8', errors='ignore')
        
        # If this is not the first chunk, try to find a good split point
        if start > 0:
            # Look for the last newline or space in the overlap region
            overlap_text = chunk[:CHUNK_OVERLAP]
            split_point = max(
                overlap_text.rfind('\n'),
                overlap_text.rfind(' '),
                CHUNK_OVERLAP // 2  # Fallback to middle of overlap if no good split point
            )
            if split_point > 0:
                chunk = chunk[split_point:].lstrip()
        
        chunks.append(chunk)
        start += CHUNK_SIZE - CHUNK_OVERLAP
    
    return chunks

def generate_chunk_id(page_url: str, chunk_index: int) -> str:
    """Generate a unique ID for a chunk."""
    return hashlib.md5(f"{page_url}_{chunk_index}".encode()).hexdigest()

async def process_documents():
    """Main function to process documents and store embeddings."""
    load_dotenv()
    
    # Initialize models and clients
    model = SentenceTransformer('all-MiniLM-L6-v2')
    db_conn = get_db_connection()
    qdrant_client = get_qdrant_client()
    
    try:
        create_qdrant_collection(qdrant_client)
        
        with db_conn.cursor() as cur:
            # Get all documents that haven't been processed yet
            cur.execute("""
                SELECT id, page_url, markdown 
                FROM th.insurer_page_content 
                WHERE markdown IS NOT NULL
                ORDER BY id;
            """)
            
            while True:
                batch = cur.fetchmany(BATCH_SIZE)
                if not batch:
                    break
                
                points_to_upsert = []
                
                for doc_id, page_url, markdown in batch:
                    if not markdown:
                        continue
                    
                    # Split document into chunks if needed
                    chunks = chunk_text(markdown)
                    
                    for chunk_idx, chunk in enumerate(chunks):
                        # Generate embedding for the chunk
                        embedding = model.encode(chunk)
                        
                        # Create point for Qdrant
                        point = models.PointStruct(
                            id=generate_chunk_id(page_url, chunk_idx),
                            vector=embedding.tolist(),
                            payload={
                                "doc_id": doc_id,
                                "page_url": page_url,
                                "chunk_index": chunk_idx,
                                "chunk_text": chunk,
                                "total_chunks": len(chunks)
                            }
                        )
                        points_to_upsert.append(point)
                
                if points_to_upsert:
                    # Upsert points to Qdrant
                    qdrant_client.upsert(
                        collection_name=COLLECTION_NAME,
                        points=points_to_upsert
                    )
                    logging.info(f"Upserted {len(points_to_upsert)} chunks to Qdrant")
                
                # Small delay between batches
                await asyncio.sleep(0.1)
    
    except Exception as e:
        logging.error(f"Error processing documents: {e}")
        raise
    finally:
        db_conn.close()
        logging.info("Database connection closed")

if __name__ == "__main__":
    asyncio.run(process_documents()) 