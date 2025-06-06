import os
import psycopg2
from psycopg2 import sql, extras
from dotenv import load_dotenv
import logging
import asyncio
import aiohttp
import json
from typing import List, Dict, Any
from collections import Counter

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Constants
BATCH_SIZE = 1 # Smaller batch size for API rate limits
SYSTEM_PROMPT = """You are an expert insurance content analyst. Your task is to analyze a collection of insurance-related web pages and suggest a comprehensive list of classification categories that would be most useful for organizing this content.

For each page, identify:
1. The main topic/purpose of the page
2. Any sub-topics or related themes
3. The type of content (e.g., informational, transactional, legal, etc.)
4. The target audience

Respond in JSON format with a list of suggested categories, where each category has:
{
    "category_name": "string",
    "description": "string explaining what belongs in this category",
    "example_topics": ["list", "of", "example", "topics"],
    "target_audience": ["list", "of", "target", "audiences"]
}"""

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

async def analyze_content(text: str, session: aiohttp.ClientSession) -> Dict[str, Any]:
    """Get content analysis from Azure OpenAI API."""
    endpoint = os.getenv("AZURE_ENDPOINT")
    api_key = os.getenv("AZURE_API_KEY")
    
    if not endpoint or not api_key:
        raise ValueError("Azure OpenAI API credentials are missing in .env file")
    
    headers = {
        "Content-Type": "application/json",
        "api-key": api_key
    }
    
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": f"Please analyze this insurance page content and suggest appropriate classification categories:\n\n{text}"}
    ]
    
    data = {
        "messages": messages,
        "temperature": 0.7,  # Higher temperature for more diverse category suggestions
        "max_tokens": 1000
    }
    
    try:
        await asyncio.sleep(1)  # Rate limiting
        async with session.post(endpoint, headers=headers, json=data) as response:
            if response.status != 200:
                error_text = await response.text()
                if "429" in error_text:
                    logging.warning("Rate limit exceeded for Azure API")
                    await asyncio.sleep(5)  # Wait longer on rate limit
                    raise Exception("Rate limit exceeded")
                raise Exception(f"Azure API error: {error_text}")
            
            result = await response.json()
            try:
                analysis_text = result["choices"][0]["message"]["content"]
                analysis_text = analysis_text.replace("```json", "").replace("```", "")
                analysis = json.loads(analysis_text)
                return analysis
            except (json.JSONDecodeError, KeyError) as e:
                logging.error(f"Error parsing analysis response: {e}")
                logging.error(f"Raw response: {analysis_text}")
                raise
    except Exception as e:
        if "429" in str(e):
            logging.warning("Rate limit exceeded for Azure API")
        else:
            logging.error(f"Error getting analysis: {e}")
        raise

async def process_documents():
    """Main function to analyze documents and generate classification categories."""
    load_dotenv()
    
    # Initialize clients
    db_conn = None
    http_session = None
    
    try:
        db_conn = get_db_connection()
        http_session = aiohttp.ClientSession()
        
        # Dictionary to store category suggestions and their frequencies
        category_suggestions = Counter()
        category_details = {}
        
        with db_conn.cursor() as cur:
            # Get a sample of documents for analysis
            cur.execute("""
                SELECT id, page_url, markdown 
                FROM th.insurer_page_content 
                WHERE markdown IS NOT NULL
                ORDER BY id
                LIMIT 100;  -- Analyze a sample of pages
            """)
            
            while True:
                batch = cur.fetchmany(BATCH_SIZE)
                if not batch:
                    break
                
                for doc_id, page_url, markdown in batch:
                    if not markdown:
                        continue
                    
                    try:
                        # Get content analysis from Azure API
                        analysis = await analyze_content(markdown, http_session)
                        
                        # Process the suggested categories
                        for category in analysis:
                            category_name = category["category_name"]
                            category_suggestions[category_name] += 1
                            
                            # Store the most detailed description we've seen for each category
                            if category_name not in category_details or \
                               len(category["description"]) > len(category_details[category_name]["description"]):
                                category_details[category_name] = category
                        
                        logging.info(f"Analyzed page {page_url}")
                        
                    except Exception as e:
                        logging.error(f"Error processing page {page_url}: {e}")
                        continue
                
                # Small delay between batches to respect API rate limits
                await asyncio.sleep(2)
        
        # Print the final category suggestions
        print("\nSuggested Classification Categories:")
        print("===================================")
        for category_name, count in category_suggestions.most_common():
            details = category_details[category_name]
            print(f"\nCategory: {category_name}")
            print(f"Frequency: {count} pages")
            print(f"Description: {details['description']}")
            print(f"Example Topics: {', '.join(details['example_topics'])}")
            print(f"Target Audience: {', '.join(details['target_audience'])}")
            print("-" * 50)
        
        # Save the categories to a JSON file
        with open('insurer_page_categories.json', 'w') as f:
            json.dump({
                'categories': [
                    {
                        'name': category_name,
                        'frequency': count,
                        **category_details[category_name]
                    }
                    for category_name, count in category_suggestions.most_common()
                ]
            }, f, indent=2)
        
        logging.info("Category analysis complete. Results saved to insurer_page_categories.json")
    
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