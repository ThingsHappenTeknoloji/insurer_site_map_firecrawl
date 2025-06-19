# Insurer Website Scraping and Embedding Pipeline

This project is a data pipeline designed to scrape, process, and create vector embeddings for the content of insurer websites. The pipeline uses Firecrawl for scraping, PostgreSQL for storing raw and processed data, Azure OpenAI for generating embeddings, and Qdrant for storing the vector embeddings.

## Project Workflow

The pipeline consists of three main stages:

1.  **Scraping (`scrape_insurer_pages.py`)**:
    *   Fetches URLs of insurer web pages from a PostgreSQL database (`th.insurer_pages` table).
    *   Uses the Firecrawl service to scrape the content of these pages.
    *   Stores the scraped content (in Markdown format) and other metadata into the `th.insurer_page_content` table in PostgreSQL.

2.  **Content Classification (`insurer_page_classifier.py`)**:
    *   This is an analysis step. It takes a sample of the scraped content from the database.
    *   It uses an Azure OpenAI model to analyze the text and suggest suitable classification categories.
    *   The results are aggregated and saved into `insurer_page_categories.json`. This provides insights into the types of content being scraped.

3.  **Embedding (`embed_insurer_pages.py`)**:
    *   Fetches the scraped page content from PostgreSQL.
    *   Splits the text content into smaller, manageable chunks suitable for embedding models.
    *   Uses Azure OpenAI's `text-embedding-3-large` model to convert each text chunk into a vector embedding.
    *   Stores the generated embeddings in a Qdrant vector database collection named `insurer_pages_c4ai`.
    *   Updates a flag in the PostgreSQL table to mark the content as "embedded" to prevent reprocessing.

The project also contains `_crawl4ai` versions of scripts, which likely perform similar actions but may be configured for a different data source or scraping strategy.

## Alternative Workflow using `crawl4ai`

This project includes an alternative data processing workflow using the `crawl4ai` library. Unlike the Firecrawl-based scripts that separate URL discovery from content scraping, the `crawl4ai` scripts perform both actions in a single step.

-   **`process_insurer_sitemaps_crawl4ai.py`**:
    -   Fetches insurer root URLs from the `th.insurer` table.
    -   Uses `crawl4ai` to asynchronously crawl the websites, discovering internal links and scraping their content simultaneously.
    -   Stores the scraped content directly into the `th.insurer_pages_c4ai` table, which includes a `markdown` column for the page content. This script bypasses the need for a separate scraping step like `scrape_insurer_pages.py`.

This approach provides a more integrated and efficient way to gather page content directly during the crawling phase.

## Prerequisites

- Python 3.12+
- A running PostgreSQL instance.
- A running Qdrant instance.
- An Azure account with access to OpenAI models.
- A Firecrawl API key.

## Setup

1.  **Clone the repository:**
    ```bash
    git clone <repository-url>
    cd insurer_site_map_firecrawl
    ```

2.  **Create a virtual environment and install dependencies:**
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
    pip install -r requirements.txt
    ```

3.  **Configure Environment Variables:**
    Create a `.env` file in the root of the project and add the following credentials.

    ```dotenv
    # PostgreSQL Connection
    PGHOST=your_postgres_host
    PGDATABASE=your_postgres_db
    PGUSER=your_postgres_user
    PGPASSWORD=your_postgres_password

    # Firecrawl API Key
    FC_APIKEY=your_firecrawl_api_key

    # Azure OpenAI
    AZURE_ENDPOINT=your_azure_openai_endpoint_for_chat
    AZURE_EMBEDDING_ENDPOINT=your_azure_openai_endpoint_for_embeddings
    AZURE_API_KEY=your_azure_api_key

    # Qdrant
    QDRANT_URL=http://localhost:6333
    QDRANT_API_KEY=your_qdrant_api_key # (if applicable)
    ```

## Running the Pipeline

You can run the scripts in sequence. Ensure your database contains the initial URLs to be scraped in the `th.insurer_pages` table.

1.  **Run the scraper:**
    ```bash
    python scrape_insurer_pages.py
    ```

2.  **Run the classifier (optional analysis step):**
    ```bash
    python insurer_page_classifier.py
    ```

3.  **Run the embedder:**
    ```bash
    python embed_insurer_pages.py
    ```

### Running the `crawl4ai` Processor

Alternatively, to use the `crawl4ai` workflow, which crawls and scrapes in one step, run the following script. This populates the `th.insurer_pages_c4ai` table.

```bash
python process_insurer_sitemaps_crawl4ai.py
```
