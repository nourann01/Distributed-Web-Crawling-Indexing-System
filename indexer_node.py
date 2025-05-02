import time
import logging
import json
import os
import boto3
import hashlib
import shutil
import psycopg2
from psycopg2 import sql
from psycopg2.extras import DictCursor
from datetime import datetime
from whoosh.index import create_in, open_dir
from whoosh.fields import Schema, TEXT, ID, DATETIME, KEYWORD, STORED
from whoosh.analysis import StemmingAnalyzer, CharsetFilter, RegexTokenizer
from whoosh.support.charset import accent_map
from whoosh.qparser import QueryParser, MultifieldParser, OrGroup, FuzzyTermPlugin, PhrasePlugin
import whoosh.index as index
from urllib.parse import urlparse
import re
from bs4 import BeautifulSoup

# AWS SQS Configuration
sqs = boto3.client('sqs', region_name='us-east-1')
indexer_queue_url = 'https://sqs.us-east-1.amazonaws.com/969510159350/indexer-queue.fifo'
result_queue_url = 'https://sqs.us-east-1.amazonaws.com/969510159350/result-queue.fifo'

# RDS PostgreSQL Configuration
DB_CONFIG = {
    'host': 'dbdistproj.c8v2o28aq6x6.us-east-1.rds.amazonaws.com',  # Replace with your RDS endpoint
    'dbname': 'dbdistproj',
    'user': 'bialy',
    'password': 'midomido15',  # Use environment variables in production
    'port': 5432
}

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - Indexer - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("indexer.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("indexer")

# Constants
INDEX_DIR = "search_index"
MAX_IDLE_COUNT = 30
PROCESSING_DELAY = 3
BACKOFF_TIME = 10
MAX_RETRIES = 3

# Add a version file to track schema version
SCHEMA_VERSION = "1.2"  # Incremented for PostgreSQL integration
SCHEMA_VERSION_FILE = os.path.join(INDEX_DIR, "schema_version.txt")

def create_custom_analyzer():
    """Create a custom analyzer with stemming and accent handling"""
    # Create a stemming analyzer that also handles accents
    my_analyzer = StemmingAnalyzer()
    # Add a filter to handle accented characters
    my_analyzer = my_analyzer | CharsetFilter(accent_map)
    return my_analyzer

def setup_index():
    """Initialize or open the search index with an enhanced schema"""
    if not os.path.exists(INDEX_DIR):
        os.makedirs(INDEX_DIR)
        create_new_index = True
    else:
        # Check if schema version has changed
        create_new_index = False
        if os.path.exists(SCHEMA_VERSION_FILE):
            with open(SCHEMA_VERSION_FILE, 'r') as f:
                stored_version = f.read().strip()
                if stored_version != SCHEMA_VERSION:
                    logger.info(f"Schema version changed from {stored_version} to {SCHEMA_VERSION}. Recreating index.")
                    create_new_index = True
        else:
            # No version file exists, assume we need to recreate
            create_new_index = True
    
    # If we need to create a new index, remove the old one first
    if create_new_index and os.path.exists(INDEX_DIR):
        try:
            # Backup the old index first
            backup_dir = f"{INDEX_DIR}_backup_{int(time.time())}"
            if os.path.isdir(INDEX_DIR) and os.listdir(INDEX_DIR):  # Only if it exists and is not empty
                shutil.copytree(INDEX_DIR, backup_dir)
                logger.info(f"Backed up old index to {backup_dir}")
            
            # Remove all files except the directory itself
            for filename in os.listdir(INDEX_DIR):
                file_path = os.path.join(INDEX_DIR, filename)
                try:
                    if os.path.isfile(file_path) or os.path.islink(file_path):
                        os.unlink(file_path)
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)
                except Exception as e:
                    logger.error(f"Failed to delete {file_path}. Reason: {e}")
        except Exception as e:
            logger.error(f"Error during index recreation: {e}")

    # Enhanced schema with more field types and better analyzers
    analyzer = create_custom_analyzer()
    
    schema = Schema(
        url=ID(stored=True, unique=True),
        domain=KEYWORD(stored=True, commas=True),  # Store domain for filtering
        title=TEXT(stored=True, analyzer=analyzer),
        content=TEXT(stored=True, analyzer=analyzer),
        keywords=KEYWORD(stored=True, commas=True),  # For extracted keywords
        summary=STORED,  # Store a content summary
        content_type=KEYWORD(stored=True),  # Document type (html, pdf, etc)
        timestamp=DATETIME(stored=True),
        last_updated=DATETIME(stored=True)
    )

    if not index.exists_in(INDEX_DIR):
        idx = create_in(INDEX_DIR, schema)
        logger.info(f"Created new index in {INDEX_DIR}")
        
        # Write version file
        with open(SCHEMA_VERSION_FILE, 'w') as f:
            f.write(SCHEMA_VERSION)
    else:
        idx = open_dir(INDEX_DIR)
        logger.info(f"Opened existing index in {INDEX_DIR}")

        # Still update version file to prevent future unnecessary rebuilds
        if create_new_index:
            with open(SCHEMA_VERSION_FILE, 'w') as f:
                f.write(SCHEMA_VERSION)

    return idx

def setup_database():
    """Initialize PostgreSQL database tables if they don't exist"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Create tables if they don't exist
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS indexed_documents (
            id SERIAL PRIMARY KEY,
            url TEXT UNIQUE NOT NULL,
            domain TEXT NOT NULL,
            title TEXT,
            summary TEXT,
            content_type TEXT,
            keywords TEXT,
            timestamp TIMESTAMP,
            last_updated TIMESTAMP,
            index_status TEXT
        )
        """)
        
        # Create table for search statistics
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS search_statistics (
            id SERIAL PRIMARY KEY,
            search_term TEXT NOT NULL,
            search_count INTEGER DEFAULT 1,
            last_searched TIMESTAMP
        )
        """)
        
        conn.commit()
        logger.info("Database tables initialized successfully")
        
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Database initialization error: {e}")
        return False

def extract_domain(url):
    """Extract domain from URL"""
    parsed_url = urlparse(url)
    return parsed_url.netloc

def extract_keywords(content, max_keywords=10):
    """
    Enhanced keyword extraction using frequency analysis and HTML structure awareness
    """
    if not content:
        return []
        
    # Check if the content is HTML and extract text more intelligently
    if content.strip().startswith('<') and '>' in content:
        try:
            soup = BeautifulSoup(content, 'html.parser')
            
            # Give higher weight to words in headings and emphasized text
            important_tags = soup.find_all(['h1', 'h2', 'h3', 'strong', 'b', 'em'])
            important_text = ' '.join([tag.get_text() for tag in important_tags])
            
            # Get regular content
            regular_text = soup.get_text()
            
            # Combine with extra weight to important text
            text = regular_text + ' ' + important_text * 3
        except Exception as e:
            logger.error(f"Error parsing HTML for keyword extraction: {e}")
            text = content
    else:
        text = content
    
    # Convert to lowercase and remove common punctuation
    text = re.sub(r'[^\w\s]', ' ', text.lower())
    
    # Extended stop words list
    stop_words = set([
        'the', 'and', 'is', 'in', 'to', 'of', 'a', 'for', 'with', 'on', 'at', 'from',
        'by', 'an', 'that', 'this', 'be', 'are', 'as', 'it', 'its', 'or', 'was', 'were',
        'has', 'have', 'had', 'not', 'what', 'when', 'where', 'who', 'how', 'why',
        'but', 'if', 'because', 'as', 'until', 'while', 'than'
    ])
    
    # Filter and normalize words
    words = []
    for word in text.split():
        if word not in stop_words and len(word) > 2:
            # Basic stemming (could use a proper stemmer from NLTK in production)
            if word.endswith('ing'):
                word = word[:-3]
            elif word.endswith('s') and not word.endswith('ss'):
                word = word[:-1]
            elif word.endswith('ed'):
                word = word[:-2]
            words.append(word)
    
    # Count word frequencies
    word_freq = {}
    for word in words:
        word_freq[word] = word_freq.get(word, 0) + 1
    
    # Sort by frequency and take top keywords
    sorted_words = sorted(word_freq.items(), key=lambda x: x[1], reverse=True)
    keywords = [word for word, _ in sorted_words[:max_keywords]]
    
    return keywords

def generate_summary(content, max_length=200):
    """Generate a short summary of the content"""
    if not content:
        return "No content available"
    
    # Simple summary: first few sentences or characters
    content = re.sub(r'\s+', ' ', content).strip()
    if len(content) <= max_length:
        return content
    
    # Try to break at a sentence
    sentences = re.split(r'(?<=[.!?])\s+', content[:max_length+100])
    summary = ""
    for sentence in sentences:
        if len(summary + sentence) <= max_length:
            summary += sentence + " "
        else:
            break
    
    if not summary:  # If no good sentence break, just truncate
        summary = content[:max_length]
    
    return summary.strip() + "..."

def receive_task():
    """Receive a task from the SQS queue with error handling"""
    try:
        response = sqs.receive_message(
            QueueUrl=indexer_queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=10,
            AttributeNames=['All'],
            MessageAttributeNames=['All']
        )
        
        if 'Messages' in response:
            message = response['Messages'][0]
            try:
                data = json.loads(message['Body'])
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON in message: {e}")
                # Delete malformed message to avoid queue clogging
                sqs.delete_message(QueueUrl=indexer_queue_url, ReceiptHandle=message['ReceiptHandle'])
                return None, None
                
            receipt_handle = message['ReceiptHandle']
            return data, receipt_handle
    except Exception as e:
        logger.error(f"Error receiving message from queue: {e}")
    
    return None, None

def process_content(task):
    """Process and enhance the content for better indexing"""
    # Extract existing fields
    url = task['url']
    title = task.get('title', 'No Title')
    content = task.get('content', '')
    timestamp = task.get('timestamp', time.time())
    
    # Generate additional metadata
    domain = extract_domain(url)
    keywords = extract_keywords(content)
    summary = generate_summary(content)
    
    # Determine content type (basic implementation)
    content_type = 'html'  # Default
    if url.endswith('.pdf'):
        content_type = 'pdf'
    elif url.endswith(('.doc', '.docx')):
        content_type = 'document'
    
    # Create document with enhanced metadata
    document = {
        'url': url,
        'domain': domain,
        'title': title,
        'content': content,
        'keywords': ','.join(keywords),
        'summary': summary,
        'content_type': content_type,
        'timestamp': datetime.fromtimestamp(timestamp),
        'last_updated': datetime.now()
    }
    
    return document

def index_document(idx, document, retry_count=0):
    """Index a document with retry mechanism and PostgreSQL storage"""
    writer = None
    try:
        # First, update Whoosh index
        writer = idx.writer()
        writer.update_document(**document)
        writer.commit()
        
        # Then, store metadata in PostgreSQL
        store_document_in_db(document)
        
        return True
    except Exception as e:
        if writer:
            writer.cancel()
        
        logger.error(f"Error indexing document: {e}")
        
        # Implement exponential backoff for retries
        if retry_count < MAX_RETRIES:
            backoff_time = BACKOFF_TIME * (2 ** retry_count)
            logger.info(f"Retrying indexing in {backoff_time} seconds (attempt {retry_count+1}/{MAX_RETRIES})")
            time.sleep(backoff_time)
            return index_document(idx, document, retry_count + 1)
        else:
            logger.error(f"Failed to index document after {MAX_RETRIES} attempts")
            return False

def store_document_in_db(document):
    """Store document metadata in PostgreSQL database"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Convert datetime objects to strings for JSON serialization
        doc_copy = document.copy()
        if isinstance(doc_copy['timestamp'], datetime):
            doc_copy['timestamp'] = doc_copy['timestamp'].isoformat()
        if isinstance(doc_copy['last_updated'], datetime):
            doc_copy['last_updated'] = doc_copy['last_updated'].isoformat()
        
        cursor.execute("""
        INSERT INTO indexed_documents 
        (url, domain, title, summary, content_type, keywords, timestamp, last_updated, index_status)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (url) DO UPDATE SET
        domain = EXCLUDED.domain,
        title = EXCLUDED.title,
        summary = EXCLUDED.summary,
        content_type = EXCLUDED.content_type,
        keywords = EXCLUDED.keywords,
        last_updated = EXCLUDED.last_updated,
        index_status = EXCLUDED.index_status
        """, (
            document['url'],
            document['domain'],
            document['title'],
            document['summary'],
            document['content_type'],
            document['keywords'],
            document['timestamp'],
            document['last_updated'],
            'indexed'
        ))
        
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Error storing document in database: {e}")
        return False

def send_result(result):
    """Send indexing result to the result queue"""
    try:
        # Add message deduplication ID to ensure exactly-once delivery
        deduplication_id = hashlib.md5(json.dumps(result).encode()).hexdigest()
        
        sqs.send_message(
            QueueUrl=result_queue_url,
            MessageBody=json.dumps(result),
            MessageGroupId='indexing',
            MessageDeduplicationId=deduplication_id
        )
        logger.debug(f"Sent result for {result.get('indexed_url', 'unknown URL')}")
    except Exception as e:
        logger.error(f"Error sending result to queue: {e}")

def search_by_word(query, page=1, results_per_page=10, domain_filter=None, content_type_filter=None):
    """
    Search for documents matching the query with pagination and filtering options
    Returns search results and records search statistics in PostgreSQL
    """
    try:
        # Open the index
        if not os.path.exists(INDEX_DIR):
            logger.error("Search index directory does not exist")
            return {"error": "Search index not available"}, 0
        
        idx = open_dir(INDEX_DIR)
        
        # Create a parser for multiple fields with different weights
        fields = ["title", "content", "keywords"]
        field_boosts = {"title": 2.0, "keywords": 1.5, "content": 1.0}
        
        parser = MultifieldParser(fields, schema=idx.schema, group=OrGroup)
        parser.add_plugin(FuzzyTermPlugin())  # Allow fuzzy matching
        parser.add_plugin(PhrasePlugin())     # Support phrase queries
        
        # Parse the query
        parsed_query = parser.parse(query)
        
        # Prepare search parameters
        search_kwargs = {
            "limit": None,  # Get all results for filtering
            "terms": True,  # Include matched terms in results
            "sortedby": "score",  # Sort by relevance score
        }
        
        # Perform the search
        with idx.searcher() as searcher:
            results = searcher.search(parsed_query, **search_kwargs)
            
            # Apply post-search filters
            filtered_results = []
            for hit in results:
                # Apply domain filter if specified
                if domain_filter and hit['domain'] != domain_filter:
                    continue
                
                # Apply content type filter if specified
                if content_type_filter and hit['content_type'] != content_type_filter:
                    continue
                
                # Add to filtered results
                filtered_results.append({
                    "url": hit["url"],
                    "title": hit["title"],
                    "summary": hit["summary"],
                    "domain": hit["domain"],
                    "content_type": hit["content_type"],
                    "score": hit.score,
                    "matched_terms": list(hit.matched_terms()),
                    "keywords": hit["keywords"].split(",") if hit["keywords"] else [],
                    "timestamp": hit["timestamp"].isoformat() if hit["timestamp"] else None,
                    "last_updated": hit["last_updated"].isoformat() if hit["last_updated"] else None
                })
            
            # Calculate pagination
            total_results = len(filtered_results)
            start_idx = (page - 1) * results_per_page
            end_idx = start_idx + results_per_page
            paginated_results = filtered_results[start_idx:end_idx] if filtered_results else []
            
            # Record search statistics in PostgreSQL
            record_search_statistics(query)
            
            return paginated_results, total_results
    except Exception as e:
        logger.error(f"Search error: {e}")
        return {"error": f"Search failed: {str(e)}"}, 0

def record_search_statistics(query):
    """Record search terms and statistics in PostgreSQL"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Normalize query by converting to lowercase and removing extra spaces
        normalized_query = " ".join(query.lower().split())
        
        # Insert or update search statistics
        cursor.execute("""
        INSERT INTO search_statistics (search_term, search_count, last_searched)
        VALUES (%s, 1, %s)
        ON CONFLICT (search_term) DO UPDATE SET
        search_count = search_statistics.search_count + 1,
        last_searched = %s
        """, (normalized_query, datetime.now(), datetime.now()))
        
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        logger.error(f"Error recording search statistics: {e}")

def get_popular_searches(limit=10):
    """Get the most popular search terms from PostgreSQL"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor(cursor_factory=DictCursor)
        
        cursor.execute("""
        SELECT search_term, search_count, last_searched
        FROM search_statistics
        ORDER BY search_count DESC
        LIMIT %s
        """, (limit,))
        
        popular_searches = [dict(row) for row in cursor]
        
        cursor.close()
        conn.close()
        
        return popular_searches
    except Exception as e:
        logger.error(f"Error retrieving popular searches: {e}")
        return []

def get_indexing_stats():
    """Get indexing statistics from PostgreSQL"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor(cursor_factory=DictCursor)
        
        # Get total count
        cursor.execute("SELECT COUNT(*) as total FROM indexed_documents")
        total = cursor.fetchone()['total']
        
        # Get counts by content type
        cursor.execute("""
        SELECT content_type, COUNT(*) as count
        FROM indexed_documents
        GROUP BY content_type
        ORDER BY count DESC
        """)
        by_content_type = [dict(row) for row in cursor]
        
        # Get counts by domain (top 10)
        cursor.execute("""
        SELECT domain, COUNT(*) as count
        FROM indexed_documents
        GROUP BY domain
        ORDER BY count DESC
        LIMIT 10
        """)
        by_domain = [dict(row) for row in cursor]
        
        # Get recent additions
        cursor.execute("""
        SELECT url, title, domain, last_updated
        FROM indexed_documents
        ORDER BY last_updated DESC
        LIMIT 10
        """)
        recent = [dict(row) for row in cursor]
        
        cursor.close()
        conn.close()
        
        return {
            "total_documents": total,
            "by_content_type": by_content_type,
            "top_domains": by_domain,
            "recent_documents": recent
        }
    except Exception as e:
        logger.error(f"Error retrieving indexing statistics: {e}")
        return {"error": str(e)}

def handle_api_request(request_data):
    """Handle API requests from the master node"""
    request_type = request_data.get("type")
    
    if request_type == "search":
        query = request_data.get("query", "")
        page = int(request_data.get("page", 1))
        results_per_page = int(request_data.get("results_per_page", 10))
        domain_filter = request_data.get("domain_filter")
        content_type_filter = request_data.get("content_type_filter")
        
        results, total = search_by_word(
            query, 
            page=page, 
            results_per_page=results_per_page,
            domain_filter=domain_filter,
            content_type_filter=content_type_filter
        )
        
        return {
            "type": "search_results",
            "query": query,
            "page": page,
            "results_per_page": results_per_page,
            "total_results": total,
            "results": results
        }
    
    elif request_type == "popular_searches":
        limit = int(request_data.get("limit", 10))
        popular = get_popular_searches(limit)
        return {
            "type": "popular_searches",
            "popular_searches": popular
        }
    
    elif request_type == "stats":
        stats = get_indexing_stats()
        return {
            "type": "indexing_stats",
            "stats": stats
        }
    
    else:
        return {
            "type": "error",
            "error": f"Unknown request type: {request_type}"
        }

def indexer_process():
    """Main indexer process loop"""
    # Initialize both Whoosh index and PostgreSQL database
    idx = setup_index()
    db_ready = setup_database()
    
    if not db_ready:
        logger.error("Failed to initialize database. Exiting.")
        return
    
    indexed_count = 0
    failed_count = 0
    idle_counter = 0

    logger.info("Starting indexer process")

    while True:
        task, receipt_handle = receive_task()
        
        # Check if this is an API request instead of indexing task
        if task and "type" in task and task["type"].startswith("api_"):
            logger.info(f"Processing API request: {task['type']}")
            
            # Handle the API request
            result = handle_api_request(task)
            
            # Send the result back
            send_result(result)
            
            # Delete the message
            sqs.delete_message(QueueUrl=indexer_queue_url, ReceiptHandle=receipt_handle)
            
            continue
        
        # Regular indexing task handling
        if not task:
            idle_counter += 1
            logger.info(f"No indexing tasks. Idle count: {idle_counter}")
            if idle_counter >= MAX_IDLE_COUNT:
                logger.info("No more indexing tasks. Shutting down indexer.")
                break
            time.sleep(BACKOFF_TIME)
            continue

        idle_counter = 0
        logger.info(f"Processing URL: {task['url']}")

        # Process and enhance content
        document = process_content(task)
        
        # Index the document
        success = index_document(idx, document)
        
        # Handle the result
        if success:
            # Delete the message from queue
            sqs.delete_message(QueueUrl=indexer_queue_url, ReceiptHandle=receipt_handle)
            indexed_count += 1
            logger.info(f"Successfully indexed {task['url']} ({indexed_count} total)")
            
            # Send success result
            send_result({
                "status": "success",
                "indexed_url": task['url'],
                "indexed_count": indexed_count,
                "timestamp": datetime.now().isoformat()
            })
        else:
            failed_count += 1
            logger.error(f"Failed to index {task['url']} ({failed_count} failures)")
            
            # Send failure result
            send_result({
                "status": "failure",
                "failed_url": task['url'],
                "error": "Indexing failed",
                "failed_count": failed_count,
                "timestamp": datetime.now().isoformat()
            })

        time.sleep(PROCESSING_DELAY)

    logger.info(f"Indexer process completed. Total indexed: {indexed_count}, Failed: {failed_count}")

if __name__ == '__main__':
    try:
        indexer_process()
    except KeyboardInterrupt:
        logger.info("Indexer process interrupted by user")
    except Exception as e:
        logger.critical(f"Unhandled exception in indexer process: {e}", exc_info=True)