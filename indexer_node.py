from mpi4py import MPI
import time
import logging
import json
import os
from datetime import datetime
from whoosh.index import create_in, open_dir
from whoosh.fields import Schema, TEXT, ID, DATETIME
from whoosh.qparser import QueryParser
import whoosh.index as index

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - Indexer - %(levelname)s - %(message)s')

def indexer_process():
    """
    Process for an indexer node.
    Receives web page content, indexes it, and handles search queries (basic).
    """
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    
    logging.info(f"Indexer node started with rank {rank} of {size}")
    
    # Initialize index
    index_dir = f"search_index_{rank}"
    if not os.path.exists(index_dir):
        os.mkdir(index_dir)
        
    # Define schema for the search index
    schema = Schema(
        url=ID(stored=True, unique=True),
        title=TEXT(stored=True),
        content=TEXT(stored=True),
        crawled_by=ID(stored=True),
        timestamp=DATETIME(stored=True)
    )
    
    # Create or open index
    if not index.exists_in(index_dir):
        idx = create_in(index_dir, schema)
        logging.info(f"Indexer {rank} created new index in {index_dir}")
    else:
        idx = open_dir(index_dir)
        logging.info(f"Indexer {rank} opened existing index in {index_dir}")
    
    # Variables to track performance
    indexed_count = 0
    start_time = time.time()
    
    while True:
        status = MPI.Status()
        
        try:
            # Receive content from crawlers or termination signal from master
            content_data = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
            source_rank = status.Get_source()
            received_tag = status.Get_tag()
            
            # Check if we received a termination signal
            if received_tag == 999 and content_data == "TERMINATE":
                logging.info(f"Indexer {rank} received shutdown signal. Exiting.")
                break
                
            # Only process content with the expected tag (2)
            if received_tag != 2:
                logging.warning(f"Indexer {rank} received unexpected tag {received_tag}. Ignoring.")
                continue
                
            logging.info(f"Indexer {rank} received content from Crawler {source_rank} to index.")
            
            # --- Indexing Logic ---
            # 1. Process the received content
            if isinstance(content_data, dict):
                # Extract data fields
                url = content_data.get("url", "unknown")
                title = content_data.get("title", "No Title")
                content = content_data.get("content", "")
                crawled_by = str(content_data.get("crawled_by", source_rank))
                
                # Convert timestamp or use current time
                try:
                    timestamp = datetime.fromtimestamp(content_data.get("timestamp", time.time()))
                except (ValueError, TypeError):
                    timestamp = datetime.now()
                
                # 2. Update the search index
                writer = idx.writer()
                
                # Update document if it exists, otherwise add new
                writer.update_document(
                    url=url,
                    title=title,
                    content=content,
                    crawled_by=crawled_by,
                    timestamp=timestamp
                )
                
                writer.commit()
                indexed_count += 1
                
                # Periodic performance reporting
                if indexed_count % 10 == 0:
                    elapsed = time.time() - start_time
                    rate = indexed_count / elapsed if elapsed > 0 else 0
                    logging.info(f"Indexer {rank} performance: {indexed_count} documents indexed, {rate:.2f} docs/sec")
                
                logging.info(f"Indexer {rank} indexed content from URL: {url}")
                
                # Send status update to master
                status_msg = {
                    "indexed_url": url,
                    "indexed_count": indexed_count,
                    "indexer_rank": rank
                }
                comm.send(status_msg, dest=0, tag=99)  # Send status update to master (tag 99)
            else:
                logging.warning(f"Indexer {rank} received non-dictionary content. Skipping.")
            
        except Exception as e:
            logging.error(f"Indexer {rank} error indexing content from rank {source_rank}: {e}")
            error_msg = f"Indexer {rank} - Error indexing: {str(e)}"
            comm.send(error_msg, dest=0, tag=999)  # Report error to master (tag 999)
    
    # Final reporting before shutdown
    logging.info(f"Indexer {rank} shutting down. Indexed {indexed_count} documents total.")
    
    # Optionally: implement a function to handle search queries from master or other nodes
    def handle_search_query(query_string):
         with idx.searcher() as searcher:
             query = QueryParser("content", idx.schema).parse(query_string)
             results = searcher.search(query, limit=10)
             return [{"url": r["url"], "title": r["title"]} for r in results]

if __name__ == '__main__':
    indexer_process()