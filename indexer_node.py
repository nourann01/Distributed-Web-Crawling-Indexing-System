import time
import logging
import json
import os
import boto3
from datetime import datetime
from whoosh.index import create_in, open_dir
from whoosh.fields import Schema, TEXT, ID, DATETIME
from whoosh.qparser import QueryParser
import whoosh.index as index

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - Indexer - %(levelname)s - %(message)s')

# AWS SQS Configuration
sqs = boto3.client('sqs')
indexer_queue_url = 'https://sqs.us-east-1.amazonaws.com/969510159350/indexer-queue.fifoo'
result_queue_url = 'https://sqs.us-east-1.amazonaws.com/969510159350/result-queue.fifo'

def indexer_process():
    """
    Process for an indexer node.
    Receives web page content, indexes it, and handles search queries (basic).
    """
    rank = 1  # Define your rank for logging purposes

    logging.info(f"Indexer node started with rank {rank}")
    
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
        # Receive content data from the indexer queue
        response = sqs.receive_message(
            QueueUrl=indexer_queue_url,
            AttributeNames=['All'],
            MaxNumberOfMessages=1,
            WaitTimeSeconds=10
        )
        
        if 'Messages' in response:
            message = response['Messages'][0]
            content_data = json.loads(message['Body'])
            receipt_handle = message['ReceiptHandle']
            
            # Process the content
            logging.info(f"Indexer {rank} received content to index.")
            if isinstance(content_data, dict):
                # Extract data fields
                url = content_data.get("url", "unknown")
                title = content_data.get("title", "No Title")
                content = content_data.get("content", "")
                crawled_by = str(content_data.get("crawled_by", rank))
                
                # Convert timestamp or use current time
                try:
                    timestamp = datetime.fromtimestamp(content_data.get("timestamp", time.time()))
                except (ValueError, TypeError):
                    timestamp = datetime.now()
                
                # Update the search index
                writer = idx.writer()
                writer.update_document(
                    url=url,
                    title=title,
                    content=content,
                    crawled_by=crawled_by,
                    timestamp=timestamp
                )
                writer.commit()
                indexed_count += 1
                
                # Send result to result-queue.fifo
                result_msg = {
                    "indexed_url": url,
                    "indexed_count": indexed_count,
                    "indexer_rank": rank
                }
                sqs.send_message(
                    QueueUrl=result_queue_url,
                    MessageBody=json.dumps(result_msg),
                    MessageGroupId='indexing'
                )
                
                # Delete message from queue after processing
                sqs.delete_message(
                    QueueUrl=indexer_queue_url,
                    ReceiptHandle=receipt_handle
                )
                
                # Periodic performance reporting
                if indexed_count % 10 == 0:
                    elapsed = time.time() - start_time
                    rate = indexed_count / elapsed if elapsed > 0 else 0
                    logging.info(f"Indexer {rank} performance: {indexed_count} documents indexed, {rate:.2f} docs/sec")
                
                logging.info(f"Indexer {rank} indexed content from URL: {url}")
            else:
                logging.warning(f"Indexer {rank} received non-dictionary content. Skipping.")
        
        # Optionally, handle search queries here from the master node (not implemented)
    
    logging.info(f"Indexer {rank} shutting down. Indexed {indexed_count} documents total.")

if __name__ == '__main__':
    indexer_process()