import time
import logging
import boto3
import json
import hashlib
import botocore.exceptions
import argparse
import sys

# Parse command line arguments
def parse_arguments():
    parser = argparse.ArgumentParser(description='Master node for web crawler')
    parser.add_argument('--urls', nargs='+', help='Seed URLs to crawl')
    return parser.parse_args()

# AWS SQS Client Configuration
sqs = boto3.client('sqs', region_name='us-east-1')
crawler_queue_url = 'https://sqs.us-east-1.amazonaws.com/969510159350/crawler-queue.fifo'
crawler_result_queue_url = 'https://sqs.us-east-1.amazonaws.com/969510159350/crawler-result-queue.fifo'
indexer_queue_url = 'https://sqs.us-east-1.amazonaws.com/969510159350/indexer-queue.fifo'
indexer_result_queue_url = 'https://sqs.us-east-1.amazonaws.com/969510159350/indexer-result-queue.fifo'

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - Master - %(levelname)s - %(message)s')

# Track visited URLs to avoid reprocessing
visited_urls = set()

# Domain politeness tracker (per-domain crawl delay enforcement)
domain_last_access = {}
POLITENESS_DELAY = 5  # seconds

# Task deduplication filter using hashing
def hash_url(url):
    return hashlib.md5(url.encode('utf-8')).hexdigest()

def send_task_to_queue(url):
    """
    Send crawling task to the crawler SQS queue.
    Deduplicates based on visited_urls.
    """
    url_hash = hash_url(url)
    if url_hash in visited_urls:
        logging.debug(f"Skipping already visited URL: {url}")
        return

    visited_urls.add(url_hash)
    message = json.dumps({'url': url})

    try:
        sqs.send_message(
            QueueUrl=crawler_queue_url,
            MessageBody=message,
            MessageGroupId='crawler_tasks',
            MessageDeduplicationId=url_hash
        )
        logging.info(f"Sent URL to crawler queue: {url}")
    except botocore.exceptions.ClientError as e:
        logging.error(f"Error sending message to SQS: {e}")

def receive_crawler_results():
    """
    Receive crawler results from the crawler-result-queue.
    This provides status updates about crawled pages.
    """
    try:
        response = sqs.receive_message(
            QueueUrl=crawler_result_queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=5,
            AttributeNames=['All'],
            MessageAttributeNames=['All']
        )
    except botocore.exceptions.ClientError as e:
        logging.error(f"Error receiving message from crawler result queue: {e}")
        return []

    results = []
    if 'Messages' in response:
        for message in response['Messages']:
            try:
                result = json.loads(message['Body'])
                results.append(result)
                sqs.delete_message(
                    QueueUrl=crawler_result_queue_url,
                    ReceiptHandle=message['ReceiptHandle']
                )
            except Exception as e:
                logging.error(f"Error processing crawler result message: {e}")
    return results


def receive_indexer_results():
    """
    Receive indexer results from the indexer-result-queue.
    This provides information about indexed content.
    """
    try:
        response = sqs.receive_message(
            QueueUrl=indexer_result_queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=5,
            AttributeNames=['All'],
            MessageAttributeNames=['All']
        )
    except botocore.exceptions.ClientError as e:
        logging.error(f"Error receiving message from indexer result queue: {e}")
        return []

    results = []
    if 'Messages' in response:
        for message in response['Messages']:
            try:
                result = json.loads(message['Body'])
                results.append(result)
                sqs.delete_message(
                    QueueUrl=indexer_result_queue_url,
                    ReceiptHandle=message['ReceiptHandle']
                )
            except Exception as e:
                logging.error(f"Error processing indexer result message: {e}")
    return results

def master_process(seed_urls=None):
    """
    Master node main process.
    Sends seed URLs to crawler queue and monitors both crawler and indexer results.
    
    According to the architecture diagram:
    1. Master sends URLs to crawler-queue.fifo
    2. Crawler processes URLs and:
       - Sends extracted URLs back to crawler-queue.fifo
       - Sends crawl status to crawler-result-queue.fifo
       - Sends processed content to indexer-queue.fifo
    3. Indexer processes content and:
       - Sends index status to indexer-result-queue.fifo
       - Stores indexed data in Whoosh Index
    4. Master monitors both crawler-result-queue.fifo and indexer-result-queue.fifo
    """
    logging.info("Master node started.")

    # Use provided seed URLs or default to a fallback
    if not seed_urls:
        seed_urls = ["https://httpbin.org/html"]
    
    logging.info(f"Starting crawl with seed URLs: {seed_urls}")

    # Send all seed URLs to the crawler queue
    for url in seed_urls:
        send_task_to_queue(url)

    # Monitor both result queues for updates
    idle_counter = 0
    crawler_stats = {
        "urls_processed": 0,
        "success": 0,
        "errors": 0
    }
    indexer_stats = {
        "pages_indexed": 0,
        "keywords_indexed": 0
    }
    
    while True:
        # Process crawler results
        crawler_results = receive_crawler_results()
        if crawler_results:
            idle_counter = 0
            for result in crawler_results:
                crawler_stats["urls_processed"] += 1
                if result.get("status_code", 0) >= 200 and result.get("status_code", 0) < 300:
                    crawler_stats["success"] += 1
                else:
                    crawler_stats["errors"] += 1
                
                logging.info(f"Crawler result: {result.get('url', 'N/A')} - Status: {result.get('status_code', 'unknown')}")
                logging.info(f"Crawler stats: {crawler_stats}")
                
                # Note: According to the architecture, the crawler should send extracted URLs
                # directly back to the crawler queue, not through the master
        
        # Process indexer results
        indexer_results = receive_indexer_results()
        if indexer_results:
            idle_counter = 0
            for result in indexer_results:
                indexer_stats["pages_indexed"] += 1
                if "keywords_count" in result:
                    indexer_stats["keywords_indexed"] += result.get("keywords_count", 0)
                
                logging.info(f"Indexer result: {result.get('url', 'N/A')} indexed successfully")
                if "title" in result:
                    logging.info(f"Title: {result.get('title', 'N/A')}")
                logging.info(f"Indexer stats: {indexer_stats}")
        
        # If neither queue had results, increment idle counter
        if not crawler_results and not indexer_results:
            idle_counter += 1
            logging.info(f"No new results. Idle count: {idle_counter}")
        
        # Exit after a period of inactivity
        if idle_counter >= 30:
            logging.info("No more crawling or indexing activity. Shutting down master.")
            logging.info(f"Final crawler stats: {crawler_stats}")
            logging.info(f"Final indexer stats: {indexer_stats}")
            break

        time.sleep(5)

if __name__ == '__main__':
    args = parse_arguments()
    master_process(args.urls)