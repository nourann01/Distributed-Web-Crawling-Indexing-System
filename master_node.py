import time
import logging
import boto3
import json
import hashlib
import botocore.exceptions
import argparse
import sys
import re
from urllib.parse import urlparse

# Parse command line arguments
def parse_arguments():
    parser = argparse.ArgumentParser(description='Master node for web crawler')
    parser.add_argument('--urls', nargs='+', help='Seed URLs to crawl')
    parser.add_argument('--depth', type=int, default=3, help='Maximum crawl depth per domain (default: 3)')
    parser.add_argument('--restricted', nargs='+', help='URLs or patterns to restrict from crawling')
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

# Get domain from URL
def get_domain(url):
    parsed = urlparse(url)
    return parsed.netloc

# Task deduplication filter using hashing
def hash_url(url):
    return hashlib.md5(url.encode('utf-8')).hexdigest()

def send_task_to_queue(url, depth_limit, restricted_patterns):
    url_hash = hash_url(url)
    if url_hash in visited_urls:
        logging.debug(f"Skipping already visited URL: {url}")
        return

    visited_urls.add(url_hash)
    seed_domain = get_domain(url)
    message = json.dumps({
        'url': url,
        'depth': 0,  # Start at depth 0 for seed URLs
        'seed_domain': seed_domain,
        'depth_limit': depth_limit,
        'restricted_patterns': restricted_patterns  # Add restricted patterns to the message
    })

    try:
        sqs.send_message(
            QueueUrl=crawler_queue_url,
            MessageBody=message,
            MessageGroupId='crawler_tasks',
            MessageDeduplicationId=url_hash
        )
        logging.info(f"Sent URL to crawler queue: {url} (depth_limit: {depth_limit}, domain: {seed_domain})")
    except botocore.exceptions.ClientError as e:
        logging.error(f"Error sending message to SQS: {e}")

def receive_crawler_results():
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

def prepare_restricted_patterns(restricted_urls):
    patterns = []
    if restricted_urls:
        for url in restricted_urls:
            # Basic pattern conversion:
            # - Convert * wildcard to regex wildcard
            # - Escape special regex characters
            # - Support for domain-only restrictions
            
            # Handle domain-only restrictions (without http/https prefix)
            if not url.startswith('http'):
                if '/' not in url and '*' not in url:
                    # If it's just a domain, restrict the entire domain
                    patterns.append(f"https?://{re.escape(url)}.*")
                    continue
            
            # Replace * with .* but escape other regex special chars
            pattern = url.replace('*', '__WILDCARD__')
            pattern = re.escape(pattern)
            pattern = pattern.replace('__WILDCARD__', '.*')
            patterns.append(pattern)
    return patterns

def master_process(seed_urls=None, depth_limit=3, restricted_urls=None):
    logging.info("Master node started.")

    # Use provided seed URLs or default to a fallback
    if not seed_urls:
        seed_urls = ["https://httpbin.org/html"]
    
    # Process restricted URLs into patterns
    restricted_patterns = prepare_restricted_patterns(restricted_urls)
    if restricted_patterns:
        logging.info(f"Restricted URL patterns: {restricted_patterns}")
    
    logging.info(f"Starting crawl with seed URLs: {seed_urls}")
    logging.info(f"Maximum crawl depth per domain: {depth_limit}")

    # Send all seed URLs to the crawler queue
    for url in seed_urls:
        send_task_to_queue(url, depth_limit, restricted_patterns)

    # Monitor both result queues for updates
    idle_counter = 0
    crawler_stats = {
        "urls_processed": 0,
        "success": 0,
        "errors": 0,
        "restricted": 0,  # Count of restricted URLs encountered
        "by_depth": {}, # Track stats by depth
        "by_domain": {}  # Track stats by domain
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
                
                # Track by depth
                depth = result.get("depth", 0)
                if depth not in crawler_stats["by_depth"]:
                    crawler_stats["by_depth"][depth] = {"count": 0, "success": 0, "errors": 0}
                crawler_stats["by_depth"][depth]["count"] += 1
                
                # Track by domain
                domain = result.get("seed_domain", "unknown")
                if domain not in crawler_stats["by_domain"]:
                    crawler_stats["by_domain"][domain] = {"count": 0, "success": 0, "errors": 0}
                crawler_stats["by_domain"][domain]["count"] += 1
                
                # Track restricted URLs
                if result.get("status") == "restricted":
                    crawler_stats["restricted"] += 1
                    logging.info(f"Restricted URL skipped: {result.get('url', 'N/A')}")
                elif result.get("status") == "success":
                    crawler_stats["success"] += 1
                    crawler_stats["by_depth"][depth]["success"] += 1
                    crawler_stats["by_domain"][domain]["success"] += 1
                else:
                    crawler_stats["errors"] += 1
                    crawler_stats["by_depth"][depth]["errors"] += 1
                    crawler_stats["by_domain"][domain]["errors"] += 1
                
                logging.info(f"Crawler result: {result.get('url', 'N/A')} - Status: {result.get('status', 'unknown')} - Depth: {depth}")
                
                # Only show detailed stats periodically to avoid log flooding
                if crawler_stats["urls_processed"] % 10 == 0:
                    logging.info(f"Crawler stats: {crawler_stats}")
        
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
                
                if indexer_stats["pages_indexed"] % 10 == 0:
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

def run_interactive():
    print("===== Distributed Web Crawler =====")
    
    # Get seed URLs
    while True:
        seed_urls_input = input("Enter seed URLs (space-separated): ")
        seed_urls = [url.strip() for url in seed_urls_input.split() if url.strip()]
        if seed_urls:
            break
        print("Please enter at least one URL to start crawling.")
    
    # Get depth limit
    while True:
        try:
            depth_limit = int(input("Enter maximum crawl depth per website (recommended 1-5): "))
            if depth_limit < 1:
                print("Depth must be at least 1.")
                continue
            break
        except ValueError:
            print("Please enter a valid number.")
    
    # Get restricted URLs
    restricted_urls_input = input("Enter URLs to restrict (space-separated, leave empty to skip): ")
    restricted_urls = [url.strip() for url in restricted_urls_input.split() if url.strip()]
    
    print(f"\nStarting crawler with {len(seed_urls)} seed URLs and depth limit of {depth_limit}.")
    if restricted_urls:
        print(f"URLs matching {len(restricted_urls)} patterns will be restricted from crawling.")
    print("Press Ctrl+C to stop the crawler.")
    
    try:
        master_process(seed_urls, depth_limit, restricted_urls)
    except KeyboardInterrupt:
        print("\nCrawler stopped by user.")

if __name__ == '__main__':
    args = parse_arguments()
    
    if args.urls:
        # Run with command line arguments
        master_process(args.urls, args.depth, args.restricted)
    else:
        # Run in interactive mode
        run_interactive()