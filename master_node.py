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
result_queue_url = 'https://sqs.us-east-1.amazonaws.com/969510159350/result-queue.fifo'

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

def receive_result_from_queue():
    """
    Receive a result message from the result queue.
    """
    try:
        response = sqs.receive_message(
            QueueUrl=result_queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=10,
            AttributeNames=['All'],
            MessageAttributeNames=['All']
        )
    except botocore.exceptions.ClientError as e:
        logging.error(f"Error receiving message from SQS: {e}")
        return []

    results = []
    if 'Messages' in response:
        for message in response['Messages']:
            try:
                result = json.loads(message['Body'])
                results.append(result)
                sqs.delete_message(
                    QueueUrl=result_queue_url,
                    ReceiptHandle=message['ReceiptHandle']
                )
            except Exception as e:
                logging.error(f"Error processing message: {e}")
    return results

def master_process(seed_urls=None):
    """
    Master node main process.
    Sends seed URLs and monitors crawling/indexing results.
    """
    logging.info("Master node started.")

    # Use provided seed URLs or default to a fallback
    if not seed_urls:
        seed_urls = ["https://httpbin.org/html"]
    
    logging.info(f"Starting crawl with seed URLs: {seed_urls}")

    for url in seed_urls:
        send_task_to_queue(url)

    idle_counter = 0
    while True:
        results = receive_result_from_queue()

        if results:
            idle_counter = 0
            for result in results:
                logging.info(f"Result: {result.get('url', 'N/A')} - Status: {result.get('status_code', 'unknown')}")

                extracted_urls = result.get("extracted_urls", [])
                for new_url in extracted_urls:
                    send_task_to_queue(new_url)
        else:
            idle_counter += 1
            logging.info(f"No new results. Idle count: {idle_counter}")

        if idle_counter >= 30:
            logging.info("No more crawling activity. Shutting down master.")
            break

        time.sleep(10)

if __name__ == '__main__':
    args = parse_arguments()
    master_process(args.urls)