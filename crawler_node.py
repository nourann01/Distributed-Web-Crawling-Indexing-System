from mpi4py import MPI
import time
import logging
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import boto3
import json

# AWS SQS Client Configuration
sqs = boto3.client('sqs', region_name='us-east-1')  # Replace with your AWS region
crawler_queue_url = 'https://sqs.us-east-1.amazonaws.com/969510159350/crawler-queue.fifo'  # Replace with your SQS Queue URL
result_queue_url = 'https://sqs.us-east-1.amazonaws.com/969510159350/result-queue.fifo'  # Replace with your SQS Queue URL
indexer_queue_url = 'https://sqs.us-east-1.amazonaws.com/969510159350/indexer-queue.fifo'  # Replace with your indexer SQS Queue URL

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - Crawler - %(levelname)s - %(message)s')

def fetch_page(url):
    headers = {
        'User-Agent': 'Distributed Crawler Bot 1.0',
    }
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()  # Raises error for bad status
        return response.text
    except requests.RequestException as e:
        logging.error(f"HTTP error fetching {url}: {e}")
        return None

def parse_html(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    title = soup.title.text.strip() if soup.title else "No Title"
    body_text = soup.get_text(separator=' ', strip=True)
    return title, body_text

def extract_urls(base_url, html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    urls = []
    
    for link in soup.find_all('a', href=True):
        href = link['href']
        full_url = urljoin(base_url, href)
        parsed = urlparse(full_url)
        if parsed.scheme in ('http', 'https'):
            # Normalize URL (remove fragment)
            normalized_url = parsed._replace(fragment='').geturl()
            urls.append(normalized_url)
    return urls

def receive_task_from_sqs():
    """
    Receive a crawling task (URL) from the SQS queue.
    """
    response = sqs.receive_message(
        QueueUrl=crawler_queue_url,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=10,  # Wait up to 10 seconds for a message
        AttributeNames=['All'],
        MessageAttributeNames=['All']
    )
    
    if 'Messages' in response:
        message = response['Messages'][0]
        url = json.loads(message['Body'])['url']
        receipt_handle = message['ReceiptHandle']
        
        # Delete message from the queue after receiving it
        sqs.delete_message(
            QueueUrl=crawler_queue_url,
            ReceiptHandle=receipt_handle
        )
        
        return url
    else:
        logging.info("No messages in queue.")
        return None

def send_result_to_sqs(result):
    """
    Send crawled result (URLs and content) to the result queue.
    """
    sqs.send_message(
        QueueUrl=result_queue_url,
        MessageBody=json.dumps(result),
        MessageGroupId='1'  # FIFO Queue requires MessageGroupId
    )
    logging.info(f"Sent result to result queue: {result['url']}")

def send_to_indexer_queue(crawled_data):
    """
    Send crawled content to the indexer queue for further indexing.
    """
    sqs.send_message(
        QueueUrl=indexer_queue_url,
        MessageBody=json.dumps(crawled_data),
        MessageGroupId='1'  # FIFO Queue requires MessageGroupId
    )
    logging.info(f"Sent crawled data to indexer queue: {crawled_data['url']}")

def crawler_process():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    
    crawler_nodes = size - 2  # Assuming 1 master, at least 1 indexer
    first_indexer_rank = crawler_nodes + 1
    
    while True:
        url_to_crawl = receive_task_from_sqs()  # Receive URL from SQS
        if not url_to_crawl:
            logging.info("No more tasks in the queue. Sleeping for a while.")
            time.sleep(10)  # If no task, wait before checking again
            continue
        
        logging.info(f"Crawler {rank} crawling URL: {url_to_crawl}")
        
        # Fetch page
        page_content = fetch_page(url_to_crawl)
        if not page_content:
            # Report fetch failure and continue
            send_result_to_sqs({"url": url_to_crawl, "status": "Failed to fetch"})
            continue
        
        # Parse content
        title, body_text = parse_html(page_content)
        
        # Extract URLs
        extracted_urls = extract_urls(url_to_crawl, page_content)
        
        # Prepare content for indexing (truncate large content)
        extracted_content = {
            "url": url_to_crawl,
            "title": title,
            "content": body_text[:1000],
            "crawled_by": rank,
            "timestamp": time.time()
        }
        
        logging.info(f"Crawler {rank} crawled {url_to_crawl}, extracted {len(extracted_urls)} URLs.")
        
        # Send extracted URLs back to master via MPI
        comm.send(extracted_urls, dest=0, tag=1)
        
        # Send extracted content to an indexer node (round-robin selection)
        indexer_rank = first_indexer_rank + (rank % (size - crawler_nodes - 1))
        comm.send(extracted_content, dest=indexer_rank, tag=2)
        
        # Send crawled data to the indexer queue for further processing
        send_to_indexer_queue(extracted_content)
        
        # Send status update to result queue
        send_result_to_sqs({
            "url": url_to_crawl,
            "status": "Completed",
            "crawled_by": rank,
            "extracted_urls": extracted_urls
        })
        
        # Polite delay between requests
        time.sleep(2)

if __name__ == '__main__':
    crawler_process()