import time
import logging
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import boto3
import json

# AWS SQS Configuration
sqs = boto3.client('sqs', region_name='us-east-1')  # Adjust if you use another AWS region
crawler_queue_url = 'https://sqs.us-east-1.amazonaws.com/969510159350/crawler-queue.fifo'
indexer_queue_url = 'https://sqs.us-east-1.amazonaws.com/969510159350/indexer-queue.fifo'
result_queue_url = 'https://sqs.us-east-1.amazonaws.com/969510159350/result-queue.fifo'

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - Crawler - %(levelname)s - %(message)s')

def fetch_page(url):
    headers = {'User-Agent': 'Distributed Crawler Bot 1.0'}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        logging.error(f"Error fetching {url}: {e}")
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
            normalized_url = parsed._replace(fragment='').geturl()
            urls.append(normalized_url)
    return urls

def receive_task():
    response = sqs.receive_message(
        QueueUrl=crawler_queue_url,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=10,
        AttributeNames=['All'],
        MessageAttributeNames=['All']
    )
    if 'Messages' in response:
        message = response['Messages'][0]
        url = json.loads(message['Body'])['url']
        receipt_handle = message['ReceiptHandle']
        sqs.delete_message(QueueUrl=crawler_queue_url, ReceiptHandle=receipt_handle)
        return url
    return None

def send_result(result):
    sqs.send_message(
        QueueUrl=result_queue_url,
        MessageBody=json.dumps(result),
        MessageGroupId='1'
    )

def send_to_indexer(content):
    sqs.send_message(
        QueueUrl=indexer_queue_url,
        MessageBody=json.dumps(content),
        MessageGroupId='1'
    )

def crawler_process():
    idle_counter = 0
    while True:
        url_to_crawl = receive_task()
        if not url_to_crawl:
            idle_counter += 1
            logging.info(f"No tasks. Idle count: {idle_counter}")
            if idle_counter >= 30:  # ~5 minutes idle (10s * 30)
                logging.info("No more tasks. Shutting down crawler.")
                break
            time.sleep(10)
            continue

        idle_counter = 0
        logging.info(f"Crawling URL: {url_to_crawl}")
        page_content = fetch_page(url_to_crawl)
        if not page_content:
            send_result({"url": url_to_crawl, "status": "Failed to fetch"})
            continue

        title, body_text = parse_html(page_content)
        extracted_urls = extract_urls(url_to_crawl, page_content)

        content_for_indexer = {
            "url": url_to_crawl,
            "title": title,
            "content": body_text[:1000],
            "timestamp": time.time()
        }

        send_to_indexer(content_for_indexer)
        send_result({
            "url": url_to_crawl,
            "status": "Completed",
            "extracted_urls": extracted_urls
        })

        time.sleep(2)

if __name__ == '__main__':
    crawler_process()
