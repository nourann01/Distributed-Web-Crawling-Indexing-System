import time
import logging
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import boto3
import json
import hashlib

# AWS SQS Configuration
sqs = boto3.client('sqs', region_name='us-east-1')
crawler_queue_url = 'https://sqs.us-east-1.amazonaws.com/969510159350/crawler-queue.fifo'
indexer_queue_url = 'https://sqs.us-east-1.amazonaws.com/969510159350/indexer-queue.fifo'
result_queue_url = 'https://sqs.us-east-1.amazonaws.com/969510159350/result-queue.fifo'

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - Crawler - %(levelname)s - %(message)s')

def fetch_page(url):
    headers = {
        'User-Agent': 'DistributedCrawlerBot/1.1 (+https://example.com/bot)'
    }
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        return response
    except requests.RequestException as e:
        logging.error(f"Failed to fetch {url}: {e}")
        return None

def parse_html(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    title = soup.title.text.strip() if soup.title else "No Title"
    body_text = soup.get_text(separator=' ', strip=True)
    return title, body_text

def extract_urls(base_url, html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    found_urls = set()
    for link in soup.find_all('a', href=True):
        full_url = urljoin(base_url, link['href'])
        parsed = urlparse(full_url)
        if parsed.scheme in ('http', 'https'):
            normalized = parsed._replace(fragment='', query='').geturl()
            found_urls.add(normalized)
    return list(found_urls)

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
        try:
            task_data = json.loads(message['Body'])
            url = task_data['url']
            receipt_handle = message['ReceiptHandle']
            sqs.delete_message(QueueUrl=crawler_queue_url, ReceiptHandle=receipt_handle)
            return url
        except Exception as e:
            logging.error(f"Malformed task: {e}")
    return None

def send_result(payload):
    sqs.send_message(
        QueueUrl=result_queue_url,
        MessageBody=json.dumps(payload),
        MessageGroupId='1'
    )

def send_to_indexer(document):
    sqs.send_message(
        QueueUrl=indexer_queue_url,
        MessageBody=json.dumps(document),
        MessageGroupId='1'
    )

def generate_url_hash(url):
    return hashlib.sha256(url.encode()).hexdigest()

def crawler_process():
    idle_counter = 0

    while True:
        url = receive_task()

        if not url:
            idle_counter += 1
            logging.info(f"No task received. Idle count: {idle_counter}")
            if idle_counter >= 30:
                logging.info("No tasks for a while. Shutting down.")
                break
            time.sleep(10)
            continue

        idle_counter = 0
        logging.info(f"Crawling: {url}")

        response = fetch_page(url)
        if not response:
            send_result({
                "url": url,
                "status": "error",
                "error": "Failed to fetch"
            })
            continue

        html = response.text
        status_code = response.status_code
        content_length = len(html)
        timestamp = time.time()

        title, content = parse_html(html)
        extracted_urls = extract_urls(url, html)

        doc = {
            "url": url,
            "url_hash": generate_url_hash(url),
            "title": title,
            "content": content[:1000],
            "timestamp": timestamp
        }

        send_to_indexer(doc)

        result_payload = {
            "url": url,
            "url_hash": doc["url_hash"],
            "status": "success",
            "status_code": status_code,
            "content_length": content_length,
            "extracted_urls": extracted_urls,
            "timestamp": timestamp
        }
        send_result(result_payload)

        time.sleep(1)  # optional throttle for politeness

if __name__ == '__main__':
    crawler_process()
