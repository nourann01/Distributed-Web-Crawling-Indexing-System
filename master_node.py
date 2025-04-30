import time
import logging
import boto3
import json

# AWS SQS Client Configuration
sqs = boto3.client('sqs', region_name='us-east-1')
crawler_queue_url = 'https://sqs.us-east-1.amazonaws.com/969510159350/crawler-queue.fifo'
result_queue_url = 'https://sqs.us-east-1.amazonaws.com/969510159350/result-queue.fifo'

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - Master - %(levelname)s - %(message)s')

def send_task_to_queue(url):
    """
    Send crawling task to the crawler SQS queue.
    """
    message = json.dumps({'url': url})
    sqs.send_message(
        QueueUrl=crawler_queue_url,
        MessageBody=message,
        MessageGroupId='1'
    )
    logging.info(f"Sent URL to crawler queue: {url}")

def receive_result_from_queue():
    """
    Receive a result message from the result queue.
    """
    response = sqs.receive_message(
        QueueUrl=result_queue_url,
        MaxNumberOfMessages=10,
        WaitTimeSeconds=10,
        AttributeNames=['All'],
        MessageAttributeNames=['All']
    )
    results = []
    if 'Messages' in response:
        for message in response['Messages']:
            result = json.loads(message['Body'])
            results.append(result)
            sqs.delete_message(
                QueueUrl=result_queue_url,
                ReceiptHandle=message['ReceiptHandle']
            )
    return results

def master_process():
    """
    Master node main process.
    Sends seed URLs and monitors crawling/indexing results.
    """
    logging.info("Master node started.")

    # Seed URLs (you can add more!)
    seed_urls = [
        "https://httpbin.org/html"
    ]

    for url in seed_urls:
        send_task_to_queue(url)

    # Monitoring loop
    idle_counter = 0
    while True:
        results = receive_result_from_queue()

        if results:
            idle_counter = 0
            for result in results:
                logging.info(f"Result: {result}")

                # Optional: Dynamically push newly discovered URLs
                if result.get("extracted_urls"):
                    for new_url in result["extracted_urls"]:
                        send_task_to_queue(new_url)
        else:
            idle_counter += 1
            logging.info(f"No new results. Idle count: {idle_counter}")

        if idle_counter >= 30:  # About 5 minutes idle
            logging.info("No more crawling activity. Shutting down master.")
            break

        time.sleep(10)

if __name__ == '__main__':
    master_process()
