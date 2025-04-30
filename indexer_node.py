import time
import logging
import json
import os
import boto3
from datetime import datetime
from whoosh.index import create_in, open_dir
from whoosh.fields import Schema, TEXT, ID, DATETIME
import whoosh.index as index

# AWS SQS Configuration
sqs = boto3.client('sqs', region_name='us-east-1')
indexer_queue_url = 'https://sqs.us-east-1.amazonaws.com/969510159350/indexer-queue.fifo'
result_queue_url = 'https://sqs.us-east-1.amazonaws.com/969510159350/result-queue.fifo'

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - Indexer - %(levelname)s - %(message)s')

def setup_index():
    index_dir = "search_index"
    if not os.path.exists(index_dir):
        os.mkdir(index_dir)

    schema = Schema(
        url=ID(stored=True, unique=True),
        title=TEXT(stored=True),
        content=TEXT(stored=True),
        timestamp=DATETIME(stored=True)
    )

    if not index.exists_in(index_dir):
        idx = create_in(index_dir, schema)
    else:
        idx = open_dir(index_dir)

    return idx

def receive_task():
    response = sqs.receive_message(
        QueueUrl=indexer_queue_url,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=10,
        AttributeNames=['All'],
        MessageAttributeNames=['All']
    )
    if 'Messages' in response:
        message = response['Messages'][0]
        data = json.loads(message['Body'])
        receipt_handle = message['ReceiptHandle']
        return data, receipt_handle
    return None, None

def indexer_process():
    idx = setup_index()
    indexed_count = 0
    idle_counter = 0

    while True:
        task, receipt_handle = receive_task()
        if not task:
            idle_counter += 1
            logging.info(f"No indexing tasks. Idle count: {idle_counter}")
            if idle_counter >= 30:
                logging.info("No more indexing tasks. Shutting down indexer.")
                break
            time.sleep(10)
            continue

        idle_counter = 0
        logging.info(f"Indexing URL: {task['url']}")

        writer = idx.writer()
        try:
            writer.update_document(
                url=task['url'],
                title=task.get('title', 'No Title'),
                content=task.get('content', ''),
                timestamp=datetime.fromtimestamp(task.get('timestamp', time.time()))
            )
            writer.commit()
            # ✅ Only after successful commit, delete the message
            sqs.delete_message(QueueUrl=indexer_queue_url, ReceiptHandle=receipt_handle)

            indexed_count += 1
            send_result({
                "indexed_url": task['url'],
                "indexed_count": indexed_count
            })
        except Exception as e:
            logging.error(f"Error indexing document: {e}")
            if writer:
                writer.cancel()

        time.sleep(3)


def send_result(result):
    sqs.send_message(
        QueueUrl=result_queue_url,
        MessageBody=json.dumps(result),
        MessageGroupId='indexing'
    )

if __name__ == '__main__':
    indexer_process()