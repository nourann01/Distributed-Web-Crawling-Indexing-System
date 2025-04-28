from mpi4py import MPI
import time
import logging
import boto3
import json

# AWS SQS Client Configuration
sqs = boto3.client('sqs', region_name='us-east-1') 
crawler_queue_url = 'https://sqs.us-east-1.amazonaws.com/969510159350/crawler-queue.fifo' 
indexer_queue_url = 'https://sqs.us-east-1.amazonaws.com/969510159350/indexer-queue.fifo'  
queue_dist_url = 'https://sqs.us-east-1.amazonaws.com/969510159350/Queue_dist.fifo'  
result_queue_url = 'https://sqs.us-east-1.amazonaws.com/969510159350/result-queue.fifo'  

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - Master - %(levelname)s - %(message)s')

def send_task_to_queue(queue_url, url):
    """
    Send crawling task to the specified AWS SQS queue.
    """
    message = json.dumps({'url': url})  # Send the URL as a JSON message
    sqs.send_message(QueueUrl=queue_url, MessageBody=message, MessageGroupId='1')  # FIFO requires a MessageGroupId
    logging.info(f"Sent task to SQS queue ({queue_url}): {url}")

def receive_result_from_queue(queue_url):
    """
    Receive results from the result queue.
    """
    messages = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=20)
    if 'Messages' in messages:
        for message in messages['Messages']:
            # Process the message here
            result = json.loads(message['Body'])
            logging.info(f"Received result from queue: {result}")
            # Delete message after processing
            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=message['ReceiptHandle'])
            return result
    return None

def master_process():
    """
    Main process for the master node.
    Handles task distribution, worker management, and coordination.
    """
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()    # 0 for master
    size = comm.Get_size()    # Total number of available processes
    status = MPI.Status()

    logging.info(f"Master node started with rank {rank} of {size}")

    # Initialize task queue, database connections, etc.
    crawler_nodes = size - 2  # Assuming master and at least one indexer node
    indexer_nodes = 1  # At least one indexer node

    if crawler_nodes <= 0 or indexer_nodes <= 0:
        logging.error("Not enough nodes to run crawler and indexer. Need at least 3 nodes (1 master, 1 crawler, 1 indexer)")
        return

    active_crawler_nodes = list(range(1, 1 + crawler_nodes))  # Ranks for crawler nodes (assuming rank 0 is master)
    active_indexer_nodes = list(range(1 + crawler_nodes, size))  # Ranks for indexer nodes

    logging.info(f"Active Crawler Nodes: {active_crawler_nodes}")
    logging.info(f"Active Indexer Nodes: {active_indexer_nodes}")

    seed_urls = ["http://books.toscrape.com/"]  # Example seed URLs - replace with actual seed URLs
    task_count = 0
    crawler_tasks_assigned = 0

    # Add seed URLs to the task queue
    for url in seed_urls:
        send_task_to_queue(crawler_queue_url, url)
        task_count += 1

    while task_count > 0 or crawler_tasks_assigned > 0:
        # Continue as long as there are tasks in the queue or tasks in progress

        # Check for completed crawler tasks and results from crawler nodes
        if crawler_tasks_assigned > 0:
            if comm.iprobe(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status):
                message_source = status.Get_source()
                message_tag = status.Get_tag()
                message_data = comm.recv(source=message_source, tag=message_tag)

                if message_tag == 1:  # Crawler completed task and sent back extracted URLs
                    crawler_tasks_assigned -= 1
                    new_urls = message_data  # Assuming message_data is a list of URLs
                    if new_urls:
                        # Add newly discovered URLs to the SQS queue
                        for url in new_urls:
                            send_task_to_queue(crawler_queue_url, url)
                            task_count += 1
                    logging.info(f"Master received URLs from Crawler {message_source}, Task count: {task_count}")

                elif message_tag == 99:  # Crawler node reports status/heartbeat
                    logging.info(f"Crawler {message_source} status: {message_data}")

                elif message_tag == 999:  # Crawler node reports error
                    logging.error(f"Crawler {message_source} reported error: {message_data}")
                    crawler_tasks_assigned -= 1  # Decrement task count even on error

        # Assign new crawling tasks to available crawler nodes
        while task_count > 0 and crawler_tasks_assigned < crawler_nodes:
            url_to_crawl = seed_urls.pop(0)  # Get URL from seed URL list (FIFO)
            task_id = task_count
            task_count += 1

            available_crawler_rank = active_crawler_nodes[crawler_tasks_assigned % len(active_crawler_nodes)]  # Round-robin assignment
            comm.send(url_to_crawl, dest=available_crawler_rank, tag=0)  # Tag 0 for task assignment
            crawler_tasks_assigned += 1
            logging.info(f"Master assigned task {task_id} (crawl {url_to_crawl}) to Crawler {available_crawler_rank}")

        # Receive results from the result queue
        result = receive_result_from_queue(result_queue_url)
        if result:
            logging.info(f"Received result: {result}")

        time.sleep(0.1)  # Small delay to prevent overwhelming master in this loop

    logging.info("Master node finished URL distribution. Waiting for crawlers to complete...")

    # Send termination signal to all crawler and indexer nodes
    for node in active_crawler_nodes + active_indexer_nodes:
        comm.send("TERMINATE", dest=node, tag=999)
        logging.info(f"Sent termination signal to node {node}")

    logging.info("Master Node Finished.")

if __name__ == '__main__':
    master_process()