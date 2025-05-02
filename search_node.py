import boto3
import json
import argparse
import time
import os
from datetime import datetime

# SQS Configuration
sqs = boto3.client('sqs', region_name='us-east-1')
indexer_queue_url = 'https://sqs.us-east-1.amazonaws.com/969510159350/indexer-queue.fifo'

# Local storage
LOCAL_DB_FILE = "local_documents.json"
documents = []

def load_local_documents():
    """Load previously saved documents"""
    global documents
    if os.path.exists(LOCAL_DB_FILE):
        try:
            with open(LOCAL_DB_FILE, 'r') as file:
                documents = json.load(file)
            print(f"Loaded {len(documents)} documents from local storage")
        except Exception as e:
            print(f"Error loading local documents: {e}")
            documents = []
    else:
        print("No local document storage found, starting with empty database")
        documents = []

def save_local_documents():
    """Save documents to local storage"""
    try:
        with open(LOCAL_DB_FILE, 'w') as file:
            json.dump(documents, file, indent=2)
        print(f"Saved {len(documents)} documents to local storage")
    except Exception as e:
        print(f"Error saving documents: {e}")

def fetch_messages_from_sqs(max_messages=10):
    """Fetch messages from SQS and store them locally"""
    try:
        # Receive up to 10 messages
        response = sqs.receive_message(
            QueueUrl=indexer_queue_url,
            MaxNumberOfMessages=max_messages,
            WaitTimeSeconds=5,
            VisibilityTimeout=30  # Don't delete, just make invisible temporarily
        )
        
        if 'Messages' not in response:
            print("No new messages in queue")
            return 0
            
        count = 0
        for message in response['Messages']:
            try:
                # Parse the message body
                doc = json.loads(message['Body'])
                
                # Add timestamp if not present
                if 'timestamp' not in doc:
                    doc['timestamp'] = datetime.now().timestamp()
                
                # Check if document already exists (by URL)
                existing = next((i for i, item in enumerate(documents) 
                               if item['url'] == doc['url']), None)
                
                if existing is not None:
                    # Update existing document
                    documents[existing] = doc
                    print(f"Updated document: {doc['url']}")
                else:
                    # Add new document
                    documents.append(doc)
                    print(f"Added document: {doc['url']}")
                
                count += 1
                
                # Keep message in queue (don't delete it)
                # This allows other processes to also see the message
                
            except json.JSONDecodeError:
                print(f"Invalid JSON in message: {message['Body'][:100]}...")
            except Exception as e:
                print(f"Error processing message: {e}")
        
        return count
        
    except Exception as e:
        print(f"Error fetching messages: {e}")
        return 0

def search_documents(query, limit=10):
    """Search for documents containing the query term"""
    query = query.lower()
    results = []
    
    for doc in documents:
        title = doc.get('title', '').lower()
        content = doc.get('content', '').lower()
        url = doc.get('url', '').lower()
        
        if query in title or query in content or query in url:
            results.append(doc)
            
            if len(results) >= limit:
                break
    
    return results

def main():
    parser = argparse.ArgumentParser(description='SQS-based document search')
    parser.add_argument('--fetch', action='store_true', help='Fetch new documents from SQS')
    parser.add_argument('--search', type=str, help='Search term')
    parser.add_argument('--limit', type=int, default=10, help='Max results to return')
    
    args = parser.parse_args()
    
    # Load existing documents
    load_local_documents()
    
    if args.fetch:
        # Fetch new documents
        print("Fetching documents from SQS...")
        count = fetch_messages_from_sqs(10)
        print(f"Fetched {count} new documents")
        # Save updated documents
        save_local_documents()
    
    if args.search:
        # Search for documents
        print(f"Searching for: '{args.search}'")
        results = search_documents(args.search, args.limit)
        
        if not results:
            print("No matching documents found")
        else:
            print(f"Found {len(results)} matching documents:")
            print("-" * 80)
            
            for i, doc in enumerate(results, 1):
                print(f"{i}. {doc.get('title', 'No Title')}")
                print(f"   URL: {doc.get('url', 'No URL')}")
                
                # Print snippet of content around the search term
                content = doc.get('content', '')
                if args.search.lower() in content.lower():
                    idx = content.lower().find(args.search.lower())
                    start = max(0, idx - 50)
                    end = min(len(content), idx + 50 + len(args.search))
                    snippet = "..." + content[start:end] + "..."
                    print(f"   Snippet: {snippet}")
                
                print("-" * 80)
    
    # If no arguments provided, show help
    if not (args.fetch or args.search):
        parser.print_help()

if __name__ == "__main__":
    main()