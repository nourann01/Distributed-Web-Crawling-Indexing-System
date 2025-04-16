from mpi4py import MPI
import time
import logging
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - Crawler - %(levelname)s - %(message)s')

def crawler_process():
    """
    Process for a crawler node.
    Fetches web pages, extracts URLs, and sends results back to the master.
    """
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()
    
    logging.info(f"Crawler node started with rank {rank} of {size}")
    
    # Calculate the indexer node ranks (assuming at least one indexer after all crawlers)
    crawler_nodes = size - 2  # Assuming master and at least one indexer node
    first_indexer_rank = crawler_nodes + 1
    
    while True:
        status = MPI.Status()
        
        try:
            url_to_crawl = comm.recv(source=0, tag=MPI.ANY_TAG, status=status)
            received_tag = status.Get_tag()
            
            # Check if we received a termination signal
            if received_tag == 999 and url_to_crawl == "TERMINATE":
                logging.info(f"Crawler {rank} received shutdown signal. Exiting.")
                break
                
            if received_tag != 0:
                logging.warning(f"Crawler {rank} received unexpected tag {received_tag}. Ignoring.")
                continue
                
            logging.info(f"Crawler {rank} received URL: {url_to_crawl}")
            
            # --- Web Crawling Logic ---
            # 1. Fetch web page content
            headers = {
                'User-Agent': 'Distributed Crawler Bot 1.0',
            }
            response = requests.get(url_to_crawl, headers=headers, timeout=10)
            response.raise_for_status()  # Raise an exception for 4XX/5XX responses
            
            # 2. Parse content
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 3. Extract new URLs from the page
            extracted_urls = []
            base_url = url_to_crawl
            
            for link in soup.find_all('a', href=True):
                href = link['href']
                full_url = urljoin(base_url, href)
                
                # Basic URL normalization and filtering
                parsed_url = urlparse(full_url)
                if parsed_url.scheme in ('http', 'https'):
                    extracted_urls.append(full_url)
            
            # 4. Extract relevant text content for indexing
            title = soup.title.text.strip() if soup.title else "No Title"
            body_text = soup.get_text()
            extracted_content = {
                "url": url_to_crawl,
                "title": title,
                "content": body_text[:1000],  # Truncate content to avoid sending massive amounts of data
                "crawled_by": rank,
                "timestamp": time.time()
            }
            
            logging.info(f"Crawler {rank} crawled {url_to_crawl}, extracted {len(extracted_urls)} URLs.")
            
            # --- Send extracted URLs back to master ---
            comm.send(extracted_urls, dest=0, tag=1)  # Tag 1 for sending extracted URLs
            
            # --- Send extracted content to indexer node ---
            # Select an indexer in round-robin fashion
            indexer_rank = first_indexer_rank + (rank % (size - crawler_nodes - 1))
            comm.send(extracted_content, dest=indexer_rank, tag=2)  # Tag 2 for sending content to indexer
            
            # Send status update to master
            comm.send(f"Completed crawling {url_to_crawl}", dest=0, tag=99)  # Status update (tag 99)
            
            # Implement a polite delay to avoid hammering servers
            time.sleep(2)  # Respect robots.txt in a real implementation
            
        except requests.exceptions.RequestException as e:
            logging.error(f"Crawler {rank} HTTP error crawling {url_to_crawl}: {e}")
            comm.send(f"HTTP Error crawling {url_to_crawl}: {e}", dest=0, tag=999)  # Report error to master
            
        except Exception as e:
            logging.error(f"Crawler {rank} error crawling {url_to_crawl}: {e}")
            comm.send(f"Error crawling {url_to_crawl}: {e}", dest=0, tag=999)  # Report error to master

if __name__ == '__main__':
    crawler_process()