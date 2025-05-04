import json
import argparse
import psycopg2
from psycopg2.extras import DictCursor
from datetime import datetime

# PostgreSQL RDS Configuration
DB_HOST = 'dbdistproj-new.c8v2o28aq6x6.us-east-1.rds.amazonaws.com'
DB_PORT = 5432
DB_NAME = 'dbdistproj'
DB_USER = 'bialy'
DB_PASSWORD = 'midomido15'

def connect_to_db():
    """Connect to PostgreSQL RDS database"""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        print("Connected to PostgreSQL RDS")
        return conn
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
        return None

def check_database_status(conn):
    """Check database statistics and status"""
    try:
        cursor = conn.cursor(cursor_factory=DictCursor)

        # Count total documents
        cursor.execute("SELECT COUNT(*) FROM indexed_documents")
        docs_count = cursor.fetchone()[0]

        # Get status of indexed documents
        cursor.execute("""
        SELECT content_type, COUNT(*) as count 
        FROM indexed_documents 
        GROUP BY content_type 
        ORDER BY count DESC
        """)
        content_type_counts = cursor.fetchall()

        # Get most recent document
        cursor.execute("""
        SELECT url, last_updated 
        FROM indexed_documents 
        ORDER BY last_updated DESC 
        LIMIT 1
        """)
        recent_doc = cursor.fetchone()

        # Get popular search terms
        cursor.execute("""
        SELECT search_term, search_count
        FROM search_statistics
        ORDER BY search_count DESC
        LIMIT 10
        """)
        popular_searches = cursor.fetchall()

        print("\nDatabase Status:")
        print(f"Total documents indexed: {docs_count}")
        
        print("\nContent Type Distribution:")
        for row in content_type_counts:
            print(f"  {row['content_type']}: {row['count']} documents")
        
        if recent_doc:
            print(f"\nMost recent document: {recent_doc['url']}")
            print(f"Indexed at: {recent_doc['last_updated']}")
        
        print("\nPopular search terms:")
        for row in popular_searches:
            print(f"  {row['search_term']}: {row['search_count']} searches")
        
        return {
            "docs_count": docs_count,
            "content_type_counts": [dict(row) for row in content_type_counts],
            "recent_doc": dict(recent_doc) if recent_doc else None,
            "popular_searches": [dict(row) for row in popular_searches]
        }
        
    except Exception as e:
        print(f"Error checking database status: {e}")
        return {}

def search_documents(conn, query, page=1, results_per_page=10, domain_filter=None, content_type_filter=None):
    """Search for documents containing the query term"""
    try:
        cursor = conn.cursor(cursor_factory=DictCursor)
        
        # Base query
        base_query = """
        SELECT id, url, domain, title, summary, content_type, keywords, timestamp, last_updated
        FROM indexed_documents
        WHERE 
            (title ILIKE %s OR
            summary ILIKE %s OR
            keywords ILIKE %s)
        """
        
        # Apply filters if provided
        params = [f'%{query}%', f'%{query}%', f'%{query}%']
        
        if domain_filter:
            base_query += " AND domain = %s"
            params.append(domain_filter)
            
        if content_type_filter:
            base_query += " AND content_type = %s"
            params.append(content_type_filter)
            
        # Add ordering and pagination
        offset = (page - 1) * results_per_page
        base_query += " ORDER BY last_updated DESC LIMIT %s OFFSET %s"
        params.extend([results_per_page, offset])
        
        # Execute search query
        cursor.execute(base_query, params)
        
        # Format results
        results = []
        for row in cursor.fetchall():
            # Convert datetime objects to ISO format for JSON serialization
            doc = dict(row)
            if doc['timestamp']:
                doc['timestamp'] = doc['timestamp'].isoformat()
            if doc['last_updated']:
                doc['last_updated'] = doc['last_updated'].isoformat()
                
            # Convert keywords string to list
            if doc['keywords']:
                doc['keywords'] = doc['keywords'].split(',')
            else:
                doc['keywords'] = []
                
            results.append(doc)
        
        # Get total count for pagination info
        cursor.execute("SELECT COUNT(*) FROM indexed_documents WHERE title ILIKE %s OR summary ILIKE %s OR keywords ILIKE %s",
                      (f'%{query}%', f'%{query}%', f'%{query}%'))
        total_results = cursor.fetchone()[0]
        
        # Record this search in statistics
        record_search(conn, query)
        
        return results, total_results
        
    except Exception as e:
        print(f"Error searching documents: {e}")
        return [], 0

def record_search(conn, query):
    """Record search term in search statistics"""
    try:
        cursor = conn.cursor()
        
        # Normalize query
        normalized_query = query.lower().strip()
        
        cursor.execute("""
        INSERT INTO search_statistics (search_term, search_count, last_searched)
        VALUES (%s, 1, %s)
        ON CONFLICT (search_term) DO UPDATE SET
            search_count = search_statistics.search_count + 1,
            last_searched = %s
        """, (normalized_query, datetime.now(), datetime.now()))
        
        conn.commit()
    except Exception as e:
        print(f"Error recording search statistics: {e}")
        conn.rollback()

def get_popular_searches(conn, limit=10):
    """Get most popular search terms"""
    try:
        cursor = conn.cursor(cursor_factory=DictCursor)
        
        cursor.execute("""
        SELECT search_term, search_count, last_searched
        FROM search_statistics
        ORDER BY search_count DESC
        LIMIT %s
        """, (limit,))
        
        results = []
        for row in cursor.fetchall():
            item = dict(row)
            if item['last_searched']:
                item['last_searched'] = item['last_searched'].isoformat()
            results.append(item)
            
        return results
    except Exception as e:
        print(f"Error getting popular searches: {e}")
        return []

def get_document_by_url(conn, url):
    """Get a specific document by URL"""
    try:
        cursor = conn.cursor(cursor_factory=DictCursor)
        
        cursor.execute("""
        SELECT id, url, domain, title, summary, content_type, keywords, timestamp, last_updated
        FROM indexed_documents
        WHERE url = %s
        """, (url,))
        
        row = cursor.fetchone()
        if not row:
            return None
            
        doc = dict(row)
        # Format datetime objects
        if doc['timestamp']:
            doc['timestamp'] = doc['timestamp'].isoformat()
        if doc['last_updated']:
            doc['last_updated'] = doc['last_updated'].isoformat()
            
        # Convert keywords string to list
        if doc['keywords']:
            doc['keywords'] = doc['keywords'].split(',')
        else:
            doc['keywords'] = []
            
        return doc
    except Exception as e:
        print(f"Error getting document by URL: {e}")
        return None

def api_search(conn, query, page=1, results_per_page=10, domain=None, content_type=None):
    """API endpoint for search functionality"""
    results, total = search_documents(
        conn, 
        query, 
        page=page,
        results_per_page=results_per_page,
        domain_filter=domain,
        content_type_filter=content_type
    )
    
    return {
        'query': query,
        'page': page,
        'results_per_page': results_per_page,
        'total_results': total,
        'results': results
    }

def api_popular_searches(conn, limit=10):
    """API endpoint for popular searches"""
    popular = get_popular_searches(conn, limit)
    
    return {
        'popular_searches': popular
    }

def api_stats(conn):
    """API endpoint for database statistics"""
    stats = check_database_status(conn)
    
    return {
        'stats': stats
    }

def main():
    parser = argparse.ArgumentParser(description='Search node for distributed search engine')
    parser.add_argument('--status', action='store_true', help='Show database status')
    parser.add_argument('--search', type=str, help='Search term')
    parser.add_argument('--domain', type=str, help='Filter by domain')
    parser.add_argument('--content-type', type=str, help='Filter by content type')
    parser.add_argument('--page', type=int, default=1, help='Page number')
    parser.add_argument('--limit', type=int, default=10, help='Results per page')
    parser.add_argument('--popular', action='store_true', help='Show popular searches')
    parser.add_argument('--url', type=str, help='Get document by URL')
    parser.add_argument('--json', action='store_true', help='Output in JSON format')
    
    args = parser.parse_args()
    
    # Connect to PostgreSQL
    conn = connect_to_db()
    if not conn:
        print("Failed to connect to database. Exiting.")
        return
    
    try:
        if args.status:
            # Show database status
            stats = check_database_status(conn)
            if args.json:
                print(json.dumps(stats, indent=2))
        
        elif args.search:
            # Search for documents
            print(f"Searching for: '{args.search}'")
            
            if args.json:
                # Return JSON response
                results = api_search(
                    conn, 
                    args.search, 
                    page=args.page,
                    results_per_page=args.limit,
                    domain=args.domain,
                    content_type=args.content_type
                )
                print(json.dumps(results, indent=2))
            else:
                # Display search results in human-readable format
                results, total = search_documents(
                    conn, 
                    args.search, 
                    page=args.page,
                    results_per_page=args.limit,
                    domain_filter=args.domain,
                    content_type_filter=args.content_type
                )
                
                if not results:
                    print("No matching documents found")
                else:
                    print(f"Found {total} matching documents (showing page {args.page}):")
                    print("-" * 80)
                    
                    for i, doc in enumerate(results, 1):
                        print(f"{i}. {doc.get('title', 'No Title')}")
                        print(f"   URL: {doc.get('url', 'No URL')}")
                        print(f"   Domain: {doc.get('domain', 'Unknown')}")
                        print(f"   Type: {doc.get('content_type', 'Unknown')}")
                        
                        # Print summary
                        if doc.get('summary'):
                            print(f"   Summary: {doc['summary']}")
                        
                        # Print keywords
                        if doc.get('keywords'):
                            print(f"   Keywords: {', '.join(doc['keywords'])}")
                        
                        print(f"   Last updated: {doc.get('last_updated', 'Unknown')}")
                        print("-" * 80)
                        
        elif args.popular:
            # Show popular searches
            popular = get_popular_searches(conn, args.limit)
            
            if args.json:
                print(json.dumps({'popular_searches': popular}, indent=2))
            else:
                print(f"Top {len(popular)} popular searches:")
                for i, item in enumerate(popular, 1):
                    print(f"{i}. {item['search_term']} ({item['search_count']} searches)")
                    print(f"   Last searched: {item['last_searched']}")
                    
        elif args.url:
            # Get document by URL
            doc = get_document_by_url(conn, args.url)
            
            if args.json:
                print(json.dumps({'document': doc}, indent=2))
            else:
                if not doc:
                    print(f"No document found with URL: {args.url}")
                else:
                    print(f"Document: {doc['title']}")
                    print(f"URL: {doc['url']}")
                    print(f"Domain: {doc['domain']}")
                    print(f"Type: {doc['content_type']}")
                    print(f"Keywords: {', '.join(doc['keywords'])}")
                    print(f"Summary: {doc['summary']}")
                    print(f"Last updated: {doc['last_updated']}")
        
        else:
            # If no arguments provided, show help
            parser.print_help()
    
    finally:
        # Always close database connection
        conn.close()

if __name__ == "__main__":
    main()