#!/usr/bin/env python3
"""
Script to check data count and verify filters
"""

import os
import sys
import logging
import psycopg2
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
if not os.getenv('DB_A_HOST'):
    load_dotenv('config.env')

def setup_logging():
    """Setup logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

def get_db_connection(database='B'):
    """Get database connection"""
    if database == 'A':
        conn = psycopg2.connect(
            host=os.getenv('DB_A_HOST'),
            port=os.getenv('DB_A_PORT'),
            database=os.getenv('DB_A_NAME'),
            user=os.getenv('DB_A_USER'),
            password=os.getenv('DB_A_PASSWORD')
        )
    else:
        conn = psycopg2.connect(
            host=os.getenv('DB_B_HOST'),
            port=os.getenv('DB_B_PORT'),
            database=os.getenv('DB_B_NAME'),
            user=os.getenv('DB_B_USER'),
            password=os.getenv('DB_B_PASSWORD')
        )
    return conn

def check_data_counts(logger, start_date, end_date, warehouse_id):
    """Check various data counts to understand the filtering"""
    logger.info("=== CHECKING DATA COUNTS ===")
    
    conn_b = get_db_connection('B')
    
    try:
        cursor_b = conn_b.cursor()
        
        # Check 1: Total orders in order_main for the date range and warehouse
        cursor_b.execute("""
            SELECT COUNT(*) FROM order_main 
            WHERE faktur_date BETWEEN %s AND %s 
            AND warehouse_id = %s
        """, (start_date, end_date, warehouse_id))
        order_count = cursor_b.fetchone()[0]
        logger.info(f"Total orders in order_main: {order_count}")
        
        # Check 2: Total outbound_documents
        cursor_b.execute("""
            SELECT COUNT(*) FROM outbound_documents
        """)
        doc_count = cursor_b.fetchone()[0]
        logger.info(f"Total outbound_documents: {doc_count}")
        
        # Check 3: Total outbound_items
        cursor_b.execute("""
            SELECT COUNT(*) FROM outbound_items
        """)
        item_count = cursor_b.fetchone()[0]
        logger.info(f"Total outbound_items: {item_count}")
        
        # Check 4: Orders with matching do_number in outbound_documents
        cursor_b.execute("""
            SELECT COUNT(DISTINCT om.order_id)
            FROM order_main om
            JOIN outbound_documents odoc ON odoc.document_reference = om.do_number
            WHERE om.faktur_date BETWEEN %s AND %s 
            AND om.warehouse_id = %s
        """, (start_date, end_date, warehouse_id))
        matching_orders = cursor_b.fetchone()[0]
        logger.info(f"Orders with matching do_number: {matching_orders}")
        
        # Check 5: Outbound items for matching orders
        cursor_b.execute("""
            SELECT COUNT(oi.id)
            FROM outbound_items oi
            JOIN outbound_documents odoc ON odoc.id = oi.outbound_document_id
            JOIN order_main om ON om.do_number = odoc.document_reference
            WHERE om.faktur_date BETWEEN %s AND %s 
            AND om.warehouse_id = %s
        """, (start_date, end_date, warehouse_id))
        matching_items = cursor_b.fetchone()[0]
        logger.info(f"Outbound items for matching orders: {matching_items}")
        
        # Check 6: Sample of faktur_date range in order_main
        cursor_b.execute("""
            SELECT MIN(faktur_date), MAX(faktur_date), COUNT(*)
            FROM order_main 
            WHERE warehouse_id = %s
        """, (warehouse_id,))
        date_range = cursor_b.fetchone()
        logger.info(f"Date range for warehouse {warehouse_id}: {date_range[0]} to {date_range[1]} (total: {date_range[2]})")
        
        # Check 7: Sample of faktur_date range for the specific date range
        cursor_b.execute("""
            SELECT MIN(faktur_date), MAX(faktur_date), COUNT(*)
            FROM order_main 
            WHERE faktur_date BETWEEN %s AND %s 
            AND warehouse_id = %s
        """, (start_date, end_date, warehouse_id))
        filtered_date_range = cursor_b.fetchone()
        logger.info(f"Filtered date range: {filtered_date_range[0]} to {filtered_date_range[1]} (total: {filtered_date_range[2]})")
        
        return {
            'order_count': order_count,
            'doc_count': doc_count,
            'item_count': item_count,
            'matching_orders': matching_orders,
            'matching_items': matching_items
        }
        
    except Exception as e:
        logger.error(f"Error checking data counts: {e}")
        return {}
    finally:
        conn_b.close()

def main():
    """Main function"""
    if len(sys.argv) != 4:
        print("Usage: python3 check_data_count.py <start_date> <end_date> <warehouse_id>")
        print("Example: python3 check_data_count.py 2025-01-01 2025-01-30 4512")
        sys.exit(1)
    
    start_date = sys.argv[1]
    end_date = sys.argv[2]
    warehouse_id = int(sys.argv[3])
    
    logger = setup_logging()
    
    logger.info(f"Checking data for date range: {start_date} to {end_date}")
    logger.info(f"Warehouse ID: {warehouse_id}")
    
    check_data_counts(logger, start_date, end_date, warehouse_id)

if __name__ == "__main__":
    main() 