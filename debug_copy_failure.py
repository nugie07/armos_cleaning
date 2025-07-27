#!/usr/bin/env python3
"""
Debug script to investigate copy failure on specific date
"""

import os
import sys
import logging
import psycopg2
from datetime import datetime
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

def get_db_connection(database='A'):
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

def check_failed_date_data(logger, warehouse_id):
    """Check data for the specific date where copy failed"""
    logger.info("=== INVESTIGATING COPY FAILURE ===")
    logger.info(f"Warehouse ID: {warehouse_id}")
    
    # Connect to both databases
    conn_a = get_db_connection('A')
    conn_b = get_db_connection('B')
    
    try:
        cursor_a = conn_a.cursor()
        cursor_b = conn_b.cursor()
        
        # Check orders for 2025-04-09 specifically
        logger.info("=== CHECKING 2025-04-09 DATA ===")
        
        # Database A - Orders on 2025-04-09
        query_a = """
        SELECT COUNT(*) as total_orders
        FROM "order" 
        WHERE faktur_date = '2025-04-09' 
        AND warehouse_id = %s
        """
        cursor_a.execute(query_a, (warehouse_id,))
        total_orders_a = cursor_a.fetchone()[0]
        logger.info(f"Database A - Orders on 2025-04-09: {total_orders_a}")
        
        # Database B - Orders on 2025-04-09
        query_b = """
        SELECT COUNT(*) as total_orders
        FROM order_main 
        WHERE faktur_date = '2025-04-09' 
        AND warehouse_id = %s
        """
        cursor_b.execute(query_b, (str(warehouse_id),))
        total_orders_b = cursor_b.fetchone()[0]
        logger.info(f"Database B - Orders on 2025-04-09: {total_orders_b}")
        
        # Check for specific problematic orders
        logger.info("=== CHECKING SPECIFIC MISSING ORDERS ===")
        
        # Get sample of missing orders
        query_missing = """
        SELECT order_id, faktur_id, customer_id, do_number, created_date
        FROM "order" 
        WHERE faktur_date = '2025-04-09' 
        AND warehouse_id = %s
        ORDER BY order_id
        LIMIT 10
        """
        cursor_a.execute(query_missing, (warehouse_id,))
        missing_orders = cursor_a.fetchall()
        
        logger.info("Sample missing orders from 2025-04-09:")
        for order in missing_orders:
            order_id, faktur_id, customer_id, do_number, created_date = order
            logger.info(f"  order_id={order_id}, faktur_id={faktur_id}, customer={customer_id}, do={do_number}, created={created_date}")
        
        # Check for data anomalies
        logger.info("=== CHECKING FOR DATA ANOMALIES ===")
        
        # Check for NULL values in critical fields
        query_null_check = """
        SELECT COUNT(*) as null_count
        FROM "order" 
        WHERE faktur_date = '2025-04-09' 
        AND warehouse_id = %s
        AND (faktur_id IS NULL OR customer_id IS NULL)
        """
        cursor_a.execute(query_null_check, (warehouse_id,))
        null_count = cursor_a.fetchone()[0]
        logger.info(f"Orders with NULL faktur_id or customer_id: {null_count}")
        
        # Check for very long faktur_id
        query_long_faktur = """
        SELECT faktur_id, LENGTH(faktur_id) as faktur_length
        FROM "order" 
        WHERE faktur_date = '2025-04-09' 
        AND warehouse_id = %s
        AND LENGTH(faktur_id) > 50
        ORDER BY LENGTH(faktur_id) DESC
        LIMIT 5
        """
        cursor_a.execute(query_long_faktur, (warehouse_id,))
        long_fakturs = cursor_a.fetchall()
        
        if long_fakturs:
            logger.warning("Found faktur_id with unusual length:")
            for faktur_id, length in long_fakturs:
                logger.warning(f"  faktur_id={faktur_id}, length={length}")
        
        # Check for special characters in faktur_id
        query_special_chars = """
        SELECT faktur_id
        FROM "order" 
        WHERE faktur_date = '2025-04-09' 
        AND warehouse_id = %s
        AND faktur_id ~ '[^a-zA-Z0-9\-]'
        LIMIT 5
        """
        cursor_a.execute(query_special_chars, (warehouse_id,))
        special_chars = cursor_a.fetchall()
        
        if special_chars:
            logger.warning("Found faktur_id with special characters:")
            for (faktur_id,) in special_chars:
                logger.warning(f"  faktur_id={faktur_id}")
        
        # Check order details for 2025-04-09
        logger.info("=== CHECKING ORDER DETAILS FOR 2025-04-09 ===")
        
        query_details_a = """
        SELECT COUNT(*) as total_details
        FROM order_detail od
        JOIN "order" o ON od.order_id = o.order_id
        WHERE o.faktur_date = '2025-04-09' 
        AND o.warehouse_id = %s
        """
        cursor_a.execute(query_details_a, (warehouse_id,))
        total_details_a = cursor_a.fetchone()[0]
        logger.info(f"Database A - Order details for 2025-04-09: {total_details_a}")
        
        query_details_b = """
        SELECT COUNT(*) as total_details
        FROM order_detail_main odm
        JOIN order_main om ON odm.order_id = om.order_id
        WHERE om.faktur_date = '2025-04-09' 
        AND om.warehouse_id = %s
        """
        cursor_b.execute(query_details_b, (str(warehouse_id),))
        total_details_b = cursor_b.fetchone()[0]
        logger.info(f"Database B - Order details for 2025-04-09: {total_details_b}")
        
        # Check for problematic order details
        query_problematic_details = """
        SELECT od.order_detail_id, od.order_id, od.product_id, od.line_id, o.faktur_id
        FROM order_detail od
        JOIN "order" o ON od.order_id = o.order_id
        WHERE o.faktur_date = '2025-04-09' 
        AND o.warehouse_id = %s
        AND (od.product_id IS NULL OR od.line_id IS NULL)
        LIMIT 5
        """
        cursor_a.execute(query_problematic_details, (warehouse_id,))
        problematic_details = cursor_a.fetchall()
        
        if problematic_details:
            logger.warning("Found order details with NULL values:")
            for detail_id, order_id, product_id, line_id, faktur_id in problematic_details:
                logger.warning(f"  detail_id={detail_id}, order_id={order_id}, product_id={product_id}, line_id={line_id}, faktur_id={faktur_id}")
        
        logger.info("=== INVESTIGATION COMPLETED ===")
        
    except Exception as e:
        logger.error(f"Error during investigation: {e}")
        raise
    finally:
        cursor_a.close()
        cursor_b.close()
        conn_a.close()
        conn_b.close()

def main():
    """Main function"""
    logger = setup_logging()
    
    if len(sys.argv) != 2:
        logger.error("Usage: python3 debug_copy_failure.py <warehouse_id>")
        sys.exit(1)
    
    warehouse_id = int(sys.argv[1])
    
    try:
        check_failed_date_data(logger, warehouse_id)
    except Exception as e:
        logger.error(f"Script failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 