#!/usr/bin/env python3
"""
Debug script to investigate order details copy issues
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

def get_db_connection(database_type):
    """Get database connection"""
    if database_type == 'A':
        return psycopg2.connect(
            host=os.getenv('DB_A_HOST'),
            port=os.getenv('DB_A_PORT'),
            database=os.getenv('DB_A_NAME'),
            user=os.getenv('DB_A_USER'),
            password=os.getenv('DB_A_PASSWORD')
        )
    else:
        return psycopg2.connect(
            host=os.getenv('DB_B_HOST'),
            port=os.getenv('DB_B_PORT'),
            database=os.getenv('DB_B_NAME'),
            user=os.getenv('DB_B_USER'),
            password=os.getenv('DB_B_PASSWORD')
        )

def debug_order_details(logger, warehouse_id, start_date, end_date):
    """Debug order details copy issues"""
    # Convert string dates to datetime.date for comparison
    start_date_obj = datetime.strptime(start_date, '%Y-%m-%d').date()
    end_date_obj = datetime.strptime(end_date, '%Y-%m-%d').date()
    
    conn_a = get_db_connection('A')
    conn_b = get_db_connection('B')
    
    try:
        cursor_a = conn_a.cursor()
        cursor_b = conn_b.cursor()
        
        logger.info(f"=== ORDER DETAILS DEBUG ===")
        logger.info(f"Warehouse ID: {warehouse_id}")
        logger.info(f"Date Range: {start_date} to {end_date}")
        
        # Check order_id=99998 specifically
        logger.info(f"\n=== CHECKING ORDER 99998 DETAILS ===")
        
        # Check order in Database A
        cursor_a.execute("""
            SELECT order_id, faktur_id, faktur_date, customer_id, warehouse_id, 
                   do_number, status, created_date
            FROM "order" 
            WHERE order_id = 99998
        """)
        
        order_99998_a = cursor_a.fetchone()
        if order_99998_a:
            logger.info(f"✅ Order 99998 found in Database A:")
            logger.info(f"  order_id={order_99998_a[0]}, faktur_id={order_99998_a[1]}, date={order_99998_a[2]}")
            logger.info(f"  customer={order_99998_a[3]}, warehouse={order_99998_a[4]}, do={order_99998_a[5]}")
        else:
            logger.info(f"❌ Order 99998 NOT found in Database A")
            return
        
        # Check order in Database B
        cursor_b.execute("""
            SELECT order_id, faktur_id, faktur_date, customer_id, warehouse_id, 
                   do_number, status, created_date
            FROM order_main 
            WHERE order_id = 99998
        """)
        
        order_99998_b = cursor_b.fetchone()
        if order_99998_b:
            logger.info(f"✅ Order 99998 found in Database B:")
            logger.info(f"  order_id={order_99998_b[0]}, faktur_id={order_99998_b[1]}, date={order_99998_b[2]}")
            logger.info(f"  customer={order_99998_b[3]}, warehouse={order_99998_b[4]}, do={order_99998_b[5]}")
        else:
            logger.info(f"❌ Order 99998 NOT found in Database B")
            return
        
        # Check order details in Database A
        logger.info(f"\n=== ORDER DETAILS IN DATABASE A ===")
        cursor_a.execute("""
            SELECT order_detail_id, order_id, product_id, line_id, quantity_faktur, 
                   net_price, status, created_date
            FROM order_detail 
            WHERE order_id = 99998
            ORDER BY line_id
        """)
        
        order_details_a = cursor_a.fetchall()
        logger.info(f"Found {len(order_details_a)} order details in Database A")
        
        for detail in order_details_a:
            logger.info(f"  detail_id={detail[0]}, order_id={detail[1]}, product={detail[2]}, line={detail[3]}")
            logger.info(f"    qty={detail[4]}, price={detail[5]}, status={detail[6]}")
        
        # Check order details in Database B
        logger.info(f"\n=== ORDER DETAILS IN DATABASE B ===")
        cursor_b.execute("""
            SELECT order_detail_id, order_id, product_id, line_id, quantity_faktur, 
                   net_price, status, created_date
            FROM order_detail_main 
            WHERE order_id = 99998
            ORDER BY line_id
        """)
        
        order_details_b = cursor_b.fetchall()
        logger.info(f"Found {len(order_details_b)} order details in Database B")
        
        for detail in order_details_b:
            logger.info(f"  detail_id={detail[0]}, order_id={detail[1]}, product={detail[2]}, line={detail[3]}")
            logger.info(f"    qty={detail[4]}, price={detail[5]}, status={detail[6]}")
        
        # Check copy query logic
        logger.info(f"\n=== COPY QUERY ANALYSIS ===")
        
        # Simulate the copy query for order details
        faktur_id = order_99998_a[1]
        faktur_date = order_99998_a[2]
        customer_id = order_99998_a[3]
        
        logger.info(f"Copy parameters:")
        logger.info(f"  faktur_id: {faktur_id}")
        logger.info(f"  faktur_date: {faktur_date}")
        logger.info(f"  customer_id: {customer_id}")
        logger.info(f"  warehouse_id: {warehouse_id}")
        
        # Check if order exists in Database B for lookup
        if customer_id is None:
            lookup_query = """
            SELECT order_id FROM order_main 
            WHERE faktur_id = %s AND faktur_date = %s AND customer_id IS NULL
            """
            lookup_params = (faktur_id, faktur_date)
        else:
            lookup_query = """
            SELECT order_id FROM order_main 
            WHERE faktur_id = %s AND faktur_date = %s AND customer_id = %s::VARCHAR
            """
            lookup_params = (faktur_id, faktur_date, customer_id)
        
        logger.info(f"Lookup query: {lookup_query}")
        logger.info(f"Lookup params: {lookup_params}")
        
        cursor_b.execute(lookup_query, lookup_params)
        lookup_result = cursor_b.fetchone()
        
        if lookup_result:
            logger.info(f"✅ Lookup successful: order_id={lookup_result[0]}")
        else:
            logger.info(f"❌ Lookup failed: No order found in Database B")
            
            # Check what orders exist with this faktur_id
            cursor_b.execute("""
                SELECT order_id, faktur_id, faktur_date, customer_id, warehouse_id
                FROM order_main 
                WHERE faktur_id = %s
                ORDER BY order_id
            """, (faktur_id,))
            
            similar_orders = cursor_b.fetchall()
            logger.info(f"Orders with faktur_id {faktur_id} in Database B:")
            for order in similar_orders:
                logger.info(f"  order_id={order[0]}, date={order[2]}, customer={order[3]}, warehouse={order[4]}")
        
    except Exception as e:
        logger.error(f"Error debugging order details: {e}")
        raise
    finally:
        cursor_a.close()
        cursor_b.close()
        conn_a.close()
        conn_b.close()

def main():
    """Main function"""
    logger = setup_logging()
    
    if len(sys.argv) != 4:
        logger.error("Usage: python3 debug_order_details.py <warehouse_id> <start_date> <end_date>")
        logger.error("Example: python3 debug_order_details.py 4512 2025-03-01 2025-03-31")
        sys.exit(1)
    
    warehouse_id = int(sys.argv[1])
    start_date = sys.argv[2]
    end_date = sys.argv[3]
    
    logger.info("=== ORDER DETAILS DEBUG ===")
    logger.info(f"Warehouse ID: {warehouse_id}")
    logger.info(f"Date Range: {start_date} to {end_date}")
    
    try:
        debug_order_details(logger, warehouse_id, start_date, end_date)
        logger.info("=== DEBUG COMPLETED ===")
    except Exception as e:
        logger.error(f"Debug failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 