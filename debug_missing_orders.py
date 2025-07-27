#!/usr/bin/env python3
"""
Debug script to investigate specific missing orders
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

def check_missing_orders(logger, warehouse_id, start_date, end_date):
    """Check specific missing orders"""
    # Convert string dates to datetime.date for comparison
    start_date_obj = datetime.strptime(start_date, '%Y-%m-%d').date()
    end_date_obj = datetime.strptime(end_date, '%Y-%m-%d').date()
    
    conn_a = get_db_connection('A')
    conn_b = get_db_connection('B')
    
    try:
        cursor_a = conn_a.cursor()
        cursor_b = conn_b.cursor()
        
        # Get missing order IDs from debug output
        missing_order_ids = [
            12529, 12613, 12689, 12837, 12854, 12856, 12857, 12859, 12860, 12861
        ]
        
        logger.info(f"=== INVESTIGATING MISSING ORDERS ===")
        logger.info(f"Warehouse ID: {warehouse_id}")
        logger.info(f"Date Range: {start_date} to {end_date}")
        
        for order_id in missing_order_ids:
            logger.info(f"\n--- Checking Order ID: {order_id} ---")
            
            # Check in Database A
            cursor_a.execute("""
                SELECT order_id, faktur_id, faktur_date, customer_id, warehouse_id, 
                       do_number, status, created_date
                FROM "order" 
                WHERE order_id = %s
            """, (order_id,))
            
            order_a = cursor_a.fetchone()
            if order_a:
                logger.info(f"✅ Found in Database A:")
                logger.info(f"  order_id={order_a[0]}, faktur_id={order_a[1]}, date={order_a[2]}")
                logger.info(f"  customer={order_a[3]}, warehouse={order_a[4]}, do={order_a[5]}")
                logger.info(f"  status={order_a[6]}, created={order_a[7]}")
                
                # Check if it should be included in our copy criteria
                if (order_a[2] >= start_date_obj and order_a[2] <= end_date_obj and 
                    order_a[4] == warehouse_id):
                    logger.info(f"  ✅ Should be copied (matches criteria)")
                else:
                    logger.info(f"  ❌ Should NOT be copied (doesn't match criteria)")
            else:
                logger.info(f"❌ NOT found in Database A")
            
            # Check in Database B
            cursor_b.execute("""
                SELECT order_id, faktur_id, faktur_date, customer_id, warehouse_id, 
                       do_number, status, created_date
                FROM order_main 
                WHERE order_id = %s
            """, (order_id,))
            
            order_b = cursor_b.fetchone()
            if order_b:
                logger.info(f"✅ Found in Database B:")
                logger.info(f"  order_id={order_b[0]}, faktur_id={order_b[1]}, date={order_b[2]}")
                logger.info(f"  customer={order_b[3]}, warehouse={order_b[4]}, do={order_b[5]}")
                logger.info(f"  status={order_b[6]}, created={order_b[7]}")
            else:
                logger.info(f"❌ NOT found in Database B")
        
        # Check order_id=99998 specifically
        logger.info(f"\n=== CHECKING ORDER ID 99998 (for order details) ===")
        
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
            
            if (order_99998_a[2] >= start_date_obj and order_99998_a[2] <= end_date_obj and 
                order_99998_a[4] == warehouse_id):
                logger.info(f"  ✅ Should be copied (matches criteria)")
            else:
                logger.info(f"  ❌ Should NOT be copied (doesn't match criteria)")
        else:
            logger.info(f"❌ Order 99998 NOT found in Database A")
        
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
        
    except Exception as e:
        logger.error(f"Error checking missing orders: {e}")
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
        logger.error("Usage: python3 debug_missing_orders.py <warehouse_id> <start_date> <end_date>")
        logger.error("Example: python3 debug_missing_orders.py 4512 2025-03-01 2025-03-31")
        sys.exit(1)
    
    warehouse_id = int(sys.argv[1])
    start_date = sys.argv[2]
    end_date = sys.argv[3]
    
    logger.info("=== MISSING ORDERS INVESTIGATION ===")
    logger.info(f"Warehouse ID: {warehouse_id}")
    logger.info(f"Date Range: {start_date} to {end_date}")
    
    try:
        check_missing_orders(logger, warehouse_id, start_date, end_date)
        logger.info("=== INVESTIGATION COMPLETED ===")
    except Exception as e:
        logger.error(f"Investigation failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 