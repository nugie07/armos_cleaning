#!/usr/bin/env python3
"""
Debug script for April 2025 missing orders
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

def debug_april_missing(logger, warehouse_id):
    """Debug April 2025 missing orders"""
    conn_a = get_db_connection('A')
    conn_b = get_db_connection('B')
    
    try:
        cursor_a = conn_a.cursor()
        cursor_b = conn_b.cursor()
        
        logger.info(f"=== APRIL 2025 MISSING ORDERS DEBUG ===")
        logger.info(f"Warehouse ID: {warehouse_id}")
        
        # Check total orders by date in April
        logger.info(f"\n=== ORDERS BY DATE IN APRIL 2025 ===")
        
        cursor_a.execute("""
            SELECT faktur_date, COUNT(*) as order_count
            FROM "order" 
            WHERE warehouse_id = %s 
            AND faktur_date >= '2025-04-01' 
            AND faktur_date <= '2025-04-30'
            GROUP BY faktur_date
            ORDER BY faktur_date
        """, (warehouse_id,))
        
        orders_by_date_a = cursor_a.fetchall()
        logger.info(f"Database A - Orders by date:")
        for date, count in orders_by_date_a:
            logger.info(f"  {date}: {count} orders")
        
        cursor_b.execute("""
            SELECT faktur_date, COUNT(*) as order_count
            FROM order_main 
            WHERE warehouse_id = %s 
            AND faktur_date >= '2025-04-01' 
            AND faktur_date <= '2025-04-30'
            GROUP BY faktur_date
            ORDER BY faktur_date
        """, (warehouse_id,))
        
        orders_by_date_b = cursor_b.fetchall()
        logger.info(f"Database B - Orders by date:")
        for date, count in orders_by_date_b:
            logger.info(f"  {date}: {count} orders")
        
        # Check specific missing orders from 2025-04-15
        logger.info(f"\n=== MISSING ORDERS FROM 2025-04-15 ===")
        
        cursor_a.execute("""
            SELECT order_id, faktur_id, customer_id, warehouse_id, status, created_date
            FROM "order" 
            WHERE warehouse_id = %s 
            AND faktur_date = '2025-04-15'
            AND customer_id IS NULL
            ORDER BY order_id
            LIMIT 10
        """, (warehouse_id,))
        
        missing_orders_a = cursor_a.fetchall()
        logger.info(f"Database A - Orders with customer_id IS NULL on 2025-04-15:")
        for order in missing_orders_a:
            logger.info(f"  order_id={order[0]}, faktur_id={order[1]}, customer={order[2]}, warehouse={order[3]}")
            
            # Check if this order exists in Database B
            cursor_b.execute("""
                SELECT order_id, faktur_id, customer_id, warehouse_id
                FROM order_main 
                WHERE order_id = %s
            """, (order[0],))
            
            order_b = cursor_b.fetchone()
            if order_b:
                logger.info(f"    ✅ Found in Database B")
            else:
                logger.info(f"    ❌ NOT found in Database B")
        
        # Check order details count by date
        logger.info(f"\n=== ORDER DETAILS BY DATE IN APRIL 2025 ===")
        
        cursor_a.execute("""
            SELECT o.faktur_date, COUNT(od.order_detail_id) as detail_count
            FROM "order" o
            LEFT JOIN order_detail od ON o.order_id = od.order_id
            WHERE o.warehouse_id = %s 
            AND o.faktur_date >= '2025-04-01' 
            AND o.faktur_date <= '2025-04-30'
            GROUP BY o.faktur_date
            ORDER BY o.faktur_date
        """, (warehouse_id,))
        
        details_by_date_a = cursor_a.fetchall()
        logger.info(f"Database A - Order details by date:")
        for date, count in details_by_date_a:
            logger.info(f"  {date}: {count} details")
        
        cursor_b.execute("""
            SELECT om.faktur_date, COUNT(odm.order_detail_id) as detail_count
            FROM order_main om
            LEFT JOIN order_detail_main odm ON om.order_id = odm.order_id
            WHERE om.warehouse_id = %s 
            AND om.faktur_date >= '2025-04-01' 
            AND om.faktur_date <= '2025-04-30'
            GROUP BY om.faktur_date
            ORDER BY om.faktur_date
        """, (warehouse_id,))
        
        details_by_date_b = cursor_b.fetchall()
        logger.info(f"Database B - Order details by date:")
        for date, count in details_by_date_b:
            logger.info(f"  {date}: {count} details")
        
        # Check for duplicate order details
        logger.info(f"\n=== CHECKING FOR DUPLICATE ORDER DETAILS ===")
        
        cursor_b.execute("""
            SELECT order_id, product_id, line_id, COUNT(*) as duplicate_count
            FROM order_detail_main
            GROUP BY order_id, product_id, line_id
            HAVING COUNT(*) > 1
            ORDER BY duplicate_count DESC
            LIMIT 10
        """)
        
        duplicates = cursor_b.fetchall()
        if duplicates:
            logger.info(f"Found {len(duplicates)} duplicate order details:")
            for dup in duplicates:
                logger.info(f"  order_id={dup[0]}, product={dup[1]}, line={dup[2]}, count={dup[3]}")
        else:
            logger.info(f"No duplicate order details found")
        
    except Exception as e:
        logger.error(f"Error debugging April missing orders: {e}")
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
        logger.error("Usage: python3 debug_april_missing.py <warehouse_id>")
        logger.error("Example: python3 debug_april_missing.py 4512")
        sys.exit(1)
    
    warehouse_id = int(sys.argv[1])
    
    logger.info("=== APRIL 2025 MISSING ORDERS DEBUG ===")
    logger.info(f"Warehouse ID: {warehouse_id}")
    
    try:
        debug_april_missing(logger, warehouse_id)
        logger.info("=== DEBUG COMPLETED ===")
    except Exception as e:
        logger.error(f"Debug failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 