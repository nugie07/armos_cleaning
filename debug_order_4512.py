#!/usr/bin/env python3
"""
Debug script to check order and order_detail data for warehouse_id 4512
"""

import os
import psycopg2
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv('.env')
load_dotenv('config.env')

def setup_logging():
    """Setup logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('./logs/debug_order_4512.log')
        ]
    )
    return logging.getLogger(__name__)

def get_db_connection(database_type):
    """Get database connection"""
    try:
        if database_type.upper() == 'A':
            conn = psycopg2.connect(
                host=os.getenv('DB_A_HOST'),
                port=os.getenv('DB_A_PORT'),
                database=os.getenv('DB_A_NAME'),
                user=os.getenv('DB_A_USER'),
                password=os.getenv('DB_A_PASSWORD')
            )
        else:
            raise ValueError(f"Unsupported database type: {database_type}")
        
        return conn
    except Exception as e:
        logging.error(f"Failed to connect to database {database_type}: {str(e)}")
        raise

def debug_order_data():
    """Debug order data for warehouse_id 4512"""
    logger = setup_logging()
    
    logger.info("=== Order Data Debug for Warehouse 4512 ===")
    
    try:
        # Connect to database A
        conn = get_db_connection('A')
        logger.info("✓ Connected to Database A successfully")
        
        # Check order data
        logger.info("\n--- Checking Order Data ---")
        
        # Count total orders
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM \"order\"")
            total_orders = cursor.fetchone()[0]
            logger.info(f"Total orders in database: {total_orders}")
        
        # Count orders in date range
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT COUNT(*) FROM "order" 
                WHERE faktur_date BETWEEN '2025-03-01' AND '2025-03-31'
            """)
            orders_in_range = cursor.fetchone()[0]
            logger.info(f"Orders in date range (2025-03-01 to 2025-03-31): {orders_in_range}")
        
        # Count orders with warehouse_id 4512
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT COUNT(*) FROM "order" 
                WHERE warehouse_id = 4512
            """)
            orders_warehouse_4512 = cursor.fetchone()[0]
            logger.info(f"Orders with warehouse_id = 4512: {orders_warehouse_4512}")
        
        # Count orders with date range AND warehouse_id 4512
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT COUNT(*) FROM "order" 
                WHERE faktur_date BETWEEN '2025-03-01' AND '2025-03-31'
                AND warehouse_id = 4512
            """)
            orders_both_conditions = cursor.fetchone()[0]
            logger.info(f"Orders with date range AND warehouse_id = 4512: {orders_both_conditions}")
        
        # Show sample order data
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT order_id, faktur_id, faktur_date, customer_id, warehouse_id, do_number
                FROM "order" 
                WHERE faktur_date BETWEEN '2025-03-01' AND '2025-03-31'
                AND warehouse_id = 4512
                ORDER BY faktur_date
                LIMIT 5
            """)
            sample_orders = cursor.fetchall()
            logger.info(f"\nSample order data:")
            for order in sample_orders:
                logger.info(f"  Order ID: {order[0]}, Faktur ID: {order[1]}, Date: {order[2]}, Customer: {order[3]}, Warehouse: {order[4]}, DO: {order[5]}")
        
        # Check order_detail data
        logger.info("\n--- Checking Order Detail Data ---")
        
        # Count total order_details
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM order_detail")
            total_order_details = cursor.fetchone()[0]
            logger.info(f"Total order_details in database: {total_order_details}")
        
        # Count order_details for orders in date range with warehouse_id 4512
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT COUNT(*) FROM order_detail od
                JOIN "order" o ON od.order_id = o.order_id
                WHERE o.faktur_date BETWEEN '2025-03-01' AND '2025-03-31'
                AND o.warehouse_id = 4512
            """)
            order_details_filtered = cursor.fetchone()[0]
            logger.info(f"Order details for orders with date range AND warehouse_id = 4512: {order_details_filtered}")
        
        # Show sample order_detail data
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT od.order_detail_id, od.order_id, o.do_number, o.faktur_date, o.warehouse_id
                FROM order_detail od
                JOIN "order" o ON od.order_id = o.order_id
                WHERE o.faktur_date BETWEEN '2025-03-01' AND '2025-03-31'
                AND o.warehouse_id = 4512
                ORDER BY o.faktur_date
                LIMIT 10
            """)
            sample_order_details = cursor.fetchall()
            logger.info(f"\nSample order_detail data:")
            for detail in sample_order_details:
                logger.info(f"  Detail ID: {detail[0]}, Order ID: {detail[1]}, DO: {detail[2]}, Date: {detail[3]}, Warehouse: {detail[4]}")
        
        # Check data types
        logger.info("\n--- Checking Data Types ---")
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'order' 
                AND column_name IN ('warehouse_id', 'faktur_date')
            """)
            column_types = cursor.fetchall()
            logger.info("Order table column types:")
            for col in column_types:
                logger.info(f"  {col[0]}: {col[1]}")
        
        conn.close()
        logger.info("✓ Database connection closed")
        
    except Exception as e:
        logger.error(f"Error during debug: {str(e)}")
        raise

if __name__ == "__main__":
    debug_order_data() 