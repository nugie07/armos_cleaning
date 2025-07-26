#!/usr/bin/env python3
"""
Debug script to check order and order_detail data for specific warehouse_id and date range
"""

import os
import psycopg2
from dotenv import load_dotenv
import logging
import argparse
import sys

def setup_logging():
    """Setup logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('./logs/debug_order.log')
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

# Load environment variables
load_dotenv('.env')
load_dotenv('config.env')

def debug_order_data(warehouse_id, start_date, end_date):
    """Debug order data for specific warehouse_id and date range"""
    logger = setup_logging()
    
    logger.info(f"=== Order Data Debug for Warehouse {warehouse_id} ===")
    logger.info(f"Date Range: {start_date} to {end_date}")
    
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
                WHERE faktur_date BETWEEN %s AND %s
            """, (start_date, end_date))
            orders_in_range = cursor.fetchone()[0]
            logger.info(f"Orders in date range ({start_date} to {end_date}): {orders_in_range}")
        
        # Count orders with warehouse_id
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT COUNT(*) FROM "order" 
                WHERE warehouse_id = %s
            """, (warehouse_id,))
            orders_warehouse = cursor.fetchone()[0]
            logger.info(f"Orders with warehouse_id = {warehouse_id}: {orders_warehouse}")
        
        # Count orders with date range AND warehouse_id
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT COUNT(*) FROM "order" 
                WHERE faktur_date BETWEEN %s AND %s
                AND warehouse_id = %s
            """, (start_date, end_date, warehouse_id))
            orders_both_conditions = cursor.fetchone()[0]
            logger.info(f"Orders with date range AND warehouse_id = {warehouse_id}: {orders_both_conditions}")
        
        # Show sample order data
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT order_id, faktur_id, faktur_date, customer_id, warehouse_id, do_number
                FROM "order" 
                WHERE faktur_date BETWEEN %s AND %s
                AND warehouse_id = %s
                ORDER BY faktur_date
                LIMIT 5
            """, (start_date, end_date, warehouse_id))
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
        
        # Count order_details for orders in date range with warehouse_id
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT COUNT(*) FROM order_detail od
                JOIN "order" o ON od.order_id = o.order_id
                WHERE o.faktur_date BETWEEN %s AND %s
                AND o.warehouse_id = %s
            """, (start_date, end_date, warehouse_id))
            order_details_filtered = cursor.fetchone()[0]
            logger.info(f"Order details for orders with date range AND warehouse_id = {warehouse_id}: {order_details_filtered}")
        
        # Show sample order_detail data
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT od.order_detail_id, od.order_id, o.do_number, o.faktur_date, o.warehouse_id
                FROM order_detail od
                JOIN "order" o ON od.order_id = o.order_id
                WHERE o.faktur_date BETWEEN %s AND %s
                AND o.warehouse_id = %s
                ORDER BY o.faktur_date
                LIMIT 10
            """, (start_date, end_date, warehouse_id))
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

def main():
    """Main function with command line arguments"""
    parser = argparse.ArgumentParser(description='Debug order and order_detail data for specific warehouse_id and date range')
    parser.add_argument('warehouse_id', type=str, help='Warehouse ID to filter by')
    parser.add_argument('start_date', type=str, help='Start date (YYYY-MM-DD)')
    parser.add_argument('end_date', type=str, help='End date (YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    try:
        debug_order_data(args.warehouse_id, args.start_date, args.end_date)
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 