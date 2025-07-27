#!/usr/bin/env python3
"""
Debug script to check DO numbers from order_main table
"""

import os
import psycopg2
from dotenv import load_dotenv
import logging
import argparse
import sys

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
            logging.FileHandler('./logs/debug_do_numbers.log')
        ]
    )
    return logging.getLogger(__name__)

def get_db_connection(database_type):
    """Get database connection"""
    try:
        if database_type.upper() == 'B':
            conn = psycopg2.connect(
                host=os.getenv('DB_B_HOST'),
                port=os.getenv('DB_B_PORT'),
                database=os.getenv('DB_B_NAME'),
                user=os.getenv('DB_B_USER'),
                password=os.getenv('DB_B_PASSWORD')
            )
        else:
            raise ValueError(f"Unsupported database type: {database_type}")
        
        return conn
    except Exception as e:
        logging.error(f"Failed to connect to database {database_type}: {str(e)}")
        raise

def debug_do_numbers(warehouse_id, start_date, end_date):
    """Debug DO numbers from order_main table"""
    logger = setup_logging()
    
    logger.info("=== DO Numbers Debug ===")
    logger.info(f"Warehouse ID: {warehouse_id}")
    logger.info(f"Date Range: {start_date} to {end_date}")
    
    try:
        # Connect to database B
        conn = get_db_connection('B')
        logger.info("✓ Connected to Database B successfully")
        
        # Use warehouse_id as string (VARCHAR in database)
        logger.info(f"Using warehouse_id as string: {warehouse_id}")
        warehouse_param = warehouse_id
        
        # Check total records in order_main
        logger.info("\n--- Checking order_main table ---")
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM order_main")
            total_orders = cursor.fetchone()[0]
            logger.info(f"Total orders in order_main: {total_orders}")
        
        # Check orders in date range
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT COUNT(*) FROM order_main 
                WHERE faktur_date BETWEEN %s AND %s
            """, (start_date, end_date))
            orders_in_range = cursor.fetchone()[0]
            logger.info(f"Orders in date range ({start_date} to {end_date}): {orders_in_range}")
        
        # Check orders with warehouse_id
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT COUNT(*) FROM order_main 
                WHERE warehouse_id = %s
            """, (warehouse_param,))
            orders_warehouse = cursor.fetchone()[0]
            logger.info(f"Orders with warehouse_id = {warehouse_id}: {orders_warehouse}")
        
        # Check orders with both conditions
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT COUNT(*) FROM order_main 
                WHERE warehouse_id = %s 
                AND faktur_date BETWEEN %s AND %s
            """, (warehouse_param, start_date, end_date))
            orders_both_conditions = cursor.fetchone()[0]
            logger.info(f"Orders with warehouse_id AND date range: {orders_both_conditions}")
        
        # Check DO numbers with both conditions
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT COUNT(*) FROM order_main 
                WHERE warehouse_id = %s 
                AND faktur_date BETWEEN %s AND %s
                AND do_number IS NOT NULL
            """, (warehouse_param, start_date, end_date))
            do_numbers_count = cursor.fetchone()[0]
            logger.info(f"Orders with DO numbers (not null): {do_numbers_count}")
        
        # Show sample data
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT order_id, faktur_id, faktur_date, warehouse_id, do_number
                FROM order_main 
                WHERE warehouse_id = %s 
                AND faktur_date BETWEEN %s AND %s
                ORDER BY faktur_date
                LIMIT 10
            """, (warehouse_param, start_date, end_date))
            sample_orders = cursor.fetchall()
            logger.info(f"\nSample order data:")
            for order in sample_orders:
                logger.info(f"  Order ID: {order[0]}, Faktur ID: {order[1]}, Date: {order[2]}, Warehouse: {order[3]}, DO: {order[4]}")
        
        # Show unique DO numbers
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT DISTINCT do_number 
                FROM order_main 
                WHERE warehouse_id = %s 
                AND faktur_date BETWEEN %s AND %s
                AND do_number IS NOT NULL
                ORDER BY do_number
                LIMIT 10
            """, (warehouse_param, start_date, end_date))
            unique_do_numbers = cursor.fetchall()
            logger.info(f"\nUnique DO numbers found:")
            for do_num in unique_do_numbers:
                logger.info(f"  {do_num[0]}")
        
        # Check data types
        logger.info("\n--- Checking Data Types ---")
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'order_main' 
                AND column_name IN ('warehouse_id', 'faktur_date', 'do_number')
            """)
            column_types = cursor.fetchall()
            logger.info("order_main table column types:")
            for col in column_types:
                logger.info(f"  {col[0]}: {col[1]}")
        
        # Check warehouse_id values
        logger.info("\n--- Checking Warehouse ID Values ---")
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT DISTINCT warehouse_id, COUNT(*) as count
                FROM order_main 
                WHERE faktur_date BETWEEN %s AND %s
                GROUP BY warehouse_id
                ORDER BY count DESC
                LIMIT 10
            """, (start_date, end_date))
            warehouse_counts = cursor.fetchall()
            logger.info("Warehouse ID distribution in date range:")
            for warehouse in warehouse_counts:
                logger.info(f"  {warehouse[0]}: {warehouse[1]} orders")
        
        conn.close()
        logger.info("✓ Database connection closed")
        
    except Exception as e:
        logger.error(f"Error during debug: {str(e)}")
        raise

def main():
    """Main function with command line arguments"""
    parser = argparse.ArgumentParser(description='Debug DO numbers from order_main table')
    parser.add_argument('warehouse_id', type=str, help='Warehouse ID to filter by')
    parser.add_argument('start_date', type=str, help='Start date (YYYY-MM-DD)')
    parser.add_argument('end_date', type=str, help='End date (YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    try:
        debug_do_numbers(args.warehouse_id, args.start_date, args.end_date)
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 