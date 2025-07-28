#!/usr/bin/env python3
"""
Debug script to check table structure and column count
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

def check_table_structure(logger):
    """Check the exact structure of order table in Database A"""
    logger.info("=== CHECKING TABLE STRUCTURE ===")
    
    conn_a = get_db_connection('A')
    conn_b = get_db_connection('B')
    
    try:
        cursor_a = conn_a.cursor()
        cursor_b = conn_b.cursor()
        
        # Check order table structure in Database A
        logger.info("--- Database A (order table) ---")
        cursor_a.execute("""
            SELECT column_name, data_type, ordinal_position
            FROM information_schema.columns 
            WHERE table_name = 'order' 
            ORDER BY ordinal_position
        """)
        order_columns_a = cursor_a.fetchall()
        
        logger.info(f"Total columns in order table (DB A): {len(order_columns_a)}")
        for col in order_columns_a:
            logger.info(f"  {col[2]}. {col[0]}: {col[1]}")
        
        # Check order_main table structure in Database B
        logger.info("--- Database B (order_main table) ---")
        cursor_b.execute("""
            SELECT column_name, data_type, ordinal_position
            FROM information_schema.columns 
            WHERE table_name = 'order_main' 
            ORDER BY ordinal_position
        """)
        order_columns_b = cursor_b.fetchall()
        
        logger.info(f"Total columns in order_main table (DB B): {len(order_columns_b)}")
        for col in order_columns_b:
            logger.info(f"  {col[2]}. {col[0]}: {col[1]}")
        
        # Check order_detail table structure in Database A
        logger.info("--- Database A (order_detail table) ---")
        cursor_a.execute("""
            SELECT column_name, data_type, ordinal_position
            FROM information_schema.columns 
            WHERE table_name = 'order_detail' 
            ORDER BY ordinal_position
        """)
        detail_columns_a = cursor_a.fetchall()
        
        logger.info(f"Total columns in order_detail table (DB A): {len(detail_columns_a)}")
        for col in detail_columns_a:
            logger.info(f"  {col[2]}. {col[0]}: {col[1]}")
        
        # Check order_detail_main table structure in Database B
        logger.info("--- Database B (order_detail_main table) ---")
        cursor_b.execute("""
            SELECT column_name, data_type, ordinal_position
            FROM information_schema.columns 
            WHERE table_name = 'order_detail_main' 
            ORDER BY ordinal_position
        """)
        detail_columns_b = cursor_b.fetchall()
        
        logger.info(f"Total columns in order_detail_main table (DB B): {len(detail_columns_b)}")
        for col in detail_columns_b:
            logger.info(f"  {col[2]}. {col[0]}: {col[1]}")
        
        # Test SELECT query to see actual column count
        logger.info("--- Testing SELECT query for order table ---")
        test_query = """
        SELECT 
            order_id, faktur_id, faktur_date, delivery_date, do_number, status, skip_count,
            created_date, created_by, updated_date, updated_by, notes, customer_id,
            warehouse_id, delivery_type_id, order_integration_id, origin_name,
            origin_address_1, origin_address_2, origin_city, origin_zipcode,
            origin_phone, origin_email, destination_name, destination_address_1,
            destination_address_2, destination_city, destination_zip_code,
            destination_phone, destination_email, client_id, cancel_reason,
            rdo_integration_id, address_change, divisi, pre_status
        FROM "order"
        LIMIT 1
        """
        
        cursor_a.execute(test_query)
        result = cursor_a.fetchone()
        if result:
            logger.info(f"SELECT query returned {len(result)} columns")
            logger.info("Column values:")
            for i, value in enumerate(result):
                logger.info(f"  {i+1}: {value}")
        else:
            logger.warning("No data found in order table")
        
    except Exception as e:
        logger.error(f"Error checking table structure: {e}")
        raise
    finally:
        conn_a.close()
        conn_b.close()

def main():
    """Main function"""
    logger = setup_logging()
    
    try:
        check_table_structure(logger)
    except Exception as e:
        logger.error(f"Script failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 