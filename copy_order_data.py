#!/usr/bin/env python3
"""
Script to copy order and order_detail data from Database A to Database B
Includes date range selection and comprehensive logging
"""

import os
import sys
import logging
import psycopg2
import argparse
from datetime import datetime, date
from dotenv import load_dotenv

# Load environment variables - try .env first, then config.env
load_dotenv('.env')
load_dotenv('config.env')

# Configure logging
def setup_logging():
    """Setup logging configuration"""
    log_dir = os.path.dirname(os.getenv('LOG_FILE', './logs/database_operations.log'))
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    logging.basicConfig(
        level=getattr(logging, os.getenv('LOG_LEVEL', 'INFO')),
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(os.getenv('LOG_FILE', './logs/database_operations.log')),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

def get_db_connection(database_type):
    """Get database connection based on type (A or B)"""
    try:
        if database_type.upper() == 'A':
            conn = psycopg2.connect(
                host=os.getenv('DB_A_HOST'),
                port=os.getenv('DB_A_PORT'),
                database=os.getenv('DB_A_NAME'),
                user=os.getenv('DB_A_USER'),
                password=os.getenv('DB_A_PASSWORD')
            )
        elif database_type.upper() == 'B':
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

def copy_order_data(source_conn, target_conn, start_date, end_date, logger):
    """Copy order data from source to target database"""
    batch_size = int(os.getenv('BATCH_SIZE', 1000))
    max_retries = int(os.getenv('MAX_RETRIES', 3))
    
    # Get total count for progress tracking
    count_query = """
    SELECT COUNT(*) FROM "order" 
    WHERE faktur_date >= %s AND faktur_date <= %s
    """
    
    try:
        with source_conn.cursor() as cursor:
            cursor.execute(count_query, (start_date, end_date))
            total_records = cursor.fetchone()[0]
        
        logger.info(f"Found {total_records} order records to copy")
        
        if total_records == 0:
            logger.warning("No order records found for the specified date range")
            return 0
        
        # Copy data in batches
        offset = 0
        copied_count = 0
        
        while offset < total_records:
            # Fetch batch from source
            select_query = """
            SELECT 
                faktur_id, faktur_date, delivery_date, do_number, status, skip_count,
                created_date, created_by, updated_date, updated_by, notes, customer_id,
                warehouse_id, delivery_type_id, order_integration_id, origin_name,
                origin_address_1, origin_address_2, origin_city, origin_zipcode,
                origin_phone, origin_email, destination_name, destination_address_1,
                destination_address_2, destination_city, destination_zip_code,
                destination_phone, destination_email, client_id, cancel_reason,
                rdo_integration_id, address_change, divisi, pre_status
            FROM "order" 
            WHERE faktur_date >= %s AND faktur_date <= %s
            ORDER BY faktur_date
            LIMIT %s OFFSET %s
            """
            
            with source_conn.cursor() as cursor:
                cursor.execute(select_query, (start_date, end_date, batch_size, offset))
                batch_data = cursor.fetchall()
            
            if not batch_data:
                break
            
            # Insert batch into target
            insert_query = """
            INSERT INTO order_main (
                faktur_id, faktur_date, delivery_date, do_number, status, skip_count,
                created_date, created_by, updated_date, updated_by, notes, customer_id,
                warehouse_id, delivery_type_id, order_integration_id, origin_name,
                origin_address_1, origin_address_2, origin_city, origin_zipcode,
                origin_phone, origin_email, destination_name, destination_address_1,
                destination_address_2, destination_city, destination_zip_code,
                destination_phone, destination_email, client_id, cancel_reason,
                rdo_integration_id, address_change, divisi, pre_status
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                     %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                     %s, %s, %s)
            ON CONFLICT (faktur_id) DO NOTHING
            """
            
            retry_count = 0
            while retry_count < max_retries:
                try:
                    with target_conn.cursor() as cursor:
                        cursor.executemany(insert_query, batch_data)
                        target_conn.commit()
                    
                    batch_copied = len(batch_data)
                    copied_count += batch_copied
                    offset += batch_size
                    
                    logger.info(f"Copied {batch_copied} order records (Total: {copied_count}/{total_records})")
                    break
                    
                except Exception as e:
                    retry_count += 1
                    target_conn.rollback()
                    logger.warning(f"Retry {retry_count}/{max_retries} for order batch: {str(e)}")
                    
                    if retry_count >= max_retries:
                        logger.error(f"Failed to copy order batch after {max_retries} retries")
                        raise
        
        logger.info(f"Order data copy completed. Total copied: {copied_count}")
        return copied_count
        
    except Exception as e:
        logger.error(f"Error copying order data: {str(e)}")
        raise

def copy_order_detail_data(source_conn, target_conn, start_date, end_date, logger):
    """Copy order_detail data from source to target database"""
    batch_size = int(os.getenv('BATCH_SIZE', 1000))
    max_retries = int(os.getenv('MAX_RETRIES', 3))
    
    # Get total count for progress tracking
    count_query = """
    SELECT COUNT(*) FROM order_detail od
    JOIN "order" o ON od.order_id = o.order_id
    WHERE o.faktur_date >= %s AND o.faktur_date <= %s
    """
    
    try:
        with source_conn.cursor() as cursor:
            cursor.execute(count_query, (start_date, end_date))
            total_records = cursor.fetchone()[0]
        
        logger.info(f"Found {total_records} order_detail records to copy")
        
        if total_records == 0:
            logger.warning("No order_detail records found for the specified date range")
            return 0
        
        # Copy data in batches
        offset = 0
        copied_count = 0
        
        while offset < total_records:
            # Fetch batch from source
            select_query = """
            SELECT 
                od.quantity_faktur, od.net_price, od.quantity_wms, od.quantity_delivery,
                od.quantity_loading, od.quantity_unloading, od.status, od.cancel_reason,
                od.notes, om.order_id, od.product_id, od.unit_id, od.pack_id, od.line_id,
                od.unloading_latitude, od.unloading_longitude, od.origin_uom, od.origin_qty,
                od.total_ctn, od.total_pcs
            FROM order_detail od
            JOIN "order" o ON od.order_id = o.order_id
            JOIN order_main om ON o.faktur_id = om.faktur_id
            WHERE o.faktur_date >= %s AND o.faktur_date <= %s
            ORDER BY o.faktur_date
            LIMIT %s OFFSET %s
            """
            
            with source_conn.cursor() as cursor:
                cursor.execute(select_query, (start_date, end_date, batch_size, offset))
                batch_data = cursor.fetchall()
            
            if not batch_data:
                break
            
            # Insert batch into target
            insert_query = """
            INSERT INTO order_detail_main (
                quantity_faktur, net_price, quantity_wms, quantity_delivery,
                quantity_loading, quantity_unloading, status, cancel_reason,
                notes, order_id, product_id, unit_id, pack_id, line_id,
                unloading_latitude, unloading_longitude, origin_uom, origin_qty,
                total_ctn, total_pcs
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                     %s, %s, %s, %s)
            ON CONFLICT (order_id, product_id, line_id) DO NOTHING
            """
            
            retry_count = 0
            while retry_count < max_retries:
                try:
                    with target_conn.cursor() as cursor:
                        cursor.executemany(insert_query, batch_data)
                        target_conn.commit()
                    
                    batch_copied = len(batch_data)
                    copied_count += batch_copied
                    offset += batch_size
                    
                    logger.info(f"Copied {batch_copied} order_detail records (Total: {copied_count}/{total_records})")
                    break
                    
                except Exception as e:
                    retry_count += 1
                    target_conn.rollback()
                    logger.warning(f"Retry {retry_count}/{max_retries} for order_detail batch: {str(e)}")
                    
                    if retry_count >= max_retries:
                        logger.error(f"Failed to copy order_detail batch after {max_retries} retries")
                        raise
        
        logger.info(f"Order detail data copy completed. Total copied: {copied_count}")
        return copied_count
        
    except Exception as e:
        logger.error(f"Error copying order_detail data: {str(e)}")
        raise

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Copy order data from Database A to Database B')
    parser.add_argument('--start-date', required=True, type=str, 
                       help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', required=True, type=str, 
                       help='End date (YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    # Validate date format
    try:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()
    except ValueError as e:
        print(f"Invalid date format: {e}")
        sys.exit(1)
    
    if start_date > end_date:
        print("Start date cannot be after end date")
        sys.exit(1)
    
    logger = setup_logging()
    
    logger.info(f"Starting order data copy process from {start_date} to {end_date}")
    
    source_conn = None
    target_conn = None
    
    try:
        # Connect to both databases
        source_conn = get_db_connection('A')
        target_conn = get_db_connection('B')
        
        logger.info("Connected to both databases successfully")
        
        # Copy order data first
        logger.info("Starting order data copy...")
        order_count = copy_order_data(source_conn, target_conn, start_date, end_date, logger)
        
        # Copy order_detail data
        logger.info("Starting order_detail data copy...")
        detail_count = copy_order_detail_data(source_conn, target_conn, start_date, end_date, logger)
        
        logger.info(f"Data copy completed successfully!")
        logger.info(f"Orders copied: {order_count}")
        logger.info(f"Order details copied: {detail_count}")
        
    except Exception as e:
        logger.error(f"Data copy failed: {str(e)}")
        sys.exit(1)
    finally:
        if source_conn:
            source_conn.close()
            logger.info("Source database connection closed")
        if target_conn:
            target_conn.close()
            logger.info("Target database connection closed")

if __name__ == "__main__":
    main() 