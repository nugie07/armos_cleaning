#!/usr/bin/env python3
"""
Script to copy mst_product data from Database A to Database B
Separate script from order data copy with comprehensive logging
"""

import os
import sys
import logging
import psycopg2
import argparse
from datetime import datetime
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

def copy_product_data(source_conn, target_conn, logger, force_update=False):
    """Copy mst_product data from source to target database"""
    batch_size = int(os.getenv('BATCH_SIZE', 1000))
    max_retries = int(os.getenv('MAX_RETRIES', 3))
    
    # Get total count for progress tracking
    count_query = "SELECT COUNT(*) FROM mst_product"
    
    try:
        with source_conn.cursor() as cursor:
            cursor.execute(count_query)
            total_records = cursor.fetchone()[0]
        
        logger.info(f"Found {total_records} product records to copy")
        
        if total_records == 0:
            logger.warning("No product records found in source database")
            return 0
        
        # Copy data in batches
        offset = 0
        copied_count = 0
        updated_count = 0
        
        while offset < total_records:
            # Fetch batch from source
            select_query = """
            SELECT 
                sku, height, width, length, name, price, type_product_id, qty,
                volume, weight, base_uom, pack_id, warehouse_id
            FROM mst_product
            ORDER BY sku
            LIMIT %s OFFSET %s
            """
            
            with source_conn.cursor() as cursor:
                cursor.execute(select_query, (batch_size, offset))
                batch_data = cursor.fetchall()
            
            if not batch_data:
                break
            
            # Insert or update batch into target
            if force_update:
                # Use UPSERT (INSERT ... ON CONFLICT UPDATE)
                insert_query = """
                INSERT INTO mst_product_main (
                    sku, height, width, length, name, price, type_product_id, qty,
                    volume, weight, base_uom, pack_id, warehouse_id, synced_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (sku) DO UPDATE SET
                    height = EXCLUDED.height,
                    width = EXCLUDED.width,
                    length = EXCLUDED.length,
                    name = EXCLUDED.name,
                    price = EXCLUDED.price,
                    type_product_id = EXCLUDED.type_product_id,
                    qty = EXCLUDED.qty,
                    volume = EXCLUDED.volume,
                    weight = EXCLUDED.weight,
                    base_uom = EXCLUDED.base_uom,
                    pack_id = EXCLUDED.pack_id,
                    warehouse_id = EXCLUDED.warehouse_id,
                    synced_at = CURRENT_TIMESTAMP
                """
            else:
                # Use INSERT ... ON CONFLICT DO NOTHING
                insert_query = """
                INSERT INTO mst_product_main (
                    sku, height, width, length, name, price, type_product_id, qty,
                    volume, weight, base_uom, pack_id, warehouse_id, synced_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (sku) DO NOTHING
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
                    
                    logger.info(f"Processed {batch_copied} product records (Total: {copied_count}/{total_records})")
                    break
                    
                except Exception as e:
                    retry_count += 1
                    target_conn.rollback()
                    logger.warning(f"Retry {retry_count}/{max_retries} for product batch: {str(e)}")
                    
                    if retry_count >= max_retries:
                        logger.error(f"Failed to copy product batch after {max_retries} retries")
                        raise
        
        # Get final statistics
        with target_conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM mst_product_main")
            final_count = cursor.fetchone()[0]
        
        logger.info(f"Product data copy completed. Total processed: {copied_count}")
        logger.info(f"Total products in target database: {final_count}")
        return copied_count
        
    except Exception as e:
        logger.error(f"Error copying product data: {str(e)}")
        raise

def validate_product_data(source_conn, target_conn, logger):
    """Validate that product data was copied correctly"""
    try:
        # Count records in both databases
        with source_conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM mst_product")
            source_count = cursor.fetchone()[0]
        
        with target_conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM mst_product_main")
            target_count = cursor.fetchone()[0]
        
        logger.info(f"Source database product count: {source_count}")
        logger.info(f"Target database product count: {target_count}")
        
        if source_count == target_count:
            logger.info("Product data validation successful - counts match")
            return True
        else:
            logger.warning(f"Product data validation failed - counts don't match (source: {source_count}, target: {target_count})")
            return False
            
    except Exception as e:
        logger.error(f"Error during product data validation: {str(e)}")
        return False

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Copy product data from Database A to Database B')
    parser.add_argument('--force-update', action='store_true', 
                       help='Force update existing products (default: skip existing)')
    parser.add_argument('--validate', action='store_true',
                       help='Validate data after copy')
    
    args = parser.parse_args()
    
    logger = setup_logging()
    
    logger.info("Starting product data copy process")
    if args.force_update:
        logger.info("Force update mode enabled - existing products will be updated")
    
    source_conn = None
    target_conn = None
    
    try:
        # Connect to both databases
        source_conn = get_db_connection('A')
        target_conn = get_db_connection('B')
        
        logger.info("Connected to both databases successfully")
        
        # Copy product data
        logger.info("Starting product data copy...")
        copied_count = copy_product_data(source_conn, target_conn, logger, args.force_update)
        
        # Validate if requested
        if args.validate:
            logger.info("Starting data validation...")
            validation_success = validate_product_data(source_conn, target_conn, logger)
            if not validation_success:
                logger.warning("Data validation failed - please check the logs")
        
        logger.info(f"Product data copy completed successfully!")
        logger.info(f"Products processed: {copied_count}")
        
    except Exception as e:
        logger.error(f"Product data copy failed: {str(e)}")
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