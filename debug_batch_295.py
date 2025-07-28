#!/usr/bin/env python3
"""
Debug script to identify the exact problematic record in batch 295
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

def debug_batch_295(logger, start_date, end_date, warehouse_id):
    """Debug batch 295 to find the problematic record"""
    logger.info("=== DEBUGGING BATCH 295 ===")
    
    conn_a = get_db_connection('A')
    
    try:
        cursor = conn_a.cursor()
        
        # Get batch 295 (offset 29500, size 100)
        logger.info("Fetching batch 295 (offset 29500, size 100)...")
        
        cursor.execute("""
            SELECT * FROM "order" 
            WHERE faktur_date >= %s AND faktur_date <= %s 
            AND warehouse_id = %s
            AND faktur_id IS NOT NULL AND customer_id IS NOT NULL
            ORDER BY faktur_date
            LIMIT 100 OFFSET 29500
        """, (start_date, end_date, warehouse_id))
        
        batch_data = cursor.fetchall()
        logger.info(f"Found {len(batch_data)} records in batch 295")
        
        # Test each record individually
        logger.info("Testing each record in batch 295...")
        
        for i, row in enumerate(batch_data):
            try:
                # Extract key fields
                order_id = row[0]
                faktur_id = row[1]
                faktur_date = row[2]
                delivery_date = row[3]
                do_number = row[4]
                status = row[5]
                skip_count = row[6]
                created_date = row[7]
                created_by = row[8]
                updated_date = row[9]
                updated_by = row[10]
                notes = row[11]
                customer_id = row[12]
                warehouse_id_val = row[13]
                
                # Test string conversion for each date field
                str(faktur_date)
                str(delivery_date) if delivery_date else None
                str(created_date) if created_date else None
                str(updated_date) if updated_date else None
                
                logger.info(f"✅ Record {i+1}: Order ID {order_id} - OK")
                
            except Exception as e:
                logger.error(f"❌ ERROR in record {i+1}: {e}")
                logger.error(f"  Order ID: {order_id}")
                logger.error(f"  faktur_id: {faktur_id}")
                logger.error(f"  faktur_date: {faktur_date}")
                logger.error(f"  delivery_date: {delivery_date}")
                logger.error(f"  created_date: {created_date}")
                logger.error(f"  updated_date: {updated_date}")
                logger.error(f"  customer_id: {customer_id}")
                logger.error(f"  warehouse_id: {warehouse_id_val}")
                
                # Show the problematic record details
                logger.error("=== PROBLEMATIC RECORD DETAILS ===")
                for j, value in enumerate(row):
                    try:
                        str_value = str(value)
                        logger.error(f"  Column {j}: {str_value}")
                    except Exception as str_error:
                        logger.error(f"  Column {j}: ERROR converting to string - {str_error}")
                
                return False
        
        logger.info("✅ All records in batch 295 processed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"Error in debug_batch_295: {e}")
        return False
    finally:
        conn_a.close()

def find_problematic_records(logger, start_date, end_date, warehouse_id):
    """Find all records with year 252025 in any date field"""
    logger.info("=== FINDING ALL RECORDS WITH YEAR 252025 ===")
    
    conn_a = get_db_connection('A')
    
    try:
        cursor = conn_a.cursor()
        
        # Find records with year 252025 in any date field
        logger.info("Searching for records with year 252025...")
        
        cursor.execute("""
            SELECT order_id, faktur_id, faktur_date, delivery_date, created_date, updated_date, customer_id, warehouse_id
            FROM "order"
            WHERE warehouse_id = %s
            AND faktur_date >= %s AND faktur_date <= %s
            AND faktur_id IS NOT NULL AND customer_id IS NOT NULL
            AND (
                faktur_date::text LIKE '%252025%' OR
                delivery_date::text LIKE '%252025%' OR
                created_date::text LIKE '%252025%' OR
                updated_date::text LIKE '%252025%'
            )
            ORDER BY order_id
        """, (warehouse_id, start_date, end_date))
        
        problematic_records = cursor.fetchall()
        
        if problematic_records:
            logger.warning(f"Found {len(problematic_records)} records with year 252025:")
            for record in problematic_records:
                try:
                    logger.warning(f"  Order ID: {record[0]}")
                    logger.warning(f"    faktur_id: {record[1]}")
                    logger.warning(f"    faktur_date: {record[2]}")
                    logger.warning(f"    delivery_date: {record[3]}")
                    logger.warning(f"    created_date: {record[4]}")
                    logger.warning(f"    updated_date: {record[5]}")
                    logger.warning(f"    customer_id: {record[6]}")
                    logger.warning(f"    warehouse_id: {record[7]}")
                    logger.warning("    ---")
                except IndexError as idx_error:
                    logger.error(f"  Error accessing record fields: {idx_error}")
                    logger.error(f"  Record has {len(record)} columns: {record}")
        else:
            logger.info("No records with year 252025 found in the specified range")
        
        return problematic_records
        
    except Exception as e:
        logger.error(f"Error in find_problematic_records: {e}")
        return []
    finally:
        conn_a.close()

def main():
    """Main function"""
    logger = setup_logging()
    
    if len(sys.argv) != 4:
        logger.error("Usage: python3 debug_batch_295.py <start_date> <end_date> <warehouse_id>")
        logger.error("Example: python3 debug_batch_295.py 2024-05-01 2025-05-31 4512")
        sys.exit(1)
    
    start_date = sys.argv[1]
    end_date = sys.argv[2]
    warehouse_id = int(sys.argv[3])
    
    try:
        logger.info("=== DEBUGGING BATCH 295 PROBLEM ===")
        logger.info(f"Date Range: {start_date} to {end_date}")
        logger.info(f"Warehouse ID: {warehouse_id}")
        
        # Step 1: Find all problematic records
        problematic_records = find_problematic_records(logger, start_date, end_date, warehouse_id)
        
        # Step 2: Debug batch 295 specifically
        success = debug_batch_295(logger, start_date, end_date, warehouse_id)
        
        if success:
            logger.info("✅ Batch 295 debug completed successfully!")
        else:
            logger.error("❌ Batch 295 debug failed!")
        
    except Exception as e:
        logger.error(f"Script failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 