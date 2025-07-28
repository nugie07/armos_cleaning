#!/usr/bin/env python3
"""
Debug script to identify the exact cause of year out of range error during copy
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

def test_copy_query(logger, start_date, end_date, warehouse_id):
    """Test the exact copy query to identify the problematic record"""
    logger.info("=== TESTING COPY QUERY ===")
    
    conn_a = get_db_connection('A')
    
    try:
        cursor = conn_a.cursor()
        
        # Test the exact query from copy script
        logger.info("Testing the exact SELECT query from copy script...")
        
        test_query = """
        SELECT * FROM "order" 
        WHERE faktur_date >= %s AND faktur_date <= %s 
        AND warehouse_id = %s
        AND faktur_id IS NOT NULL AND customer_id IS NOT NULL
        ORDER BY faktur_date
        LIMIT 1000 OFFSET 0
        """
        
        try:
            cursor.execute(test_query, (start_date, end_date, warehouse_id))
            results = cursor.fetchall()
            logger.info(f"✅ Query executed successfully. Found {len(results)} records.")
            
            # Test each record individually to find the problematic one
            logger.info("Testing each record individually...")
            for i, row in enumerate(results):
                try:
                    # Try to access each date field
                    order_id = row[0]
                    faktur_date = row[2]
                    delivery_date = row[3]
                    created_date = row[7]
                    updated_date = row[9]
                    
                    # Test string conversion
                    str(faktur_date)
                    str(delivery_date) if delivery_date else None
                    str(created_date) if created_date else None
                    str(updated_date) if updated_date else None
                    
                    if i % 100 == 0:
                        logger.info(f"Processed {i} records successfully...")
                        
                except Exception as e:
                    logger.error(f"❌ Error processing record {i} (order_id: {order_id}): {e}")
                    logger.error(f"  faktur_date: {faktur_date}")
                    logger.error(f"  delivery_date: {delivery_date}")
                    logger.error(f"  created_date: {created_date}")
                    logger.error(f"  updated_date: {updated_date}")
                    return False
            
            logger.info("✅ All records processed successfully!")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error executing query: {e}")
            return False
        
    except Exception as e:
        logger.error(f"Error in test_copy_query: {e}")
        return False
    finally:
        conn_a.close()

def test_batch_processing(logger, start_date, end_date, warehouse_id):
    """Test batch processing to find the exact batch causing the error"""
    logger.info("=== TESTING BATCH PROCESSING ===")
    
    conn_a = get_db_connection('A')
    
    try:
        cursor = conn_a.cursor()
        
        # Get total count
        cursor.execute("""
            SELECT COUNT(*) FROM "order" 
            WHERE faktur_date >= %s AND faktur_date <= %s 
            AND warehouse_id = %s
            AND faktur_id IS NOT NULL AND customer_id IS NOT NULL
        """, (start_date, end_date, warehouse_id))
        
        total_orders = cursor.fetchone()[0]
        logger.info(f"Total orders to process: {total_orders}")
        
        # Test in smaller batches
        batch_size = 100
        offset = 0
        
        while offset < total_orders:
            logger.info(f"Testing batch: offset {offset}, size {batch_size}")
            
            try:
                cursor.execute("""
                    SELECT * FROM "order" 
                    WHERE faktur_date >= %s AND faktur_date <= %s 
                    AND warehouse_id = %s
                    AND faktur_id IS NOT NULL AND customer_id IS NOT NULL
                    ORDER BY faktur_date
                    LIMIT %s OFFSET %s
                """, (start_date, end_date, warehouse_id, batch_size, offset))
                
                batch_data = cursor.fetchall()
                logger.info(f"✅ Batch {offset//batch_size + 1} successful: {len(batch_data)} records")
                
                # Test each record in this batch
                for i, row in enumerate(batch_data):
                    try:
                        # Test date fields
                        str(row[2])  # faktur_date
                        str(row[3]) if row[3] else None  # delivery_date
                        str(row[7]) if row[7] else None  # created_date
                        str(row[9]) if row[9] else None  # updated_date
                    except Exception as e:
                        logger.error(f"❌ Error in batch {offset//batch_size + 1}, record {i}: {e}")
                        logger.error(f"  Order ID: {row[0]}")
                        logger.error(f"  faktur_date: {row[2]}")
                        logger.error(f"  delivery_date: {row[3]}")
                        logger.error(f"  created_date: {row[7]}")
                        logger.error(f"  updated_date: {row[9]}")
                        return False
                
                offset += batch_size
                
            except Exception as e:
                logger.error(f"❌ Error in batch {offset//batch_size + 1}: {e}")
                return False
        
        logger.info("✅ All batches processed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"Error in test_batch_processing: {e}")
        return False
    finally:
        conn_a.close()

def main():
    """Main function"""
    logger = setup_logging()
    
    if len(sys.argv) != 4:
        logger.error("Usage: python3 debug_copy_error.py <start_date> <end_date> <warehouse_id>")
        logger.error("Example: python3 debug_copy_error.py 2024-05-01 2025-05-31 4512")
        sys.exit(1)
    
    start_date = sys.argv[1]
    end_date = sys.argv[2]
    warehouse_id = int(sys.argv[3])
    
    try:
        logger.info("=== DEBUGGING COPY ERROR ===")
        logger.info(f"Date Range: {start_date} to {end_date}")
        logger.info(f"Warehouse ID: {warehouse_id}")
        
        # Test 1: Basic query
        success1 = test_copy_query(logger, start_date, end_date, warehouse_id)
        
        if success1:
            # Test 2: Batch processing
            success2 = test_batch_processing(logger, start_date, end_date, warehouse_id)
            
            if success2:
                logger.info("✅ All tests passed! The issue might be elsewhere.")
            else:
                logger.error("❌ Batch processing failed!")
        else:
            logger.error("❌ Basic query failed!")
        
    except Exception as e:
        logger.error(f"Script failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 