#!/usr/bin/env python3
"""
Robust debug script to handle extreme date values in Batch 30
that cause PostgreSQL queries to fail
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

def find_problematic_records_safe(logger, start_date, end_date, warehouse_id):
    """Find problematic records using safer queries that avoid date conversion"""
    logger.info("=== FINDING PROBLEMATIC RECORDS (SAFE METHOD) ===")
    
    conn_a = get_db_connection('A')
    
    try:
        cursor_a = conn_a.cursor()
        
        # First, let's get the order_ids in the problematic range without date conversion
        logger.info("Getting order IDs in the problematic range...")
        cursor_a.execute("""
            SELECT order_id, faktur_date::text, delivery_date::text, created_date::text, updated_date::text
            FROM "order" 
            WHERE warehouse_id = %s
            AND faktur_id IS NOT NULL AND customer_id IS NOT NULL
            ORDER BY faktur_date
            LIMIT 1000 OFFSET 29000
        """, (warehouse_id,))
        
        records = cursor_a.fetchall()
        logger.info(f"Retrieved {len(records)} records from the problematic range")
        
        problematic_records = []
        
        for i, record in enumerate(records):
            order_id = record[0]
            faktur_date_text = record[1]
            delivery_date_text = record[2]
            created_date_text = record[3]
            updated_date_text = record[4]
            
            # Check for extreme values in text representation
            if (faktur_date_text and '252025' in faktur_date_text) or \
               (delivery_date_text and '252025' in delivery_date_text) or \
               (created_date_text and '252025' in created_date_text) or \
               (updated_date_text and '252025' in updated_date_text):
                
                logger.error(f"❌ Found problematic record {i+1}:")
                logger.error(f"  order_id: {order_id}")
                logger.error(f"  faktur_date: {faktur_date_text}")
                logger.error(f"  delivery_date: {delivery_date_text}")
                logger.error(f"  created_date: {created_date_text}")
                logger.error(f"  updated_date: {updated_date_text}")
                
                problematic_records.append({
                    'index': i+1,
                    'order_id': order_id,
                    'faktur_date': faktur_date_text,
                    'delivery_date': delivery_date_text,
                    'created_date': created_date_text,
                    'updated_date': updated_date_text
                })
        
        if problematic_records:
            logger.error(f"Found {len(problematic_records)} problematic records with extreme dates")
        else:
            logger.info("No records with extreme dates found in text representation")
        
        return problematic_records
        
    except Exception as e:
        logger.error(f"Error in find_problematic_records_safe: {e}")
        return []
    finally:
        conn_a.close()

def test_smaller_batches(logger, start_date, end_date, warehouse_id):
    """Test smaller batches to isolate the problematic range"""
    logger.info("=== TESTING SMALLER BATCHES ===")
    
    conn_a = get_db_connection('A')
    
    try:
        cursor_a = conn_a.cursor()
        
        # Test different offset ranges to find where the error starts
        test_offsets = [28900, 28950, 28975, 28990, 28995, 28998, 28999, 29000, 29001, 29002, 29005, 29010, 29025, 29050, 29100]
        
        for offset in test_offsets:
            try:
                logger.info(f"Testing offset {offset}...")
                cursor_a.execute("""
                    SELECT COUNT(*) FROM "order" 
                    WHERE warehouse_id = %s
                    AND faktur_id IS NOT NULL AND customer_id IS NOT NULL
                    ORDER BY faktur_date
                    LIMIT 10 OFFSET %s
                """, (warehouse_id, offset))
                
                count = cursor_a.fetchone()[0]
                logger.info(f"✅ Offset {offset}: {count} records - OK")
                
            except Exception as e:
                logger.error(f"❌ Offset {offset}: ERROR - {e}")
                return offset
        
        logger.info("All test offsets passed")
        return None
        
    except Exception as e:
        logger.error(f"Error in test_smaller_batches: {e}")
        return None
    finally:
        conn_a.close()

def get_records_around_problematic_offset(logger, warehouse_id, problematic_offset):
    """Get records around the problematic offset to identify the exact record"""
    logger.info(f"=== GETTING RECORDS AROUND OFFSET {problematic_offset} ===")
    
    conn_a = get_db_connection('A')
    
    try:
        cursor_a = conn_a.cursor()
        
        # Get a few records before and after the problematic offset
        start_offset = max(0, problematic_offset - 5)
        end_offset = problematic_offset + 5
        
        logger.info(f"Checking records from offset {start_offset} to {end_offset}")
        
        for offset in range(start_offset, end_offset + 1):
            try:
                cursor_a.execute("""
                    SELECT order_id, faktur_date::text, delivery_date::text, created_date::text, updated_date::text
                    FROM "order" 
                    WHERE warehouse_id = %s
                    AND faktur_id IS NOT NULL AND customer_id IS NOT NULL
                    ORDER BY faktur_date
                    LIMIT 1 OFFSET %s
                """, (warehouse_id, offset))
                
                record = cursor_a.fetchone()
                if record:
                    order_id = record[0]
                    faktur_date_text = record[1]
                    delivery_date_text = record[2]
                    created_date_text = record[3]
                    updated_date_text = record[4]
                    
                    logger.info(f"Offset {offset}: order_id {order_id}")
                    logger.info(f"  faktur_date: {faktur_date_text}")
                    logger.info(f"  delivery_date: {delivery_date_text}")
                    logger.info(f"  created_date: {created_date_text}")
                    logger.info(f"  updated_date: {updated_date_text}")
                    
                    # Check for extreme values
                    if (faktur_date_text and '252025' in faktur_date_text) or \
                       (delivery_date_text and '252025' in delivery_date_text) or \
                       (created_date_text and '252025' in created_date_text) or \
                       (updated_date_text and '252025' in updated_date_text):
                        logger.error(f"❌ PROBLEMATIC RECORD FOUND at offset {offset}!")
                        return offset
                else:
                    logger.info(f"Offset {offset}: No record found")
                    
            except Exception as e:
                logger.error(f"❌ Error at offset {offset}: {e}")
                return offset
        
        return None
        
    except Exception as e:
        logger.error(f"Error in get_records_around_problematic_offset: {e}")
        return None
    finally:
        conn_a.close()

def fix_problematic_record(logger, warehouse_id, problematic_offset):
    """Fix the problematic record by setting invalid dates to NULL"""
    logger.info(f"=== FIXING PROBLEMATIC RECORD AT OFFSET {problematic_offset} ===")
    
    conn_a = get_db_connection('A')
    
    try:
        cursor_a = conn_a.cursor()
        
        # First, get the order_id at the problematic offset
        cursor_a.execute("""
            SELECT order_id FROM "order" 
            WHERE warehouse_id = %s
            AND faktur_id IS NOT NULL AND customer_id IS NOT NULL
            ORDER BY faktur_date
            LIMIT 1 OFFSET %s
        """, (warehouse_id, problematic_offset))
        
        result = cursor_a.fetchone()
        if not result:
            logger.error("No record found at problematic offset")
            return False
        
        order_id = result[0]
        logger.info(f"Found problematic order_id: {order_id}")
        
        # Fix the problematic dates
        cursor_a.execute("""
            UPDATE "order"
            SET delivery_date = NULL,
                created_date = CURRENT_DATE,
                updated_date = CURRENT_DATE
            WHERE order_id = %s
        """, (order_id,))
        
        conn_a.commit()
        logger.info(f"✅ Fixed order_id {order_id}")
        return True
        
    except Exception as e:
        conn_a.rollback()
        logger.error(f"Error fixing problematic record: {e}")
        return False
    finally:
        conn_a.close()

def main():
    """Main function"""
    if len(sys.argv) != 4:
        print("Usage: python3 debug_batch_30_robust.py <start_date> <end_date> <warehouse_id>")
        print("Example: python3 debug_batch_30_robust.py 2024-05-01 2025-05-31 4512")
        sys.exit(1)
    
    start_date = sys.argv[1]
    end_date = sys.argv[2]
    warehouse_id = int(sys.argv[3])
    
    logger = setup_logging()
    
    logger.info(f"Robust debugging for date range: {start_date} to {end_date}")
    logger.info(f"Warehouse ID: {warehouse_id}")
    
    # Step 1: Find problematic records using safe method
    logger.info("\n" + "="*60)
    problematic_records = find_problematic_records_safe(logger, start_date, end_date, warehouse_id)
    
    # Step 2: Test smaller batches to isolate the problem
    logger.info("\n" + "="*60)
    problematic_offset = test_smaller_batches(logger, start_date, end_date, warehouse_id)
    
    if problematic_offset is not None:
        # Step 3: Get records around the problematic offset
        logger.info("\n" + "="*60)
        exact_problematic_offset = get_records_around_problematic_offset(logger, warehouse_id, problematic_offset)
        
        if exact_problematic_offset is not None:
            # Step 4: Fix the problematic record
            logger.info("\n" + "="*60)
            fix_success = fix_problematic_record(logger, warehouse_id, exact_problematic_offset)
            
            if fix_success:
                logger.info("✅ Problematic record has been fixed!")
                logger.info("You can now try running the copy script again.")
            else:
                logger.error("❌ Failed to fix the problematic record")
        else:
            logger.error("❌ Could not identify the exact problematic record")
    else:
        logger.info("✅ No problematic offset found in the test range")
    
    # Summary
    logger.info("\n" + "="*60)
    logger.info("SUMMARY:")
    logger.info(f"Problematic records found: {len(problematic_records)}")
    logger.info(f"Problematic offset: {problematic_offset}")
    
    if problematic_records or problematic_offset is not None:
        logger.error("❌ Found problematic records that need fixing!")
    else:
        logger.info("✅ No problematic records found!")

if __name__ == "__main__":
    main() 