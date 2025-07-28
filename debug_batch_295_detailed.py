#!/usr/bin/env python3
"""
Detailed debug script to identify the exact problematic record in batch 295
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

def test_batch_295_with_executemany(logger, start_date, end_date, warehouse_id):
    """Test batch 295 using executemany like the copy script does"""
    logger.info("=== TESTING BATCH 295 WITH EXECUTEMANY ===")
    
    conn_a = get_db_connection('A')
    conn_b = get_db_connection('B')
    
    try:
        cursor_a = conn_a.cursor()
        cursor_b = conn_b.cursor()
        
        # Get batch 295 (offset 29500, size 100)
        logger.info("Fetching batch 295 (offset 29500, size 100)...")
        
        cursor_a.execute("""
            SELECT * FROM "order" 
            WHERE faktur_date >= %s AND faktur_date <= %s 
            AND warehouse_id = %s
            AND faktur_id IS NOT NULL AND customer_id IS NOT NULL
            ORDER BY faktur_date
            LIMIT 100 OFFSET 29500
        """, (start_date, end_date, warehouse_id))
        
        batch_data = cursor_a.fetchall()
        logger.info(f"Found {len(batch_data)} records in batch 295")
        
        # Test executemany like the copy script does
        logger.info("Testing executemany with batch 295...")
        
        insert_query = """
            INSERT INTO order_main (
                order_id, faktur_id, faktur_date, delivery_date, do_number, status, 
                skip_count, created_date, created_by, updated_date, updated_by, notes, 
                customer_id, warehouse_id, delivery_type_id, order_integration_id, 
                origin_name, origin_address_1, origin_address_2, origin_city, 
                origin_zipcode, origin_phone, origin_email, destination_name, 
                destination_address_1, destination_address_2, destination_city, 
                destination_zip_code, destination_phone, destination_email, client_id, 
                cancel_reason, rdo_integration_id, address_change, divisi, pre_status
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            ) ON CONFLICT (order_id) DO UPDATE SET
                faktur_id = EXCLUDED.faktur_id, faktur_date = EXCLUDED.faktur_date, 
                delivery_date = EXCLUDED.delivery_date, do_number = EXCLUDED.do_number, 
                status = EXCLUDED.status, skip_count = EXCLUDED.skip_count, 
                created_date = EXCLUDED.created_date, created_by = EXCLUDED.created_by, 
                updated_date = EXCLUDED.updated_date, updated_by = EXCLUDED.updated_by, 
                notes = EXCLUDED.notes, customer_id = EXCLUDED.customer_id, 
                warehouse_id = EXCLUDED.warehouse_id, delivery_type_id = EXCLUDED.delivery_type_id, 
                order_integration_id = EXCLUDED.order_integration_id, origin_name = EXCLUDED.origin_name, 
                origin_address_1 = EXCLUDED.origin_address_1, origin_address_2 = EXCLUDED.origin_address_2, 
                origin_city = EXCLUDED.origin_city, origin_zipcode = EXCLUDED.origin_zipcode, 
                origin_phone = EXCLUDED.origin_phone, origin_email = EXCLUDED.origin_email, 
                destination_name = EXCLUDED.destination_name, destination_address_1 = EXCLUDED.destination_address_1, 
                destination_address_2 = EXCLUDED.destination_address_2, destination_city = EXCLUDED.destination_city, 
                destination_zip_code = EXCLUDED.destination_zip_code, destination_phone = EXCLUDED.destination_phone, 
                destination_email = EXCLUDED.destination_email, client_id = EXCLUDED.client_id, 
                cancel_reason = EXCLUDED.cancel_reason, rdo_integration_id = EXCLUDED.rdo_integration_id, 
                address_change = EXCLUDED.address_change, divisi = EXCLUDED.divisi, 
                pre_status = EXCLUDED.pre_status
        """
        
        try:
            cursor_b.executemany(insert_query, batch_data)
            logger.info("✅ executemany with batch 295 successful!")
            return True
        except Exception as e:
            logger.error(f"❌ executemany with batch 295 failed: {e}")
            return False
        
    except Exception as e:
        logger.error(f"Error in test_batch_295_with_executemany: {e}")
        return False
    finally:
        conn_a.close()
        conn_b.close()

def test_individual_records_batch_295(logger, start_date, end_date, warehouse_id):
    """Test each record in batch 295 individually with executemany"""
    logger.info("=== TESTING INDIVIDUAL RECORDS IN BATCH 295 ===")
    
    conn_a = get_db_connection('A')
    conn_b = get_db_connection('B')
    
    try:
        cursor_a = conn_a.cursor()
        cursor_b = conn_b.cursor()
        
        # Get batch 295 (offset 29500, size 100)
        logger.info("Fetching batch 295 (offset 29500, size 100)...")
        
        cursor_a.execute("""
            SELECT * FROM "order" 
            WHERE faktur_date >= %s AND faktur_date <= %s 
            AND warehouse_id = %s
            AND faktur_id IS NOT NULL AND customer_id IS NOT NULL
            ORDER BY faktur_date
            LIMIT 100 OFFSET 29500
        """, (start_date, end_date, warehouse_id))
        
        batch_data = cursor_a.fetchall()
        logger.info(f"Found {len(batch_data)} records in batch 295")
        
        # Test each record individually
        logger.info("Testing each record individually...")
        
        insert_query = """
            INSERT INTO order_main (
                order_id, faktur_id, faktur_date, delivery_date, do_number, status, 
                skip_count, created_date, created_by, updated_date, updated_by, notes, 
                customer_id, warehouse_id, delivery_type_id, order_integration_id, 
                origin_name, origin_address_1, origin_address_2, origin_city, 
                origin_zipcode, origin_phone, origin_email, destination_name, 
                destination_address_1, destination_address_2, destination_city, 
                destination_zip_code, destination_phone, destination_email, client_id, 
                cancel_reason, rdo_integration_id, address_change, divisi, pre_status
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            ) ON CONFLICT (order_id) DO UPDATE SET
                faktur_id = EXCLUDED.faktur_id, faktur_date = EXCLUDED.faktur_date, 
                delivery_date = EXCLUDED.delivery_date, do_number = EXCLUDED.do_number, 
                status = EXCLUDED.status, skip_count = EXCLUDED.skip_count, 
                created_date = EXCLUDED.created_date, created_by = EXCLUDED.created_by, 
                updated_date = EXCLUDED.updated_date, updated_by = EXCLUDED.updated_by, 
                notes = EXCLUDED.notes, customer_id = EXCLUDED.customer_id, 
                warehouse_id = EXCLUDED.warehouse_id, delivery_type_id = EXCLUDED.delivery_type_id, 
                order_integration_id = EXCLUDED.order_integration_id, origin_name = EXCLUDED.origin_name, 
                origin_address_1 = EXCLUDED.origin_address_1, origin_address_2 = EXCLUDED.origin_address_2, 
                origin_city = EXCLUDED.origin_city, origin_zipcode = EXCLUDED.origin_zipcode, 
                origin_phone = EXCLUDED.origin_phone, origin_email = EXCLUDED.origin_email, 
                destination_name = EXCLUDED.destination_name, destination_address_1 = EXCLUDED.destination_address_1, 
                destination_address_2 = EXCLUDED.destination_address_2, destination_city = EXCLUDED.destination_city, 
                destination_zip_code = EXCLUDED.destination_zip_code, destination_phone = EXCLUDED.destination_phone, 
                destination_email = EXCLUDED.destination_email, client_id = EXCLUDED.client_id, 
                cancel_reason = EXCLUDED.cancel_reason, rdo_integration_id = EXCLUDED.rdo_integration_id, 
                address_change = EXCLUDED.address_change, divisi = EXCLUDED.divisi, 
                pre_status = EXCLUDED.pre_status
        """
        
        for i, row in enumerate(batch_data):
            try:
                cursor_b.execute(insert_query, row)
                logger.info(f"✅ Record {i+1}: Order ID {row[0]} - OK")
            except Exception as e:
                logger.error(f"❌ ERROR in record {i+1}: {e}")
                logger.error(f"  Order ID: {row[0]}")
                logger.error(f"  faktur_id: {row[1]}")
                logger.error(f"  faktur_date: {row[2]}")
                logger.error(f"  delivery_date: {row[3]}")
                logger.error(f"  created_date: {row[7]}")
                logger.error(f"  updated_date: {row[9]}")
                logger.error(f"  customer_id: {row[12]}")
                logger.error(f"  warehouse_id: {row[13]}")
                
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
        logger.error(f"Error in test_individual_records_batch_295: {e}")
        return False
    finally:
        conn_a.close()
        conn_b.close()

def find_records_with_extreme_dates(logger, start_date, end_date, warehouse_id):
    """Find records with extreme date values"""
    logger.info("=== FINDING RECORDS WITH EXTREME DATES ===")
    
    conn_a = get_db_connection('A')
    
    try:
        cursor = conn_a.cursor()
        
        # Find records with extreme date values
        logger.info("Searching for records with extreme date values...")
        
        cursor.execute("""
            SELECT order_id, faktur_id, faktur_date, delivery_date, created_date, updated_date, customer_id, warehouse_id
            FROM "order"
            WHERE warehouse_id = %s
            AND faktur_date >= %s AND faktur_date <= %s
            AND faktur_id IS NOT NULL AND customer_id IS NOT NULL
            AND (
                EXTRACT(YEAR FROM faktur_date) > 2100 OR
                EXTRACT(YEAR FROM delivery_date) > 2100 OR
                EXTRACT(YEAR FROM created_date) > 2100 OR
                EXTRACT(YEAR FROM updated_date) > 2100 OR
                faktur_date::text LIKE '%252025%' OR
                delivery_date::text LIKE '%252025%' OR
                created_date::text LIKE '%252025%' OR
                updated_date::text LIKE '%252025%'
            )
            ORDER BY order_id
        """, (warehouse_id, start_date, end_date))
        
        extreme_records = cursor.fetchall()
        
        if extreme_records:
            logger.warning(f"Found {len(extreme_records)} records with extreme dates:")
            for record in extreme_records:
                logger.warning(f"  Order ID: {record[0]}")
                logger.warning(f"    faktur_id: {record[1]}")
                logger.warning(f"    faktur_date: {record[2]}")
                logger.warning(f"    delivery_date: {record[3]}")
                logger.warning(f"    created_date: {record[4]}")
                logger.warning(f"    updated_date: {record[5]}")
                logger.warning(f"    customer_id: {record[6]}")
                logger.warning(f"    warehouse_id: {record[7]}")
                logger.warning("    ---")
        else:
            logger.info("No records with extreme dates found in the specified range")
        
        return extreme_records
        
    except Exception as e:
        logger.error(f"Error in find_records_with_extreme_dates: {e}")
        return []
    finally:
        conn_a.close()

def main():
    """Main function"""
    logger = setup_logging()
    
    if len(sys.argv) != 4:
        logger.error("Usage: python3 debug_batch_295_detailed.py <start_date> <end_date> <warehouse_id>")
        logger.error("Example: python3 debug_batch_295_detailed.py 2024-05-01 2025-05-31 4512")
        sys.exit(1)
    
    start_date = sys.argv[1]
    end_date = sys.argv[2]
    warehouse_id = int(sys.argv[3])
    
    try:
        logger.info("=== DETAILED DEBUGGING BATCH 295 ===")
        logger.info(f"Date Range: {start_date} to {end_date}")
        logger.info(f"Warehouse ID: {warehouse_id}")
        
        # Step 1: Find records with extreme dates
        extreme_records = find_records_with_extreme_dates(logger, start_date, end_date, warehouse_id)
        
        # Step 2: Test batch 295 with executemany
        success1 = test_batch_295_with_executemany(logger, start_date, end_date, warehouse_id)
        
        # Step 3: Test individual records in batch 295
        success2 = test_individual_records_batch_295(logger, start_date, end_date, warehouse_id)
        
        if success1 and success2:
            logger.info("✅ All batch 295 tests completed successfully!")
        else:
            logger.error("❌ Some batch 295 tests failed!")
        
    except Exception as e:
        logger.error(f"Script failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 