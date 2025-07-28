#!/usr/bin/env python3
"""
Targeted debug script to find the exact problematic record in Batch 30
that's causing the "year 252025 is out of range" error
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

def debug_batch_30_records(logger, start_date, end_date, warehouse_id):
    """Debug the exact records in Batch 30 (offset 29000)"""
    logger.info("=== DEBUGGING BATCH 30 RECORDS (OFFSET 29000) ===")
    
    conn_a = get_db_connection('A')
    
    try:
        cursor_a = conn_a.cursor()
        
        # Fetch exactly Batch 30 (offset 29000, limit 1000)
        cursor_a.execute("""
            SELECT * FROM "order" 
            WHERE faktur_date >= %s AND faktur_date <= %s 
            AND warehouse_id = %s
            AND faktur_id IS NOT NULL AND customer_id IS NOT NULL
            ORDER BY faktur_date
            LIMIT 1000 OFFSET 29000
        """, (start_date, end_date, warehouse_id))
        
        batch_data = cursor_a.fetchall()
        logger.info(f"Retrieved {len(batch_data)} records from Batch 30")
        
        if not batch_data:
            logger.warning("No records found in Batch 30")
            return
        
        # Test each record individually
        problematic_records = []
        
        for i, row in enumerate(batch_data):
            try:
                # Test string conversion of all fields
                for j, field in enumerate(row):
                    if field is not None:
                        str(field)
                
                # Test specific date fields
                order_id = row[0]
                faktur_date = row[2]
                delivery_date = row[3]
                created_date = row[7]
                updated_date = row[9]
                
                # Test date string conversion
                if faktur_date:
                    str(faktur_date)
                if delivery_date:
                    str(delivery_date)
                if created_date:
                    str(created_date)
                if updated_date:
                    str(updated_date)
                
                logger.debug(f"Record {i+1}: order_id {order_id} - OK")
                
            except Exception as e:
                logger.error(f"❌ ERROR in record {i+1}: {e}")
                logger.error(f"  order_id: {row[0] if len(row) > 0 else 'N/A'}")
                logger.error(f"  faktur_date: {row[2] if len(row) > 2 else 'N/A'}")
                logger.error(f"  delivery_date: {row[3] if len(row) > 3 else 'N/A'}")
                logger.error(f"  created_date: {row[7] if len(row) > 7 else 'N/A'}")
                logger.error(f"  updated_date: {row[9] if len(row) > 9 else 'N/A'}")
                logger.error(f"  Full record: {row}")
                
                problematic_records.append({
                    'index': i+1,
                    'order_id': row[0] if len(row) > 0 else 'N/A',
                    'error': str(e),
                    'record': row
                })
        
        if problematic_records:
            logger.error(f"Found {len(problematic_records)} problematic records:")
            for pr in problematic_records:
                logger.error(f"  Record {pr['index']}: order_id {pr['order_id']} - {pr['error']}")
        else:
            logger.info("✅ No problematic records found in Batch 30")
        
        return problematic_records
        
    except Exception as e:
        logger.error(f"Error in debug_batch_30_records: {e}")
        return []
    finally:
        conn_a.close()

def test_batch_30_executemany(logger, start_date, end_date, warehouse_id):
    """Test executemany with Batch 30 records"""
    logger.info("=== TESTING EXECUTEMANY WITH BATCH 30 ===")
    
    conn_a = get_db_connection('A')
    conn_b = get_db_connection('B')
    
    try:
        cursor_a = conn_a.cursor()
        cursor_b = conn_b.cursor()
        
        # Fetch Batch 30
        cursor_a.execute("""
            SELECT * FROM "order" 
            WHERE faktur_date >= %s AND faktur_date <= %s 
            AND warehouse_id = %s
            AND faktur_id IS NOT NULL AND customer_id IS NOT NULL
            ORDER BY faktur_date
            LIMIT 1000 OFFSET 29000
        """, (start_date, end_date, warehouse_id))
        
        batch_data = cursor_a.fetchall()
        logger.info(f"Retrieved {len(batch_data)} records from Batch 30")
        
        if not batch_data:
            logger.warning("No records found in Batch 30")
            return
        
        # Test executemany with the exact insert query
        insert_query = """
        INSERT INTO order_main 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (order_id) DO UPDATE SET
            faktur_id = EXCLUDED.faktur_id,
            faktur_date = EXCLUDED.faktur_date,
            delivery_date = EXCLUDED.delivery_date,
            do_number = EXCLUDED.do_number,
            status = EXCLUDED.status,
            skip_count = EXCLUDED.skip_count,
            created_date = EXCLUDED.created_date,
            created_by = EXCLUDED.created_by,
            updated_date = EXCLUDED.updated_date,
            updated_by = EXCLUDED.updated_by,
            notes = EXCLUDED.notes,
            customer_id = EXCLUDED.customer_id,
            warehouse_id = EXCLUDED.warehouse_id,
            delivery_type_id = EXCLUDED.delivery_type_id,
            order_integration_id = EXCLUDED.order_integration_id,
            origin_name = EXCLUDED.origin_name,
            origin_address_1 = EXCLUDED.origin_address_1,
            origin_address_2 = EXCLUDED.origin_address_2,
            origin_city = EXCLUDED.origin_city,
            origin_zipcode = EXCLUDED.origin_zipcode,
            origin_phone = EXCLUDED.origin_phone,
            origin_email = EXCLUDED.origin_email,
            destination_name = EXCLUDED.destination_name,
            destination_address_1 = EXCLUDED.destination_address_1,
            destination_address_2 = EXCLUDED.destination_address_2,
            destination_city = EXCLUDED.destination_city,
            destination_zip_code = EXCLUDED.destination_zip_code,
            destination_phone = EXCLUDED.destination_phone,
            destination_email = EXCLUDED.destination_email,
            client_id = EXCLUDED.client_id,
            cancel_reason = EXCLUDED.cancel_reason,
            rdo_integration_id = EXCLUDED.rdo_integration_id,
            address_change = EXCLUDED.address_change,
            divisi = EXCLUDED.divisi,
            pre_status = EXCLUDED.pre_status
        """
        
        try:
            cursor_b.executemany(insert_query, batch_data)
            logger.info("✅ Batch 30 executemany successful")
            conn_b.rollback()
            return True
        except Exception as e:
            logger.error(f"❌ ERROR in Batch 30 executemany: {e}")
            
            # Test individual records to find the problematic one
            logger.info("Testing individual records in Batch 30...")
            for i, row in enumerate(batch_data):
                try:
                    cursor_b.execute(insert_query, row)
                    conn_b.rollback()
                    logger.debug(f"  Record {i+1}: OK")
                except Exception as record_error:
                    logger.error(f"❌ ERROR in record {i+1}: {record_error}")
                    logger.error(f"  order_id: {row[0] if len(row) > 0 else 'N/A'}")
                    logger.error(f"  faktur_date: {row[2] if len(row) > 2 else 'N/A'}")
                    logger.error(f"  delivery_date: {row[3] if len(row) > 3 else 'N/A'}")
                    logger.error(f"  created_date: {row[7] if len(row) > 7 else 'N/A'}")
                    logger.error(f"  updated_date: {row[9] if len(row) > 9 else 'N/A'}")
                    logger.error(f"  Full record: {row}")
                    return False
            
            return False
        
    except Exception as e:
        logger.error(f"Error in test_batch_30_executemany: {e}")
        return False
    finally:
        conn_a.close()
        conn_b.close()

def find_records_with_extreme_dates_batch_30(logger, start_date, end_date, warehouse_id):
    """Find records with extreme date values in Batch 30"""
    logger.info("=== SEARCHING FOR EXTREME DATES IN BATCH 30 ===")
    
    conn_a = get_db_connection('A')
    
    try:
        cursor_a = conn_a.cursor()
        
        # Search for records with extreme years in Batch 30
        cursor_a.execute("""
            SELECT order_id, faktur_date, delivery_date, created_date, updated_date
            FROM "order" 
            WHERE faktur_date >= %s AND faktur_date <= %s 
            AND warehouse_id = %s
            AND faktur_id IS NOT NULL AND customer_id IS NOT NULL
            AND (
                EXTRACT(YEAR FROM faktur_date) > 2100 OR EXTRACT(YEAR FROM faktur_date) < 1900
                OR (delivery_date IS NOT NULL AND (EXTRACT(YEAR FROM delivery_date) > 2100 OR EXTRACT(YEAR FROM delivery_date) < 1900))
                OR (created_date IS NOT NULL AND (EXTRACT(YEAR FROM created_date) > 2100 OR EXTRACT(YEAR FROM created_date) < 1900))
                OR (updated_date IS NOT NULL AND (EXTRACT(YEAR FROM updated_date) > 2100 OR EXTRACT(YEAR FROM updated_date) < 1900))
            )
            ORDER BY faktur_date
            LIMIT 1000 OFFSET 29000
        """, (start_date, end_date, warehouse_id))
        
        extreme_records = cursor_a.fetchall()
        
        if extreme_records:
            logger.warning(f"Found {len(extreme_records)} records with extreme dates in Batch 30:")
            for record in extreme_records:
                logger.warning(f"  Order ID: {record[0]}")
                logger.warning(f"    faktur_date: {record[1]}")
                logger.warning(f"    delivery_date: {record[2]}")
                logger.warning(f"    created_date: {record[3]}")
                logger.warning(f"    updated_date: {record[4]}")
                logger.warning("    ---")
        else:
            logger.info("No records with extreme dates found in Batch 30")
        
        # Also try string-based search for "252025"
        cursor_a.execute("""
            SELECT order_id, faktur_date, delivery_date, created_date, updated_date
            FROM "order" 
            WHERE faktur_date >= %s AND faktur_date <= %s 
            AND warehouse_id = %s
            AND faktur_id IS NOT NULL AND customer_id IS NOT NULL
            AND (
                faktur_date::text LIKE '%252025%'
                OR (delivery_date IS NOT NULL AND delivery_date::text LIKE '%252025%')
                OR (created_date IS NOT NULL AND created_date::text LIKE '%252025%')
                OR (updated_date IS NOT NULL AND updated_date::text LIKE '%252025%')
            )
            ORDER BY faktur_date
            LIMIT 1000 OFFSET 29000
        """, (start_date, end_date, warehouse_id))
        
        text_records = cursor_a.fetchall()
        
        if text_records:
            logger.warning(f"Found {len(text_records)} records with '252025' in date fields in Batch 30:")
            for record in text_records:
                logger.warning(f"  Order ID: {record[0]}")
                logger.warning(f"    faktur_date: {record[1]}")
                logger.warning(f"    delivery_date: {record[2]}")
                logger.warning(f"    created_date: {record[3]}")
                logger.warning(f"    updated_date: {record[4]}")
                logger.warning("    ---")
        else:
            logger.info("No records with '252025' found in Batch 30")
        
        return extreme_records + text_records
        
    except Exception as e:
        logger.error(f"Error in find_records_with_extreme_dates_batch_30: {e}")
        return []
    finally:
        conn_a.close()

def main():
    """Main function"""
    if len(sys.argv) != 4:
        print("Usage: python3 debug_batch_30_exact.py <start_date> <end_date> <warehouse_id>")
        print("Example: python3 debug_batch_30_exact.py 2024-05-01 2025-05-31 4512")
        sys.exit(1)
    
    start_date = sys.argv[1]
    end_date = sys.argv[2]
    warehouse_id = int(sys.argv[3])
    
    logger = setup_logging()
    
    logger.info(f"Debugging Batch 30 for date range: {start_date} to {end_date}")
    logger.info(f"Warehouse ID: {warehouse_id}")
    
    # Test 1: Debug individual records in Batch 30
    logger.info("\n" + "="*60)
    problematic_records = debug_batch_30_records(logger, start_date, end_date, warehouse_id)
    
    # Test 2: Test executemany with Batch 30
    logger.info("\n" + "="*60)
    executemany_success = test_batch_30_executemany(logger, start_date, end_date, warehouse_id)
    
    # Test 3: Search for extreme dates in Batch 30
    logger.info("\n" + "="*60)
    extreme_records = find_records_with_extreme_dates_batch_30(logger, start_date, end_date, warehouse_id)
    
    # Summary
    logger.info("\n" + "="*60)
    logger.info("SUMMARY:")
    logger.info(f"Problematic records found: {len(problematic_records)}")
    logger.info(f"Executemany test: {'✅ PASSED' if executemany_success else '❌ FAILED'}")
    logger.info(f"Extreme date records found: {len(extreme_records)}")
    
    if problematic_records or not executemany_success or extreme_records:
        logger.error("❌ Batch 30 contains problematic records!")
    else:
        logger.info("✅ Batch 30 appears to be clean!")

if __name__ == "__main__":
    main() 