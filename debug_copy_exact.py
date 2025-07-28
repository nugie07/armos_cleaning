#!/usr/bin/env python3
"""
Debug script that uses the exact same query structure as copy_data_simple.py
to accurately reproduce the "year out of range" error
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

def test_exact_query_structure(logger, start_date, end_date, warehouse_id):
    """Test the exact query structure from copy_data_simple.py"""
    logger.info("=== TESTING EXACT QUERY STRUCTURE FROM copy_data_simple.py ===")
    
    conn_a = get_db_connection('A')
    
    try:
        cursor_a = conn_a.cursor()
        
        # Get total orders (exact same query as copy_data_simple.py)
        cursor_a.execute("""
            SELECT COUNT(*) FROM "order" 
            WHERE faktur_date >= %s AND faktur_date <= %s 
            AND warehouse_id = %s
            AND faktur_id IS NOT NULL AND customer_id IS NOT NULL
        """, (start_date, end_date, warehouse_id))
        total_orders = cursor_a.fetchone()[0]
        
        logger.info(f"Total orders to process: {total_orders}")
        
        if total_orders == 0:
            logger.warning("No orders found for the specified criteria")
            return
        
        # Test batch processing (exact same structure as copy_data_simple.py)
        batch_size = 1000
        offset = 0
        batch_number = 0
        
        while offset < total_orders:
            batch_number += 1
            logger.info(f"Processing batch {batch_number} (offset: {offset})")
            
            # Fetch batch from source (exact same query as copy_data_simple.py)
            cursor_a.execute("""
                SELECT * FROM "order" 
                WHERE faktur_date >= %s AND faktur_date <= %s 
                AND warehouse_id = %s
                AND faktur_id IS NOT NULL AND customer_id IS NOT NULL
                ORDER BY faktur_date
                LIMIT %s OFFSET %s
            """, (start_date, end_date, warehouse_id, batch_size, offset))
            
            batch_data = cursor_a.fetchall()
            if not batch_data:
                logger.info(f"Batch {batch_number}: No more data")
                break
            
            logger.info(f"Batch {batch_number}: Retrieved {len(batch_data)} records")
            
            # Test each record in the batch
            for i, row in enumerate(batch_data):
                try:
                    # Test string conversion of all fields to trigger any date conversion errors
                    for j, field in enumerate(row):
                        if field is not None:
                            str(field)
                    
                    # Test specific date fields that might cause issues
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
                    
                    logger.debug(f"  Record {i+1}: order_id {order_id} - OK")
                    
                except Exception as e:
                    logger.error(f"❌ ERROR in batch {batch_number}, record {i+1}: {e}")
                    logger.error(f"  order_id: {row[0] if len(row) > 0 else 'N/A'}")
                    logger.error(f"  faktur_date: {row[2] if len(row) > 2 else 'N/A'}")
                    logger.error(f"  delivery_date: {row[3] if len(row) > 3 else 'N/A'}")
                    logger.error(f"  created_date: {row[7] if len(row) > 7 else 'N/A'}")
                    logger.error(f"  updated_date: {row[9] if len(row) > 9 else 'N/A'}")
                    logger.error(f"  Full record: {row}")
                    return False
            
            logger.info(f"✅ Batch {batch_number} completed successfully")
            offset += batch_size
        
        logger.info("✅ All batches processed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"Error in test_exact_query_structure: {e}")
        return False
    finally:
        conn_a.close()

def test_executemany_simulation(logger, start_date, end_date, warehouse_id):
    """Simulate the executemany operation from copy_data_simple.py"""
    logger.info("=== SIMULATING EXECUTEMANY OPERATION ===")
    
    conn_a = get_db_connection('A')
    conn_b = get_db_connection('B')
    
    try:
        cursor_a = conn_a.cursor()
        cursor_b = conn_b.cursor()
        
        # Get total orders
        cursor_a.execute("""
            SELECT COUNT(*) FROM "order" 
            WHERE faktur_date >= %s AND faktur_date <= %s 
            AND warehouse_id = %s
            AND faktur_id IS NOT NULL AND customer_id IS NOT NULL
        """, (start_date, end_date, warehouse_id))
        total_orders = cursor_a.fetchone()[0]
        
        logger.info(f"Total orders to process: {total_orders}")
        
        if total_orders == 0:
            logger.warning("No orders found for the specified criteria")
            return
        
        # Test batch processing with executemany simulation
        batch_size = 1000
        offset = 0
        batch_number = 0
        
        while offset < total_orders:
            batch_number += 1
            logger.info(f"Processing batch {batch_number} (offset: {offset})")
            
            # Fetch batch from source (exact same query as copy_data_simple.py)
            cursor_a.execute("""
                SELECT * FROM "order" 
                WHERE faktur_date >= %s AND faktur_date <= %s 
                AND warehouse_id = %s
                AND faktur_id IS NOT NULL AND customer_id IS NOT NULL
                ORDER BY faktur_date
                LIMIT %s OFFSET %s
            """, (start_date, end_date, warehouse_id, batch_size, offset))
            
            batch_data = cursor_a.fetchall()
            if not batch_data:
                logger.info(f"Batch {batch_number}: No more data")
                break
            
            logger.info(f"Batch {batch_number}: Retrieved {len(batch_data)} records")
            
            # Simulate the exact insert query from copy_data_simple.py
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
                # Test executemany (but don't actually commit)
                cursor_b.executemany(insert_query, batch_data)
                logger.info(f"✅ Batch {batch_number}: executemany successful")
                
                # Rollback to avoid actually inserting data
                conn_b.rollback()
                
            except Exception as e:
                logger.error(f"❌ ERROR in batch {batch_number} executemany: {e}")
                
                # Test individual records to find the problematic one
                logger.info(f"Testing individual records in batch {batch_number}...")
                for i, row in enumerate(batch_data):
                    try:
                        cursor_b.execute(insert_query, row)
                        conn_b.rollback()
                        logger.debug(f"  Record {i+1}: OK")
                    except Exception as record_error:
                        logger.error(f"❌ ERROR in batch {batch_number}, record {i+1}: {record_error}")
                        logger.error(f"  order_id: {row[0] if len(row) > 0 else 'N/A'}")
                        logger.error(f"  faktur_date: {row[2] if len(row) > 2 else 'N/A'}")
                        logger.error(f"  delivery_date: {row[3] if len(row) > 3 else 'N/A'}")
                        logger.error(f"  created_date: {row[7] if len(row) > 7 else 'N/A'}")
                        logger.error(f"  updated_date: {row[9] if len(row) > 9 else 'N/A'}")
                        logger.error(f"  Full record: {row}")
                        return False
            
            offset += batch_size
        
        logger.info("✅ All batches processed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"Error in test_executemany_simulation: {e}")
        return False
    finally:
        conn_a.close()
        conn_b.close()

def main():
    """Main function"""
    if len(sys.argv) != 4:
        print("Usage: python3 debug_copy_exact.py <start_date> <end_date> <warehouse_id>")
        print("Example: python3 debug_copy_exact.py 2024-05-01 2025-05-31 4512")
        sys.exit(1)
    
    start_date = sys.argv[1]
    end_date = sys.argv[2]
    warehouse_id = int(sys.argv[3])
    
    logger = setup_logging()
    
    logger.info(f"Debugging copy process for date range: {start_date} to {end_date}")
    logger.info(f"Warehouse ID: {warehouse_id}")
    
    # Test 1: Exact query structure
    logger.info("\n" + "="*60)
    success1 = test_exact_query_structure(logger, start_date, end_date, warehouse_id)
    
    # Test 2: Executemany simulation
    logger.info("\n" + "="*60)
    success2 = test_executemany_simulation(logger, start_date, end_date, warehouse_id)
    
    if success1 and success2:
        logger.info("\n✅ All tests passed! The copy process should work.")
    else:
        logger.error("\n❌ Tests failed! Found the problematic records.")

if __name__ == "__main__":
    main() 