#!/usr/bin/env python3
"""
Debug script to check order details for batch 295 orders
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

def get_batch_295_order_ids(logger, start_date, end_date, warehouse_id):
    """Get order IDs from batch 295"""
    logger.info("=== GETTING BATCH 295 ORDER IDs ===")
    
    conn_a = get_db_connection('A')
    
    try:
        cursor = conn_a.cursor()
        
        cursor.execute("""
            SELECT order_id FROM "order" 
            WHERE faktur_date >= %s AND faktur_date <= %s 
            AND warehouse_id = %s
            AND faktur_id IS NOT NULL AND customer_id IS NOT NULL
            ORDER BY faktur_date
            LIMIT 100 OFFSET 29500
        """, (start_date, end_date, warehouse_id))
        
        order_ids = [row[0] for row in cursor.fetchall()]
        logger.info(f"Found {len(order_ids)} order IDs in batch 295")
        
        return order_ids
        
    except Exception as e:
        logger.error(f"Error getting batch 295 order IDs: {e}")
        return []
    finally:
        conn_a.close()

def debug_order_details_for_batch(logger, order_ids):
    """Debug order details for the given order IDs"""
    logger.info("=== DEBUGGING ORDER DETAILS FOR BATCH 295 ===")
    
    conn_a = get_db_connection('A')
    
    try:
        cursor = conn_a.cursor()
        
        # Get order details for batch 295 orders
        logger.info("Fetching order details for batch 295 orders...")
        
        placeholders = ','.join(['%s'] * len(order_ids))
        cursor.execute(f"""
            SELECT * FROM order_detail 
            WHERE order_id IN ({placeholders})
            ORDER BY order_id, order_detail_id
        """, order_ids)
        
        order_details = cursor.fetchall()
        logger.info(f"Found {len(order_details)} order details for batch 295 orders")
        
        # Test each order detail individually
        logger.info("Testing each order detail...")
        
        for i, row in enumerate(order_details):
            try:
                # Extract key fields
                order_detail_id = row[0]
                quantity_faktur = row[1]
                net_price = row[2]
                quantity_wms = row[3]
                quantity_delivery = row[4]
                quantity_loading = row[5]
                quantity_unloading = row[6]
                status = row[7]
                cancel_reason = row[8]
                notes = row[9]
                order_id = row[10]
                product_id = row[11]
                unit_id = row[12]
                pack_id = row[13]
                line_id = row[14]
                unloading_latitude = row[15]
                unloading_longitude = row[16]
                origin_uom = row[17]
                origin_qty = row[18]
                total_ctn = row[19]
                total_pcs = row[20]
                
                # Test string conversion for all fields
                str(order_detail_id)
                str(quantity_faktur) if quantity_faktur else None
                str(net_price) if net_price else None
                str(quantity_wms) if quantity_wms else None
                str(quantity_delivery) if quantity_delivery else None
                str(quantity_loading) if quantity_loading else None
                str(quantity_unloading) if quantity_unloading else None
                str(status) if status else None
                str(cancel_reason) if cancel_reason else None
                str(notes) if notes else None
                str(order_id)
                str(product_id) if product_id else None
                str(unit_id) if unit_id else None
                str(pack_id) if pack_id else None
                str(line_id) if line_id else None
                str(unloading_latitude) if unloading_latitude else None
                str(unloading_longitude) if unloading_longitude else None
                str(origin_uom) if origin_uom else None
                str(origin_qty) if origin_qty else None
                str(total_ctn) if total_ctn else None
                str(total_pcs) if total_pcs else None
                
                logger.info(f"✅ Order Detail {i+1}: ID {order_detail_id}, Order {order_id} - OK")
                
            except Exception as e:
                logger.error(f"❌ ERROR in order detail {i+1}: {e}")
                logger.error(f"  Order Detail ID: {order_detail_id}")
                logger.error(f"  Order ID: {order_id}")
                logger.error(f"  Product ID: {product_id}")
                logger.error(f"  Line ID: {line_id}")
                
                # Show the problematic record details
                logger.error("=== PROBLEMATIC ORDER DETAIL RECORD ===")
                for j, value in enumerate(row):
                    try:
                        str_value = str(value)
                        logger.error(f"  Column {j}: {str_value}")
                    except Exception as str_error:
                        logger.error(f"  Column {j}: ERROR converting to string - {str_error}")
                
                return False
        
        logger.info("✅ All order details for batch 295 processed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"Error in debug_order_details_for_batch: {e}")
        return False
    finally:
        conn_a.close()

def test_order_details_insert_for_batch(logger, order_ids):
    """Test inserting order details for batch 295 orders"""
    logger.info("=== TESTING ORDER DETAILS INSERT FOR BATCH 295 ===")
    
    conn_a = get_db_connection('A')
    conn_b = get_db_connection('B')
    
    try:
        cursor_a = conn_a.cursor()
        cursor_b = conn_b.cursor()
        
        # Get order details for batch 295 orders
        logger.info("Fetching order details for batch 295 orders...")
        
        placeholders = ','.join(['%s'] * len(order_ids))
        cursor_a.execute(f"""
            SELECT * FROM order_detail 
            WHERE order_id IN ({placeholders})
            ORDER BY order_id, order_detail_id
        """, order_ids)
        
        order_details = cursor_a.fetchall()
        logger.info(f"Found {len(order_details)} order details for batch 295 orders")
        
        # Test inserting each order detail individually
        logger.info("Testing order detail insertion...")
        
        insert_query = """
            INSERT INTO order_detail_main (
                order_detail_id, quantity_faktur, net_price, quantity_wms, quantity_delivery,
                quantity_loading, quantity_unloading, status, cancel_reason, notes,
                order_id, product_id, unit_id, pack_id, line_id, unloading_latitude,
                unloading_longitude, origin_uom, origin_qty, total_ctn, total_pcs
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            ) ON CONFLICT (order_id, product_id, line_id) DO UPDATE SET
                quantity_faktur = EXCLUDED.quantity_faktur, net_price = EXCLUDED.net_price,
                quantity_wms = EXCLUDED.quantity_wms, quantity_delivery = EXCLUDED.quantity_delivery,
                quantity_loading = EXCLUDED.quantity_loading, quantity_unloading = EXCLUDED.quantity_unloading,
                status = EXCLUDED.status, cancel_reason = EXCLUDED.cancel_reason, notes = EXCLUDED.notes,
                unit_id = EXCLUDED.unit_id, pack_id = EXCLUDED.pack_id, unloading_latitude = EXCLUDED.unloading_latitude,
                unloading_longitude = EXCLUDED.unloading_longitude, origin_uom = EXCLUDED.origin_uom,
                origin_qty = EXCLUDED.origin_qty, total_ctn = EXCLUDED.total_ctn, total_pcs = EXCLUDED.total_pcs
        """
        
        for i, row in enumerate(order_details):
            try:
                # Check if parent order exists in DB B
                order_id = row[10]
                cursor_b.execute("SELECT 1 FROM order_main WHERE order_id = %s", (order_id,))
                if not cursor_b.fetchone():
                    logger.warning(f"Skipping order detail {i+1}: Order {order_id} not found in order_main")
                    continue
                
                cursor_b.execute(insert_query, row)
                logger.info(f"✅ Order Detail {i+1}: ID {row[0]}, Order {order_id} - Inserted")
                
            except Exception as e:
                logger.error(f"❌ ERROR inserting order detail {i+1}: {e}")
                logger.error(f"  Order Detail ID: {row[0]}")
                logger.error(f"  Order ID: {row[10]}")
                logger.error(f"  Product ID: {row[11]}")
                logger.error(f"  Line ID: {row[14]}")
                
                # Show the problematic record details
                logger.error("=== PROBLEMATIC ORDER DETAIL RECORD ===")
                for j, value in enumerate(row):
                    try:
                        str_value = str(value)
                        logger.error(f"  Column {j}: {str_value}")
                    except Exception as str_error:
                        logger.error(f"  Column {j}: ERROR converting to string - {str_error}")
                
                return False
        
        logger.info("✅ All order details for batch 295 inserted successfully!")
        return True
        
    except Exception as e:
        logger.error(f"Error in test_order_details_insert_for_batch: {e}")
        return False
    finally:
        conn_a.close()
        conn_b.close()

def main():
    """Main function"""
    logger = setup_logging()
    
    if len(sys.argv) != 4:
        logger.error("Usage: python3 debug_order_details_batch_295.py <start_date> <end_date> <warehouse_id>")
        logger.error("Example: python3 debug_order_details_batch_295.py 2024-05-01 2025-05-31 4512")
        sys.exit(1)
    
    start_date = sys.argv[1]
    end_date = sys.argv[2]
    warehouse_id = int(sys.argv[3])
    
    try:
        logger.info("=== DEBUGGING ORDER DETAILS FOR BATCH 295 ===")
        logger.info(f"Date Range: {start_date} to {end_date}")
        logger.info(f"Warehouse ID: {warehouse_id}")
        
        # Step 1: Get order IDs from batch 295
        order_ids = get_batch_295_order_ids(logger, start_date, end_date, warehouse_id)
        
        if not order_ids:
            logger.error("No order IDs found for batch 295")
            return
        
        # Step 2: Debug order details for these orders
        success1 = debug_order_details_for_batch(logger, order_ids)
        
        # Step 3: Test inserting order details
        success2 = test_order_details_insert_for_batch(logger, order_ids)
        
        if success1 and success2:
            logger.info("✅ All order details tests completed successfully!")
        else:
            logger.error("❌ Some order details tests failed!")
        
    except Exception as e:
        logger.error(f"Script failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 