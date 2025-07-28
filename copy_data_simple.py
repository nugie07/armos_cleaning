#!/usr/bin/env python3
"""
Simple copy script using batch processing with exact column matching
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

def copy_orders_simple(logger, start_date, end_date, warehouse_id):
    """Copy orders using batch processing with exact column matching"""
    logger.info("=== COPYING ORDERS (SIMPLE METHOD) ===")
    
    conn_a = get_db_connection('A')
    conn_b = get_db_connection('B')
    
    try:
        cursor_a = conn_a.cursor()
        cursor_b = conn_b.cursor()
        
        # Get total orders to copy
        cursor_a.execute("""
            SELECT COUNT(*) FROM "order" 
            WHERE faktur_date >= %s AND faktur_date <= %s 
            AND warehouse_id = %s
            AND faktur_id IS NOT NULL AND customer_id IS NOT NULL
        """, (start_date, end_date, warehouse_id))
        total_orders = cursor_a.fetchone()[0]
        
        logger.info(f"Total orders to copy: {total_orders}")
        
        if total_orders == 0:
            logger.warning("No orders found for the specified criteria")
            return 0
        
        # Copy orders in batches
        batch_size = 1000
        offset = 0
        copied_count = 0
        
        while offset < total_orders:
            # Fetch batch from source
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
                break
            
            # Insert batch into target
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
            
            cursor_b.executemany(insert_query, batch_data)
            conn_b.commit()
            
            copied_count += len(batch_data)
            logger.info(f"Copied {len(batch_data)} orders (Total: {copied_count}/{total_orders})")
            
            offset += batch_size
        
        logger.info(f"✅ Order copy completed. Total copied: {copied_count}")
        return copied_count
        
    except Exception as e:
        conn_b.rollback()
        logger.error(f"Error copying orders: {e}")
        raise
    finally:
        conn_a.close()
        conn_b.close()

def copy_order_details_simple(logger, start_date, end_date, warehouse_id):
    """Copy order details using batch processing"""
    logger.info("=== COPYING ORDER DETAILS (SIMPLE METHOD) ===")
    
    conn_a = get_db_connection('A')
    conn_b = get_db_connection('B')
    
    try:
        cursor_a = conn_a.cursor()
        cursor_b = conn_b.cursor()
        
        # Get total order details to copy (only for orders that exist in order_main)
        cursor_a.execute("""
            SELECT COUNT(*) FROM order_detail od
            JOIN "order" o ON od.order_id = o.order_id
            WHERE o.faktur_date >= %s AND o.faktur_date <= %s 
            AND o.warehouse_id = %s
            AND o.faktur_id IS NOT NULL AND o.customer_id IS NOT NULL
        """, (start_date, end_date, warehouse_id))
        total_details = cursor_a.fetchone()[0]
        
        logger.info(f"Total order details to copy: {total_details}")
        
        if total_details == 0:
            logger.warning("No order details found for the specified criteria")
            return 0
        
        # Copy order details in batches
        batch_size = 1000
        offset = 0
        copied_count = 0
        
        while offset < total_details:
            # Fetch batch from source
            cursor_a.execute("""
                SELECT 
                    od.quantity_faktur, od.net_price, od.quantity_wms, od.quantity_delivery,
                    od.quantity_loading, od.quantity_unloading, od.status, od.cancel_reason,
                    od.notes, od.order_id, od.product_id, od.unit_id, od.pack_id, od.line_id,
                    od.unloading_latitude, od.unloading_longitude, od.origin_uom, od.origin_qty,
                    od.total_ctn, od.total_pcs
                FROM order_detail od
                JOIN "order" o ON od.order_id = o.order_id
                WHERE o.faktur_date >= %s AND o.faktur_date <= %s 
                AND o.warehouse_id = %s
                AND o.faktur_id IS NOT NULL AND o.customer_id IS NOT NULL
                ORDER BY o.faktur_date
                LIMIT %s OFFSET %s
            """, (start_date, end_date, warehouse_id, batch_size, offset))
            
            batch_data = cursor_a.fetchall()
            if not batch_data:
                break
            
            # Filter out order details for orders that don't exist in order_main
            filtered_batch = []
            for row in batch_data:
                order_id = row[9]  # order_id is at index 9
                
                # Check if order exists in order_main
                cursor_b.execute("SELECT 1 FROM order_main WHERE order_id = %s", (order_id,))
                if cursor_b.fetchone():
                    filtered_batch.append(row)
                else:
                    logger.warning(f"Skipping order detail for order_id {order_id} (order not found in order_main)")
            
            if not filtered_batch:
                logger.info(f"No valid order details in batch {offset//batch_size + 1}")
                offset += batch_size
                continue
            
            # Insert filtered batch into target
            insert_query = """
            INSERT INTO order_detail_main (
                quantity_faktur, net_price, quantity_wms, quantity_delivery,
                quantity_loading, quantity_unloading, status, cancel_reason,
                notes, order_id, product_id, unit_id, pack_id, line_id,
                unloading_latitude, unloading_longitude, origin_uom, origin_qty,
                total_ctn, total_pcs
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (order_id, product_id, line_id) DO UPDATE SET
                quantity_faktur = EXCLUDED.quantity_faktur,
                net_price = EXCLUDED.net_price,
                quantity_wms = EXCLUDED.quantity_wms,
                quantity_delivery = EXCLUDED.quantity_delivery,
                quantity_loading = EXCLUDED.quantity_loading,
                quantity_unloading = EXCLUDED.quantity_unloading,
                status = EXCLUDED.status,
                cancel_reason = EXCLUDED.cancel_reason,
                notes = EXCLUDED.notes,
                unit_id = EXCLUDED.unit_id,
                pack_id = EXCLUDED.pack_id,
                unloading_latitude = EXCLUDED.unloading_latitude,
                unloading_longitude = EXCLUDED.unloading_longitude,
                origin_uom = EXCLUDED.origin_uom,
                origin_qty = EXCLUDED.origin_qty,
                total_ctn = EXCLUDED.total_ctn,
                total_pcs = EXCLUDED.total_pcs
            """
            
            cursor_b.executemany(insert_query, filtered_batch)
            conn_b.commit()
            
            copied_count += len(filtered_batch)
            logger.info(f"Copied {len(filtered_batch)} order details (Total: {copied_count}/{total_details})")
            
            offset += batch_size
        
        logger.info(f"✅ Order detail copy completed. Total copied: {copied_count}")
        return copied_count
        
    except Exception as e:
        conn_b.rollback()
        logger.error(f"Error copying order details: {e}")
        raise
    finally:
        conn_a.close()
        conn_b.close()

def main():
    """Main function"""
    logger = setup_logging()
    
    if len(sys.argv) != 4:
        logger.error("Usage: python3 copy_data_simple.py <start_date> <end_date> <warehouse_id>")
        logger.error("Example: python3 copy_data_simple.py 2025-04-01 2025-04-30 4512")
        sys.exit(1)
    
    start_date = sys.argv[1]
    end_date = sys.argv[2]
    warehouse_id = int(sys.argv[3])
    
    try:
        logger.info("=== COPYING DATA WITH SIMPLE METHOD ===")
        logger.info(f"Date Range: {start_date} to {end_date}")
        logger.info(f"Warehouse ID: {warehouse_id}")
        
        # Step 1: Copy orders
        copied_orders = copy_orders_simple(logger, start_date, end_date, warehouse_id)
        
        # Step 2: Copy order details
        copied_details = copy_order_details_simple(logger, start_date, end_date, warehouse_id)
        
        logger.info("=== COPY COMPLETED ===")
        logger.info(f"Orders copied: {copied_orders}")
        logger.info(f"Order details copied: {copied_details}")
        
    except Exception as e:
        logger.error(f"Script failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 