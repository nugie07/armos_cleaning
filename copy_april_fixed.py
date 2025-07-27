#!/usr/bin/env python3
"""
Specialized script to copy April 2025 data with enhanced error handling
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

def clean_april_data(logger):
    """Clean April 2025 data from Database B"""
    logger.info("=== CLEANING APRIL 2025 DATA ===")
    
    conn_b = get_db_connection('B')
    try:
        cursor = conn_b.cursor()
        
        # Get current counts
        cursor.execute("""
            SELECT COUNT(*) FROM order_main 
            WHERE faktur_date >= '2025-04-01' AND faktur_date <= '2025-04-30'
        """)
        april_orders = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT COUNT(*) FROM order_detail_main odm
            JOIN order_main om ON odm.order_id = om.order_id
            WHERE om.faktur_date >= '2025-04-01' AND om.faktur_date <= '2025-04-30'
        """)
        april_details = cursor.fetchone()[0]
        
        logger.info(f"Current April orders: {april_orders}, April details: {april_details}")
        
        # Delete April order details first (due to foreign key)
        cursor.execute("""
            DELETE FROM order_detail_main 
            WHERE order_id IN (
                SELECT order_id FROM order_main 
                WHERE faktur_date >= '2025-04-01' AND faktur_date <= '2025-04-30'
            )
        """)
        deleted_details = cursor.rowcount
        
        # Delete April orders
        cursor.execute("""
            DELETE FROM order_main 
            WHERE faktur_date >= '2025-04-01' AND faktur_date <= '2025-04-30'
        """)
        deleted_orders = cursor.rowcount
        
        conn_b.commit()
        logger.info(f"Cleaned: {deleted_orders} orders, {deleted_details} order details")
        
    except Exception as e:
        conn_b.rollback()
        logger.error(f"Error cleaning April data: {e}")
        raise
    finally:
        conn_b.close()

def copy_april_orders(logger, warehouse_id):
    """Copy April 2025 orders with detailed logging"""
    logger.info("=== COPYING APRIL 2025 ORDERS ===")
    
    conn_a = get_db_connection('A')
    conn_b = get_db_connection('B')
    
    try:
        cursor_a = conn_a.cursor()
        cursor_b = conn_b.cursor()
        
        # Get total valid orders for April
        cursor_a.execute("""
            SELECT COUNT(*) FROM "order" 
            WHERE faktur_date >= '2025-04-01' AND faktur_date <= '2025-04-30'
            AND warehouse_id = %s
            AND faktur_id IS NOT NULL AND customer_id IS NOT NULL
        """, (warehouse_id,))
        total_orders = cursor_a.fetchone()[0]
        
        logger.info(f"Total valid orders to copy: {total_orders}")
        
        # Copy orders in smaller batches
        batch_size = 500
        offset = 0
        copied_count = 0
        
        while offset < total_orders:
            cursor_a.execute("""
                SELECT 
                    order_id, faktur_id, faktur_date, delivery_date, do_number, status, skip_count,
                    created_date, created_by, updated_date, updated_by, notes, customer_id,
                    warehouse_id, delivery_type_id, order_integration_id, origin_name,
                    origin_address_1, origin_address_2, origin_city, origin_zipcode,
                    origin_phone, origin_email, destination_name, destination_address_1,
                    destination_address_2, destination_city, destination_zip_code,
                    destination_phone, destination_email, client_id, cancel_reason,
                    rdo_integration_id, address_change, divisi, pre_status
                FROM "order"
                WHERE faktur_date >= '2025-04-01' AND faktur_date <= '2025-04-30'
                AND warehouse_id = %s
                AND faktur_id IS NOT NULL AND customer_id IS NOT NULL
                ORDER BY faktur_date
                LIMIT %s OFFSET %s
            """, (warehouse_id, batch_size, offset))
            
            batch_data = cursor_a.fetchall()
            if not batch_data:
                break
            
            # Insert batch
            insert_query = """
            INSERT INTO order_main (
                order_id, faktur_id, faktur_date, delivery_date, do_number, status, skip_count,
                created_date, created_by, updated_date, updated_by, notes, customer_id,
                warehouse_id, delivery_type_id, order_integration_id, origin_name,
                origin_address_1, origin_address_2, origin_city, origin_zipcode,
                origin_phone, origin_email, destination_name, destination_address_1,
                destination_address_2, destination_city, destination_zip_code,
                destination_phone, destination_email, client_id, cancel_reason,
                rdo_integration_id, address_change, divisi, pre_status
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                     %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
        
        logger.info(f"Order copy completed. Total copied: {copied_count}")
        return copied_count
        
    except Exception as e:
        conn_b.rollback()
        logger.error(f"Error copying orders: {e}")
        raise
    finally:
        conn_a.close()
        conn_b.close()

def copy_april_order_details(logger, warehouse_id):
    """Copy April 2025 order details with enhanced error handling"""
    logger.info("=== COPYING APRIL 2025 ORDER DETAILS ===")
    
    conn_a = get_db_connection('A')
    conn_b = get_db_connection('B')
    
    try:
        cursor_a = conn_a.cursor()
        cursor_b = conn_b.cursor()
        
        # Get total order details for April
        cursor_a.execute("""
            SELECT COUNT(*) FROM order_detail od
            JOIN "order" o ON od.order_id = o.order_id
            WHERE o.faktur_date >= '2025-04-01' AND o.faktur_date <= '2025-04-30'
            AND o.warehouse_id = %s
            AND o.faktur_id IS NOT NULL AND o.customer_id IS NOT NULL
        """, (warehouse_id,))
        total_details = cursor_a.fetchone()[0]
        
        logger.info(f"Total order details to copy: {total_details}")
        
        # Copy order details in smaller batches
        batch_size = 500
        offset = 0
        copied_count = 0
        failed_count = 0
        
        while offset < total_details:
            cursor_a.execute("""
                SELECT 
                    od.quantity_faktur, od.net_price, od.quantity_wms, od.quantity_delivery,
                    od.quantity_loading, od.quantity_unloading, od.status, od.cancel_reason,
                    od.notes, od.product_id, od.unit_id, od.pack_id, od.line_id,
                    od.unloading_latitude, od.unloading_longitude, od.origin_uom, od.origin_qty,
                    od.total_ctn, od.total_pcs, o.faktur_id, o.faktur_date, o.customer_id
                FROM order_detail od
                JOIN "order" o ON od.order_id = o.order_id
                WHERE o.faktur_date >= '2025-04-01' AND o.faktur_date <= '2025-04-30'
                AND o.warehouse_id = %s
                AND o.faktur_id IS NOT NULL AND o.customer_id IS NOT NULL
                ORDER BY o.faktur_date
                LIMIT %s OFFSET %s
            """, (warehouse_id, batch_size, offset))
            
            batch_data = cursor_a.fetchall()
            if not batch_data:
                break
            
            # Process each record to get order_id
            processed_records = []
            for record in batch_data:
                faktur_id, faktur_date, customer_id = record[-3], record[-2], record[-1]
                
                # Get order_id from target database
                if customer_id is None:
                    lookup_query = """
                    SELECT order_id FROM order_main 
                    WHERE faktur_id = %s AND faktur_date = %s AND customer_id IS NULL
                    """
                    lookup_params = (faktur_id, faktur_date)
                else:
                    lookup_query = """
                    SELECT order_id FROM order_main 
                    WHERE faktur_id = %s AND faktur_date = %s AND customer_id = %s::VARCHAR
                    """
                    lookup_params = (faktur_id, faktur_date, customer_id)
                
                cursor_b.execute(lookup_query, lookup_params)
                result = cursor_b.fetchone()
                
                if result:
                    order_id = result[0]
                    processed_record = record[:-3] + (order_id,)
                    processed_records.append(processed_record)
                else:
                    failed_count += 1
                    logger.warning(f"Order not found for faktur_id: {faktur_id}, date: {faktur_date}, customer: {customer_id}")
            
            if processed_records:
                # Insert batch
                insert_query = """
                INSERT INTO order_detail_main (
                    quantity_faktur, net_price, quantity_wms, quantity_delivery,
                    quantity_loading, quantity_unloading, status, cancel_reason,
                    notes, product_id, unit_id, pack_id, line_id,
                    unloading_latitude, unloading_longitude, origin_uom, origin_qty,
                    total_ctn, total_pcs, order_id
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
                
                cursor_b.executemany(insert_query, processed_records)
                conn_b.commit()
                
                copied_count += len(processed_records)
                logger.info(f"Copied {len(processed_records)} order details (Total: {copied_count}/{total_details})")
            
            offset += batch_size
        
        logger.info(f"Order detail copy completed. Total copied: {copied_count}, Failed: {failed_count}")
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
    
    if len(sys.argv) != 2:
        logger.error("Usage: python3 copy_april_fixed.py <warehouse_id>")
        sys.exit(1)
    
    warehouse_id = int(sys.argv[1])
    
    try:
        logger.info("=== APRIL 2025 DATA COPY - ENHANCED VERSION ===")
        logger.info(f"Warehouse ID: {warehouse_id}")
        
        # Step 1: Clean April data
        clean_april_data(logger)
        
        # Step 2: Copy orders
        copied_orders = copy_april_orders(logger, warehouse_id)
        
        # Step 3: Copy order details
        copied_details = copy_april_order_details(logger, warehouse_id)
        
        logger.info("=== COPY COMPLETED ===")
        logger.info(f"Orders copied: {copied_orders}")
        logger.info(f"Order details copied: {copied_details}")
        
    except Exception as e:
        logger.error(f"Script failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 