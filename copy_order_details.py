#!/usr/bin/env python3
"""
Script to copy order detail data from Database A to order_detail_main in Database B
Based on mapping rules from outbound_items and related tables
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
    # Create logs directory if it doesn't exist
    if not os.path.exists('logs'):
        os.makedirs('logs')
    
    # Create log filename with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_filename = f'logs/copy_order_details_{timestamp}.log'
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

def get_db_connection(database='B'):
    """Get database connection - all tables are in Database B"""
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

def get_outbound_data(logger, start_date, end_date, warehouse_id):
    """Get outbound data based on the specified query"""
    logger.info("=== GETTING OUTBOUND DATA ===")
    
    conn_b = get_db_connection('B')  # Use Database B for source data
    
    try:
        cursor_b = conn_b.cursor()
        
        # Query to get outbound data based on document_reference
        # Use order_main table in Database B
        query = """
        SELECT 
            oi.id as outbound_item_id,
            oi.product_id,
            oi.qty,
            oi.uom,
            oi.pack_id,
            oi.line_id,
            odoc.id as outbound_document_id,
            odoc.document_reference,
            om.order_id,
            om.do_number,
            om.faktur_date,
            om.warehouse_id
        FROM outbound_items oi
        LEFT JOIN outbound_documents odoc ON odoc.id = oi.outbound_document_id
        LEFT JOIN order_main om ON om.do_number = odoc.document_reference
        WHERE om.faktur_date BETWEEN %s AND %s
        AND om.warehouse_id = %s
        ORDER BY om.faktur_date, oi.id
        """
        
        cursor_b.execute(query, (start_date, end_date, warehouse_id))
        results = cursor_b.fetchall()
        
        logger.info(f"Retrieved {len(results)} outbound items")
        
        # Convert to list of dictionaries for easier processing
        outbound_data = []
        for row in results:
            outbound_data.append({
                'outbound_item_id': row[0],
                'product_id': row[1],
                'qty': row[2],
                'uom': row[3],
                'pack_id': row[4],
                'line_id': row[5],
                'outbound_document_id': row[6],
                'document_reference': row[7],
                'order_id': row[8],
                'do_number': row[9],
                'faktur_date': row[10],
                'warehouse_id': row[11]
            })
        
        return outbound_data
        
    except Exception as e:
        logger.error(f"Error getting outbound data: {e}")
        return []
    finally:
        conn_b.close()

def get_product_net_price(logger, product_id, outbound_document_id):
    """Get product net price from outbound_items"""
    conn_b = get_db_connection('B')  # Use Database B
    
    try:
        cursor_b = conn_b.cursor()
        
        query = """
        SELECT product_net_price 
        FROM outbound_items 
        WHERE product_id = %s AND outbound_document_id = %s
        LIMIT 1
        """
        
        cursor_b.execute(query, (product_id, outbound_document_id))
        result = cursor_b.fetchone()
        
        return result[0] if result else None
        
    except Exception as e:
        logger.error(f"Error getting product net price for product_id {product_id}: {e}")
        return None
    finally:
        conn_b.close()

def get_conversion_data(logger, product_id, outbound_document_id):
    """Get conversion data from outbound_conversions"""
    conn_b = get_db_connection('B')  # Use Database B
    
    try:
        cursor_b = conn_b.cursor()
        
        query = """
        SELECT numerator, denominator 
        FROM outbound_conversions 
        WHERE product_id = %s AND outbound_document_id = %s
        LIMIT 1
        """
        
        cursor_b.execute(query, (product_id, outbound_document_id))
        result = cursor_b.fetchone()
        
        if result:
            return {'numerator': result[0], 'denominator': result[1]}
        return None
        
    except Exception as e:
        logger.error(f"Error getting conversion data for product_id {product_id}: {e}")
        return None
    finally:
        conn_b.close()

def calculate_quantities(logger, qty, uom, conversion_data):
    """Calculate quantities based on UOM and conversion rules"""
    quantity_faktur = None
    total_pcs = None
    total_ctn = None
    
    try:
        if uom and uom.upper() != 'PCS':
            # If UOM is not PCS, use conversion
            if conversion_data:
                quantity_faktur = qty * conversion_data['numerator']
                total_pcs = qty * conversion_data['numerator']
            else:
                # If no conversion data, use original qty
                quantity_faktur = qty
                total_pcs = qty
        else:
            # If UOM is PCS, use original qty
            quantity_faktur = qty
            total_pcs = qty
        
        # Calculate total_ctn if UOM is CTN
        if uom and uom.upper() == 'CTN':
            total_ctn = qty
        else:
            total_ctn = None
            
    except Exception as e:
        logger.error(f"Error calculating quantities: {e}")
        quantity_faktur = qty
        total_pcs = qty
        total_ctn = None
    
    return quantity_faktur, total_pcs, total_ctn

def insert_order_details(logger, order_details_data):
    """Insert order details into order_detail_main table"""
    logger.info("=== INSERTING ORDER DETAILS ===")
    
    conn_b = get_db_connection('B')
    
    try:
        cursor_b = conn_b.cursor()
        
        # Insert query for order_detail_main
        insert_query = """
        INSERT INTO order_detail_main (
            quantity_faktur, net_price, quantity_wms, quantity_delivery,
            quantity_loading, quantity_unloading, status, cancel_reason, notes,
            order_id, product_id, unit_id, pack_id, line_id, unloading_latitude,
            unloading_longitude, origin_uom, origin_qty, total_ctn, total_pcs
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        ) ON CONFLICT (order_id, product_id, line_id) DO UPDATE SET
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
        
        inserted_count = 0
        skipped_count = 0
        
        for detail in order_details_data:
            try:
                # Prepare data for insertion
                insert_data = (
                    detail['quantity_faktur'],      # quantity_faktur
                    detail['net_price'],            # net_price
                    None,                           # quantity_wms
                    None,                           # quantity_delivery
                    None,                           # quantity_loading
                    None,                           # quantity_unloading
                    None,                           # status
                    None,                           # cancel_reason
                    None,                           # notes
                    detail['order_id'],             # order_id
                    detail['product_id'],           # product_id
                    None,                           # unit_id
                    detail['pack_id'],              # pack_id
                    detail['line_id'],              # line_id
                    None,                           # unloading_latitude
                    None,                           # unloading_longitude
                    detail['origin_uom'],           # origin_uom
                    detail['origin_qty'],           # origin_qty
                    detail['total_ctn'],            # total_ctn
                    detail['total_pcs']             # total_pcs
                )
                
                cursor_b.execute(insert_query, insert_data)
                inserted_count += 1
                
                if inserted_count % 100 == 0:
                    logger.info(f"Inserted {inserted_count} order details...")
                
            except Exception as e:
                logger.error(f"Error inserting order detail for order_id {detail['order_id']}, product_id {detail['product_id']}: {e}")
                skipped_count += 1
                continue
        
        conn_b.commit()
        logger.info(f"✅ Order details insertion completed!")
        logger.info(f"Total inserted: {inserted_count}")
        logger.info(f"Total skipped: {skipped_count}")
        
        return inserted_count, skipped_count
        
    except Exception as e:
        conn_b.rollback()
        logger.error(f"Error in insert_order_details: {e}")
        return 0, 0
    finally:
        conn_b.close()

def copy_order_details(logger, start_date, end_date, warehouse_id):
    """Main function to copy order details"""
    logger.info("=== STARTING ORDER DETAILS COPY PROCESS ===")
    logger.info(f"Date range: {start_date} to {end_date}")
    logger.info(f"Warehouse ID: {warehouse_id}")
    
    # Step 1: Get outbound data
    outbound_data = get_outbound_data(logger, start_date, end_date, warehouse_id)
    
    if not outbound_data:
        logger.warning("No outbound data found for the specified criteria")
        return 0, 0
    
    # Step 2: Process and transform data
    logger.info("=== PROCESSING AND TRANSFORMING DATA ===")
    
    order_details_data = []
    
    for i, item in enumerate(outbound_data):
        try:
            logger.debug(f"Processing item {i+1}/{len(outbound_data)}: order_id {item['order_id']}, product_id {item['product_id']}")
            
            # Get product net price
            net_price = get_product_net_price(logger, item['product_id'], item['outbound_document_id'])
            
            # Get conversion data
            conversion_data = get_conversion_data(logger, item['product_id'], item['outbound_document_id'])
            
            # Calculate quantities based on UOM and conversion rules
            quantity_faktur, total_pcs, total_ctn = calculate_quantities(
                logger, item['qty'], item['uom'], conversion_data
            )
            
            # Prepare order detail data
            order_detail = {
                'order_id': item['order_id'],
                'product_id': item['product_id'],
                'quantity_faktur': quantity_faktur,
                'net_price': net_price,
                'pack_id': item['pack_id'],
                'line_id': item['line_id'],
                'origin_uom': item['uom'],
                'origin_qty': item['qty'],
                'total_ctn': total_ctn,
                'total_pcs': total_pcs
            }
            
            order_details_data.append(order_detail)
            
            if (i + 1) % 100 == 0:
                logger.info(f"Processed {i+1}/{len(outbound_data)} items...")
                
        except Exception as e:
            logger.error(f"Error processing item {i+1}: {e}")
            continue
    
    logger.info(f"Processed {len(order_details_data)} order details")
    
    # Step 3: Insert data into order_detail_main
    inserted_count, skipped_count = insert_order_details(logger, order_details_data)
    
    return inserted_count, skipped_count

def main():
    """Main function"""
    if len(sys.argv) != 4:
        print("Usage: python3 copy_order_details.py <start_date> <end_date> <warehouse_id>")
        print("Example: python3 copy_order_details.py 2025-01-01 2025-01-30 4512")
        sys.exit(1)
    
    start_date = sys.argv[1]
    end_date = sys.argv[2]
    warehouse_id = int(sys.argv[3])
    
    logger = setup_logging()
    
    try:
        inserted_count, skipped_count = copy_order_details(logger, start_date, end_date, warehouse_id)
        
        logger.info("\n" + "="*60)
        logger.info("COPY PROCESS SUMMARY:")
        logger.info(f"Date range: {start_date} to {end_date}")
        logger.info(f"Warehouse ID: {warehouse_id}")
        logger.info(f"Total inserted: {inserted_count}")
        logger.info(f"Total skipped: {skipped_count}")
        
        if inserted_count > 0:
            logger.info("✅ Order details copy process completed successfully!")
        else:
            logger.warning("⚠️ No order details were inserted")
            
    except Exception as e:
        logger.error(f"❌ Copy process failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 