#!/usr/bin/env python3
"""
Optimized Order Details Copy Script
Uses JOIN queries to reduce database calls from 223,956 to 1-2 queries
Estimated time: 5-10 minutes instead of 5.5 hours
"""

import os
import sys
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
import logging
from datetime import datetime

# Load environment variables
load_dotenv('config.env')

def setup_logging():
    """Setup logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(f'copy_order_details_optimized_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
        ]
    )
    return logging.getLogger(__name__)

def get_db_connection(database='B'):
    """Get database connection"""
    if database == 'B':
        return psycopg2.connect(
            host=os.getenv('DB_B_HOST'),
            database=os.getenv('DB_B_NAME'),
            user=os.getenv('DB_B_USER'),
            password=os.getenv('DB_B_PASSWORD'),
            port=os.getenv('DB_B_PORT')
        )

def get_optimized_outbound_data(logger, start_date, end_date, warehouse_id):
    """Get all outbound data with JOIN queries in one go"""
    conn_b = get_db_connection('B')
    
    try:
        cursor_b = conn_b.cursor()
        
        # Single optimized query with JOINs to get all data at once
        query = """
        SELECT 
            oi.id,
            oi.product_id as sku,
            oi.quantity as qty,
            oi.uom,
            oi.pack_id,
            oi.line_id,
            oi.outbound_document_id,
            odoc.document_reference,
            om.order_id,
            om.do_number,
            om.faktur_date,
            om.warehouse_id,
            mp.mst_product_id,
            oi.product_net_price,
            oc.numerator,
            oc.denominator
        FROM outbound_items oi
        LEFT JOIN outbound_documents odoc ON odoc.id = oi.outbound_document_id
        LEFT JOIN order_main om ON om.do_number = odoc.document_reference
        LEFT JOIN mst_product_main mp ON (
            mp.sku = oi.product_id 
            AND mp.pack_id = oi.pack_id 
            AND mp.warehouse_id = %s
        )
        LEFT JOIN outbound_conversions oc ON oi.id = oc.outbound_item_id
        WHERE om.faktur_date BETWEEN %s AND %s
        AND om.warehouse_id = %s
        ORDER BY om.faktur_date, oi.id
        """
        
        logger.info("Executing optimized JOIN query...")
        cursor_b.execute(query, (str(warehouse_id), start_date, end_date, warehouse_id))
        
        results = cursor_b.fetchall()
        logger.info(f"Retrieved {len(results)} records with optimized query")
        
        # Transform results into structured data
        outbound_data = []
        for row in results:
            outbound_data.append({
                'id': row[0],
                'sku': row[1],
                'qty': row[2],
                'uom': row[3],
                'pack_id': row[4],
                'line_id': row[5],
                'outbound_document_id': row[6],
                'document_reference': row[7],
                'order_id': row[8],
                'do_number': row[9],
                'faktur_date': row[10],
                'warehouse_id': row[11],
                'product_id': row[12],  # From mst_product_main
                'net_price': row[13],   # From outbound_items
                'numerator': row[14],   # From outbound_conversions
                'denominator': row[15]  # From outbound_conversions
            })
        
        return outbound_data
        
    except Exception as e:
        logger.error(f"Error getting optimized outbound data: {e}")
        return []
    finally:
        conn_b.close()

def calculate_quantities_optimized(qty, uom, numerator, denominator):
    """Calculate quantities based on UOM and conversion rules"""
    quantity_faktur = None
    total_pcs = None
    total_ctn = None
    
    try:
        if uom and uom.upper() != 'PCS':
            # If UOM is not PCS, use conversion
            if numerator and denominator:
                quantity_faktur = qty * numerator
                total_pcs = qty * numerator
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
            total_ctn = 0
        
        return quantity_faktur, total_pcs, total_ctn
        
    except Exception as e:
        # Return safe defaults if calculation fails
        return qty, qty, 0

def insert_order_details_batch(logger, order_details_data, batch_size=1000):
    """Insert order details in batches with UPSERT"""
    if not order_details_data:
        logger.warning("No order details data to insert")
        return 0, 0
    
    conn_b = get_db_connection('B')
    
    try:
        cursor_b = conn_b.cursor()
        
        inserted_count = 0
        skipped_count = 0
        
        # Process in batches
        for i in range(0, len(order_details_data), batch_size):
            batch = order_details_data[i:i + batch_size]
            
            # Prepare batch data
            batch_values = []
            for item in batch:
                if not item['product_id']:
                    skipped_count += 1
                    continue
                    
                batch_values.append((
                    item['order_id'],
                    item['product_id'],
                    item['quantity_faktur'],
                    item['net_price'],
                    item['pack_id'],
                    item['line_id'],
                    item['origin_uom'],
                    item['origin_qty'],
                    item['total_ctn'],
                    item['total_pcs']
                ))
            
            if not batch_values:
                continue
            
            # UPSERT query for batch
            query = """
            INSERT INTO order_detail_main (
                order_id, product_id, quantity_faktur, net_price, 
                pack_id, line_id, origin_uom, origin_qty, total_ctn, total_pcs
            ) VALUES %s
            ON CONFLICT (order_id, product_id, line_id) 
            DO UPDATE SET
                quantity_faktur = EXCLUDED.quantity_faktur,
                net_price = EXCLUDED.net_price,
                pack_id = EXCLUDED.pack_id,
                origin_uom = EXCLUDED.origin_uom,
                origin_qty = EXCLUDED.origin_qty,
                total_ctn = EXCLUDED.total_ctn,
                total_pcs = EXCLUDED.total_pcs
            """
            
            # Execute batch insert
            psycopg2.extras.execute_values(cursor_b, query, batch_values)
            conn_b.commit()
            
            inserted_count += len(batch_values)
            logger.info(f"Inserted batch {i//batch_size + 1}: {len(batch_values)} records (Total: {inserted_count})")
        
        return inserted_count, skipped_count
        
    except Exception as e:
        logger.error(f"Error inserting order details: {e}")
        conn_b.rollback()
        return 0, 0
    finally:
        conn_b.close()

def copy_order_details_optimized(logger, start_date, end_date, warehouse_id):
    """Main optimized function to copy order details"""
    logger.info("=== STARTING OPTIMIZED ORDER DETAILS COPY PROCESS ===")
    logger.info(f"Date range: {start_date} to {end_date}")
    logger.info(f"Warehouse ID: {warehouse_id}")
    
    # Step 1: Get all data with single optimized query
    logger.info("=== EXECUTING OPTIMIZED DATA RETRIEVAL ===")
    outbound_data = get_optimized_outbound_data(logger, start_date, end_date, warehouse_id)
    
    if not outbound_data:
        logger.warning("No outbound data found for the specified criteria")
        return 0, 0
    
    # Step 2: Process and transform data in memory
    logger.info("=== PROCESSING AND TRANSFORMING DATA ===")
    
    order_details_data = []
    valid_count = 0
    skipped_count = 0
    
    for i, item in enumerate(outbound_data):
        try:
            # Skip if no product_id found
            if not item['product_id']:
                skipped_count += 1
                if skipped_count <= 10:  # Log first 10 skips
                    logger.warning(f"Skipping item {i+1}: No product_id found for sku={item['sku']}, pack_id={item['pack_id']}")
                continue
            
            # Calculate quantities
            quantity_faktur, total_pcs, total_ctn = calculate_quantities_optimized(
                item['qty'], item['uom'], item['numerator'], item['denominator']
            )
            
            # Prepare order detail data
            order_detail = {
                'order_id': item['order_id'],
                'product_id': item['product_id'],
                'quantity_faktur': quantity_faktur,
                'net_price': item['net_price'],
                'pack_id': item['pack_id'],
                'line_id': item['line_id'],
                'origin_uom': item['uom'],
                'origin_qty': item['qty'],
                'total_ctn': total_ctn,
                'total_pcs': total_pcs
            }
            
            order_details_data.append(order_detail)
            valid_count += 1
            
            if (i + 1) % 5000 == 0:
                logger.info(f"Processed {i+1}/{len(outbound_data)} items (Valid: {valid_count}, Skipped: {skipped_count})")
                
        except Exception as e:
            logger.error(f"Error processing item {i+1}: {e}")
            skipped_count += 1
            continue
    
    logger.info(f"Processing complete: {valid_count} valid items, {skipped_count} skipped items")
    
    # Step 3: Batch insert data into order_detail_main
    logger.info("=== BATCH INSERTING TO DATABASE ===")
    inserted_count, final_skipped_count = insert_order_details_batch(logger, order_details_data)
    
    return inserted_count, final_skipped_count

def main():
    """Main function"""
    if len(sys.argv) != 4:
        print("Usage: python3 copy_order_details_optimized.py <start_date> <end_date> <warehouse_id>")
        print("Example: python3 copy_order_details_optimized.py 2025-01-01 2025-01-30 4512")
        sys.exit(1)
    
    start_date = sys.argv[1]
    end_date = sys.argv[2]
    warehouse_id = int(sys.argv[3])
    
    logger = setup_logging()
    
    try:
        start_time = datetime.now()
        inserted_count, skipped_count = copy_order_details_optimized(logger, start_date, end_date, warehouse_id)
        end_time = datetime.now()
        
        duration = end_time - start_time
        
        logger.info("\n" + "="*60)
        logger.info("OPTIMIZED COPY PROCESS SUMMARY:")
        logger.info(f"Date range: {start_date} to {end_date}")
        logger.info(f"Warehouse ID: {warehouse_id}")
        logger.info(f"Total inserted: {inserted_count}")
        logger.info(f"Total skipped: {skipped_count}")
        logger.info(f"Total duration: {duration}")
        
        if inserted_count > 0:
            logger.info("✅ Optimized order details copy process completed successfully!")
        else:
            logger.warning("⚠️ No order details were inserted")
            
    except Exception as e:
        logger.error(f"❌ Optimized copy process failed: {e}")

if __name__ == "__main__":
    main() 