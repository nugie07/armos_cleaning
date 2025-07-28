#!/usr/bin/env python3
"""
Debug script to investigate missing order details
"""

import os
import sys
import psycopg2
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv('.env')

def setup_logging():
    """Setup logging configuration"""
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
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

def debug_specific_do_number(logger, do_number):
    """Debug specific DO number to see why it's not being processed"""
    conn_b = get_db_connection('B')
    
    try:
        cursor_b = conn_b.cursor()
        
        logger.info(f"=== DEBUGGING DO NUMBER: {do_number} ===")
        
        # Step 1: Check outbound_documents
        logger.info("1. Checking outbound_documents...")
        query1 = """
        SELECT id, document_reference, create_date 
        FROM outbound_documents 
        WHERE document_reference = %s
        """
        cursor_b.execute(query1, (do_number,))
        doc_result = cursor_b.fetchone()
        
        if doc_result:
            logger.info(f"   ✓ Found in outbound_documents: id={doc_result[0]}, created_date={doc_result[2]}")
            doc_id = doc_result[0]
        else:
            logger.error(f"   ✗ NOT FOUND in outbound_documents")
            return
        
        # Step 2: Check outbound_items
        logger.info("2. Checking outbound_items...")
        query2 = """
        SELECT COUNT(*) as item_count, 
               MIN(product_id) as sample_product_id,
               MIN(pack_id) as sample_pack_id
        FROM outbound_items 
        WHERE outbound_document_id = %s
        """
        cursor_b.execute(query2, (doc_id,))
        item_result = cursor_b.fetchone()
        
        if item_result and item_result[0] > 0:
            logger.info(f"   ✓ Found {item_result[0]} items in outbound_items")
            logger.info(f"   ✓ Sample product_id: {item_result[1]}, pack_id: {item_result[2]}")
        else:
            logger.error(f"   ✗ NO ITEMS found in outbound_items")
            return
        
        # Step 3: Check order_main
        logger.info("3. Checking order_main...")
        query3 = """
        SELECT order_id, faktur_date, warehouse_id, do_number
        FROM order_main 
        WHERE do_number = %s
        """
        cursor_b.execute(query3, (do_number,))
        order_result = cursor_b.fetchone()
        
        if order_result:
            logger.info(f"   ✓ Found in order_main: order_id={order_result[0]}, faktur_date={order_result[1]}, warehouse_id={order_result[2]}")
            order_id = order_result[0]
            faktur_date = order_result[1]
            warehouse_id = order_result[2]
        else:
            logger.error(f"   ✗ NOT FOUND in order_main")
            return
        
        # Step 4: Check mst_product_main for sample product
        logger.info("4. Checking mst_product_main...")
        query4 = """
        SELECT COUNT(*) as product_count
        FROM mst_product_main 
        WHERE sku = %s AND pack_id = %s AND warehouse_id = %s
        """
        cursor_b.execute(query4, (item_result[1], item_result[2], str(warehouse_id)))
        product_result = cursor_b.fetchone()
        
        if product_result and product_result[0] > 0:
            logger.info(f"   ✓ Found {product_result[0]} matching products in mst_product_main")
        else:
            logger.warning(f"   ⚠ NO MATCHING PRODUCTS in mst_product_main")
            logger.info(f"   Trying fallback lookup...")
            
            # Fallback lookup
            query4b = """
            SELECT COUNT(*) as product_count
            FROM mst_product_main 
            WHERE sku = %s AND warehouse_id = %s
            """
            cursor_b.execute(query4b, (item_result[1], str(warehouse_id)))
            product_fallback = cursor_b.fetchone()
            
            if product_fallback and product_fallback[0] > 0:
                logger.info(f"   ✓ Found {product_fallback[0]} products with fallback lookup")
            else:
                logger.error(f"   ✗ NO PRODUCTS found even with fallback")
        
        # Step 5: Check if already exists in order_detail_main
        logger.info("5. Checking order_detail_main...")
        query5 = """
        SELECT COUNT(*) as detail_count
        FROM order_detail_main 
        WHERE order_id = %s
        """
        cursor_b.execute(query5, (order_id,))
        detail_result = cursor_b.fetchone()
        
        if detail_result and detail_result[0] > 0:
            logger.info(f"   ✓ Already has {detail_result[0]} records in order_detail_main")
        else:
            logger.info(f"   ⚠ NO RECORDS in order_detail_main for this order_id")
        
        # Step 6: Test the full JOIN query
        logger.info("6. Testing full JOIN query...")
        query6 = """
        SELECT 
            oi.id,
            oi.product_id as sku,
            oi.qty,
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
        WHERE odoc.document_reference = %s
        """
        
        cursor_b.execute(query6, (str(warehouse_id), do_number))
        join_results = cursor_b.fetchall()
        
        logger.info(f"   ✓ JOIN query returned {len(join_results)} records")
        
        if join_results:
            # Show sample record
            sample = join_results[0]
            logger.info(f"   Sample record:")
            logger.info(f"     - product_id: {sample[1]}")
            logger.info(f"     - mst_product_id: {sample[12]}")
            logger.info(f"     - order_id: {sample[8]}")
            logger.info(f"     - warehouse_id: {sample[11]}")
        
        logger.info("=== DEBUG COMPLETE ===")
        
    except Exception as e:
        logger.error(f"Error debugging DO number {do_number}: {e}")
    finally:
        conn_b.close()

def main():
    """Main function"""
    if len(sys.argv) != 2:
        print("Usage: python3 debug_missing_order_details.py <do_number>")
        print("Example: python3 debug_missing_order_details.py B10SI2501-2722")
        sys.exit(1)
    
    do_number = sys.argv[1]
    logger = setup_logging()
    
    try:
        debug_specific_do_number(logger, do_number)
    except Exception as e:
        logger.error(f"Debug process failed: {e}")

if __name__ == "__main__":
    main() 