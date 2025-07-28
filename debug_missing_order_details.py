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

def debug_multiple_do_numbers(logger, do_numbers):
    """Debug multiple DO numbers to see why they're not being processed"""
    conn_b = get_db_connection('B')
    
    try:
        cursor_b = conn_b.cursor()
        
        logger.info(f"=== DEBUGGING {len(do_numbers)} DO NUMBERS ===")
        
        # Step 1: Check outbound_documents
        logger.info("1. Checking outbound_documents...")
        placeholders = ','.join(['%s'] * len(do_numbers))
        query1 = f"""
        SELECT id, document_reference, create_date 
        FROM outbound_documents 
        WHERE document_reference IN ({placeholders})
        """
        cursor_b.execute(query1, do_numbers)
        doc_results = cursor_b.fetchall()
        
        logger.info(f"   ✓ Found {len(doc_results)} documents in outbound_documents")
        
        if not doc_results:
            logger.error(f"   ✗ NO DOCUMENTS found in outbound_documents")
            return
        
        # Get document IDs
        doc_ids = [row[0] for row in doc_results]
        found_do_numbers = [row[1] for row in doc_results]
        
        # Step 2: Check outbound_items
        logger.info("2. Checking outbound_items...")
        placeholders = ','.join(['%s'] * len(doc_ids))
        query2 = f"""
        SELECT outbound_document_id, COUNT(*) as item_count
        FROM outbound_items 
        WHERE outbound_document_id IN ({placeholders})
        GROUP BY outbound_document_id
        """
        cursor_b.execute(query2, doc_ids)
        item_results = cursor_b.fetchall()
        
        total_items = sum(row[1] for row in item_results)
        logger.info(f"   ✓ Found {total_items} total items in outbound_items")
        logger.info(f"   ✓ Items per document: {dict(item_results)}")
        
        # Step 3: Check order_main
        logger.info("3. Checking order_main...")
        placeholders = ','.join(['%s'] * len(found_do_numbers))
        query3 = f"""
        SELECT do_number, order_id, faktur_date, warehouse_id
        FROM order_main 
        WHERE do_number IN ({placeholders})
        """
        cursor_b.execute(query3, found_do_numbers)
        order_results = cursor_b.fetchall()
        
        logger.info(f"   ✓ Found {len(order_results)} orders in order_main")
        
        if order_results:
            # Show sample orders
            for i, order in enumerate(order_results[:5]):  # Show first 5
                logger.info(f"   Sample {i+1}: do_number={order[0]}, order_id={order[1]}, faktur_date={order[2]}, warehouse_id={order[3]}")
            
            if len(order_results) > 5:
                logger.info(f"   ... and {len(order_results) - 5} more orders")
        
        # Step 4: Check order_detail_main
        logger.info("4. Checking order_detail_main...")
        if order_results:
            order_ids = [row[1] for row in order_results]
            placeholders = ','.join(['%s'] * len(order_ids))
            query4 = f"""
            SELECT order_id, COUNT(*) as detail_count
            FROM order_detail_main 
            WHERE order_id IN ({placeholders})
            GROUP BY order_id
            """
            cursor_b.execute(query4, order_ids)
            detail_results = cursor_b.fetchall()
            
            orders_with_details = len(detail_results)
            orders_without_details = len(order_ids) - orders_with_details
            
            logger.info(f"   ✓ Orders with details: {orders_with_details}")
            logger.info(f"   ⚠ Orders without details: {orders_without_details}")
            
            if detail_results:
                total_details = sum(row[1] for row in detail_results)
                logger.info(f"   ✓ Total detail records: {total_details}")
        
        # Step 5: Test the full JOIN query
        logger.info("5. Testing full JOIN query...")
        placeholders = ','.join(['%s'] * len(found_do_numbers))
        query5 = f"""
        SELECT 
            odoc.document_reference,
            om.order_id,
            om.faktur_date,
            om.warehouse_id,
            COUNT(oi.id) as item_count,
            COUNT(mp.mst_product_id) as product_matches
        FROM outbound_documents odoc
        LEFT JOIN outbound_items oi ON odoc.id = oi.outbound_document_id
        LEFT JOIN order_main om ON om.do_number = odoc.document_reference
        LEFT JOIN mst_product_main mp ON (
            mp.sku = oi.product_id 
            AND mp.pack_id = oi.pack_id 
            AND mp.warehouse_id = om.warehouse_id
        )
        WHERE odoc.document_reference IN ({placeholders})
        GROUP BY odoc.document_reference, om.order_id, om.faktur_date, om.warehouse_id
        ORDER BY odoc.document_reference
        """
        
        cursor_b.execute(query5, found_do_numbers)
        join_results = cursor_b.fetchall()
        
        logger.info(f"   ✓ JOIN query returned {len(join_results)} records")
        
        if join_results:
            # Show sample records
            logger.info(f"   Sample JOIN results:")
            for i, result in enumerate(join_results[:10]):  # Show first 10
                logger.info(f"     {i+1}. DO: {result[0]}, Order: {result[1]}, Date: {result[2]}, Items: {result[4]}, Products: {result[5]}")
            
            if len(join_results) > 10:
                logger.info(f"     ... and {len(join_results) - 10} more records")
        
        # Step 6: Summary
        logger.info("6. Summary Analysis...")
        missing_do_numbers = set(do_numbers) - set(found_do_numbers)
        if missing_do_numbers:
            logger.warning(f"   ⚠ DO numbers NOT found in outbound_documents: {len(missing_do_numbers)}")
            logger.warning(f"   Missing: {list(missing_do_numbers)[:5]}...")  # Show first 5
        
        logger.info("=== DEBUG COMPLETE ===")
        
    except Exception as e:
        logger.error(f"Error debugging DO numbers: {e}")
    finally:
        conn_b.close()

def debug_specific_do_number(logger, do_number):
    """Debug specific DO number to see why it's not being processed"""
    debug_multiple_do_numbers(logger, [do_number])

def main():
    """Main function"""
    if len(sys.argv) < 2:
        print("Usage: python3 debug_missing_order_details.py <do_number1> [do_number2] ...")
        print("Example: python3 debug_missing_order_details.py B10SI2501-2722")
        print("Example: python3 debug_missing_order_details.py B10SI2502-0936 B10SI2502-1063")
        sys.exit(1)
    
    do_numbers = sys.argv[1:]
    logger = setup_logging()
    
    try:
        if len(do_numbers) == 1:
            debug_specific_do_number(logger, do_numbers[0])
        else:
            debug_multiple_do_numbers(logger, do_numbers)
    except Exception as e:
        logger.error(f"Debug process failed: {e}")

if __name__ == "__main__":
    main() 