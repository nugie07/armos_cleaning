#!/usr/bin/env python3
"""
Debug script to investigate missing order details in February 2025
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

def debug_february_gap(logger, warehouse_id=4512):
    """Debug missing order details in February 2025"""
    conn_b = get_db_connection('B')
    
    try:
        cursor_b = conn_b.cursor()
        
        logger.info("=== DEBUGGING FEBRUARY 2025 GAP ===")
        logger.info(f"Warehouse ID: {warehouse_id}")
        
        # Step 1: Find orders without details in February 2025
        logger.info("1. Finding orders without details in February 2025...")
        query1 = """
        SELECT o.order_id, o.do_number, o.faktur_date, o.warehouse_id
        FROM order_main o
        LEFT JOIN order_detail_main od ON o.order_id = od.order_id
        WHERE o.faktur_date BETWEEN '2025-02-01' AND '2025-02-28'
        AND o.warehouse_id = %s
        AND od.order_id IS NULL
        ORDER BY o.faktur_date, o.order_id
        """
        
        cursor_b.execute(query1, (warehouse_id,))
        missing_orders = cursor_b.fetchall()
        
        logger.info(f"   ✓ Found {len(missing_orders)} orders without details")
        
        if not missing_orders:
            logger.info("   ✓ No missing orders found!")
            return
        
        # Show sample missing orders
        logger.info("   Sample missing orders:")
        for i, order in enumerate(missing_orders[:10]):
            logger.info(f"     {i+1}. Order ID: {order[0]}, DO: {order[1]}, Date: {order[2]}")
        
        if len(missing_orders) > 10:
            logger.info(f"     ... and {len(missing_orders) - 10} more orders")
        
        # Step 2: Check if these DO numbers have data in outbound_items
        logger.info("2. Checking outbound_items for missing DO numbers...")
        
        do_numbers = [order[1] for order in missing_orders if order[1]]
        
        if not do_numbers:
            logger.warning("   ⚠ No DO numbers found in missing orders")
            return
        
        # Check outbound_documents
        placeholders = ','.join(['%s'] * len(do_numbers))
        query2 = f"""
        SELECT odoc.document_reference, COUNT(oi.id) as item_count
        FROM outbound_documents odoc
        LEFT JOIN outbound_items oi ON odoc.id = oi.outbound_document_id
        WHERE odoc.document_reference IN ({placeholders})
        GROUP BY odoc.document_reference
        ORDER BY item_count DESC
        """
        
        cursor_b.execute(query2, do_numbers)
        outbound_results = cursor_b.fetchall()
        
        logger.info(f"   ✓ Found {len(outbound_results)} DO numbers with outbound data")
        
        # Categorize results
        do_with_items = []
        do_without_items = []
        
        for result in outbound_results:
            if result[1] > 0:
                do_with_items.append(result[0])
            else:
                do_without_items.append(result[0])
        
        logger.info(f"   ✓ DO numbers WITH items: {len(do_with_items)}")
        logger.info(f"   ⚠ DO numbers WITHOUT items: {len(do_without_items)}")
        
        # Show sample DO numbers with items
        if do_with_items:
            logger.info("   Sample DO numbers with items:")
            for i, do_num in enumerate(do_with_items[:10]):
                logger.info(f"     {i+1}. {do_num}")
            
            if len(do_with_items) > 10:
                logger.info(f"     ... and {len(do_with_items) - 10} more")
        
        # Step 3: Detailed analysis of DO numbers with items
        if do_with_items:
            logger.info("3. Detailed analysis of DO numbers with items...")
            
            # Get detailed info for first few DO numbers
            sample_do_numbers = do_with_items[:5]
            placeholders = ','.join(['%s'] * len(sample_do_numbers))
            
            query3 = f"""
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
                AND mp.warehouse_id = om.warehouse_id::varchar
            )
            WHERE odoc.document_reference IN ({placeholders})
            GROUP BY odoc.document_reference, om.order_id, om.faktur_date, om.warehouse_id
            ORDER BY odoc.document_reference
            """
            
            cursor_b.execute(query3, sample_do_numbers)
            detailed_results = cursor_b.fetchall()
            
            logger.info("   Detailed analysis:")
            for result in detailed_results:
                logger.info(f"     DO: {result[0]}, Order: {result[1]}, Date: {result[2]}, Items: {result[4]}, Products: {result[5]}")
        
        # Step 4: Summary
        logger.info("4. Summary Analysis...")
        logger.info(f"   Total missing orders: {len(missing_orders)}")
        logger.info(f"   DO numbers with outbound data: {len(do_with_items)}")
        logger.info(f"   DO numbers without outbound data: {len(do_without_items)}")
        
        if do_with_items:
            logger.info(f"   Potential candidates for copy: {len(do_with_items)} DO numbers")
            logger.info("   These DO numbers have data in outbound_items but no order_detail_main records")
        
        logger.info("=== DEBUG COMPLETE ===")
        
        # Return DO numbers with items for further investigation
        return do_with_items
        
    except Exception as e:
        logger.error(f"Error debugging February gap: {e}")
        return []
    finally:
        conn_b.close()

def main():
    """Main function"""
    logger = setup_logging()
    
    try:
        do_numbers_with_items = debug_february_gap(logger)
        
        if do_numbers_with_items:
            logger.info("\n" + "="*60)
            logger.info("DO NUMBERS WITH ITEMS (for further investigation):")
            for do_num in do_numbers_with_items:
                logger.info(f"  {do_num}")
            
            logger.info(f"\nTotal: {len(do_numbers_with_items)} DO numbers")
            logger.info("You can use these DO numbers with debug_missing_order_details.py")
            
    except Exception as e:
        logger.error(f"Debug process failed: {e}")

if __name__ == "__main__":
    main() 