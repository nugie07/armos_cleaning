#!/usr/bin/env python3
"""
Clean April 2025 data from Database B (safe - only removes April data)
"""

import os
import sys
import logging
import psycopg2
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
if not os.getenv('DB_B_HOST'):
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

def get_db_connection():
    """Get Database B connection"""
    return psycopg2.connect(
        host=os.getenv('DB_B_HOST'),
        port=os.getenv('DB_B_PORT'),
        database=os.getenv('DB_B_NAME'),
        user=os.getenv('DB_B_USER'),
        password=os.getenv('DB_B_PASSWORD')
    )

def clean_april_data(logger, warehouse_id):
    """Clean April 2025 data from Database B"""
    conn = get_db_connection()
    
    try:
        cursor = conn.cursor()
        
        logger.info(f"=== CLEANING APRIL 2025 DATA ===")
        logger.info(f"Warehouse ID: {warehouse_id}")
        
        # Check current data counts
        logger.info(f"\n=== CURRENT DATA COUNTS ===")
        
        cursor.execute("""
            SELECT COUNT(*) FROM order_main 
            WHERE warehouse_id = %s::VARCHAR 
            AND faktur_date >= '2025-04-01' 
            AND faktur_date <= '2025-04-30'
        """, (warehouse_id,))
        
        april_orders = cursor.fetchone()[0]
        logger.info(f"April 2025 orders in Database B: {april_orders}")
        
        cursor.execute("""
            SELECT COUNT(*) FROM order_detail_main odm
            JOIN order_main om ON odm.order_id = om.order_id
            WHERE om.warehouse_id = %s::VARCHAR 
            AND om.faktur_date >= '2025-04-01' 
            AND om.faktur_date <= '2025-04-30'
        """, (warehouse_id,))
        
        april_details = cursor.fetchone()[0]
        logger.info(f"April 2025 order details in Database B: {april_details}")
        
        # Check other months data (should remain untouched)
        cursor.execute("""
            SELECT COUNT(*) FROM order_main 
            WHERE warehouse_id = %s::VARCHAR 
            AND faktur_date < '2025-04-01'
        """, (warehouse_id,))
        
        other_orders = cursor.fetchone()[0]
        logger.info(f"Other months orders in Database B: {other_orders}")
        
        cursor.execute("""
            SELECT COUNT(*) FROM order_detail_main odm
            JOIN order_main om ON odm.order_id = om.order_id
            WHERE om.warehouse_id = %s::VARCHAR 
            AND om.faktur_date < '2025-04-01'
        """, (warehouse_id,))
        
        other_details = cursor.fetchone()[0]
        logger.info(f"Other months order details in Database B: {other_details}")
        
        # Confirm before deletion
        logger.info(f"\n=== CONFIRMATION REQUIRED ===")
        logger.info(f"This will delete:")
        logger.info(f"  - {april_orders} April 2025 orders")
        logger.info(f"  - {april_details} April 2025 order details")
        logger.info(f"But will preserve:")
        logger.info(f"  - {other_orders} orders from other months")
        logger.info(f"  - {other_details} order details from other months")
        
        # Delete April 2025 order details first (due to foreign key)
        logger.info(f"\n=== DELETING APRIL 2025 ORDER DETAILS ===")
        
        cursor.execute("""
            DELETE FROM order_detail_main 
            WHERE order_id IN (
                SELECT order_id FROM order_main 
                WHERE warehouse_id = %s::VARCHAR 
                AND faktur_date >= '2025-04-01' 
                AND faktur_date <= '2025-04-30'
            )
        """, (warehouse_id,))
        
        deleted_details = cursor.rowcount
        logger.info(f"Deleted {deleted_details} April 2025 order details")
        
        # Delete April 2025 orders
        logger.info(f"\n=== DELETING APRIL 2025 ORDERS ===")
        
        cursor.execute("""
            DELETE FROM order_main 
            WHERE warehouse_id = %s::VARCHAR 
            AND faktur_date >= '2025-04-01' 
            AND faktur_date <= '2025-04-30'
        """, (warehouse_id,))
        
        deleted_orders = cursor.rowcount
        logger.info(f"Deleted {deleted_orders} April 2025 orders")
        
        # Verify deletion
        logger.info(f"\n=== VERIFICATION ===")
        
        cursor.execute("""
            SELECT COUNT(*) FROM order_main 
            WHERE warehouse_id = %s::VARCHAR 
            AND faktur_date >= '2025-04-01' 
            AND faktur_date <= '2025-04-30'
        """, (warehouse_id,))
        
        remaining_april_orders = cursor.fetchone()[0]
        logger.info(f"Remaining April 2025 orders: {remaining_april_orders}")
        
        cursor.execute("""
            SELECT COUNT(*) FROM order_detail_main odm
            JOIN order_main om ON odm.order_id = om.order_id
            WHERE om.warehouse_id = %s::VARCHAR 
            AND om.faktur_date >= '2025-04-01' 
            AND om.faktur_date <= '2025-04-30'
        """, (warehouse_id,))
        
        remaining_april_details = cursor.fetchone()[0]
        logger.info(f"Remaining April 2025 order details: {remaining_april_details}")
        
        # Verify other months data is intact
        cursor.execute("""
            SELECT COUNT(*) FROM order_main 
            WHERE warehouse_id = %s::VARCHAR 
            AND faktur_date < '2025-04-01'
        """, (warehouse_id,))
        
        remaining_other_orders = cursor.fetchone()[0]
        logger.info(f"Other months orders (should be unchanged): {remaining_other_orders}")
        
        if remaining_other_orders == other_orders:
            logger.info(f"✅ Other months data preserved successfully")
        else:
            logger.error(f"❌ Other months data was affected!")
        
        # Commit the transaction
        conn.commit()
        logger.info(f"\n=== CLEANUP COMPLETED SUCCESSFULLY ===")
        
    except Exception as e:
        logger.error(f"Error cleaning April data: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

def main():
    """Main function"""
    logger = setup_logging()
    
    if len(sys.argv) != 2:
        logger.error("Usage: python3 clean_april_data.py <warehouse_id>")
        logger.error("Example: python3 clean_april_data.py 4512")
        sys.exit(1)
    
    warehouse_id = int(sys.argv[1])
    
    logger.info("=== APRIL 2025 DATA CLEANUP ===")
    logger.info(f"Warehouse ID: {warehouse_id}")
    logger.info(f"WARNING: This will delete ALL April 2025 data from Database B!")
    
    try:
        clean_april_data(logger, warehouse_id)
        logger.info("=== CLEANUP COMPLETED ===")
    except Exception as e:
        logger.error(f"Cleanup failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 