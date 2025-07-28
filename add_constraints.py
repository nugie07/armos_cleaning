#!/usr/bin/env python3
"""
Add unique constraints to order_detail_main table for UPSERT functionality
"""

import os
import sys
import logging
import psycopg2
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

def get_db_connection():
    """Get database connection to DB B"""
    conn = psycopg2.connect(
        host=os.getenv('DB_B_HOST'),
        port=os.getenv('DB_B_PORT'),
        database=os.getenv('DB_B_NAME'),
        user=os.getenv('DB_B_USER'),
        password=os.getenv('DB_B_PASSWORD')
    )
    return conn

def add_constraints(logger):
    """Add unique constraints to order_detail_main table"""
    logger.info("=== ADDING CONSTRAINTS TO ORDER_DETAIL_MAIN ===")
    
    conn = get_db_connection()
    
    try:
        cursor = conn.cursor()
        
        # Check if constraint already exists
        cursor.execute("""
            SELECT constraint_name 
            FROM information_schema.table_constraints 
            WHERE table_name = 'order_detail_main' 
            AND constraint_type = 'UNIQUE'
            AND constraint_name LIKE '%order_product_line%'
        """)
        
        existing_constraints = cursor.fetchall()
        
        if existing_constraints:
            logger.info("Unique constraint already exists. Skipping...")
            return
        
        # Add unique constraint for UPSERT functionality
        logger.info("Adding unique constraint on (order_id, product_id, line_id)...")
        
        add_constraint_query = """
        ALTER TABLE order_detail_main 
        ADD CONSTRAINT order_detail_main_order_product_line_unique 
        UNIQUE (order_id, product_id, line_id)
        """
        
        cursor.execute(add_constraint_query)
        conn.commit()
        
        logger.info("✅ Unique constraint added successfully!")
        
        # Verify the constraint was added
        cursor.execute("""
            SELECT constraint_name 
            FROM information_schema.table_constraints 
            WHERE table_name = 'order_detail_main' 
            AND constraint_type = 'UNIQUE'
            AND constraint_name LIKE '%order_product_line%'
        """)
        
        constraints = cursor.fetchall()
        if constraints:
            logger.info(f"✅ Constraint verified: {constraints[0][0]}")
        else:
            logger.warning("⚠️ Constraint not found after creation")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error adding constraints: {e}")
        raise
    finally:
        conn.close()

def main():
    """Main function"""
    logger = setup_logging()
    
    try:
        add_constraints(logger)
        logger.info("=== CONSTRAINT ADDITION COMPLETED ===")
        
    except Exception as e:
        logger.error(f"Script failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 