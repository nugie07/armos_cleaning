#!/usr/bin/env python3
"""
Create tables in Database B with exact same structure as Database A
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

def get_db_connection(database='B'):
    """Get database connection"""
    if database == 'B':
        conn = psycopg2.connect(
            host=os.getenv('DB_B_HOST'),
            port=os.getenv('DB_B_PORT'),
            database=os.getenv('DB_B_NAME'),
            user=os.getenv('DB_B_USER'),
            password=os.getenv('DB_B_PASSWORD')
        )
    else:
        conn = psycopg2.connect(
            host=os.getenv('DB_A_HOST'),
            port=os.getenv('DB_A_PORT'),
            database=os.getenv('DB_A_NAME'),
            user=os.getenv('DB_A_USER'),
            password=os.getenv('DB_A_PASSWORD')
        )
    return conn

def create_order_main_table(logger):
    """Create order_main table with exact same structure as order table in DB A"""
    logger.info("=== CREATING ORDER_MAIN TABLE ===")
    
    conn = get_db_connection('B')
    try:
        cursor = conn.cursor()
        
        # Drop existing table if exists
        cursor.execute("DROP TABLE IF EXISTS order_main CASCADE")
        logger.info("Dropped existing order_main table")
        
        # Create order_main table with exact same structure as order table in DB A
        create_table_query = """
        CREATE TABLE order_main (
            order_id INTEGER PRIMARY KEY,  -- Changed from SERIAL to INTEGER for direct copy
            faktur_id VARCHAR NOT NULL,
            faktur_date DATE,
            delivery_date DATE,
            do_number VARCHAR,
            status VARCHAR DEFAULT 'new',  -- Simplified from order_status_enum
            skip_count SMALLINT DEFAULT 0,
            created_date DATE DEFAULT CURRENT_DATE NOT NULL,
            created_by VARCHAR NOT NULL,
            updated_date DATE DEFAULT CURRENT_DATE,
            updated_by VARCHAR,
            notes VARCHAR,
            customer_id INTEGER,
            warehouse_id INTEGER,
            delivery_type_id INTEGER,
            order_integration_id VARCHAR,
            origin_name VARCHAR,
            origin_address_1 VARCHAR,
            origin_address_2 VARCHAR,
            origin_city VARCHAR,
            origin_zipcode VARCHAR,
            origin_phone VARCHAR,
            origin_email VARCHAR,
            destination_name VARCHAR,
            destination_address_1 VARCHAR,
            destination_address_2 VARCHAR,
            destination_city VARCHAR,
            destination_zip_code VARCHAR,
            destination_phone VARCHAR,
            destination_email VARCHAR,
            client_id VARCHAR DEFAULT 'BBM',
            cancel_reason VARCHAR,
            rdo_integration_id VARCHAR,
            address_change BOOLEAN DEFAULT FALSE NOT NULL,
            divisi VARCHAR,
            pre_status VARCHAR  -- Simplified from order_pre_status_enum
        )
        """
        
        cursor.execute(create_table_query)
        conn.commit()
        logger.info("✅ order_main table created successfully")
        
        # Create indexes for better performance
        cursor.execute("CREATE INDEX idx_order_main_faktur_date ON order_main(faktur_date)")
        cursor.execute("CREATE INDEX idx_order_main_warehouse_id ON order_main(warehouse_id)")
        cursor.execute("CREATE INDEX idx_order_main_customer_id ON order_main(customer_id)")
        cursor.execute("CREATE INDEX idx_order_main_faktur_id ON order_main(faktur_id)")
        conn.commit()
        logger.info("✅ Indexes created for order_main table")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error creating order_main table: {e}")
        raise
    finally:
        conn.close()

def create_order_detail_main_table(logger):
    """Create order_detail_main table with exact same structure as order_detail table in DB A"""
    logger.info("=== CREATING ORDER_DETAIL_MAIN TABLE ===")
    
    conn = get_db_connection('B')
    try:
        cursor = conn.cursor()
        
        # Drop existing table if exists
        cursor.execute("DROP TABLE IF EXISTS order_detail_main CASCADE")
        logger.info("Dropped existing order_detail_main table")
        
        # Create order_detail_main table with exact same structure as order_detail table in DB A
        create_table_query = """
        CREATE TABLE order_detail_main (
            order_detail_id SERIAL PRIMARY KEY,  -- Keep auto-increment for this
            quantity_faktur INTEGER,
            net_price INTEGER,
            quantity_wms INTEGER,
            quantity_delivery INTEGER DEFAULT 0,
            quantity_loading INTEGER DEFAULT 0,
            quantity_unloading INTEGER DEFAULT 0,
            status VARCHAR,  -- Simplified from order_detail_status_enum
            cancel_reason VARCHAR,
            notes VARCHAR,
            order_id INTEGER NOT NULL,
            product_id INTEGER,
            unit_id INTEGER,
            pack_id VARCHAR,
            line_id VARCHAR,
            unloading_latitude VARCHAR,
            unloading_longitude VARCHAR,
            origin_uom VARCHAR,
            origin_qty INTEGER DEFAULT 0,
            total_ctn INTEGER DEFAULT 0,
            total_pcs INTEGER DEFAULT 0,
            FOREIGN KEY (order_id) REFERENCES order_main(order_id) ON DELETE CASCADE
        )
        """
        
        cursor.execute(create_table_query)
        conn.commit()
        logger.info("✅ order_detail_main table created successfully")
        
        # Create indexes for better performance
        cursor.execute("CREATE INDEX idx_order_detail_main_order_id ON order_detail_main(order_id)")
        cursor.execute("CREATE INDEX idx_order_detail_main_product_id ON order_detail_main(product_id)")
        cursor.execute("CREATE INDEX idx_order_detail_main_line_id ON order_detail_main(line_id)")
        conn.commit()
        logger.info("✅ Indexes created for order_detail_main table")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error creating order_detail_main table: {e}")
        raise
    finally:
        conn.close()

def verify_table_structure(logger):
    """Verify that tables were created with correct structure"""
    logger.info("=== VERIFYING TABLE STRUCTURE ===")
    
    conn_b = get_db_connection('B')
    conn_a = get_db_connection('A')
    
    try:
        cursor_b = conn_b.cursor()
        cursor_a = conn_a.cursor()
        
        # Check order_main structure
        cursor_b.execute("""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns 
            WHERE table_name = 'order_main' 
            ORDER BY ordinal_position
        """)
        order_main_columns = cursor_b.fetchall()
        
        logger.info("order_main table structure:")
        for col in order_main_columns:
            logger.info(f"  {col[0]}: {col[1]} (nullable: {col[2]}, default: {col[3]})")
        
        # Check order_detail_main structure
        cursor_b.execute("""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns 
            WHERE table_name = 'order_detail_main' 
            ORDER BY ordinal_position
        """)
        order_detail_columns = cursor_b.fetchall()
        
        logger.info("order_detail_main table structure:")
        for col in order_detail_columns:
            logger.info(f"  {col[0]}: {col[1]} (nullable: {col[2]}, default: {col[3]})")
        
        # Check table counts
        cursor_b.execute("SELECT COUNT(*) FROM order_main")
        order_count = cursor_b.fetchone()[0]
        
        cursor_b.execute("SELECT COUNT(*) FROM order_detail_main")
        detail_count = cursor_b.fetchone()[0]
        
        logger.info(f"✅ Tables created successfully - order_main: {order_count} records, order_detail_main: {detail_count} records")
        
    except Exception as e:
        logger.error(f"Error verifying table structure: {e}")
        raise
    finally:
        conn_b.close()
        conn_a.close()

def main():
    """Main function"""
    logger = setup_logging()
    
    try:
        logger.info("=== CREATING TABLES WITH EXACT STRUCTURE ===")
        logger.info("Creating tables in Database B with same structure as Database A")
        
        # Step 1: Create order_main table
        create_order_main_table(logger)
        
        # Step 2: Create order_detail_main table
        create_order_detail_main_table(logger)
        
        # Step 3: Verify table structure
        verify_table_structure(logger)
        
        logger.info("=== ALL TABLES CREATED SUCCESSFULLY ===")
        logger.info("Tables are ready for data copying from Database A")
        
    except Exception as e:
        logger.error(f"Script failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 