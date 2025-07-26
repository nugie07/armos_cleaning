#!/usr/bin/env python3
"""
Script to create tables in Database B
Creates: order_main, order_detail_main, mst_product_main
"""

import os
import sys
import logging
import psycopg2
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables - try .env first, then config.env
load_dotenv('.env')
load_dotenv('config.env')

# Configure logging
def setup_logging():
    """Setup logging configuration"""
    log_dir = os.path.dirname(os.getenv('LOG_FILE', './logs/database_operations.log'))
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    logging.basicConfig(
        level=getattr(logging, os.getenv('LOG_LEVEL', 'INFO')),
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(os.getenv('LOG_FILE', './logs/database_operations.log')),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

def get_db_connection(database_type):
    """Get database connection based on type (A or B)"""
    try:
        if database_type.upper() == 'B':
            conn = psycopg2.connect(
                host=os.getenv('DB_B_HOST'),
                port=os.getenv('DB_B_PORT'),
                database=os.getenv('DB_B_NAME'),
                user=os.getenv('DB_B_USER'),
                password=os.getenv('DB_B_PASSWORD')
            )
        else:
            raise ValueError(f"Unsupported database type: {database_type}")
        
        return conn
    except Exception as e:
        logging.error(f"Failed to connect to database {database_type}: {str(e)}")
        raise

def create_order_main_table(conn):
    """Create order_main table"""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS order_main (
        order_id SERIAL PRIMARY KEY,
        faktur_id VARCHAR(255),
        faktur_date DATE,
        delivery_date DATE,
        do_number VARCHAR(255),
        status VARCHAR(50),
        skip_count INTEGER DEFAULT 0,
        created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        created_by VARCHAR(255),
        updated_date TIMESTAMP,
        updated_by VARCHAR(255),
        notes TEXT,
        customer_id INTEGER,
        warehouse_id INTEGER,
        delivery_type_id INTEGER,
        order_integration_id VARCHAR(255),
        origin_name VARCHAR(255),
        origin_address_1 TEXT,
        origin_address_2 TEXT,
        origin_city VARCHAR(100),
        origin_zipcode VARCHAR(20),
        origin_phone VARCHAR(50),
        origin_email VARCHAR(255),
        destination_name VARCHAR(255),
        destination_address_1 TEXT,
        destination_address_2 TEXT,
        destination_city VARCHAR(100),
        destination_zip_code VARCHAR(20),
        destination_phone VARCHAR(50),
        destination_email VARCHAR(255),
        client_id INTEGER,
        cancel_reason TEXT,
        rdo_integration_id VARCHAR(255),
        address_change BOOLEAN DEFAULT FALSE,
        divisi VARCHAR(100),
        pre_status VARCHAR(50)
    );
    """
    
    try:
        with conn.cursor() as cursor:
            cursor.execute(create_table_sql)
            conn.commit()
            logging.info("Table order_main created successfully")
    except Exception as e:
        conn.rollback()
        logging.error(f"Failed to create order_main table: {str(e)}")
        raise

def create_order_detail_main_table(conn):
    """Create order_detail_main table"""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS order_detail_main (
        order_detail_id SERIAL PRIMARY KEY,
        quantity_faktur DECIMAL(10,2),
        net_price DECIMAL(15,2),
        quantity_wms DECIMAL(10,2),
        quantity_delivery DECIMAL(10,2),
        quantity_loading DECIMAL(10,2),
        quantity_unloading DECIMAL(10,2),
        status VARCHAR(50),
        cancel_reason TEXT,
        notes TEXT,
        order_id INTEGER REFERENCES order_main(order_id),
        product_id INTEGER,
        unit_id INTEGER,
        pack_id INTEGER,
        line_id INTEGER,
        unloading_latitude DECIMAL(10,8),
        unloading_longitude DECIMAL(11,8),
        origin_uom VARCHAR(50),
        origin_qty DECIMAL(10,2),
        total_ctn INTEGER,
        total_pcs INTEGER
    );
    """
    
    try:
        with conn.cursor() as cursor:
            cursor.execute(create_table_sql)
            conn.commit()
            logging.info("Table order_detail_main created successfully")
    except Exception as e:
        conn.rollback()
        logging.error(f"Failed to create order_detail_main table: {str(e)}")
        raise

def create_mst_product_main_table(conn):
    """Create mst_product_main table"""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS mst_product_main (
        mst_product_id SERIAL PRIMARY KEY,
        sku VARCHAR(255) UNIQUE,
        height DECIMAL(10,2),
        width DECIMAL(10,2),
        length DECIMAL(10,2),
        name VARCHAR(255),
        price DECIMAL(15,2),
        type_product_id INTEGER,
        qty DECIMAL(10,2),
        volume DECIMAL(10,2),
        weight DECIMAL(10,2),
        base_uom VARCHAR(50),
        pack_id INTEGER,
        warehouse_id INTEGER,
        synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        allocated_qty DECIMAL(10,2) DEFAULT 0,
        available_qty DECIMAL(10,2) DEFAULT 0
    );
    """
    
    try:
        with conn.cursor() as cursor:
            cursor.execute(create_table_sql)
            conn.commit()
            logging.info("Table mst_product_main created successfully")
    except Exception as e:
        conn.rollback()
        logging.error(f"Failed to create mst_product_main table: {str(e)}")
        raise

def main():
    """Main function to create all tables"""
    logger = setup_logging()
    
    logger.info("Starting table creation process...")
    
    try:
        # Connect to database B
        conn = get_db_connection('B')
        logger.info("Connected to database B successfully")
        
        # Create tables
        create_order_main_table(conn)
        create_order_detail_main_table(conn)
        create_mst_product_main_table(conn)
        
        logger.info("All tables created successfully!")
        
    except Exception as e:
        logger.error(f"Table creation failed: {str(e)}")
        sys.exit(1)
    finally:
        if 'conn' in locals():
            conn.close()
            logger.info("Database connection closed")

if __name__ == "__main__":
    main() 