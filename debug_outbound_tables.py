#!/usr/bin/env python3
"""
Debug script to check outbound tables structure and data
"""

import os
import psycopg2
import argparse
import logging
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv('.env')
load_dotenv('config.env')

def setup_logging():
    """Setup logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def get_db_connection():
    """Get database connection to Database B"""
    try:
        conn = psycopg2.connect(
            host=os.getenv('DB_B_HOST'),
            port=os.getenv('DB_B_PORT'),
            database=os.getenv('DB_B_NAME'),
            user=os.getenv('DB_B_USER'),
            password=os.getenv('DB_B_PASSWORD')
        )
        return conn
    except Exception as e:
        logging.error(f"Failed to connect to database B: {str(e)}")
        raise

def check_table_structure(db_conn, table_name, logger):
    """Check table structure"""
    try:
        query = """
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns 
        WHERE table_name = %s 
        ORDER BY ordinal_position
        """
        
        with db_conn.cursor() as cursor:
            cursor.execute(query, (table_name,))
            columns = cursor.fetchall()
            
        logger.info(f"=== Table {table_name} Structure ===")
        for col in columns:
            logger.info(f"  {col[0]}: {col[1]} ({'NULL' if col[2] == 'YES' else 'NOT NULL'})")
            
        return len(columns) > 0
        
    except Exception as e:
        logger.error(f"Error checking table {table_name}: {str(e)}")
        return False

def check_table_data(db_conn, table_name, logger, limit=5):
    """Check table data sample"""
    try:
        count_query = f"SELECT COUNT(*) FROM {table_name}"
        sample_query = f"SELECT * FROM {table_name} LIMIT {limit}"
        
        with db_conn.cursor() as cursor:
            cursor.execute(count_query)
            count = cursor.fetchone()[0]
            
            cursor.execute(sample_query)
            samples = cursor.fetchall()
            
        logger.info(f"=== Table {table_name} Data ===")
        logger.info(f"Total records: {count}")
        
        if samples:
            logger.info(f"Sample records (first {len(samples)}):")
            for i, sample in enumerate(samples, 1):
                logger.info(f"  Record {i}: {sample}")
                
        return count
        
    except Exception as e:
        logger.error(f"Error checking data for table {table_name}: {str(e)}")
        return 0

def check_orders_without_details(db_conn, warehouse_id, start_date, end_date, logger):
    """Check orders without order_detail_main"""
    try:
        query = """
        SELECT COUNT(*) as total_orders,
               COUNT(CASE WHEN odm.order_id IS NULL THEN 1 END) as orders_without_details
        FROM order_main om
        LEFT JOIN order_detail_main odm ON om.id = odm.order_id
        WHERE om.warehouse_id = %s 
        AND om.faktur_date >= %s 
        AND om.faktur_date <= %s
        """
        
        with db_conn.cursor() as cursor:
            cursor.execute(query, (warehouse_id, start_date, end_date))
            result = cursor.fetchone()
            
        logger.info(f"=== Orders Analysis ===")
        logger.info(f"Total orders in date range: {result[0]}")
        logger.info(f"Orders without details: {result[1]}")
        logger.info(f"Orders with details: {result[0] - result[1]}")
        
        return result[1]  # Return count of orders without details
        
    except Exception as e:
        logger.error(f"Error checking orders without details: {str(e)}")
        return 0

def check_outbound_relationships(db_conn, warehouse_id, start_date, end_date, logger):
    """Check outbound table relationships"""
    try:
        # Get sample faktur_ids from order_main
        sample_query = """
        SELECT faktur_id 
        FROM order_main 
        WHERE warehouse_id = %s 
        AND faktur_date >= %s 
        AND faktur_date <= %s
        LIMIT 5
        """
        
        with db_conn.cursor() as cursor:
            cursor.execute(sample_query, (warehouse_id, start_date, end_date))
            faktur_ids = [row[0] for row in cursor.fetchall()]
            
        if not faktur_ids:
            logger.warning("No faktur_ids found in order_main")
            return
            
        logger.info(f"=== Outbound Relationships Check ===")
        logger.info(f"Sample faktur_ids: {faktur_ids}")
        
        # Check outbound_documents
        doc_query = """
        SELECT document_reference, COUNT(*) as doc_count
        FROM outbound_documents 
        WHERE document_reference = ANY(%s)
        GROUP BY document_reference
        """
        
        with db_conn.cursor() as cursor:
            cursor.execute(doc_query, (faktur_ids,))
            doc_results = cursor.fetchall()
            
        logger.info(f"Outbound documents found: {len(doc_results)}")
        for doc_ref, count in doc_results:
            logger.info(f"  {doc_ref}: {count} document(s)")
            
        # Check outbound_items
        if doc_results:
            doc_refs = [doc[0] for doc in doc_results]
            item_query = """
            SELECT od.document_reference, COUNT(oi.id) as item_count
            FROM outbound_documents od
            LEFT JOIN outbound_items oi ON od.id = oi.outbound_document_id
            WHERE od.document_reference = ANY(%s)
            GROUP BY od.document_reference
            """
            
            with db_conn.cursor() as cursor:
                cursor.execute(item_query, (doc_refs,))
                item_results = cursor.fetchall()
                
            logger.info(f"Outbound items found:")
            for doc_ref, count in item_results:
                logger.info(f"  {doc_ref}: {count} item(s)")
                
    except Exception as e:
        logger.error(f"Error checking outbound relationships: {str(e)}")

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Debug outbound tables')
    parser.add_argument('--warehouse-id', required=True, type=str,
                       help='Warehouse ID to filter data')
    parser.add_argument('--start-date', required=True, type=str, 
                       help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', required=True, type=str, 
                       help='End date (YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    # Validate date format
    try:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()
    except ValueError as e:
        print(f"Invalid date format: {e}")
        return 1
    
    logger = setup_logging()
    
    logger.info("=== Outbound Tables Debug ===")
    logger.info(f"Warehouse ID: {args.warehouse_id}")
    logger.info(f"Date Range: {start_date} to {end_date}")
    
    db_conn = None
    
    try:
        # Connect to database B
        db_conn = get_db_connection()
        logger.info("✓ Connected to Database B successfully")
        
        # Check table structures
        tables = ['order_main', 'order_detail_main', 'outbound_documents', 'outbound_items', 'outbound_conversions']
        
        for table in tables:
            exists = check_table_structure(db_conn, table, logger)
            if exists:
                check_table_data(db_conn, table, logger)
            logger.info("")
        
        # Check orders without details
        orders_without_details = check_orders_without_details(db_conn, args.warehouse_id, start_date, end_date, logger)
        logger.info("")
        
        # Check outbound relationships
        check_outbound_relationships(db_conn, args.warehouse_id, start_date, end_date, logger)
        
        logger.info("=== Debug Summary ===")
        logger.info(f"Orders without details: {orders_without_details}")
        
        return 0
        
    except Exception as e:
        logger.error(f"Debug failed: {str(e)}")
        return 1
        
    finally:
        if db_conn:
            db_conn.close()
            logger.info("Database connection closed")

if __name__ == "__main__":
    exit(main()) 