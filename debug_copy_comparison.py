#!/usr/bin/env python3
"""
Debug script to compare data between Database A and Database B
Check if copy process is working correctly
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

def get_db_connection(database_type):
    """Get database connection"""
    try:
        if database_type.upper() == 'A':
            conn = psycopg2.connect(
                host=os.getenv('DB_A_HOST'),
                port=os.getenv('DB_A_PORT'),
                database=os.getenv('DB_A_NAME'),
                user=os.getenv('DB_A_USER'),
                password=os.getenv('DB_A_PASSWORD')
            )
        elif database_type.upper() == 'B':
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

def check_orders_comparison(db_a_conn, db_b_conn, warehouse_id, start_date, end_date, logger):
    """Compare orders between Database A and Database B"""
    try:
        # Check Database A (Source)
        query_a = """
        SELECT COUNT(*) as total_orders,
               COUNT(DISTINCT faktur_id) as unique_faktur_ids,
               MIN(faktur_date) as min_date,
               MAX(faktur_date) as max_date
        FROM "order"
        WHERE faktur_date >= %s 
        AND faktur_date <= %s 
        AND warehouse_id = %s
        """
        
        with db_a_conn.cursor() as cursor:
            cursor.execute(query_a, (start_date, end_date, warehouse_id))
            result_a = cursor.fetchone()
            
        # Check Database B (Target)
        query_b = """
        SELECT COUNT(*) as total_orders,
               COUNT(DISTINCT faktur_id) as unique_faktur_ids,
               MIN(faktur_date) as min_date,
               MAX(faktur_date) as max_date
        FROM order_main
        WHERE faktur_date >= %s 
        AND faktur_date <= %s 
        AND warehouse_id = %s
        """
        
        with db_b_conn.cursor() as cursor:
            cursor.execute(query_b, (start_date, end_date, warehouse_id))
            result_b = cursor.fetchone()
            
        logger.info("=== Orders Comparison ===")
        logger.info(f"Database A (Source):")
        logger.info(f"  Total orders: {result_a[0]}")
        logger.info(f"  Unique faktur_ids: {result_a[1]}")
        logger.info(f"  Date range: {result_a[2]} to {result_a[3]}")
        
        logger.info(f"Database B (Target):")
        logger.info(f"  Total orders: {result_b[0]}")
        logger.info(f"  Unique faktur_ids: {result_b[1]}")
        logger.info(f"  Date range: {result_b[2]} to {result_b[3]}")
        
        # Check differences
        if result_a[0] != result_b[0]:
            logger.warning(f"❌ Order count mismatch: A={result_a[0]}, B={result_b[0]}")
            
            # Find missing orders
            missing_query = """
            SELECT o.faktur_id, o.faktur_date, o.customer_id
            FROM "order" o
            LEFT JOIN order_main om ON o.faktur_id = om.faktur_id 
                AND o.faktur_date = om.faktur_date 
                AND o.customer_id::VARCHAR = om.customer_id
            WHERE o.faktur_date >= %s 
            AND o.faktur_date <= %s 
            AND o.warehouse_id = %s
            AND om.order_id IS NULL
            LIMIT 10
            """
            
            with db_a_conn.cursor() as cursor:
                cursor.execute(missing_query, (start_date, end_date, warehouse_id))
                missing_orders = cursor.fetchall()
                
            if missing_orders:
                logger.warning(f"Sample missing orders (first 10):")
                for order in missing_orders:
                    logger.warning(f"  faktur_id: {order[0]}, date: {order[1]}, customer: {order[2]}")
        else:
            logger.info("✅ Order counts match!")
            
        return result_a[0], result_b[0]
        
    except Exception as e:
        logger.error(f"Error comparing orders: {str(e)}")
        return 0, 0

def check_order_details_comparison(db_a_conn, db_b_conn, warehouse_id, start_date, end_date, logger):
    """Compare order details between Database A and Database B"""
    try:
        # Check Database A (Source)
        query_a = """
        SELECT COUNT(*) as total_details
        FROM order_detail od
        JOIN "order" o ON od.order_id = o.order_id
        WHERE o.faktur_date >= %s 
        AND o.faktur_date <= %s 
        AND o.warehouse_id = %s
        """
        
        with db_a_conn.cursor() as cursor:
            cursor.execute(query_a, (start_date, end_date, warehouse_id))
            result_a = cursor.fetchone()
            
        # Check Database B (Target)
        query_b = """
        SELECT COUNT(*) as total_details
        FROM order_detail_main odm
        JOIN order_main om ON odm.order_id = om.order_id
        WHERE om.faktur_date >= %s 
        AND om.faktur_date <= %s 
        AND om.warehouse_id = %s
        """
        
        with db_b_conn.cursor() as cursor:
            cursor.execute(query_b, (start_date, end_date, warehouse_id))
            result_b = cursor.fetchone()
            
        logger.info("=== Order Details Comparison ===")
        logger.info(f"Database A (Source): {result_a[0]} order details")
        logger.info(f"Database B (Target): {result_b[0]} order details")
        
        if result_a[0] != result_b[0]:
            logger.warning(f"❌ Order detail count mismatch: A={result_a[0]}, B={result_b[0]}")
        else:
            logger.info("✅ Order detail counts match!")
            
        return result_a[0], result_b[0]
        
    except Exception as e:
        logger.error(f"Error comparing order details: {str(e)}")
        return 0, 0

def check_sample_data(db_a_conn, db_b_conn, warehouse_id, start_date, end_date, logger):
    """Check sample data from both databases"""
    try:
        logger.info("=== Sample Data Comparison ===")
        
        # Sample from Database A
        query_a = """
        SELECT faktur_id, faktur_date, customer_id, warehouse_id
        FROM "order"
        WHERE faktur_date >= %s 
        AND faktur_date <= %s 
        AND warehouse_id = %s
        ORDER BY faktur_date
        LIMIT 5
        """
        
        with db_a_conn.cursor() as cursor:
            cursor.execute(query_a, (start_date, end_date, warehouse_id))
            sample_a = cursor.fetchall()
            
        logger.info(f"Database A sample (first 5):")
        for i, record in enumerate(sample_a, 1):
            logger.info(f"  {i}. faktur_id: {record[0]}, date: {record[1]}, customer: {record[2]}, warehouse: {record[3]}")
            
        # Sample from Database B
        query_b = """
        SELECT faktur_id, faktur_date, customer_id, warehouse_id
        FROM order_main
        WHERE faktur_date >= %s 
        AND faktur_date <= %s 
        AND warehouse_id = %s
        ORDER BY faktur_date
        LIMIT 5
        """
        
        with db_b_conn.cursor() as cursor:
            cursor.execute(query_b, (start_date, end_date, warehouse_id))
            sample_b = cursor.fetchall()
            
        logger.info(f"Database B sample (first 5):")
        for i, record in enumerate(sample_b, 1):
            logger.info(f"  {i}. faktur_id: {record[0]}, date: {record[1]}, customer: {record[2]}, warehouse: {record[3]}")
            
    except Exception as e:
        logger.error(f"Error checking sample data: {str(e)}")

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Compare data between Database A and Database B')
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
    
    logger.info("=== Copy Comparison Debug ===")
    logger.info(f"Warehouse ID: {args.warehouse_id}")
    logger.info(f"Date Range: {start_date} to {end_date}")
    
    db_a_conn = None
    db_b_conn = None
    
    try:
        # Connect to both databases
        db_a_conn = get_db_connection('A')
        db_b_conn = get_db_connection('B')
        logger.info("✓ Connected to both databases successfully")
        
        # Compare orders
        orders_a, orders_b = check_orders_comparison(db_a_conn, db_b_conn, args.warehouse_id, start_date, end_date, logger)
        logger.info("")
        
        # Compare order details
        details_a, details_b = check_order_details_comparison(db_a_conn, db_b_conn, args.warehouse_id, start_date, end_date, logger)
        logger.info("")
        
        # Check sample data
        check_sample_data(db_a_conn, db_b_conn, args.warehouse_id, start_date, end_date, logger)
        
        logger.info("=== Summary ===")
        logger.info(f"Orders: A={orders_a}, B={orders_b}, Match={orders_a == orders_b}")
        logger.info(f"Order Details: A={details_a}, B={details_b}, Match={details_a == details_b}")
        
        return 0
        
    except Exception as e:
        logger.error(f"Debug failed: {str(e)}")
        return 1
        
    finally:
        if db_a_conn:
            db_a_conn.close()
        if db_b_conn:
            db_b_conn.close()
        logger.info("Database connections closed")

if __name__ == "__main__":
    exit(main()) 