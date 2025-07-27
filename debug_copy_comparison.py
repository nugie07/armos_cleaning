#!/usr/bin/env python3
"""
Debug script to compare data between Database A (source) and Database B (target)
for order and order_detail tables to identify differences in copy operations.
"""

import os
import sys
import argparse
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

def compare_order_counts(db_a_conn, db_b_conn, warehouse_id_a, warehouse_id_b, start_date, end_date, logger):
    """Compare order counts between Database A and B"""
    logger.info("=== ORDER COUNT COMPARISON ===")
    
    # Database A query (source)
    query_a = """
    SELECT COUNT(*) as total_orders
    FROM "order"
    WHERE faktur_date >= %s AND faktur_date <= %s AND warehouse_id = %s
    """
    
    # Database B query (target)
    query_b = """
    SELECT COUNT(*) as total_orders
    FROM order_main
    WHERE faktur_date >= %s AND faktur_date <= %s AND warehouse_id = %s
    """
    
    try:
        # Get counts from both databases
        with db_a_conn.cursor() as cursor:
            cursor.execute(query_a, (start_date, end_date, warehouse_id_a))
            count_a = cursor.fetchone()[0]
        
        with db_b_conn.cursor() as cursor:
            cursor.execute(query_b, (start_date, end_date, warehouse_id_b))
            count_b = cursor.fetchone()[0]
        
        logger.info(f"Database A (source) orders: {count_a}")
        logger.info(f"Database B (target) orders: {count_b}")
        logger.info(f"Difference: {count_a - count_b}")
        
        if count_a != count_b:
            logger.warning(f"❌ Order count mismatch! Missing {count_a - count_b} orders in Database B")
        else:
            logger.info("✅ Order counts match")
        
        return count_a, count_b
        
    except Exception as e:
        logger.error(f"Error comparing order counts: {str(e)}")
        raise

def compare_order_detail_counts(db_a_conn, db_b_conn, warehouse_id_a, warehouse_id_b, start_date, end_date, logger):
    """Compare order_detail counts between Database A and B"""
    logger.info("=== ORDER DETAIL COUNT COMPARISON ===")
    
    # Database A query (source)
    query_a = """
    SELECT COUNT(*) as total_order_details
    FROM order_detail od
    JOIN "order" o ON od.order_id = o.order_id
    WHERE o.faktur_date >= %s AND o.faktur_date <= %s AND o.warehouse_id = %s
    """
    
    # Database B query (target)
    query_b = """
    SELECT COUNT(*) as total_order_details
    FROM order_detail_main odm
    JOIN order_main om ON odm.order_id = om.order_id
    WHERE om.faktur_date >= %s AND om.faktur_date <= %s AND om.warehouse_id = %s
    """
    
    try:
        # Get counts from both databases
        with db_a_conn.cursor() as cursor:
            cursor.execute(query_a, (start_date, end_date, warehouse_id_a))
            count_a = cursor.fetchone()[0]
        
        with db_b_conn.cursor() as cursor:
            cursor.execute(query_b, (start_date, end_date, warehouse_id_b))
            count_b = cursor.fetchone()[0]
        
        logger.info(f"Database A (source) order_details: {count_a}")
        logger.info(f"Database B (target) order_details: {count_b}")
        logger.info(f"Difference: {count_a - count_b}")
        
        if count_a != count_b:
            logger.warning(f"❌ Order detail count mismatch! Missing {count_a - count_b} order_details in Database B")
        else:
            logger.info("✅ Order detail counts match")
        
        return count_a, count_b
        
    except Exception as e:
        logger.error(f"Error comparing order detail counts: {str(e)}")
        raise

def find_missing_orders(db_a_conn, db_b_conn, warehouse_id_a, warehouse_id_b, start_date, end_date, logger):
    """Find orders that exist in Database A but not in Database B"""
    logger.info("=== FINDING MISSING ORDERS ===")
    
    # First check if order_main table exists in Database B
    check_table_query = """
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name = 'order_main'
    );
    """
    
    try:
        with db_b_conn.cursor() as cursor:
            cursor.execute(check_table_query)
            table_exists = cursor.fetchone()[0]
        
        if not table_exists:
            logger.warning("❌ order_main table does not exist in Database B")
            logger.warning("Please run create_tables.py first to create the required tables")
            return []
        
        # Get all orders from Database A first
        query_a = """
        SELECT o.order_id, o.faktur_id, o.faktur_date, o.customer_id, o.warehouse_id
        FROM "order" o
        WHERE o.faktur_date >= %s AND o.faktur_date <= %s AND o.warehouse_id = %s
        ORDER BY o.faktur_date, o.order_id
        """
        
        with db_a_conn.cursor() as cursor:
            cursor.execute(query_a, (start_date, end_date, warehouse_id_a))
            all_orders_a = cursor.fetchall()
        
        # Get all order_ids from Database B
        query_b = """
        SELECT order_id FROM order_main
        WHERE faktur_date >= %s AND faktur_date <= %s AND warehouse_id = %s
        """
        
        with db_b_conn.cursor() as cursor:
            cursor.execute(query_b, (start_date, end_date, warehouse_id_b))
            existing_order_ids = {row[0] for row in cursor.fetchall()}
        
        # Find missing orders
        missing_orders = []
        for order in all_orders_a:
            if order[0] not in existing_order_ids:  # order_id not in Database B
                missing_orders.append(order)
                if len(missing_orders) >= 10:  # Limit to 10
                    break
        
        if missing_orders:
            logger.warning(f"Found {len(missing_orders)} missing orders (showing first 10):")
            for order in missing_orders:
                logger.warning(f"  Missing: order_id={order[0]}, faktur_id={order[1]}, date={order[2]}, customer={order[3]}, warehouse={order[4]}")
        else:
            logger.info("✅ No missing orders found")
        
        return missing_orders
        
    except Exception as e:
        logger.error(f"Error finding missing orders: {str(e)}")
        raise

def find_missing_order_details(db_a_conn, db_b_conn, warehouse_id_a, warehouse_id_b, start_date, end_date, logger):
    """Find order_details that exist in Database A but not in Database B"""
    logger.info("=== FINDING MISSING ORDER DETAILS ===")
    
    # First check if order_detail_main table exists in Database B
    check_table_query = """
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name = 'order_detail_main'
    );
    """
    
    try:
        with db_b_conn.cursor() as cursor:
            cursor.execute(check_table_query)
            table_exists = cursor.fetchone()[0]
        
        if not table_exists:
            logger.warning("❌ order_detail_main table does not exist in Database B")
            logger.warning("Please run create_tables.py first to create the required tables")
            return []
        
        # Get all order details from Database A first
        query_a = """
        SELECT od.order_detail_id, od.order_id, o.faktur_id, o.faktur_date, od.product_id, od.line_id
        FROM order_detail od
        JOIN "order" o ON od.order_id = o.order_id
        WHERE o.faktur_date >= %s AND o.faktur_date <= %s AND o.warehouse_id = %s
        ORDER BY o.faktur_date, od.order_id
        """
        
        with db_a_conn.cursor() as cursor:
            cursor.execute(query_a, (start_date, end_date, warehouse_id_a))
            all_details_a = cursor.fetchall()
        
        # Get all order detail keys from Database B
        query_b = """
        SELECT odm.order_id, odm.product_id, odm.line_id FROM order_detail_main odm
        JOIN order_main om ON odm.order_id = om.order_id
        WHERE om.faktur_date >= %s AND om.faktur_date <= %s AND om.warehouse_id = %s
        """
        
        with db_b_conn.cursor() as cursor:
            cursor.execute(query_b, (start_date, end_date, warehouse_id_b))
            existing_detail_keys = {(row[0], row[1], row[2]) for row in cursor.fetchall()}
        
        # Find missing order details
        missing_details = []
        for detail in all_details_a:
            detail_key = (detail[1], detail[4], detail[5])  # (order_id, product_id, line_id)
            if detail_key not in existing_detail_keys:
                missing_details.append(detail)
                if len(missing_details) >= 10:  # Limit to 10
                    break
        
        if missing_details:
            logger.warning(f"Found {len(missing_details)} missing order details (showing first 10):")
            for detail in missing_details:
                logger.warning(f"  Missing: detail_id={detail[0]}, order_id={detail[1]}, faktur_id={detail[2]}, date={detail[3]}, product={detail[4]}, line={detail[5]}")
        else:
            logger.info("✅ No missing order details found")
        
        return missing_details
        
    except Exception as e:
        logger.error(f"Error finding missing order details: {str(e)}")
        raise

def show_sample_data_comparison(db_a_conn, db_b_conn, warehouse_id_a, warehouse_id_b, start_date, end_date, logger):
    """Show sample data from both databases for comparison"""
    logger.info("=== SAMPLE DATA COMPARISON ===")
    
    # Sample orders from Database A
    query_a = """
    SELECT order_id, faktur_id, faktur_date, customer_id, warehouse_id, do_number
    FROM "order"
    WHERE faktur_date >= %s AND faktur_date <= %s AND warehouse_id = %s
    ORDER BY faktur_date, order_id
    LIMIT 5
    """
    
    # Sample orders from Database B
    query_b = """
    SELECT order_id, faktur_id, faktur_date, customer_id, warehouse_id, do_number
    FROM order_main
    WHERE faktur_date >= %s AND faktur_date <= %s AND warehouse_id = %s
    ORDER BY faktur_date, order_id
    LIMIT 5
    """
    
    try:
        logger.info("--- Database A (Source) Sample Orders ---")
        with db_a_conn.cursor() as cursor:
            cursor.execute(query_a, (start_date, end_date, warehouse_id_a))
            orders_a = cursor.fetchall()
            for order in orders_a:
                logger.info(f"  order_id={order[0]}, faktur_id={order[1]}, date={order[2]}, customer={order[3]}, warehouse={order[4]}, do={order[5]}")
        
        # Check if order_main table exists before querying Database B
        check_table_query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_name = 'order_main'
        );
        """
        
        with db_b_conn.cursor() as cursor:
            cursor.execute(check_table_query)
            table_exists = cursor.fetchone()[0]
        
        if table_exists:
            logger.info("--- Database B (Target) Sample Orders ---")
            with db_b_conn.cursor() as cursor:
                cursor.execute(query_b, (start_date, end_date, warehouse_id_b))
                orders_b = cursor.fetchall()
                for order in orders_b:
                    logger.info(f"  order_id={order[0]}, faktur_id={order[1]}, date={order[2]}, customer={order[3]}, warehouse={order[4]}, do={order[5]}")
        else:
            logger.warning("❌ order_main table does not exist in Database B - skipping sample data")
        
    except Exception as e:
        logger.error(f"Error showing sample data: {str(e)}")
        raise

def check_copy_queries(db_a_conn, warehouse_id_a, start_date, end_date, logger):
    """Show the actual queries that would be used for copying"""
    logger.info("=== COPY QUERIES ANALYSIS ===")
    
    # Order copy query
    order_query = """
    SELECT 
        order_id, faktur_id, faktur_date, delivery_date, do_number, status, skip_count,
        created_date, created_by, updated_date, updated_by, notes, customer_id,
        warehouse_id, delivery_type_id, order_integration_id, origin_name,
        origin_address_1, origin_address_2, origin_city, origin_zipcode,
        origin_phone, origin_email, destination_name, destination_address_1,
        destination_address_2, destination_city, destination_zip_code,
        destination_phone, destination_email, client_id, cancel_reason,
        rdo_integration_id, address_change, divisi, pre_status
    FROM "order"
    WHERE faktur_date >= %s AND faktur_date <= %s AND warehouse_id = %s
    ORDER BY faktur_date
    LIMIT 3
    """
    
    # Order detail copy query
    detail_query = """
    SELECT 
        od.quantity_faktur, od.net_price, od.quantity_wms, od.quantity_delivery,
        od.quantity_loading, od.quantity_unloading, od.status, od.cancel_reason,
        od.notes, od.product_id, od.unit_id, od.pack_id, od.line_id,
        od.unloading_latitude, od.unloading_longitude, od.origin_uom, od.origin_qty,
        od.total_ctn, od.total_pcs, o.faktur_id, o.faktur_date, o.customer_id
    FROM order_detail od
    JOIN "order" o ON od.order_id = o.order_id
    WHERE o.faktur_date >= %s AND o.faktur_date <= %s AND o.warehouse_id = %s
    ORDER BY o.faktur_date
    LIMIT 3
    """
    
    try:
        logger.info("--- Order Copy Query Sample ---")
        with db_a_conn.cursor() as cursor:
            cursor.execute(order_query, (start_date, end_date, warehouse_id_a))
            orders = cursor.fetchall()
            for order in orders:
                logger.info(f"  order_id={order[0]}, faktur_id={order[1]}, date={order[2]}, customer={order[12]}, warehouse={order[13]}")
        
        logger.info("--- Order Detail Copy Query Sample ---")
        with db_a_conn.cursor() as cursor:
            cursor.execute(detail_query, (start_date, end_date, warehouse_id_a))
            details = cursor.fetchall()
            for detail in details:
                logger.info(f"  product={detail[9]}, line={detail[12]}, faktur_id={detail[19]}, date={detail[20]}, customer={detail[21]}")
        
    except Exception as e:
        logger.error(f"Error checking copy queries: {str(e)}")
        raise

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Debug script to compare data between Database A and B')
    parser.add_argument('--warehouse-id', required=True, type=str,
                       help='Warehouse ID to filter data')
    parser.add_argument('--start-date', required=True, type=str, 
                       help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', required=True, type=str, 
                       help='End date (YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    # Setup logging
    logger = setup_logging()
    
    logger.info("=== DATABASE COPY COMPARISON DEBUG ===")
    logger.info(f"Warehouse ID: {args.warehouse_id}")
    logger.info(f"Date Range: {args.start_date} to {args.end_date}")
    
    try:
        # Connect to both databases
        logger.info("Connecting to Database A (source)...")
        db_a_conn = get_db_connection('A')
        logger.info("✅ Connected to Database A successfully")
        
        logger.info("Connecting to Database B (target)...")
        db_b_conn = get_db_connection('B')
        logger.info("✅ Connected to Database B successfully")
        
        # Use warehouse_id as integer for Database A (INTEGER) and string for Database B (VARCHAR)
        try:
            warehouse_param_a = int(args.warehouse_id)  # Integer for Database A
            warehouse_param_b = args.warehouse_id  # String for Database B
            logger.info(f"Using warehouse_id as integer for DB A: {warehouse_param_a}, string for DB B: {warehouse_param_b}")
        except ValueError:
            warehouse_param_a = args.warehouse_id  # Fallback to string for DB A
            warehouse_param_b = args.warehouse_id  # String for Database B
            logger.info(f"Using warehouse_id as string for both DBs: {warehouse_param_a}")
        
        # Run comparisons
        compare_order_counts(db_a_conn, db_b_conn, warehouse_param_a, warehouse_param_b, args.start_date, args.end_date, logger)
        compare_order_detail_counts(db_a_conn, db_b_conn, warehouse_param_a, warehouse_param_b, args.start_date, args.end_date, logger)
        find_missing_orders(db_a_conn, db_b_conn, warehouse_param_a, warehouse_param_b, args.start_date, args.end_date, logger)
        find_missing_order_details(db_a_conn, db_b_conn, warehouse_param_a, warehouse_param_b, args.start_date, args.end_date, logger)
        show_sample_data_comparison(db_a_conn, db_b_conn, warehouse_param_a, warehouse_param_b, args.start_date, args.end_date, logger)
        check_copy_queries(db_a_conn, warehouse_param_a, args.start_date, args.end_date, logger)
        
        logger.info("=== DEBUG COMPLETED ===")
        
    except Exception as e:
        logger.error(f"Debug failed: {str(e)}")
        sys.exit(1)
    finally:
        if 'db_a_conn' in locals():
            db_a_conn.close()
            logger.info("Database A connection closed")
        if 'db_b_conn' in locals():
            db_b_conn.close()
            logger.info("Database B connection closed")

if __name__ == "__main__":
    main() 