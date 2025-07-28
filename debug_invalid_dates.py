#!/usr/bin/env python3
"""
Debug script to detect invalid dates in order table
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

def get_db_connection(database='A'):
    """Get database connection"""
    if database == 'A':
        conn = psycopg2.connect(
            host=os.getenv('DB_A_HOST'),
            port=os.getenv('DB_A_PORT'),
            database=os.getenv('DB_A_NAME'),
            user=os.getenv('DB_A_USER'),
            password=os.getenv('DB_A_PASSWORD')
        )
    else:
        conn = psycopg2.connect(
            host=os.getenv('DB_B_HOST'),
            port=os.getenv('DB_B_PORT'),
            database=os.getenv('DB_B_NAME'),
            user=os.getenv('DB_B_USER'),
            password=os.getenv('DB_B_PASSWORD')
        )
    return conn

def check_invalid_dates(logger, start_date, end_date, warehouse_id):
    """Check for invalid dates in order table"""
    logger.info("=== CHECKING FOR INVALID DATES ===")
    
    conn_a = get_db_connection('A')
    
    try:
        cursor = conn_a.cursor()
        
        # Check for invalid faktur_date
        logger.info("Checking faktur_date for invalid values...")
        cursor.execute("""
            SELECT order_id, faktur_date, faktur_id, customer_id, warehouse_id
            FROM "order"
            WHERE warehouse_id = %s
            AND (faktur_date IS NULL OR faktur_date < '1900-01-01' OR faktur_date > '2100-12-31')
            ORDER BY order_id
            LIMIT 20
        """, (warehouse_id,))
        
        invalid_faktur_dates = cursor.fetchall()
        
        if invalid_faktur_dates:
            logger.warning(f"Found {len(invalid_faktur_dates)} orders with invalid faktur_date:")
            for row in invalid_faktur_dates:
                logger.warning(f"  Order ID: {row[0]}, faktur_date: {row[1]}, faktur_id: {row[2]}, customer_id: {row[3]}")
        else:
            logger.info("No invalid faktur_date found")
        
        # Check for invalid delivery_date
        logger.info("Checking delivery_date for invalid values...")
        cursor.execute("""
            SELECT order_id, delivery_date, faktur_id, customer_id, warehouse_id
            FROM "order"
            WHERE warehouse_id = %s
            AND (delivery_date IS NOT NULL AND (delivery_date < '1900-01-01' OR delivery_date > '2100-12-31'))
            ORDER BY order_id
            LIMIT 20
        """, (warehouse_id,))
        
        invalid_delivery_dates = cursor.fetchall()
        
        if invalid_delivery_dates:
            logger.warning(f"Found {len(invalid_delivery_dates)} orders with invalid delivery_date:")
            for row in invalid_delivery_dates:
                logger.warning(f"  Order ID: {row[0]}, delivery_date: {row[1]}, faktur_id: {row[2]}, customer_id: {row[3]}")
        else:
            logger.info("No invalid delivery_date found")
        
        # Check for invalid created_date
        logger.info("Checking created_date for invalid values...")
        cursor.execute("""
            SELECT order_id, created_date, faktur_id, customer_id, warehouse_id
            FROM "order"
            WHERE warehouse_id = %s
            AND (created_date IS NOT NULL AND (created_date < '1900-01-01' OR created_date > '2100-12-31'))
            ORDER BY order_id
            LIMIT 20
        """, (warehouse_id,))
        
        invalid_created_dates = cursor.fetchall()
        
        if invalid_created_dates:
            logger.warning(f"Found {len(invalid_created_dates)} orders with invalid created_date:")
            for row in invalid_created_dates:
                logger.warning(f"  Order ID: {row[0]}, created_date: {row[1]}, faktur_id: {row[2]}, customer_id: {row[3]}")
        else:
            logger.info("No invalid created_date found")
        
        # Check for invalid updated_date
        logger.info("Checking updated_date for invalid values...")
        cursor.execute("""
            SELECT order_id, updated_date, faktur_id, customer_id, warehouse_id
            FROM "order"
            WHERE warehouse_id = %s
            AND (updated_date IS NOT NULL AND (updated_date < '1900-01-01' OR updated_date > '2100-12-31'))
            ORDER BY order_id
            LIMIT 20
        """, (warehouse_id,))
        
        invalid_updated_dates = cursor.fetchall()
        
        if invalid_updated_dates:
            logger.warning(f"Found {len(invalid_updated_dates)} orders with invalid updated_date:")
            for row in invalid_updated_dates:
                logger.warning(f"  Order ID: {row[0]}, updated_date: {row[1]}, faktur_id: {row[2]}, customer_id: {row[3]}")
        else:
            logger.info("No invalid updated_date found")
        
        # Check for specific date range that might cause issues
        logger.info("Checking for orders in the specified date range...")
        cursor.execute("""
            SELECT COUNT(*) FROM "order"
            WHERE faktur_date >= %s AND faktur_date <= %s 
            AND warehouse_id = %s
            AND faktur_id IS NOT NULL AND customer_id IS NOT NULL
        """, (start_date, end_date, warehouse_id))
        
        valid_orders = cursor.fetchone()[0]
        logger.info(f"Valid orders in date range {start_date} to {end_date}: {valid_orders}")
        
        # Check for orders that would be excluded
        cursor.execute("""
            SELECT COUNT(*) FROM "order"
            WHERE faktur_date >= %s AND faktur_date <= %s 
            AND warehouse_id = %s
            AND (faktur_id IS NULL OR customer_id IS NULL)
        """, (start_date, end_date, warehouse_id))
        
        excluded_orders = cursor.fetchone()[0]
        logger.info(f"Orders excluded due to NULL faktur_id or customer_id: {excluded_orders}")
        
        # Check for orders with invalid dates in the range
        cursor.execute("""
            SELECT COUNT(*) FROM "order"
            WHERE warehouse_id = %s
            AND (faktur_date < '1900-01-01' OR faktur_date > '2100-12-31'
                 OR delivery_date < '1900-01-01' OR delivery_date > '2100-12-31'
                 OR created_date < '1900-01-01' OR created_date > '2100-12-31'
                 OR updated_date < '1900-01-01' OR updated_date > '2100-12-31')
        """, (warehouse_id,))
        
        total_invalid = cursor.fetchone()[0]
        logger.info(f"Total orders with invalid dates: {total_invalid}")
        
        return {
            'invalid_faktur_dates': len(invalid_faktur_dates),
            'invalid_delivery_dates': len(invalid_delivery_dates),
            'invalid_created_dates': len(invalid_created_dates),
            'invalid_updated_dates': len(invalid_updated_dates),
            'valid_orders': valid_orders,
            'excluded_orders': excluded_orders,
            'total_invalid': total_invalid
        }
        
    except Exception as e:
        logger.error(f"Error checking invalid dates: {e}")
        raise
    finally:
        conn_a.close()

def main():
    """Main function"""
    logger = setup_logging()
    
    if len(sys.argv) != 4:
        logger.error("Usage: python3 debug_invalid_dates.py <start_date> <end_date> <warehouse_id>")
        logger.error("Example: python3 debug_invalid_dates.py 2024-05-01 2025-05-31 4512")
        sys.exit(1)
    
    start_date = sys.argv[1]
    end_date = sys.argv[2]
    warehouse_id = int(sys.argv[3])
    
    try:
        logger.info("=== DEBUGGING INVALID DATES ===")
        logger.info(f"Date Range: {start_date} to {end_date}")
        logger.info(f"Warehouse ID: {warehouse_id}")
        
        results = check_invalid_dates(logger, start_date, end_date, warehouse_id)
        
        logger.info("=== SUMMARY ===")
        logger.info(f"Invalid faktur_dates: {results['invalid_faktur_dates']}")
        logger.info(f"Invalid delivery_dates: {results['invalid_delivery_dates']}")
        logger.info(f"Invalid created_dates: {results['invalid_created_dates']}")
        logger.info(f"Invalid updated_dates: {results['invalid_updated_dates']}")
        logger.info(f"Valid orders in range: {results['valid_orders']}")
        logger.info(f"Excluded orders: {results['excluded_orders']}")
        logger.info(f"Total invalid dates: {results['total_invalid']}")
        
    except Exception as e:
        logger.error(f"Script failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 