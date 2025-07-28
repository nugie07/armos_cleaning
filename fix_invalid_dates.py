#!/usr/bin/env python3
"""
Fix invalid dates in order table
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

def fix_invalid_dates(logger, warehouse_id):
    """Fix invalid dates in order table"""
    logger.info("=== FIXING INVALID DATES ===")
    
    conn_a = get_db_connection('A')
    
    try:
        cursor = conn_a.cursor()
        
        # Fix invalid faktur_date (set to NULL if invalid)
        logger.info("Fixing invalid faktur_date...")
        cursor.execute("""
            UPDATE "order"
            SET faktur_date = NULL
            WHERE warehouse_id = %s
            AND (faktur_date < '1900-01-01' OR faktur_date > '2100-12-31')
        """, (warehouse_id,))
        
        faktur_fixed = cursor.rowcount
        logger.info(f"Fixed {faktur_fixed} invalid faktur_date records")
        
        # Fix invalid delivery_date using safer approach
        logger.info("Fixing invalid delivery_date...")
        try:
            cursor.execute("""
                UPDATE "order"
                SET delivery_date = NULL
                WHERE warehouse_id = %s
                AND (delivery_date < '1900-01-01' OR delivery_date > '2100-12-31')
            """, (warehouse_id,))
            
            delivery_fixed = cursor.rowcount
            logger.info(f"Fixed {delivery_fixed} invalid delivery_date records")
            
        except Exception as e:
            logger.warning(f"Error with standard delivery_date fix: {e}")
            logger.info("Trying alternative approach for delivery_date...")
            
            # Alternative approach: fix specific problematic patterns
            cursor.execute("""
                UPDATE "order"
                SET delivery_date = NULL
                WHERE warehouse_id = %s
                AND delivery_date IS NOT NULL
                AND delivery_date::text LIKE '%252025%'
            """, (warehouse_id,))
            
            delivery_fixed = cursor.rowcount
            logger.info(f"Fixed {delivery_fixed} delivery_date records with year 252025")
        
        # Fix invalid created_date (set to current date if invalid)
        logger.info("Fixing invalid created_date...")
        try:
            cursor.execute("""
                UPDATE "order"
                SET created_date = CURRENT_DATE
                WHERE warehouse_id = %s
                AND (created_date < '1900-01-01' OR created_date > '2100-12-31')
            """, (warehouse_id,))
            
            created_fixed = cursor.rowcount
            logger.info(f"Fixed {created_fixed} invalid created_date records")
            
        except Exception as e:
            logger.warning(f"Error with created_date fix: {e}")
            created_fixed = 0
        
        # Fix invalid updated_date (set to current date if invalid)
        logger.info("Fixing invalid updated_date...")
        try:
            cursor.execute("""
                UPDATE "order"
                SET updated_date = CURRENT_DATE
                WHERE warehouse_id = %s
                AND (updated_date < '1900-01-01' OR updated_date > '2100-12-31')
            """, (warehouse_id,))
            
            updated_fixed = cursor.rowcount
            logger.info(f"Fixed {updated_fixed} invalid updated_date records")
            
        except Exception as e:
            logger.warning(f"Error with updated_date fix: {e}")
            updated_fixed = 0
        
        conn_a.commit()
        
        total_fixed = faktur_fixed + delivery_fixed + created_fixed + updated_fixed
        logger.info(f"✅ Total records fixed: {total_fixed}")
        
        return {
            'faktur_fixed': faktur_fixed,
            'delivery_fixed': delivery_fixed,
            'created_fixed': created_fixed,
            'updated_fixed': updated_fixed,
            'total_fixed': total_fixed
        }
        
    except Exception as e:
        conn_a.rollback()
        logger.error(f"Error fixing invalid dates: {e}")
        raise
    finally:
        conn_a.close()

def main():
    """Main function"""
    logger = setup_logging()
    
    if len(sys.argv) != 2:
        logger.error("Usage: python3 fix_invalid_dates.py <warehouse_id>")
        logger.error("Example: python3 fix_invalid_dates.py 4512")
        sys.exit(1)
    
    warehouse_id = int(sys.argv[1])
    
    try:
        logger.info("=== FIXING INVALID DATES ===")
        logger.info(f"Warehouse ID: {warehouse_id}")
        
        results = fix_invalid_dates(logger, warehouse_id)
        
        logger.info("=== FIX SUMMARY ===")
        logger.info(f"faktur_date fixed: {results['faktur_fixed']}")
        logger.info(f"delivery_date fixed: {results['delivery_fixed']}")
        logger.info(f"created_date fixed: {results['created_fixed']}")
        logger.info(f"updated_date fixed: {results['updated_fixed']}")
        logger.info(f"Total fixed: {results['total_fixed']}")
        
        if results['total_fixed'] > 0:
            logger.info("✅ Invalid dates have been fixed. You can now run the copy script.")
        else:
            logger.info("ℹ️ No invalid dates found.")
        
    except Exception as e:
        logger.error(f"Script failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 