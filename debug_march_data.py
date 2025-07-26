#!/usr/bin/env python3
"""
Debug script specifically for March 2025 data
"""

import os
import sys
import psycopg2
from dotenv import load_dotenv

# Load environment variables
load_dotenv('.env')
load_dotenv('config.env')

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
        else:
            raise ValueError(f"Unsupported database type: {database_type}")
        
        return conn
    except Exception as e:
        print(f"Failed to connect to database {database_type}: {str(e)}")
        raise

def debug_march_data():
    """Debug March 2025 data specifically"""
    
    print("=== March 2025 Data Debug ===")
    
    try:
        conn = get_db_connection('A')
        print("✓ Connected to Database A successfully")
        
        with conn.cursor() as cursor:
            # Check total orders in March 2025
            cursor.execute("""
                SELECT COUNT(*) FROM "order" 
                WHERE faktur_date >= '2025-03-01' AND faktur_date <= '2025-03-31'
            """)
            march_orders = cursor.fetchone()[0]
            print(f"Total orders in March 2025: {march_orders}")
            
            # Check warehouse_ids in March 2025
            cursor.execute("""
                SELECT DISTINCT warehouse_id FROM "order" 
                WHERE faktur_date >= '2025-03-01' AND faktur_date <= '2025-03-31'
                ORDER BY warehouse_id
            """)
            march_warehouses = cursor.fetchall()
            print(f"Warehouse IDs in March 2025: {len(march_warehouses)}")
            for (warehouse_id,) in march_warehouses:
                print(f"  - {warehouse_id}")
            
            # Check specific warehouse_id 9844 in March
            cursor.execute("""
                SELECT COUNT(*) FROM "order" 
                WHERE faktur_date >= '2025-03-01' AND faktur_date <= '2025-03-31'
                AND warehouse_id = 9844
            """)
            march_9844_count = cursor.fetchone()[0]
            print(f"Orders with warehouse_id = 9844 in March: {march_9844_count}")
            
            # Check with string warehouse_id
            cursor.execute("""
                SELECT COUNT(*) FROM "order" 
                WHERE faktur_date >= '2025-03-01' AND faktur_date <= '2025-03-31'
                AND warehouse_id = '9844'
            """)
            march_9844_string_count = cursor.fetchone()[0]
            print(f"Orders with warehouse_id = '9844' (string) in March: {march_9844_string_count}")
            
            # Show sample data from March
            cursor.execute("""
                SELECT faktur_id, faktur_date, warehouse_id, customer_id, do_number
                FROM "order" 
                WHERE faktur_date >= '2025-03-01' AND faktur_date <= '2025-03-31'
                LIMIT 10
            """)
            march_sample = cursor.fetchall()
            print(f"\nSample March 2025 data (first 10):")
            for row in march_sample:
                print(f"  faktur_id: {row[0]}, date: {row[1]}, warehouse: {row[2]}, customer: {row[3]}, do_number: {row[4]}")
            
            # Check if there are any orders with warehouse_id 9844 at all
            cursor.execute("""
                SELECT COUNT(*) FROM "order" WHERE warehouse_id = 9844
            """)
            total_9844 = cursor.fetchone()[0]
            print(f"\nTotal orders with warehouse_id = 9844 (all time): {total_9844}")
            
            # Show date range for warehouse_id 9844
            cursor.execute("""
                SELECT MIN(faktur_date), MAX(faktur_date) FROM "order" 
                WHERE warehouse_id = 9844
            """)
            date_range = cursor.fetchone()
            print(f"Date range for warehouse_id 9844: {date_range[0]} to {date_range[1]}")
            
            # Check if there are any orders in March at all
            cursor.execute("""
                SELECT MIN(faktur_date), MAX(faktur_date) FROM "order" 
                WHERE faktur_date >= '2025-03-01' AND faktur_date <= '2025-03-31'
            """)
            march_range = cursor.fetchone()
            print(f"Date range in March 2025: {march_range[0]} to {march_range[1]}")
        
        conn.close()
        print("\n✓ Database connection closed")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    debug_march_data() 