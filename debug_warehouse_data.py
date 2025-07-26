#!/usr/bin/env python3
"""
Debug script to check warehouse data in Database A
"""

import os
import sys
import logging
import psycopg2
import argparse
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables - try .env first, then config.env
load_dotenv('.env')
load_dotenv('config.env')

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
        else:
            raise ValueError(f"Unsupported database type: {database_type}")
        
        return conn
    except Exception as e:
        print(f"Failed to connect to database {database_type}: {str(e)}")
        raise

def debug_warehouse_data():
    """Debug warehouse data in order table"""
    
    print("=== Warehouse Data Debug ===")
    
    try:
        # Connect to database A
        conn = get_db_connection('A')
        print("✓ Connected to Database A successfully")
        
        with conn.cursor() as cursor:
            # Check total orders
            cursor.execute("SELECT COUNT(*) FROM \"order\"")
            total_orders = cursor.fetchone()[0]
            print(f"Total orders in database: {total_orders}")
            
            # Check orders in date range
            cursor.execute("""
                SELECT COUNT(*) FROM "order" 
                WHERE faktur_date >= '2025-03-01' AND faktur_date <= '2025-03-31'
            """)
            orders_in_range = cursor.fetchone()[0]
            print(f"Orders in date range (2025-03-01 to 2025-03-31): {orders_in_range}")
            
            # Check unique warehouse_ids
            cursor.execute("SELECT DISTINCT warehouse_id FROM \"order\" ORDER BY warehouse_id")
            warehouse_ids = cursor.fetchall()
            print(f"Unique warehouse_ids found: {len(warehouse_ids)}")
            print("Warehouse IDs:")
            for (warehouse_id,) in warehouse_ids[:20]:  # Show first 20
                print(f"  - {warehouse_id}")
            if len(warehouse_ids) > 20:
                print(f"  ... and {len(warehouse_ids) - 20} more")
            
            # Check specific warehouse_id
            cursor.execute("SELECT COUNT(*) FROM \"order\" WHERE warehouse_id = '9844'")
            warehouse_9844_count = cursor.fetchone()[0]
            print(f"Orders with warehouse_id = '9844': {warehouse_9844_count}")
            
            # Check with date range and warehouse_id
            cursor.execute("""
                SELECT COUNT(*) FROM "order" 
                WHERE faktur_date >= '2025-03-01' AND faktur_date <= '2025-03-31' 
                AND warehouse_id = '9844'
            """)
            filtered_count = cursor.fetchone()[0]
            print(f"Orders with date range AND warehouse_id = '9844': {filtered_count}")
            
            # Show sample data
            if filtered_count > 0:
                cursor.execute("""
                    SELECT faktur_id, faktur_date, warehouse_id, customer_id 
                    FROM "order" 
                    WHERE faktur_date >= '2025-03-01' AND faktur_date <= '2025-03-31' 
                    AND warehouse_id = '9844'
                    LIMIT 5
                """)
                sample_data = cursor.fetchall()
                print("\nSample data:")
                for row in sample_data:
                    print(f"  faktur_id: {row[0]}, date: {row[1]}, warehouse: {row[2]}, customer: {row[3]}")
            
            # Check data types
            cursor.execute("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'order' AND column_name IN ('warehouse_id', 'faktur_date')
            """)
            column_types = cursor.fetchall()
            print("\nColumn data types:")
            for col_name, data_type in column_types:
                print(f"  {col_name}: {data_type}")
        
        conn.close()
        print("\n✓ Database connection closed")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    debug_warehouse_data() 