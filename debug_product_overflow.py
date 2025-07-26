#!/usr/bin/env python3
"""
Debug script to check product data that might cause numeric overflow
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

def debug_product_overflow():
    """Debug product data that might cause numeric overflow"""
    
    print("=== Product Data Overflow Debug ===")
    
    try:
        conn = get_db_connection('A')
        print("✓ Connected to Database A successfully")
        
        with conn.cursor() as cursor:
            # Check for large values in decimal fields
            decimal_fields = ['height', 'width', 'length', 'price', 'qty', 'volume', 'weight']
            
            for field in decimal_fields:
                print(f"\n--- Checking {field} field ---")
                
                # Check for very large values
                cursor.execute(f"""
                    SELECT sku, {field}, LENGTH(CAST({field} AS TEXT)) as length
                    FROM mst_product 
                    WHERE {field} IS NOT NULL 
                    AND LENGTH(CAST({field} AS TEXT)) > 8
                    ORDER BY {field} DESC
                    LIMIT 10
                """)
                
                large_values = cursor.fetchall()
                if large_values:
                    print(f"Large values in {field}:")
                    for sku, value, length in large_values:
                        print(f"  SKU: {sku}, {field}: {value}, Length: {length}")
                else:
                    print(f"No large values found in {field}")
                
                # Check for negative values
                cursor.execute(f"""
                    SELECT sku, {field}
                    FROM mst_product 
                    WHERE {field} < 0
                    ORDER BY {field}
                    LIMIT 5
                """)
                
                negative_values = cursor.fetchall()
                if negative_values:
                    print(f"Negative values in {field}:")
                    for sku, value in negative_values:
                        print(f"  SKU: {sku}, {field}: {value}")
                else:
                    print(f"No negative values found in {field}")
            
            # Check sample data around the problematic batch (around 2000-3000 records)
            print(f"\n--- Sample data around batch 2000-3000 ---")
            cursor.execute("""
                SELECT sku, height, width, length, price, qty, volume, weight
                FROM mst_product
                ORDER BY sku
                LIMIT 10 OFFSET 2000
            """)
            
            sample_data = cursor.fetchall()
            print("Sample data:")
            for row in sample_data:
                print(f"  SKU: {row[0]}, height: {row[1]}, width: {row[2]}, length: {row[3]}, price: {row[4]}, qty: {row[5]}, volume: {row[6]}, weight: {row[7]}")
        
        conn.close()
        print("\n✓ Database connection closed")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    debug_product_overflow() 