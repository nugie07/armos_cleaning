#!/usr/bin/env python3
"""
Test script to verify database connections and basic functionality
"""

import os
import sys
import psycopg2
from dotenv import load_dotenv

# Load environment variables - try .env first, then config.env
load_dotenv('.env')
load_dotenv('config.env')

def test_connection(database_type):
    """Test database connection"""
    try:
        if database_type.upper() == 'A':
            conn = psycopg2.connect(
                host=os.getenv('DB_A_HOST'),
                port=os.getenv('DB_A_PORT'),
                database=os.getenv('DB_A_NAME'),
                user=os.getenv('DB_A_USER'),
                password=os.getenv('DB_A_PASSWORD')
            )
            print(f"✓ Database A connection successful")
            
            # Test if required tables exist
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT table_name FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name IN ('order', 'order_detail', 'mst_product')
                """)
                tables = [row[0] for row in cursor.fetchall()]
                
                required_tables = ['order', 'order_detail', 'mst_product']
                missing_tables = [table for table in required_tables if table not in tables]
                
                if missing_tables:
                    print(f"⚠  Missing tables in Database A: {missing_tables}")
                else:
                    print(f"✓ All required tables found in Database A")
                
                # Get record counts
                for table in tables:
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cursor.fetchone()[0]
                    print(f"  - {table}: {count} records")
            
            conn.close()
            
        elif database_type.upper() == 'B':
            conn = psycopg2.connect(
                host=os.getenv('DB_B_HOST'),
                port=os.getenv('DB_B_PORT'),
                database=os.getenv('DB_B_NAME'),
                user=os.getenv('DB_B_USER'),
                password=os.getenv('DB_B_PASSWORD')
            )
            print(f"✓ Database B connection successful")
            
            # Test if target tables exist
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT table_name FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name IN ('order_main', 'order_detail_main', 'mst_product_main')
                """)
                tables = [row[0] for row in cursor.fetchall()]
                
                if tables:
                    print(f"✓ Target tables found in Database B: {tables}")
                    for table in tables:
                        cursor.execute(f"SELECT COUNT(*) FROM {table}")
                        count = cursor.fetchone()[0]
                        print(f"  - {table}: {count} records")
                else:
                    print(f"ℹ  No target tables found in Database B (run create_tables.py first)")
            
            conn.close()
        else:
            print(f"✗ Unknown database type: {database_type}")
            return False
            
        return True
        
    except Exception as e:
        print(f"✗ Database {database_type} connection failed: {str(e)}")
        return False

def main():
    """Main test function"""
    print("Database Connection Test")
    print("=" * 40)
    
    # Test Database A
    print("\nTesting Database A (Source):")
    db_a_ok = test_connection('A')
    
    # Test Database B
    print("\nTesting Database B (Target):")
    db_b_ok = test_connection('B')
    
    # Summary
    print("\n" + "=" * 40)
    print("Test Summary:")
    if db_a_ok and db_b_ok:
        print("✓ All database connections successful")
        print("✓ Ready to run database operations")
    else:
        print("✗ Some database connections failed")
        print("  Please check your config.env file and database settings")
        sys.exit(1)

if __name__ == "__main__":
    main() 