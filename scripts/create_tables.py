#!/usr/bin/env python3
"""
Script to create tables in both databases
"""
import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.database import engine_a, engine_b, Base
from models.database_a_models import Order, OrderDetail
from models.database_b_models import (
    CleansedOutboundDocuments,
    CleansedOutboundItems,
    CleansedOutboundConversions,
    CleaningPayloadResults
)

def create_database_a_tables():
    """Create tables in Database A"""
    print("Creating tables in Database A...")
    try:
        # Create tables for Database A
        Order.__table__.create(engine_a, checkfirst=True)
        OrderDetail.__table__.create(engine_a, checkfirst=True)
        print("✓ Database A tables created successfully")
    except Exception as e:
        print(f"✗ Error creating Database A tables: {str(e)}")

def create_database_b_tables():
    """Create tables in Database B"""
    print("Creating tables in Database B...")
    try:
        # Create tables for Database B
        CleansedOutboundDocuments.__table__.create(engine_b, checkfirst=True)
        CleansedOutboundItems.__table__.create(engine_b, checkfirst=True)
        CleansedOutboundConversions.__table__.create(engine_b, checkfirst=True)
        CleaningPayloadResults.__table__.create(engine_b, checkfirst=True)
        print("✓ Database B tables created successfully")
    except Exception as e:
        print(f"✗ Error creating Database B tables: {str(e)}")

def main():
    """Main function"""
    print("Order Cleaning - Table Creation Script")
    print("=" * 50)
    
    # Create tables in Database A
    create_database_a_tables()
    print()
    
    # Create tables in Database B
    create_database_b_tables()
    print()
    
    print("Table creation completed!")

if __name__ == "__main__":
    main() 