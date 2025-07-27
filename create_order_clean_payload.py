#!/usr/bin/env python3
"""
Script to create order clean payload from outbound tables
Combines outbound_documents, outbound_items, and outbound_conversion into JSON
"""

import os
import psycopg2
import json
import argparse
import logging
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv('.env')
load_dotenv('config.env')

def setup_logging():
    """Setup logging configuration"""
    # Create logs directory if it doesn't exist
    os.makedirs('./logs', exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('./logs/order_clean_payload.log')
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

def create_order_clean_payload_table(conn):
    """Create order_clean_payload table if it doesn't exist"""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS order_clean_payload (
        id SERIAL PRIMARY KEY,
        warehouse_id VARCHAR(255),
        outbound_reference VARCHAR(255) UNIQUE,
        payload_data JSONB,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        faktur_date DATE,
        client_id VARCHAR(255)
    );
    """
    
    try:
        with conn.cursor() as cursor:
            cursor.execute(create_table_sql)
            conn.commit()
            logging.info("Table order_clean_payload created successfully")
    except Exception as e:
        conn.rollback()
        logging.error(f"Failed to create order_clean_payload table: {str(e)}")
        raise

def get_do_numbers_from_order_main(db_conn, warehouse_id, start_date, end_date, logger):
    """Get DO numbers from order_main table based on date range and warehouse_id"""
    try:
        # Use warehouse_id as string (VARCHAR in database)
        logger.info(f"Using warehouse_id as string: {warehouse_id}")
        warehouse_param = warehouse_id
        
        # Get DO numbers from order_main
        logger.info("Fetching DO numbers from order_main...")
        do_query = """
        SELECT DISTINCT do_number 
        FROM order_main 
        WHERE warehouse_id = %s 
        AND faktur_date BETWEEN %s AND %s
        AND do_number IS NOT NULL
        ORDER BY do_number
        """
        
        with db_conn.cursor() as cursor:
            cursor.execute(do_query, (warehouse_param, start_date, end_date))
            do_numbers = [row[0] for row in cursor.fetchall()]
        
        logger.info(f"Found {len(do_numbers)} unique DO numbers from order_main")
        if do_numbers:
            logger.info(f"Sample DO numbers: {do_numbers[:5]}")
        
        return do_numbers
        
    except Exception as e:
        logger.error(f"Error fetching DO numbers: {str(e)}")
        raise

def get_outbound_data_by_do_numbers(db_conn, do_numbers, logger):
    """Get outbound data from all three tables based on DO numbers"""
    try:
        if not do_numbers:
            logger.warning("No DO numbers provided, returning empty results")
            return [], [], []
        
        # Convert list to tuple for SQL IN clause
        do_numbers_tuple = tuple(do_numbers)
        
        # Get outbound documents by DO numbers
        logger.info("Fetching outbound documents by DO numbers...")
        docs_query = """
        SELECT 
            id, document_reference, picklist_reference, driver_name, kernet_name,
            nomer_kendaraan, create_date, prnt_date, ok_date, picklist_date,
            whs_date, dev_date, created_user, prnt_user, ok_user, picklist_user,
            whs_user, dev_user, client_id, origin_id, origin_name, destination_id,
            destination_name, destination_address_1, divisi, order_value
        FROM outbound_documents 
        WHERE document_reference IN %s
        ORDER BY create_date
        """
        
        with db_conn.cursor() as cursor:
            cursor.execute(docs_query, (do_numbers_tuple,))
            documents = cursor.fetchall()
        
        logger.info(f"Found {len(documents)} matching outbound documents")
        
        # Get outbound items for matching documents
        logger.info("Fetching outbound items...")
        items_query = """
        SELECT 
            oi.id, oi.line_id, oi.warehouse_id, oi.pack_id, oi.product_id,
            oi.product_type, oi.qty, oi.uom, oi.product_net_price, oi.group_id,
            oi.group_description, oi.product_description, oi.outbound_document_id
        FROM outbound_items oi
        JOIN outbound_documents od ON oi.outbound_document_id = od.id
        WHERE od.document_reference IN %s
        ORDER BY oi.outbound_document_id, oi.line_id
        """
        
        with db_conn.cursor() as cursor:
            cursor.execute(items_query, (do_numbers_tuple,))
            items = cursor.fetchall()
        
        logger.info(f"Found {len(items)} outbound items")
        
        # Get outbound conversions (optional - table might not exist)
        logger.info("Fetching outbound conversions...")
        try:
            conv_query = """
            SELECT 
                oc.id, oc.uom, oc.numerator, oc.denominator, oc.outbound_item_id
            FROM outbound_conversions oc
            JOIN outbound_items oi ON oc.outbound_item_id = oi.id
            JOIN outbound_documents od ON oi.outbound_document_id = od.id
            WHERE od.document_reference IN %s
            ORDER BY oc.outbound_item_id
            """
            
            with db_conn.cursor() as cursor:
                cursor.execute(conv_query, (do_numbers_tuple,))
                conversions = cursor.fetchall()
            
            logger.info(f"Found {len(conversions)} outbound conversions")
            
        except Exception as e:
            if "does not exist" in str(e):
                logger.warning("Table outbound_conversions does not exist, skipping conversions")
                conversions = []
            else:
                raise e
        
        return documents, items, conversions
        
    except Exception as e:
        logger.error(f"Error fetching outbound data: {str(e)}")
        raise

def build_json_payload(documents, items, conversions, logger):
    """Build JSON payload from the fetched data"""
    try:
        payloads = []
        
        # Group items by document
        items_by_doc = {}
        for item in items:
            doc_id = item[12]  # outbound_document_id
            if doc_id not in items_by_doc:
                items_by_doc[doc_id] = []
            items_by_doc[doc_id].append(item)
        
        # Group conversions by item
        conv_by_item = {}
        for conv in conversions:
            item_id = conv[4]  # outbound_item_id
            if item_id not in conv_by_item:
                conv_by_item[item_id] = []
            conv_by_item[item_id].append(conv)
        
        # Build payload for each document
        for doc in documents:
            doc_id = doc[0]  # id
            doc_items = items_by_doc.get(doc_id, [])
            
            # Build items array
            items_array = []
            for item in doc_items:
                item_id = item[0]  # id
                item_conversions = conv_by_item.get(item_id, [])
                
                # Build conversion array
                conversion_array = []
                for conv in item_conversions:
                    conversion_array.append({
                        "uom": conv[1],  # uom
                        "numerator": float(conv[2]) if conv[2] else 1,  # numerator
                        "denominator": float(conv[3]) if conv[3] else 1  # denominator
                    })
                
                # Build item object
                item_obj = {
                    "warehouse_id": str(item[2]) if item[2] else "",  # warehouse_id
                    "line_id": str(item[1]) if item[1] else "",  # line_id
                    "product_id": str(item[4]) if item[4] else "",  # product_id
                    "product_description": item[11] if item[11] else "",  # product_description
                    "group_id": str(item[9]) if item[9] else "",  # group_id
                    "group_description": item[10] if item[10] else "",  # group_description
                    "product_type": str(item[5]) if item[5] else "",  # product_type
                    "qty": float(item[6]) if item[6] else 0,  # qty
                    "uom": item[7] if item[7] else "",  # uom
                    "pack_id": str(item[3]) if item[3] else "",  # pack_id
                    "product_net_price": float(item[8]) if item[8] else 0,  # product_net_price
                    "conversion": conversion_array,
                    "image_url": [""]  # Default empty array
                }
                items_array.append(item_obj)
            
            # Build main payload object
            payload = {
                "warehouse_id": str(doc[20]) if doc[20] else "",  # origin_id as warehouse_id
                "client_id": str(doc[18]) if doc[18] else "",  # client_id
                "outbound_reference": doc[1] if doc[1] else "",  # document_reference
                "divisi": doc[24] if doc[24] else "",  # divisi
                "faktur_date": doc[6].strftime('%Y-%m-%d') if doc[6] else "",  # create_date
                "request_delivery_date": doc[10].strftime('%Y-%m-%d') if doc[10] else "",  # dev_date
                "origin_name": doc[20] if doc[20] else "",  # origin_name
                "origin_address_1": doc[20] if doc[20] else "",  # origin_name as address
                "origin_address_2": "",
                "origin_city": doc[20] if doc[20] else "",  # origin_name as city
                "origin_phone": "",
                "origin_email": "",
                "destination_id": str(doc[21]) if doc[21] else "",  # destination_id
                "destination_name": doc[22] if doc[22] else "",  # destination_name
                "destination_address_1": doc[23] if doc[23] else "",  # destination_address_1
                "destination_address_2": doc[23] if doc[23] else "",  # destination_address_1 as address_2
                "destination_city": doc[23] if doc[23] else "",  # destination_address_1 as city
                "destination_zip_code": "",
                "destination_phone": "",
                "destination_email": "",
                "order_type": "REG",
                "items": items_array
            }
            
            payloads.append({
                "warehouse_id": str(doc[20]) if doc[20] else "",
                "outbound_reference": doc[1] if doc[1] else "",
                "payload_data": payload,
                "faktur_date": doc[6] if doc[6] else None,
                "client_id": str(doc[18]) if doc[18] else ""
            })
        
        logger.info(f"Built {len(payloads)} JSON payloads")
        return payloads
        
    except Exception as e:
        logger.error(f"Error building JSON payload: {str(e)}")
        raise

def save_payloads_to_database(db_conn, payloads, logger):
    """Save JSON payloads to order_clean_payload table in Database B"""
    try:
        insert_query = """
        INSERT INTO order_clean_payload (
            warehouse_id, outbound_reference, payload_data, faktur_date, client_id
        ) VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (outbound_reference) DO UPDATE SET
            payload_data = EXCLUDED.payload_data,
            faktur_date = EXCLUDED.faktur_date,
            client_id = EXCLUDED.client_id
        """
        
        saved_count = 0
        for payload in payloads:
            try:
                with db_conn.cursor() as cursor:
                    cursor.execute(insert_query, (
                        payload["warehouse_id"],
                        payload["outbound_reference"],
                        json.dumps(payload["payload_data"]),
                        payload["faktur_date"],
                        payload["client_id"]
                    ))
                    db_conn.commit()
                    saved_count += 1
                    
            except Exception as e:
                logger.error(f"Error saving payload for {payload['outbound_reference']}: {str(e)}")
                db_conn.rollback()
                continue
        
        logger.info(f"Successfully saved {saved_count} payloads to database")
        return saved_count
        
    except Exception as e:
        logger.error(f"Error saving payloads to database: {str(e)}")
        raise

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Create order clean payload from outbound tables')
    parser.add_argument('--start-date', required=True, type=str, 
                       help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', required=True, type=str, 
                       help='End date (YYYY-MM-DD)')
    parser.add_argument('--warehouse-id', required=True, type=str,
                       help='Warehouse ID to filter data')
    
    args = parser.parse_args()
    
    # Validate date format
    try:
        start_date = datetime.strptime(args.start_date, '%Y-%m-%d').date()
        end_date = datetime.strptime(args.end_date, '%Y-%m-%d').date()
    except ValueError as e:
        print(f"Invalid date format: {e}")
        return 1
    
    logger = setup_logging()
    
    logger.info("Starting order clean payload creation process")
    logger.info(f"Date range: {start_date} to {end_date}")
    logger.info(f"Warehouse ID: {args.warehouse_id}")
    
    db_conn = None
    
    try:
        # Connect to database B only
        db_conn = get_db_connection('B')
        logger.info("Connected to Database B successfully")
        
        # Create table if not exists
        create_order_clean_payload_table(db_conn)
        
        # Get DO numbers from order_main first
        do_numbers = get_do_numbers_from_order_main(
            db_conn, args.warehouse_id, start_date, end_date, logger
        )
        
        if not do_numbers:
            logger.warning("No DO numbers found in order_main for the specified criteria")
            return 0
        
        # Get outbound data based on DO numbers
        documents, items, conversions = get_outbound_data_by_do_numbers(
            db_conn, do_numbers, logger
        )
        
        if not documents:
            logger.warning("No matching outbound documents found for the DO numbers")
            return 0
        
        # Build JSON payloads
        payloads = build_json_payload(documents, items, conversions, logger)
        
        # Save to database B
        saved_count = save_payloads_to_database(db_conn, payloads, logger)
        
        logger.info(f"Order clean payload creation completed successfully!")
        logger.info(f"Total documents processed: {len(documents)}")
        logger.info(f"Total items processed: {len(items)}")
        logger.info(f"Total conversions processed: {len(conversions)}")
        logger.info(f"Total payloads saved: {saved_count}")
        
        return 0
        
    except Exception as e:
        logger.error(f"Error in main process: {str(e)}")
        return 1
        
    finally:
        if db_conn:
            db_conn.close()
            logger.info("Database B connection closed")

if __name__ == "__main__":
    exit(main()) 