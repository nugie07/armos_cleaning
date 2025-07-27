#!/usr/bin/env python3
"""
Script to fill order_detail_main table from outbound data
Strategy:
1. Find DO numbers in order_main without order_detail_main
2. Get data from outbound_documents, outbound_items, outbound_conversions
3. Insert into order_detail_main with specific calculation rules
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
    # Create logs directory if it doesn't exist
    os.makedirs('./logs', exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('./logs/fill_order_detail_main.log')
        ]
    )
    return logging.getLogger(__name__)

def get_db_connection():
    """Get database connection to Database B"""
    try:
        conn = psycopg2.connect(
            host=os.getenv('DB_B_HOST'),
            port=os.getenv('DB_B_PORT'),
            database=os.getenv('DB_B_NAME'),
            user=os.getenv('DB_B_USER'),
            password=os.getenv('DB_B_PASSWORD')
        )
        return conn
    except Exception as e:
        logging.error(f"Failed to connect to database B: {str(e)}")
        raise

def get_orders_without_details(db_conn, warehouse_id, start_date, end_date, logger):
    """Get orders from order_main that don't have order_detail_main records"""
    try:
        query = """
        SELECT DISTINCT om.id, om.do_number, om.faktur_id, om.faktur_date, om.customer_id
        FROM order_main om
        LEFT JOIN order_detail_main odm ON om.id = odm.order_id
        WHERE om.warehouse_id = %s 
        AND om.faktur_date >= %s 
        AND om.faktur_date <= %s
        AND odm.order_id IS NULL
        ORDER BY om.faktur_date, om.do_number
        """
        
        with db_conn.cursor() as cursor:
            cursor.execute(query, (warehouse_id, start_date, end_date))
            results = cursor.fetchall()
            
        logger.info(f"Found {len(results)} orders without order_detail_main records")
        
        # Convert to list of dictionaries for easier handling
        orders = []
        for row in results:
            orders.append({
                'order_id': row[0],
                'do_number': row[1],
                'faktur_id': row[2],
                'faktur_date': row[3],
                'customer_id': row[4]
            })
            
        return orders
        
    except Exception as e:
        logger.error(f"Error getting orders without details: {str(e)}")
        raise

def get_outbound_data_for_orders(db_conn, orders, logger):
    """Get outbound data for the given orders"""
    try:
        # Get faktur_ids from orders
        faktur_ids = [order['faktur_id'] for order in orders]
        
        if not faktur_ids:
            logger.warning("No faktur_ids to process")
            return []
        
        # Get outbound documents
        doc_query = """
        SELECT id, document_reference, origin_id, destination_id, status, created_at
        FROM outbound_documents 
        WHERE document_reference = ANY(%s)
        """
        
        with db_conn.cursor() as cursor:
            cursor.execute(doc_query, (faktur_ids,))
            documents = cursor.fetchall()
            
        logger.info(f"Found {len(documents)} matching outbound documents")
        
        # Get outbound items for these documents
        doc_ids = [doc[0] for doc in documents]
        
        if not doc_ids:
            logger.warning("No outbound document IDs to process")
            return []
        
        item_query = """
        SELECT id, outbound_document_id, product_id, pack_id, line_id, 
               qty, uom, product_net_price, created_at
        FROM outbound_items 
        WHERE outbound_document_id = ANY(%s)
        """
        
        with db_conn.cursor() as cursor:
            cursor.execute(item_query, (doc_ids,))
            items = cursor.fetchall()
            
        logger.info(f"Found {len(items)} outbound items")
        
        # Get outbound conversions for these items
        item_ids = [item[0] for item in items]
        
        if not item_ids:
            logger.warning("No outbound item IDs to process")
            return []
        
        conversion_query = """
        SELECT id, outbound_item_id, numerator, denominator, from_uom, to_uom
        FROM outbound_conversions 
        WHERE outbound_item_id = ANY(%s)
        """
        
        with db_conn.cursor() as cursor:
            cursor.execute(conversion_query, (item_ids,))
            conversions = cursor.fetchall()
            
        logger.info(f"Found {len(conversions)} outbound conversions")
        
        # Build data structure
        outbound_data = []
        
        for doc in documents:
            doc_id = doc[0]
            doc_ref = doc[1]
            
            # Find items for this document
            doc_items = [item for item in items if item[1] == doc_id]
            
            for item in doc_items:
                item_id = item[0]
                
                # Find conversion for this item
                item_conversions = [conv for conv in conversions if conv[1] == item_id]
                conversion = item_conversions[0] if item_conversions else None
                
                outbound_data.append({
                    'document': {
                        'id': doc[0],
                        'document_reference': doc[1],
                        'origin_id': doc[2],
                        'destination_id': doc[3],
                        'status': doc[4],
                        'created_at': doc[5]
                    },
                    'item': {
                        'id': item[0],
                        'outbound_document_id': item[1],
                        'product_id': item[2],
                        'pack_id': item[3],
                        'line_id': item[4],
                        'qty': item[5],
                        'uom': item[6],
                        'product_net_price': item[7],
                        'created_at': item[8]
                    },
                    'conversion': conversion
                })
        
        return outbound_data
        
    except Exception as e:
        logger.error(f"Error getting outbound data: {str(e)}")
        raise

def calculate_quantities(item_data, conversion_data, logger):
    """Calculate quantities based on the rules from the image"""
    try:
        qty = item_data['qty']
        uom = item_data['uom']
        
        # Initialize quantities
        quantity_faktur = qty
        total_pcs = qty
        total_ctn = None
        
        # Rule 1: If UOM is not PCS, calculate using conversion
        if uom.upper() != 'PCS' and conversion_data:
            numerator = conversion_data[2]  # numerator from conversion
            quantity_faktur = qty * numerator
            total_pcs = qty * numerator
            logger.debug(f"UOM {uom} - Applied conversion: qty={qty} * numerator={numerator} = {quantity_faktur}")
        
        # Rule 2: If UOM is CTN, set total_ctn
        if uom.upper() == 'CTN':
            total_ctn = qty
            logger.debug(f"UOM {uom} - Set total_ctn = {total_ctn}")
        
        return {
            'quantity_faktur': quantity_faktur,
            'total_pcs': total_pcs,
            'total_ctn': total_ctn
        }
        
    except Exception as e:
        logger.error(f"Error calculating quantities: {str(e)}")
        raise

def insert_order_detail_records(db_conn, orders, outbound_data, logger):
    """Insert order detail records into order_detail_main"""
    try:
        insert_query = """
        INSERT INTO order_detail_main (
            quantity_faktur, net_price, quantity_wms, quantity_delivery, 
            quantity_loading, quantity_unloading, status, cancel_reason, 
            notes, order_id, product_id, unit_id, pack_id, line_id,
            unloading_latitude, unloading_longitude, origin_uom, origin_qty,
            total_ctn, total_pcs
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        inserted_count = 0
        
        # Create mapping from faktur_id to order_id
        order_mapping = {order['faktur_id']: order['order_id'] for order in orders}
        
        for data in outbound_data:
            try:
                doc_ref = data['document']['document_reference']
                order_id = order_mapping.get(doc_ref)
                
                if not order_id:
                    logger.warning(f"No order_id found for faktur_id: {doc_ref}")
                    continue
                
                # Calculate quantities
                quantities = calculate_quantities(data['item'], data['conversion'], logger)
                
                # Prepare values for insert
                values = (
                    quantities['quantity_faktur'],      # quantity_faktur
                    data['item']['product_net_price'],  # net_price
                    None,                               # quantity_wms
                    None,                               # quantity_delivery
                    None,                               # quantity_loading
                    None,                               # quantity_unloading
                    None,                               # status
                    None,                               # cancel_reason
                    None,                               # notes
                    order_id,                           # order_id
                    data['item']['product_id'],         # product_id
                    None,                               # unit_id
                    data['item']['pack_id'],            # pack_id
                    data['item']['line_id'],            # line_id
                    None,                               # unloading_latitude
                    None,                               # unloading_longitude
                    data['item']['uom'],                # origin_uom
                    data['item']['qty'],                # origin_qty
                    quantities['total_ctn'],            # total_ctn
                    quantities['total_pcs']             # total_pcs
                )
                
                with db_conn.cursor() as cursor:
                    cursor.execute(insert_query, values)
                
                inserted_count += 1
                
                if inserted_count % 100 == 0:
                    logger.info(f"Inserted {inserted_count} order detail records...")
                    
            except Exception as e:
                logger.error(f"Error inserting order detail for faktur_id {doc_ref}: {str(e)}")
                db_conn.rollback()
                continue
        
        db_conn.commit()
        logger.info(f"Successfully inserted {inserted_count} order detail records")
        return inserted_count
        
    except Exception as e:
        logger.error(f"Error inserting order detail records: {str(e)}")
        db_conn.rollback()
        raise

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Fill order_detail_main from outbound data')
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
    
    logger.info("Starting order detail main fill process")
    logger.info(f"Date range: {start_date} to {end_date}")
    logger.info(f"Warehouse ID: {args.warehouse_id}")
    
    db_conn = None
    
    try:
        # Connect to database B
        db_conn = get_db_connection()
        logger.info("Connected to Database B successfully")
        
        # Step 1: Get orders without order_detail_main
        orders = get_orders_without_details(db_conn, args.warehouse_id, start_date, end_date, logger)
        
        if not orders:
            logger.info("No orders found without order_detail_main records")
            return 0
        
        # Step 2: Get outbound data for these orders
        outbound_data = get_outbound_data_for_orders(db_conn, orders, logger)
        
        if not outbound_data:
            logger.info("No outbound data found for the orders")
            return 0
        
        # Step 3: Insert order detail records
        inserted_count = insert_order_detail_records(db_conn, orders, outbound_data, logger)
        
        logger.info(f"Order detail main fill process completed successfully!")
        logger.info(f"Orders processed: {len(orders)}")
        logger.info(f"Outbound records processed: {len(outbound_data)}")
        logger.info(f"Order detail records inserted: {inserted_count}")
        
        return 0
        
    except Exception as e:
        logger.error(f"Process failed: {str(e)}")
        return 1
        
    finally:
        if db_conn:
            db_conn.close()
            logger.info("Database connection closed")

if __name__ == "__main__":
    exit(main()) 