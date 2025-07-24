from sqlalchemy.orm import Session
from sqlalchemy import func, and_
from datetime import datetime
from typing import List, Dict, Any
import json

from models.database_a_models import Order, OrderDetail
from models.database_b_models import (
    CleansedOutboundDocuments, 
    CleansedOutboundItems, 
    CleansedOutboundConversions,
    CleaningPayloadResults
)
from schemas.payload_schemas import DiscrepancyResult, OrderPayloadSchema

class DataComparisonService:
    
    @staticmethod
    def compare_data_by_date_range(
        db_a: Session, 
        db_b: Session, 
        start_date: str, 
        end_date: str
    ) -> List[DiscrepancyResult]:
        """
        Compare data between Database A and Database B for a given date range
        """
        try:
            # Parse dates
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
            
            # Get data from Database A
            db_a_orders = db_a.query(
                Order.do_number,
                func.count(OrderDetail.order_detail_id).label('count')
            ).join(
                OrderDetail, Order.order_id == OrderDetail.order_id
            ).filter(
                and_(
                    Order.faktur_date >= start_dt,
                    Order.faktur_date <= end_dt
                )
            ).group_by(Order.do_number).all()
            
            # Get data from Database B
            db_b_orders = db_b.query(
                CleansedOutboundDocuments.outbound_reference,
                func.count(CleansedOutboundItems.id).label('count')
            ).join(
                CleansedOutboundItems, 
                CleansedOutboundDocuments.id == CleansedOutboundItems.document_id
            ).filter(
                and_(
                    CleansedOutboundDocuments.faktur_date >= start_dt,
                    CleansedOutboundDocuments.faktur_date <= end_dt
                )
            ).group_by(CleansedOutboundDocuments.outbound_reference).all()
            
            # Create dictionaries for easy lookup
            db_a_dict = {order.do_number: order.count for order in db_a_orders}
            db_b_dict = {order.outbound_reference: order.count for order in db_b_orders}
            
            # Find discrepancies
            discrepancies = []
            all_do_numbers = set(db_a_dict.keys()) | set(db_b_dict.keys())
            
            for do_number in all_do_numbers:
                db_a_count = db_a_dict.get(do_number, 0)
                db_b_count = db_b_dict.get(do_number, 0)
                discrepancy_count = abs(db_a_count - db_b_count)
                
                if discrepancy_count > 0:
                    # Get additional info from Database B
                    db_b_info = db_b.query(CleansedOutboundDocuments).filter(
                        CleansedOutboundDocuments.outbound_reference == do_number
                    ).first()
                    
                    discrepancies.append(DiscrepancyResult(
                        do_number=do_number,
                        db_a_count=db_a_count,
                        db_b_count=db_b_count,
                        discrepancy_count=discrepancy_count,
                        warehouse_id=db_b_info.warehouse_id if db_b_info else None,
                        client_id=db_b_info.client_id if db_b_info else None
                    ))
            
            return discrepancies
            
        except Exception as e:
            print(f"Error comparing data: {str(e)}")
            raise
    
    @staticmethod
    def create_payload_from_db_b(
        db_b: Session, 
        do_number: str
    ) -> Dict[str, Any]:
        """
        Create payload from Database B data for a specific do_number
        """
        try:
            # Get document from Database B
            document = db_b.query(CleansedOutboundDocuments).filter(
                CleansedOutboundDocuments.outbound_reference == do_number
            ).first()
            
            if not document:
                raise ValueError(f"Document with do_number {do_number} not found in Database B")
            
            # Get items for this document
            items = db_b.query(CleansedOutboundItems).filter(
                CleansedOutboundItems.document_id == document.id
            ).all()
            
            payload_items = []
            
            for item in items:
                # Get conversions for this item
                conversions = db_b.query(CleansedOutboundConversions).filter(
                    CleansedOutboundConversions.item_id == item.id
                ).all()
                
                # Parse image_url if it's stored as JSON string
                image_url = []
                if item.image_url:
                    try:
                        image_url = json.loads(item.image_url)
                    except:
                        image_url = [item.image_url] if item.image_url else []
                
                # Create conversion list
                conversion_list = []
                for conv in conversions:
                    conversion_list.append({
                        "uom": conv.uom,
                        "numerator": conv.numerator,
                        "denominator": conv.denominator
                    })
                
                # Create item payload
                item_payload = {
                    "warehouse_id": item.warehouse_id,
                    "line_id": item.line_id,
                    "product_id": item.product_id,
                    "product_description": item.product_description,
                    "group_id": item.group_id,
                    "group_description": item.group_description,
                    "product_type": item.product_type,
                    "qty": item.qty,
                    "uom": item.uom,
                    "pack_id": item.pack_id,
                    "product_net_price": item.product_net_price,
                    "conversion": conversion_list,
                    "image_url": image_url
                }
                
                payload_items.append(item_payload)
            
            # Create main payload
            payload = {
                "warehouse_id": document.warehouse_id,
                "client_id": document.client_id,
                "outbound_reference": document.outbound_reference,
                "divisi": document.divisi,
                "faktur_date": document.faktur_date.strftime("%Y-%m-%d") if document.faktur_date else "",
                "request_delivery_date": document.request_delivery_date.strftime("%Y-%m-%d") if document.request_delivery_date else "",
                "origin_name": document.origin_name,
                "origin_address_1": document.origin_address_1,
                "origin_address_2": document.origin_address_2,
                "origin_city": document.origin_city,
                "origin_phone": document.origin_phone,
                "origin_email": document.origin_email,
                "destination_id": document.destination_id,
                "destination_name": document.destination_name,
                "destination_address_1": document.destination_address_1,
                "destination_address_2": document.destination_address_2,
                "destination_city": document.destination_city,
                "destination_zip_code": document.destination_zip_code,
                "destination_phone": document.destination_phone,
                "destination_email": document.destination_email,
                "order_type": document.order_type,
                "items": payload_items
            }
            
            return payload
            
        except Exception as e:
            print(f"Error creating payload: {str(e)}")
            raise
    
    @staticmethod
    def save_payload_result(
        db_b: Session,
        do_number: str,
        payload_data: Dict[str, Any],
        db_a_count: int,
        db_b_count: int,
        discrepancy_count: int
    ) -> CleaningPayloadResults:
        """
        Save payload result to Database B
        """
        try:
            # Get warehouse_id and client_id from payload
            warehouse_id = payload_data.get("warehouse_id", "")
            client_id = payload_data.get("client_id", "")
            
            # Create new record
            payload_result = CleaningPayloadResults(
                do_number=do_number,
                warehouse_id=warehouse_id,
                client_id=client_id,
                payload_data=payload_data,
                status="created",
                db_a_count=db_a_count,
                db_b_count=db_b_count,
                discrepancy_count=discrepancy_count
            )
            
            db_b.add(payload_result)
            db_b.commit()
            db_b.refresh(payload_result)
            
            return payload_result
            
        except Exception as e:
            db_b.rollback()
            print(f"Error saving payload result: {str(e)}")
            raise 