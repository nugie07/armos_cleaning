from sqlalchemy import Column, Integer, String, DateTime, Text, Float, ForeignKey, JSON
from sqlalchemy.orm import relationship
from config.database import Base
from datetime import datetime

# Reference tables from Database B
class CleansedOutboundDocuments(Base):
    __tablename__ = "cleansed_outbound_documents"
    
    id = Column(Integer, primary_key=True, index=True)
    warehouse_id = Column(String(100))
    client_id = Column(String(100))
    outbound_reference = Column(String(255))  # This corresponds to do_number
    divisi = Column(String(100))
    faktur_date = Column(DateTime)
    request_delivery_date = Column(DateTime)
    origin_name = Column(String(255))
    origin_address_1 = Column(String(255))
    origin_address_2 = Column(String(255))
    origin_city = Column(String(100))
    origin_phone = Column(String(50))
    origin_email = Column(String(100))
    destination_id = Column(String(100))
    destination_name = Column(String(255))
    destination_address_1 = Column(String(255))
    destination_address_2 = Column(String(255))
    destination_city = Column(String(100))
    destination_zip_code = Column(String(20))
    destination_phone = Column(String(50))
    destination_email = Column(String(100))
    order_type = Column(String(50))
    created_date = Column(DateTime, default=datetime.utcnow)
    
    # Relationship
    items = relationship("CleansedOutboundItems", back_populates="document")

class CleansedOutboundItems(Base):
    __tablename__ = "cleansed_outbound_items"
    
    id = Column(Integer, primary_key=True, index=True)
    document_id = Column(Integer, ForeignKey("cleansed_outbound_documents.id"))
    warehouse_id = Column(String(100))
    line_id = Column(String(50))
    product_id = Column(String(100))
    product_description = Column(String(255))
    group_id = Column(String(100))
    group_description = Column(String(255))
    product_type = Column(String(100))
    qty = Column(Float)
    uom = Column(String(50))
    pack_id = Column(String(100))
    product_net_price = Column(Float)
    image_url = Column(Text)  # JSON array as text
    created_date = Column(DateTime, default=datetime.utcnow)
    
    # Relationship
    document = relationship("CleansedOutboundDocuments", back_populates="items")
    conversions = relationship("CleansedOutboundConversions", back_populates="item")

class CleansedOutboundConversions(Base):
    __tablename__ = "cleansed_outbound_conversions"
    
    id = Column(Integer, primary_key=True, index=True)
    item_id = Column(Integer, ForeignKey("cleansed_outbound_items.id"))
    uom = Column(String(50))
    numerator = Column(Float)
    denominator = Column(Float)
    created_date = Column(DateTime, default=datetime.utcnow)
    
    # Relationship
    item = relationship("CleansedOutboundItems", back_populates="conversions")

# Table to store cleaning payload results
class CleaningPayloadResults(Base):
    __tablename__ = "cleaning_payload_results"
    
    id = Column(Integer, primary_key=True, index=True)
    do_number = Column(String(255), index=True)
    warehouse_id = Column(String(100))
    client_id = Column(String(100))
    payload_data = Column(JSON)  # Store the complete payload as JSON
    status = Column(String(50), default="created")  # created, processed, failed
    created_date = Column(DateTime, default=datetime.utcnow)
    processed_date = Column(DateTime)
    notes = Column(Text)
    
    # Additional metadata
    db_a_count = Column(Integer)  # Count from Database A
    db_b_count = Column(Integer)  # Count from Database B
    discrepancy_count = Column(Integer)  # Difference between A and B 