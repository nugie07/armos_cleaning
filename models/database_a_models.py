from sqlalchemy import Column, Integer, String, DateTime, Text, Float, ForeignKey
from sqlalchemy.orm import relationship
from config.database import Base
from datetime import datetime

class Order(Base):
    __tablename__ = "order"
    
    order_id = Column(Integer, primary_key=True, index=True)
    faktur_id = Column(String(255))
    faktur_date = Column(DateTime)
    delivery_date = Column(DateTime)
    do_number = Column(String(255))
    status = Column(String(50))
    skip_count = Column(Integer)
    created_date = Column(DateTime, default=datetime.utcnow)
    created_by = Column(String(100))
    updated_date = Column(DateTime)
    updated_by = Column(String(100))
    notes = Column(Text)
    customer_id = Column(String(100))
    warehouse_id = Column(String(100))
    delivery_type_id = Column(String(100))
    order_integration_id = Column(String(255))
    origin_name = Column(String(255))
    origin_address_1 = Column(String(255))
    origin_address_2 = Column(String(255))
    origin_city = Column(String(100))
    origin_zipcode = Column(String(20))
    origin_phone = Column(String(50))
    origin_email = Column(String(100))
    destination_name = Column(String(255))
    destination_address_1 = Column(String(255))
    destination_address_2 = Column(String(255))
    destination_city = Column(String(100))
    destination_zip_code = Column(String(20))
    destination_phone = Column(String(50))
    destination_email = Column(String(100))
    client_id = Column(String(100))
    cancel_reason = Column(Text)
    rdo_integration_id = Column(String(255))
    address_change = Column(String(10))
    divisi = Column(String(100))
    pre_status = Column(String(50))
    
    # Relationship
    order_details = relationship("OrderDetail", back_populates="order")

class OrderDetail(Base):
    __tablename__ = "order_detail"
    
    order_detail_id = Column(Integer, primary_key=True, index=True)
    quantity_faktur = Column(Float)
    net_price = Column(Float)
    quantity_wms = Column(Float)
    quantity_delivery = Column(Float)
    quantity_loading = Column(Float)
    quantity_unloading = Column(Float)
    status = Column(String(50))
    cancel_reason = Column(Text)
    notes = Column(Text)
    order_id = Column(Integer, ForeignKey("order.order_id"))
    product_id = Column(String(100))
    unit_id = Column(String(100))
    pack_id = Column(String(100))
    line_id = Column(String(50))
    unloading_latitude = Column(Float)
    unloading_longitude = Column(Float)
    origin_uom = Column(String(50))
    origin_qty = Column(Float)
    total_ctn = Column(Float)
    total_pcs = Column(Float)
    
    # Relationship
    order = relationship("Order", back_populates="order_details") 