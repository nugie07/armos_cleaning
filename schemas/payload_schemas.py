from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime

# Schema for conversion data
class ConversionSchema(BaseModel):
    uom: str
    numerator: float
    denominator: float

# Schema for item data
class ItemSchema(BaseModel):
    warehouse_id: str
    line_id: str
    product_id: str
    product_description: str
    group_id: str
    group_description: str
    product_type: str
    qty: float
    uom: str
    pack_id: str
    product_net_price: float
    conversion: List[ConversionSchema]
    image_url: List[str]

# Schema for main payload
class OrderPayloadSchema(BaseModel):
    warehouse_id: str
    client_id: str
    outbound_reference: str
    divisi: str
    faktur_date: str
    request_delivery_date: str
    origin_name: str
    origin_address_1: str
    origin_address_2: str
    origin_city: str
    origin_phone: str
    origin_email: str
    destination_id: str
    destination_name: str
    destination_address_1: str
    destination_address_2: str
    destination_city: str
    destination_zip_code: str
    destination_phone: str
    destination_email: str
    order_type: str
    items: List[ItemSchema]

# Schema for date range request
class DateRangeRequest(BaseModel):
    start_date: str
    end_date: str

# Schema for discrepancy result
class DiscrepancyResult(BaseModel):
    do_number: str
    db_a_count: int
    db_b_count: int
    discrepancy_count: int
    warehouse_id: Optional[str] = None
    client_id: Optional[str] = None

# Schema for cleaning response
class CleaningResponse(BaseModel):
    message: str
    discrepancies: List[DiscrepancyResult]
    total_discrepancies: int

# Schema for payload creation response
class PayloadCreationResponse(BaseModel):
    message: str
    do_number: str
    payload_data: dict
    status: str 