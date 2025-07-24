from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List
import uvicorn

from config.database import get_db_a, get_db_b
from services.data_comparison_service import DataComparisonService
from schemas.payload_schemas import (
    DateRangeRequest, 
    CleaningResponse, 
    DiscrepancyResult,
    PayloadCreationResponse
)

app = FastAPI(
    title="Order Cleaning API",
    description="API untuk memperbaiki data order yang hilang antara Database A dan Database B",
    version="1.0.0"
)

@app.get("/")
async def root():
    return {"message": "Order Cleaning API is running"}

@app.post("/compare-data", response_model=CleaningResponse)
async def compare_data(
    date_range: DateRangeRequest,
    db_a: Session = Depends(get_db_a),
    db_b: Session = Depends(get_db_b)
):
    """
    Compare data between Database A and Database B for a given date range
    """
    try:
        discrepancies = DataComparisonService.compare_data_by_date_range(
            db_a=db_a,
            db_b=db_b,
            start_date=date_range.start_date,
            end_date=date_range.end_date
        )
        
        return CleaningResponse(
            message=f"Found {len(discrepancies)} discrepancies between Database A and Database B",
            discrepancies=discrepancies,
            total_discrepancies=len(discrepancies)
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error comparing data: {str(e)}")

@app.post("/create-payload/{do_number}", response_model=PayloadCreationResponse)
async def create_payload(
    do_number: str,
    db_a: Session = Depends(get_db_a),
    db_b: Session = Depends(get_db_b)
):
    """
    Create payload for a specific do_number from Database B data
    """
    try:
        # First, get the discrepancy info for this do_number
        # We'll need to get the counts from both databases
        from models.database_a_models import Order, OrderDetail
        from models.database_b_models import CleansedOutboundDocuments, CleansedOutboundItems
        from sqlalchemy import func
        
        # Get count from Database A
        db_a_count = db_a.query(func.count(OrderDetail.order_detail_id)).join(
            Order, OrderDetail.order_id == Order.order_id
        ).filter(Order.do_number == do_number).scalar()
        
        # Get count from Database B
        db_b_count = db_b.query(func.count(CleansedOutboundItems.id)).join(
            CleansedOutboundDocuments, 
            CleansedOutboundItems.document_id == CleansedOutboundDocuments.id
        ).filter(CleansedOutboundDocuments.outbound_reference == do_number).scalar()
        
        discrepancy_count = abs(db_a_count - db_b_count)
        
        # Create payload from Database B
        payload_data = DataComparisonService.create_payload_from_db_b(
            db_b=db_b,
            do_number=do_number
        )
        
        # Save payload result to Database B
        payload_result = DataComparisonService.save_payload_result(
            db_b=db_b,
            do_number=do_number,
            payload_data=payload_data,
            db_a_count=db_a_count,
            db_b_count=db_b_count,
            discrepancy_count=discrepancy_count
        )
        
        return PayloadCreationResponse(
            message=f"Payload created successfully for do_number: {do_number}",
            do_number=do_number,
            payload_data=payload_data,
            status="created"
        )
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creating payload: {str(e)}")

@app.get("/payload-results")
async def get_payload_results(
    db_b: Session = Depends(get_db_b),
    limit: int = 100,
    offset: int = 0
):
    """
    Get list of payload results from Database B
    """
    try:
        from models.database_b_models import CleaningPayloadResults
        
        results = db_b.query(CleaningPayloadResults).offset(offset).limit(limit).all()
        
        return {
            "message": f"Retrieved {len(results)} payload results",
            "results": [
                {
                    "id": result.id,
                    "do_number": result.do_number,
                    "warehouse_id": result.warehouse_id,
                    "client_id": result.client_id,
                    "status": result.status,
                    "created_date": result.created_date,
                    "db_a_count": result.db_a_count,
                    "db_b_count": result.db_b_count,
                    "discrepancy_count": result.discrepancy_count
                }
                for result in results
            ]
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving payload results: {str(e)}")

@app.get("/payload-result/{do_number}")
async def get_payload_result_by_do_number(
    do_number: str,
    db_b: Session = Depends(get_db_b)
):
    """
    Get specific payload result by do_number
    """
    try:
        from models.database_b_models import CleaningPayloadResults
        
        result = db_b.query(CleaningPayloadResults).filter(
            CleaningPayloadResults.do_number == do_number
        ).first()
        
        if not result:
            raise HTTPException(status_code=404, detail=f"Payload result for do_number {do_number} not found")
        
        return {
            "id": result.id,
            "do_number": result.do_number,
            "warehouse_id": result.warehouse_id,
            "client_id": result.client_id,
            "payload_data": result.payload_data,
            "status": result.status,
            "created_date": result.created_date,
            "processed_date": result.processed_date,
            "notes": result.notes,
            "db_a_count": result.db_a_count,
            "db_b_count": result.db_b_count,
            "discrepancy_count": result.discrepancy_count
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving payload result: {str(e)}")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000) 