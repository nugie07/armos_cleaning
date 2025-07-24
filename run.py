#!/usr/bin/env python3
"""
Main entry point for Order Cleaning Application
"""
import uvicorn
import sys
import os

# Add current directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from api.main import app

if __name__ == "__main__":
    print("Starting Order Cleaning API Server...")
    print("API Documentation: http://localhost:8000/docs")
    print("Press Ctrl+C to stop the server")
    
    uvicorn.run(
        app, 
        host="0.0.0.0", 
        port=8000,
        reload=True
    ) 