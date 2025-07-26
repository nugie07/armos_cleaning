#!/usr/bin/env python3
"""
Test script to run copy order data with detailed logging
"""

import os
import sys
import subprocess
import logging

# Load environment variables
from dotenv import load_dotenv
load_dotenv('.env')
load_dotenv('config.env')

def setup_logging():
    """Setup logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('./logs/test_copy_order.log')
        ]
    )
    return logging.getLogger(__name__)

def test_copy_order_data():
    """Test copy order data with detailed logging"""
    logger = setup_logging()
    
    logger.info("=== Testing Copy Order Data ===")
    logger.info("Warehouse ID: 4512")
    logger.info("Date Range: 2025-03-01 to 2025-03-31")
    
    try:
        # Run copy order data script
        cmd = [
            'python3', 'copy_order_data.py',
            '--start-date', '2025-03-01',
            '--end-date', '2025-03-31',
            '--warehouse-id', '4512'
        ]
        
        logger.info(f"Running command: {' '.join(cmd)}")
        
        # Run the command and capture output
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300  # 5 minutes timeout
        )
        
        logger.info(f"Return code: {result.returncode}")
        
        if result.stdout:
            logger.info("=== STDOUT ===")
            logger.info(result.stdout)
        
        if result.stderr:
            logger.info("=== STDERR ===")
            logger.info(result.stderr)
        
        if result.returncode == 0:
            logger.info("✅ Copy order data completed successfully")
        else:
            logger.error("❌ Copy order data failed")
            
        return result.returncode == 0
        
    except subprocess.TimeoutExpired:
        logger.error("❌ Command timed out after 5 minutes")
        return False
    except Exception as e:
        logger.error(f"❌ Error running copy order data: {str(e)}")
        return False

def main():
    """Main function"""
    success = test_copy_order_data()
    
    if success:
        print("✅ Test completed successfully")
        sys.exit(0)
    else:
        print("❌ Test failed")
        sys.exit(1)

if __name__ == "__main__":
    main() 