#!/bin/bash

# Database Operations Script
# This script runs all database operations in sequence

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging function
log() {
    echo -e "${BLUE}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1" >&2
}

success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

# Check if Python is installed
check_python() {
    if ! command -v python3 &> /dev/null; then
        error "Python 3 is not installed. Please install Python 3 first."
        exit 1
    fi
    log "Python 3 found: $(python3 --version)"
}

# Check if pip is installed
check_pip() {
    if ! command -v pip3 &> /dev/null; then
        error "pip3 is not installed. Please install pip3 first."
        exit 1
    fi
    log "pip3 found: $(pip3 --version)"
}

# Install Python dependencies
install_dependencies() {
    log "Installing Python dependencies..."
    if pip3 install -r requirements.txt; then
        success "Dependencies installed successfully"
    else
        error "Failed to install dependencies"
        exit 1
    fi
}

# Check if config file exists
check_config() {
    if [ ! -f "config.env" ]; then
        error "config.env file not found. Please create it with your database configuration."
        exit 1
    fi
    log "Configuration file found"
}

# Create tables
create_tables() {
    log "Creating tables in Database B..."
    if python3 create_tables.py; then
        success "Tables created successfully"
    else
        error "Failed to create tables"
        exit 1
    fi
}

# Copy product data (initial copy)
copy_products() {
    local batch_size=${1:-10000}
    local batch_delay=${2:-30}
    
    log "Copying product data from Database A to Database B (Initial Copy)..."
    log "Batch size: ${batch_size}, Batch delay: ${batch_delay}s"
    if python3 copy_product_data.py --validate --batch-size "$batch_size" --batch-delay "$batch_delay"; then
        success "Product data copied successfully"
    else
        error "Failed to copy product data"
        exit 1
    fi
}

# Copy product data with UPSERT
copy_products_upsert() {
    local batch_size=${1:-10000}
    local batch_delay=${2:-30}
    
    log "Copying product data from Database A to Database B (UPSERT)..."
    log "Batch size: ${batch_size}, Batch delay: ${batch_delay}s"
    if python3 copy_product_data_upsert.py --validate --batch-size "$batch_size" --batch-delay "$batch_delay"; then
        success "Product data UPSERT completed successfully"
    else
        error "Failed to copy product data"
        exit 1
    fi
}

# Copy order data with date range and warehouse filter
copy_orders() {
    local start_date=$1
    local end_date=$2
    local warehouse_id=$3
    
    if [ -z "$start_date" ] || [ -z "$end_date" ] || [ -z "$warehouse_id" ]; then
        warning "No date range or warehouse_id provided for order copy. Skipping order data copy."
        warning "To copy order data, run: ./run_database_operations.sh --copy-orders YYYY-MM-DD YYYY-MM-DD WAREHOUSE_ID"
        return 0
    fi
    
    log "Copying order data from Database A to Database B (${start_date} to ${end_date}, warehouse: ${warehouse_id})..."
    if python3 copy_order_data.py --start-date "$start_date" --end-date "$end_date" --warehouse-id "$warehouse_id"; then
        success "Order data copied successfully"
    else
        error "Failed to copy order data"
        exit 1
    fi
}

# Fill order detail main from outbound data
fill_order_details() {
    local start_date=$1
    local end_date=$2
    local warehouse_id=$3
    
    if [ -z "$start_date" ] || [ -z "$end_date" ] || [ -z "$warehouse_id" ]; then
        error "Start date, end date, and warehouse_id are required for order detail fill"
        warning "To fill order details, run: ./run_database_operations.sh --fill-order-details YYYY-MM-DD YYYY-MM-DD WAREHOUSE_ID"
        return 1
    fi
    
    log "Filling order_detail_main from outbound data (${start_date} to ${end_date}, warehouse: ${warehouse_id})..."
    if python3 fill_order_detail_main.py --start-date "$start_date" --end-date "$end_date" --warehouse-id "$warehouse_id"; then
        success "Order detail fill completed successfully"
    else
        error "Failed to fill order details"
        exit 1
    fi
}



# Show usage
show_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --setup-only              Only setup environment and create tables"
    echo "  --copy-products           Copy product data only (Initial Copy)"
    echo "  --copy-products-batch BATCH_SIZE DELAY  Copy product data with custom batch size and delay"
    echo "  --copy-products-upsert    Copy product data with UPSERT (Update existing)"
    echo "  --copy-products-upsert-batch BATCH_SIZE DELAY  Copy product data with UPSERT and custom batch"
    echo "  --copy-orders START END WAREHOUSE_ID   Copy order data with date range and warehouse filter"
    echo "  --copy-all START END WAREHOUSE_ID      Copy all data (products + orders with date range and warehouse filter)"
    echo "  --fill-order-details START END WAREHOUSE_ID Fill order_detail_main from outbound data"
    echo "  --help                    Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 --setup-only"
    echo "  $0 --copy-products"
    echo "  $0 --copy-products-batch 10000 30"
    echo "  $0 --copy-products-upsert"
    echo "  $0 --copy-products-upsert-batch 10000 30"
    echo "  $0 --copy-orders 2024-01-01 2024-01-31 WAREHOUSE_001"
    echo "  $0 --copy-all 2024-01-01 2024-01-31 WAREHOUSE_001"
    echo "  $0 --fill-order-details 2024-01-01 2024-01-31 WAREHOUSE_001"
}

# Main function
main() {
    log "Starting database operations..."
    
    # Check prerequisites
    check_python
    check_pip
    check_config
    
    # Install dependencies
    install_dependencies
    
    # Parse command line arguments
    case "${1:-}" in
        --setup-only)
            create_tables
            success "Setup completed successfully"
            ;;
        --copy-products)
            create_tables
            copy_products
            success "Product copy completed successfully"
            ;;
        --copy-products-batch)
            if [ -z "$2" ] || [ -z "$3" ]; then
                error "Batch size and delay are required for batch copy"
                show_usage
                exit 1
            fi
            create_tables
            copy_products "$2" "$3"
            success "Product copy completed successfully"
            ;;
        --copy-products-upsert)
            create_tables
            copy_products_upsert
            success "Product UPSERT completed successfully"
            ;;
        --copy-products-upsert-batch)
            if [ -z "$2" ] || [ -z "$3" ]; then
                error "Batch size and delay are required for batch UPSERT"
                show_usage
                exit 1
            fi
            create_tables
            copy_products_upsert "$2" "$3"
            success "Product UPSERT completed successfully"
            ;;
        --copy-orders)
            if [ -z "$2" ] || [ -z "$3" ] || [ -z "$4" ]; then
                error "Start date, end date, and warehouse_id are required for order copy"
                show_usage
                exit 1
            fi
            copy_orders "$2" "$3" "$4"
            success "Order copy completed successfully"
            ;;
        --copy-all)
            if [ -z "$2" ] || [ -z "$3" ] || [ -z "$4" ]; then
                error "Start date, end date, and warehouse_id are required for full copy"
                show_usage
                exit 1
            fi
            create_tables
            copy_products
            copy_orders "$2" "$3" "$4"
            success "All operations completed successfully"
            ;;
        --fill-order-details)
            if [ -z "$2" ] || [ -z "$3" ] || [ -z "$4" ]; then
                error "Start date, end date, and warehouse_id are required for order detail fill"
                show_usage
                exit 1
            fi
            fill_order_details "$2" "$3" "$4"
            success "Order detail fill completed successfully"
            ;;

        --help|-h)
            show_usage
            ;;
        "")
            # Default: setup only
            create_tables
            success "Setup completed successfully"
            log "Use --help to see all available options"
            ;;
        *)
            error "Unknown option: $1"
            show_usage
            exit 1
            ;;
    esac
}

# Run main function
main "$@" 