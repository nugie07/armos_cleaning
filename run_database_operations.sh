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

# Copy product data
copy_products() {
    log "Copying product data from Database A to Database B..."
    if python3 copy_product_data.py --validate; then
        success "Product data copied successfully"
    else
        error "Failed to copy product data"
        exit 1
    fi
}

# Copy order data with date range
copy_orders() {
    local start_date=$1
    local end_date=$2
    
    if [ -z "$start_date" ] || [ -z "$end_date" ]; then
        warning "No date range provided for order copy. Skipping order data copy."
        warning "To copy order data, run: ./run_database_operations.sh --copy-orders YYYY-MM-DD YYYY-MM-DD"
        return 0
    fi
    
    log "Copying order data from Database A to Database B (${start_date} to ${end_date})..."
    if python3 copy_order_data.py --start-date "$start_date" --end-date "$end_date"; then
        success "Order data copied successfully"
    else
        error "Failed to copy order data"
        exit 1
    fi
}

# Show usage
show_usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --setup-only              Only setup environment and create tables"
    echo "  --copy-products           Copy product data only"
    echo "  --copy-orders START END   Copy order data with date range (YYYY-MM-DD YYYY-MM-DD)"
    echo "  --copy-all START END      Copy all data (products + orders with date range)"
    echo "  --help                    Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 --setup-only"
    echo "  $0 --copy-products"
    echo "  $0 --copy-orders 2024-01-01 2024-01-31"
    echo "  $0 --copy-all 2024-01-01 2024-01-31"
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
        --copy-orders)
            if [ -z "$2" ] || [ -z "$3" ]; then
                error "Start date and end date are required for order copy"
                show_usage
                exit 1
            fi
            create_tables
            copy_orders "$2" "$3"
            success "Order copy completed successfully"
            ;;
        --copy-all)
            if [ -z "$2" ] || [ -z "$3" ]; then
                error "Start date and end date are required for full copy"
                show_usage
                exit 1
            fi
            create_tables
            copy_products
            copy_orders "$2" "$3"
            success "All operations completed successfully"
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