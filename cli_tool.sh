#!/bin/bash

echo "Order Cleaning CLI Tool"
echo "======================"

# Check if .env file exists
if [ ! -f .env ]; then
    echo "Error: .env file not found!"
    echo "Please copy env_example.txt to .env and configure your database settings."
    exit 1
fi

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

# Activate virtual environment
echo "Activating virtual environment..."
source venv/bin/activate

# Install dependencies
echo "Installing dependencies..."
pip install -r requirements.txt

# Create tables
echo "Creating database tables..."
python scripts/create_tables.py

# Start CLI tool
echo "Starting CLI tool..."
python cli/main.py "$@" 