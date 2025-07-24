#!/bin/bash

echo "Order Cleaning Application"
echo "========================="

# Check if .env file exists
if [ ! -f .env ]; then
    echo "Error: .env file not found!"
    echo "Please copy env_example.txt to .env and configure your database settings."
    exit 1
fi

# Check if virtual environment exists
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    if command -v python3.11 &> /dev/null; then
        python3.11 -m venv venv
        echo "✅ Created virtual environment with Python 3.11.5"
    elif command -v python3.9 &> /dev/null; then
        python3.9 -m venv venv
        echo "✅ Created virtual environment with Python 3.9"
    elif command -v python3 &> /dev/null; then
        python3 -m venv venv
        echo "✅ Created virtual environment with Python 3"
    else
        echo "❌ Error: Python3 not found. Please install Python 3.8 or higher."
        exit 1
    fi
fi

# Activate virtual environment
echo "Activating virtual environment..."
if [ -f "venv/bin/activate" ]; then
    source venv/bin/activate
    echo "✅ Virtual environment activated"
else
    echo "❌ Error: Virtual environment activation failed"
    exit 1
fi

# Install dependencies
echo "Installing dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

# Create tables
echo "Creating database tables..."
python scripts/create_tables.py

# Start the application
echo "Starting Order Cleaning API Server..."
echo "API Documentation: http://localhost:8000/docs"
echo "Press Ctrl+C to stop the server"
python run.py 