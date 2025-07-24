#!/bin/bash

echo "🚀 Order Cleaning Application - Python 3.11.5"
echo "============================================="

# Check if .env file exists
if [ ! -f .env ]; then
    echo "Error: .env file not found!"
    echo "Please copy env_example.txt to .env and configure your database settings."
    exit 1
fi

# Check if Python 3.11.5 virtual environment exists
if [ ! -d "venv311" ]; then
    echo "❌ Python 3.11.5 virtual environment not found!"
    echo "Please run ./setup_python311.sh first."
    exit 1
fi

# Check Python 3.11.5 version
echo "🐍 Checking Python 3.11.5..."
python3.11 --version

# Activate virtual environment
echo "🔌 Activating Python 3.11.5 virtual environment..."
source venv311/bin/activate

if [ $? -ne 0 ]; then
    echo "❌ Failed to activate virtual environment"
    exit 1
fi

echo "✅ Virtual environment activated"
echo "Python path: $(which python)"
echo "Python version: $(python --version)"

# Check if requirements are installed
echo "📦 Checking installed packages..."
if ! python -c "import fastapi, sqlalchemy, psycopg2" 2>/dev/null; then
    echo "⚠️ Some packages missing. Installing requirements..."
    pip install -r requirements_python311.txt
fi

# Create tables if needed
echo "🗄️ Checking database tables..."
python scripts/create_tables.py

# Start the application
echo "🚀 Starting Order Cleaning API Server with Python 3.11.5..."
echo "API Documentation: http://localhost:8000/docs"
echo "Press Ctrl+C to stop the server"
python run.py 