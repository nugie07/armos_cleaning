#!/bin/bash

echo "🛠️ Order Cleaning CLI Tool - Python 3.11.5"
echo "=========================================="

# Check if Python 3.11.5 virtual environment exists
if [ ! -d "venv311" ]; then
    echo "❌ Python 3.11.5 virtual environment not found!"
    echo "Please run ./setup_python311.sh first."
    exit 1
fi

# Check .env file
if [ ! -f .env ]; then
    echo "Error: .env file not found!"
    echo "Please copy env_example.txt to .env and configure your database settings."
    exit 1
fi

# Activate virtual environment
echo "🔌 Activating Python 3.11.5 virtual environment..."
source venv311/bin/activate

if [ $? -ne 0 ]; then
    echo "❌ Failed to activate virtual environment"
    exit 1
fi

echo "✅ Virtual environment activated"
echo "Python version: $(python --version)"

# Check if requirements are installed
if ! python -c "import click, requests" 2>/dev/null; then
    echo "⚠️ Installing missing packages..."
    pip install -r requirements_python311.txt
fi

# Start CLI tool
echo "🛠️ Starting CLI tool..."
python cli/main.py "$@" 