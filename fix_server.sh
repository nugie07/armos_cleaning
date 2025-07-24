#!/bin/bash

echo "🔧 Fixing Order Cleaning Application on Server"
echo "=============================================="

# Check current directory
echo "Current directory: $(pwd)"

# Check if we're in the right directory
if [ ! -f "requirements.txt" ]; then
    echo "❌ Error: requirements.txt not found. Please run this script from the armos_cleaning directory."
    exit 1
fi

# Check Python version
echo "🐍 Python version:"
python3 --version

# Remove existing venv if corrupted
if [ -d "venv" ]; then
    echo "🗑️ Removing existing virtual environment..."
    rm -rf venv
fi

# Create new virtual environment
echo "🔧 Creating new virtual environment..."
if command -v python3.11 &> /dev/null; then
    python3.11 -m venv venv
    echo "✅ Created virtual environment with Python 3.11.5"
elif command -v python3 &> /dev/null; then
    python3 -m venv venv
    echo "✅ Created virtual environment with Python 3"
else
    echo "❌ Failed to create virtual environment"
    echo "Trying to install python3-venv..."
    if command -v apt &> /dev/null; then
        sudo apt update
        sudo apt install -y python3-venv
        python3 -m venv venv
    elif command -v yum &> /dev/null; then
        sudo yum install -y python3-venv
        python3 -m venv venv
    else
        echo "❌ Cannot install python3-venv. Please install it manually."
        exit 1
    fi
fi

# Activate virtual environment
echo "🔌 Activating virtual environment..."
source venv/bin/activate

if [ $? -ne 0 ]; then
    echo "❌ Failed to activate virtual environment"
    exit 1
fi

echo "✅ Virtual environment activated"
echo "Python path: $(which python)"
echo "Python version: $(python --version)"

# Upgrade pip
echo "⬆️ Upgrading pip..."
pip install --upgrade pip

# Install requirements
echo "📦 Installing requirements..."
pip install -r requirements.txt

if [ $? -ne 0 ]; then
    echo "❌ Failed to install requirements"
    echo "Trying to install system dependencies..."
    if command -v apt &> /dev/null; then
        sudo apt install -y python3-dev build-essential libpq-dev
    elif command -v yum &> /dev/null; then
        sudo yum install -y python3-devel gcc postgresql-devel
    fi
    pip install -r requirements.txt
fi

# Create .env file if not exists
if [ ! -f ".env" ]; then
    echo "📝 Creating .env file..."
    cp env_example.txt .env
    echo "⚠️  Please edit .env file with your database configuration"
fi

# Create tables
echo "🗄️ Creating database tables..."
python scripts/create_tables.py

echo ""
echo "✅ Fix completed successfully!"
echo ""
echo "📋 Next steps:"
echo "1. Edit .env file with your database configuration"
echo "2. Run the application: ./start_server.sh"
echo "3. Or use CLI: ./cli_tool.sh interactive-mode"
echo ""
echo "🌐 API Documentation: http://localhost:8000/docs" 