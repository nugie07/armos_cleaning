#!/bin/bash

echo "🚀 Setting up Order Cleaning Application with Python 3.11.5"
echo "=========================================================="

# Check if Python 3.11.5 is installed
if ! command -v python3.11 &> /dev/null; then
    echo "❌ Python 3.11.5 not found. Please run ./upgrade_python.sh first."
    exit 1
fi

echo "✅ Python 3.11.5 found: $(python3.11 --version)"

# Check current directory
if [ ! -f "requirements.txt" ]; then
    echo "❌ Error: requirements.txt not found. Please run this script from the armos_cleaning directory."
    exit 1
fi

# Remove existing venv if exists
if [ -d "venv" ]; then
    echo "🗑️ Removing existing virtual environment..."
    rm -rf venv
fi

if [ -d "venv311" ]; then
    echo "🗑️ Removing existing Python 3.11 virtual environment..."
    rm -rf venv311
fi

# Create new virtual environment with Python 3.11.5
echo "🔧 Creating virtual environment with Python 3.11.5..."
python3.11 -m venv venv311

if [ $? -ne 0 ]; then
    echo "❌ Failed to create virtual environment with Python 3.11.5"
    echo "Trying alternative method..."
    python3.11 -m venv --without-pip venv311
    source venv311/bin/activate
    curl https://bootstrap.pypa.io/get-pip.py | python
else
    echo "✅ Virtual environment created successfully"
fi

# Activate virtual environment
echo "🔌 Activating virtual environment..."
source venv311/bin/activate

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

# Update requirements.txt for Python 3.11.5 compatibility
echo "📝 Updating requirements for Python 3.11.5 compatibility..."
cat > requirements_python311.txt << 'EOF'
fastapi>=0.104.1
uvicorn[standard]>=0.24.0
sqlalchemy>=2.0.23
psycopg2-binary>=2.9.9
python-dotenv>=1.0.0
pydantic>=2.5.0
python-multipart>=0.0.6
click>=8.1.7
requests>=2.31.0
typing-extensions>=4.8.0
EOF

# Install requirements
echo "📦 Installing requirements for Python 3.11.5..."
pip install -r requirements_python311.txt

if [ $? -ne 0 ]; then
    echo "❌ Failed to install requirements"
    echo "Trying to install system dependencies..."
    if command -v apt &> /dev/null; then
        sudo apt install -y python3.11-dev build-essential libpq-dev
    elif command -v yum &> /dev/null; then
        sudo yum install -y python3.11-devel gcc postgresql-devel
    fi
    pip install -r requirements_python311.txt
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

# Create symlink for easy access
echo "🔗 Creating symlink for easy access..."
ln -sf venv311 venv

echo ""
echo "✅ Setup with Python 3.11.5 completed successfully!"
echo ""
echo "📋 Next steps:"
echo "1. Edit .env file with your database configuration"
echo "2. Run the application: ./start_python311.sh"
echo "3. Or use CLI: ./cli_tool_python311.sh interactive-mode"
echo ""
echo "🌐 API Documentation: http://localhost:8000/docs"
echo ""
echo "🐍 Python version: $(python --version)"
echo "📦 Installed packages:"
pip list --format=columns 