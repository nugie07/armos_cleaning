#!/bin/bash

echo "🚀 Order Cleaning Application - Server Setup"
echo "============================================="

# Detect OS
if [ -f /etc/os-release ]; then
    . /etc/os-release
    OS=$NAME
    VER=$VERSION_ID
else
    echo "❌ Cannot detect OS"
    exit 1
fi

echo "📋 Detected OS: $OS $VER"

# Update system
echo "🔄 Updating system packages..."
if [[ "$OS" == *"Ubuntu"* ]] || [[ "$OS" == *"Debian"* ]]; then
    sudo apt update && sudo apt upgrade -y
    sudo apt install -y python3-pip python3-venv python3-dev build-essential libssl-dev libffi-dev python3-setuptools software-properties-common
    sudo apt install -y libpq-dev postgresql postgresql-contrib
    
    # Install Python 3.9
    sudo add-apt-repository ppa:deadsnakes/ppa -y
    sudo apt update
    sudo apt install -y python3.9 python3.9-venv python3.9-dev python3.9-pip
    
elif [[ "$OS" == *"CentOS"* ]] || [[ "$OS" == *"Red Hat"* ]]; then
    sudo yum update -y
    sudo yum install -y epel-release
    sudo yum install -y python3-pip python3-devel gcc openssl-devel libffi-devel
    sudo yum install -y postgresql-devel postgresql postgresql-contrib
    
    # Install Python 3.9
    sudo yum install -y python39 python39-pip python39-devel
    
elif [[ "$OS" == *"Amazon Linux"* ]]; then
    sudo yum update -y
    sudo yum install -y python3-pip python3-devel gcc openssl-devel libffi-devel
    sudo yum install -y postgresql-devel postgresql postgresql-contrib
    
    # Install Python 3.9
    sudo amazon-linux-extras install python3.9 -y
    
else
    echo "❌ Unsupported OS: $OS"
    exit 1
fi

# Upgrade pip
echo "⬆️ Upgrading pip..."
python3 -m pip install --upgrade pip

# Check Python version
echo "🐍 Python version:"
python3 --version

# Clone repository if not exists
if [ ! -d "armos_cleaning" ]; then
    echo "📥 Cloning repository..."
    git clone https://github.com/nugie07/armos_cleaning.git
fi

cd armos_cleaning

# Create virtual environment
echo "🔧 Creating virtual environment..."
if command -v python3.11 &> /dev/null; then
    python3.11 -m venv venv
    echo "✅ Using Python 3.11.5"
elif command -v python3.9 &> /dev/null; then
    python3.9 -m venv venv
    echo "✅ Using Python 3.9"
else
    python3 -m venv venv
    echo "✅ Using Python 3.8"
fi

# Activate virtual environment
echo "🔌 Activating virtual environment..."
source venv/bin/activate

# Upgrade pip in venv
echo "⬆️ Upgrading pip in virtual environment..."
pip install --upgrade pip

# Install requirements
echo "📦 Installing Python requirements..."
pip install -r requirements.txt

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
echo "✅ Setup completed successfully!"
echo ""
echo "📋 Next steps:"
echo "1. Edit .env file with your database configuration"
echo "2. Run the application: ./start.sh"
echo "3. Or use CLI: ./cli_tool.sh interactive-mode"
echo ""
echo "🌐 API Documentation: http://localhost:8000/docs" 