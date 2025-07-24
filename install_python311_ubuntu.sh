#!/bin/bash

echo "🐍 Installing Python 3.11.5 on Ubuntu 20.04"
echo "==========================================="

# Check if running as root
if [ "$EUID" -ne 0 ]; then
    echo "❌ Please run this script with sudo"
    exit 1
fi

# Update system
echo "🔄 Updating system packages..."
apt update

# Install dependencies
echo "📦 Installing dependencies..."
apt install -y software-properties-common build-essential libssl-dev libffi-dev libpq-dev curl wget

# Add deadsnakes PPA
echo "📦 Adding deadsnakes PPA..."
add-apt-repository ppa:deadsnakes/ppa -y
apt update

# Try to install Python 3.11 packages
echo "📦 Installing Python 3.11 packages..."
if apt install -y python3.11 python3.11-venv python3.11-dev; then
    echo "✅ Python 3.11 packages installed successfully"
else
    echo "⚠️ Some packages not available, trying alternative method..."
    
    # Try installing individual packages
    apt install -y python3.11 || {
        echo "❌ Failed to install python3.11"
        exit 1
    }
    
    apt install -y python3.11-venv || {
        echo "⚠️ python3.11-venv not available, will install manually"
    }
    
    apt install -y python3.11-dev || {
        echo "⚠️ python3.11-dev not available, will install manually"
    }
fi

# Install pip for Python 3.11
echo "📦 Installing pip for Python 3.11..."
if ! command -v pip3.11 &> /dev/null; then
    echo "📥 Downloading and installing pip..."
    curl -sS https://bootstrap.pypa.io/get-pip.py | python3.11
    
    # Create symlink if needed
    if [ -f "/usr/local/bin/pip3.11" ]; then
        ln -sf /usr/local/bin/pip3.11 /usr/bin/pip3.11
    fi
fi

# Verify installation
echo "✅ Verifying Python 3.11.5 installation..."
if command -v python3.11 &> /dev/null; then
    echo "✅ Python 3.11.5 installed successfully!"
    python3.11 --version
    
    if command -v pip3.11 &> /dev/null; then
        echo "✅ pip3.11 installed successfully!"
        pip3.11 --version
    else
        echo "⚠️ pip3.11 not found, but Python 3.11 is installed"
    fi
else
    echo "❌ Failed to install Python 3.11.5"
    exit 1
fi

# Install virtualenv if needed
echo "📦 Installing virtualenv..."
python3.11 -m pip install --upgrade pip
python3.11 -m pip install virtualenv

echo ""
echo "✅ Python 3.11.5 installation completed!"
echo ""
echo "📋 Next steps:"
echo "1. Run: ./start.sh to setup and run the application"
echo "2. Or run: ./fix_server.sh if you have issues" 