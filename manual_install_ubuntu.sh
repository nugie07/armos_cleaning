#!/bin/bash

echo "🐍 Manual Python 3.11.5 Installation for Ubuntu 20.04"
echo "====================================================="

# Check if running as root
if [ "$EUID" -ne 0 ]; then
    echo "❌ Please run this script with sudo"
    exit 1
fi

echo "📋 This script will install Python 3.11.5 step by step"
echo "Press Enter to continue or Ctrl+C to cancel..."
read

# Step 1: Update system
echo "🔄 Step 1: Updating system packages..."
apt update

# Step 2: Install dependencies
echo "📦 Step 2: Installing dependencies..."
apt install -y software-properties-common build-essential libssl-dev libffi-dev libpq-dev curl wget

# Step 3: Add deadsnakes PPA
echo "📦 Step 3: Adding deadsnakes PPA..."
add-apt-repository ppa:deadsnakes/ppa -y
apt update

# Step 4: Check available Python versions
echo "🔍 Step 4: Checking available Python versions..."
apt search python3.11 | grep -E "^python3\.11"

# Step 5: Install Python 3.11
echo "📦 Step 5: Installing Python 3.11..."
apt install -y python3.11

# Step 6: Install Python 3.11 venv
echo "📦 Step 6: Installing Python 3.11 venv..."
apt install -y python3.11-venv || {
    echo "⚠️ python3.11-venv not available, will install manually"
}

# Step 7: Install Python 3.11 dev
echo "📦 Step 7: Installing Python 3.11 dev..."
apt install -y python3.11-dev || {
    echo "⚠️ python3.11-dev not available, will install manually"
}

# Step 8: Install pip for Python 3.11
echo "📦 Step 8: Installing pip for Python 3.11..."
if ! command -v pip3.11 &> /dev/null; then
    echo "📥 Downloading and installing pip..."
    curl -sS https://bootstrap.pypa.io/get-pip.py | python3.11
    
    # Create symlink if needed
    if [ -f "/usr/local/bin/pip3.11" ]; then
        ln -sf /usr/local/bin/pip3.11 /usr/bin/pip3.11
        echo "✅ Created symlink for pip3.11"
    fi
fi

# Step 9: Verify installation
echo "✅ Step 9: Verifying installation..."
if command -v python3.11 &> /dev/null; then
    echo "✅ Python 3.11.5 installed successfully!"
    python3.11 --version
    
    if command -v pip3.11 &> /dev/null; then
        echo "✅ pip3.11 installed successfully!"
        pip3.11 --version
    else
        echo "⚠️ pip3.11 not found, but Python 3.11 is installed"
        echo "You can use: python3.11 -m pip instead"
    fi
else
    echo "❌ Failed to install Python 3.11.5"
    exit 1
fi

# Step 10: Install virtualenv
echo "📦 Step 10: Installing virtualenv..."
python3.11 -m pip install --upgrade pip
python3.11 -m pip install virtualenv

echo ""
echo "✅ Python 3.11.5 installation completed!"
echo ""
echo "📋 Next steps:"
echo "1. Exit root: exit"
echo "2. Run: ./start.sh to setup and run the application"
echo "3. Or run: ./fix_server.sh if you have issues" 