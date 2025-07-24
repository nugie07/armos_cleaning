#!/bin/bash

echo "🐍 Upgrading Python to 3.11.5"
echo "============================="

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

# Function to install Python 3.11.5
install_python_311() {
    echo "🔧 Installing Python 3.11.5..."
    
    if [[ "$OS" == *"Ubuntu"* ]] || [[ "$OS" == *"Debian"* ]]; then
        # Ubuntu/Debian
        echo "📦 Installing on Ubuntu/Debian..."
        sudo apt update
        sudo apt install -y software-properties-common
        sudo add-apt-repository ppa:deadsnakes/ppa -y
        sudo apt update
        sudo apt install -y python3.11 python3.11-venv python3.11-dev python3.11-pip python3.11-distutils
        
        # Install additional dependencies
        sudo apt install -y build-essential libssl-dev libffi-dev libpq-dev
        
    elif [[ "$OS" == *"CentOS"* ]] || [[ "$OS" == *"Red Hat"* ]]; then
        # CentOS/RHEL
        echo "📦 Installing on CentOS/RHEL..."
        sudo yum update -y
        sudo yum install -y epel-release
        sudo yum groupinstall -y "Development Tools"
        sudo yum install -y openssl-devel libffi-devel bzip2-devel libffi-devel
        
        # Install Python 3.11 from source
        cd /tmp
        wget https://www.python.org/ftp/python/3.11.5/Python-3.11.5.tgz
        tar -xzf Python-3.11.5.tgz
        cd Python-3.11.5
        ./configure --enable-optimizations
        make -j$(nproc)
        sudo make altinstall
        
        # Create symlink
        sudo ln -sf /usr/local/bin/python3.11 /usr/bin/python3.11
        sudo ln -sf /usr/local/bin/pip3.11 /usr/bin/pip3.11
        
    elif [[ "$OS" == *"Amazon Linux"* ]]; then
        # Amazon Linux
        echo "📦 Installing on Amazon Linux..."
        sudo yum update -y
        sudo yum groupinstall -y "Development Tools"
        sudo yum install -y openssl-devel libffi-devel bzip2-devel libffi-devel
        
        # Install Python 3.11 from source
        cd /tmp
        wget https://www.python.org/ftp/python/3.11.5/Python-3.11.5.tgz
        tar -xzf Python-3.11.5.tgz
        cd Python-3.11.5
        ./configure --enable-optimizations
        make -j$(nproc)
        sudo make altinstall
        
        # Create symlink
        sudo ln -sf /usr/local/bin/python3.11 /usr/bin/python3.11
        sudo ln -sf /usr/local/bin/pip3.11 /usr/bin/pip3.11
        
    else
        echo "❌ Unsupported OS: $OS"
        exit 1
    fi
}

# Install Python 3.11.5
install_python_311

# Verify installation
echo "✅ Verifying Python 3.11.5 installation..."
python3.11 --version

if [ $? -eq 0 ]; then
    echo "✅ Python 3.11.5 installed successfully!"
else
    echo "❌ Failed to install Python 3.11.5"
    exit 1
fi

# Upgrade pip
echo "⬆️ Upgrading pip..."
python3.11 -m pip install --upgrade pip

# Install virtualenv
echo "📦 Installing virtualenv..."
python3.11 -m pip install virtualenv

echo ""
echo "✅ Python 3.11.5 upgrade completed!"
echo ""
echo "📋 Next steps:"
echo "1. Run: ./setup_python311.sh to setup the application with Python 3.11.5"
echo "2. Or manually:"
echo "   - python3.11 -m venv venv311"
echo "   - source venv311/bin/activate"
echo "   - pip install -r requirements.txt" 