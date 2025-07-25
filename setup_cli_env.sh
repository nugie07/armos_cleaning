#!/bin/bash

echo "🔧 CLI Environment Setup"
echo "======================="

# Check if .env exists
if [ ! -f ".env" ]; then
    echo "📄 Creating .env file from template..."
    cp env_example.txt .env
    echo "✅ .env file created!"
else
    echo "📄 .env file already exists"
fi

# Get server IP
echo ""
echo "📡 Getting server IP..."
SERVER_IP=$(curl -s ifconfig.me 2>/dev/null || echo "unknown")

echo "🌐 Detected IP: $SERVER_IP"
echo ""

# Ask user for IP
read -p "Enter your server IP (or press Enter to use detected IP): " USER_IP
if [ -n "$USER_IP" ]; then
    SERVER_IP=$USER_IP
fi

echo ""
echo "🔧 Updating .env file..."

# Update API_BASE_URL in .env
if grep -q "API_BASE_URL" .env; then
    # Update existing line
    sed -i "s|API_BASE_URL=.*|API_BASE_URL=http://$SERVER_IP:8000|g" .env
else
    # Add new line
    echo "API_BASE_URL=http://$SERVER_IP:8000" >> .env
fi

echo "✅ .env updated with API_BASE_URL=http://$SERVER_IP:8000"
echo ""
echo "🚀 Now you can use the CLI:"
echo "   ./cli_tool.sh --help"
echo "   ./cli_tool.sh interactive-mode"
echo ""
echo "📝 Don't forget to configure your database settings in .env file!" 