#!/bin/bash

echo "🚀 Setting up Production Environment for GCP"
echo "============================================="

# Install nginx
echo "📦 Installing nginx..."
sudo apt update
sudo apt install -y nginx

# Create nginx configuration
echo "📝 Creating nginx configuration..."
sudo tee /etc/nginx/sites-available/armos_cleaning << EOF
server {
    listen 80;
    server_name _;

    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
    }
}
EOF

# Enable site
echo "🔗 Enabling nginx site..."
sudo ln -sf /etc/nginx/sites-available/armos_cleaning /etc/nginx/sites-enabled/
sudo rm -f /etc/nginx/sites-enabled/default

# Test nginx configuration
echo "🧪 Testing nginx configuration..."
sudo nginx -t

# Start nginx
echo "🚀 Starting nginx..."
sudo systemctl start nginx
sudo systemctl enable nginx

# Create systemd service for the application
echo "📝 Creating systemd service..."
sudo tee /etc/systemd/system/armos-cleaning.service << EOF
[Unit]
Description=Armos Cleaning API
After=network.target

[Service]
Type=simple
User=nugroho
WorkingDirectory=/home/armos_cleaning
Environment=PATH=/home/armos_cleaning/venv/bin
ExecStart=/home/armos_cleaning/venv/bin/python run.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

# Enable and start service
echo "🚀 Starting application service..."
sudo systemctl daemon-reload
sudo systemctl enable armos-cleaning
sudo systemctl start armos-cleaning

echo ""
echo "✅ Production setup completed!"
echo ""
echo "📋 Access URLs:"
echo "API Documentation: http://[EXTERNAL_IP]/docs"
echo "API Root: http://[EXTERNAL_IP]/"
echo ""
echo "📋 Service Commands:"
echo "Check status: sudo systemctl status armos-cleaning"
echo "View logs: sudo journalctl -u armos-cleaning -f"
echo "Restart: sudo systemctl restart armos-cleaning"
echo "Stop: sudo systemctl stop armos-cleaning" 