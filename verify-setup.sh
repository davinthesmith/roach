#!/bin/bash

# ROACH Setup Verification Script

echo "=================================="
echo "ROACH System Verification"
echo "=================================="
echo ""

# Check Docker
echo "Checking Docker..."
if command -v docker &> /dev/null; then
    echo "✓ Docker installed: $(docker --version)"
else
    echo "✗ Docker not found - please install Docker"
    exit 1
fi

# Check Docker Compose
echo "Checking Docker Compose..."
if command -v docker-compose &> /dev/null; then
    echo "✓ Docker Compose installed: $(docker-compose --version)"
elif docker compose version &> /dev/null; then
    echo "✓ Docker Compose installed: $(docker compose version)"
else
    echo "✗ Docker Compose not found - please install Docker Compose"
    exit 1
fi

# Check SSL Certificates
echo ""
echo "Checking SSL Certificates..."
CERT_PATH="../lets-encrypt/letsencrypt/live/toomanyprojects.dev"
if [ -f "$CERT_PATH/fullchain.pem" ] && [ -f "$CERT_PATH/privkey.pem" ]; then
    echo "✓ SSL certificates found"
    echo "  - fullchain.pem: $(stat -f%z "$CERT_PATH/fullchain.pem" 2>/dev/null || stat -c%s "$CERT_PATH/fullchain.pem" 2>/dev/null) bytes"
    echo "  - privkey.pem: $(stat -f%z "$CERT_PATH/privkey.pem" 2>/dev/null || stat -c%s "$CERT_PATH/privkey.pem" 2>/dev/null) bytes"
else
    echo "✗ SSL certificates not found at $CERT_PATH"
    echo "  Please ensure Let's Encrypt certificates are available"
fi

# Check .env file
echo ""
echo "Checking Configuration..."
if [ -f ".env" ]; then
    echo "✓ .env file exists"
    
    # Check for required variables (without showing values)
    if grep -q "WEATHERLINK_API_KEY=" .env && ! grep -q "your_api_key_here" .env; then
        echo "✓ WEATHERLINK_API_KEY configured"
    else
        echo "✗ WEATHERLINK_API_KEY not configured"
    fi
    
    if grep -q "WEATHERLINK_API_SECRET=" .env && ! grep -q "your_api_secret_here" .env; then
        echo "✓ WEATHERLINK_API_SECRET configured"
    else
        echo "✗ WEATHERLINK_API_SECRET not configured"
    fi
    
    if grep -q "WEATHERLINK_STATION_ID=" .env && ! grep -q "your_station_id_here" .env; then
        echo "✓ WEATHERLINK_STATION_ID configured"
    else
        echo "✗ WEATHERLINK_STATION_ID not configured"
    fi
else
    echo "✗ .env file not found"
    echo "  Copy .env.example to .env and configure your credentials:"
    echo "  cp .env.example .env"
    echo "  nano .env"
fi

# Check Go (optional)
echo ""
echo "Checking Go (optional for local development)..."
if command -v go &> /dev/null; then
    echo "✓ Go installed: $(go version)"
else
    echo "ℹ Go not installed (not required for Docker deployment)"
fi

echo ""
echo "=================================="
echo "File Structure:"
echo "=================================="
find . -type f ! -path './.git/*' ! -name '*.example' | grep -v "postman" | sort

echo ""
echo "=================================="
echo "Next Steps:"
echo "=================================="
echo "1. Configure .env file with your WeatherLink credentials"
echo "2. Start services: docker-compose up -d"
echo "3. View logs: docker-compose logs -f"
echo "4. Access Kafka UI: http://localhost:8080"
echo ""
echo "For detailed instructions, see README.md"
echo "=================================="
