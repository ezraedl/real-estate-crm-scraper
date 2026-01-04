#!/bin/bash
# Test script to verify JWT authentication

# Replace with your actual token from localStorage
TOKEN="your-jwt-token-here"

# Test the scraper API
echo "Testing scraper API authentication..."
curl -X GET "http://localhost:8000/health" \
  -H "Authorization: Bearer $TOKEN" \
  -v

echo -e "\n\nIf you get 401, check:"
echo "1. JWT_SECRET matches between backend and scraper .env files"
echo "2. Token is not expired"
echo "3. Token format is correct (starts with eyJ)"

