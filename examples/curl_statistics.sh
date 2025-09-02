#!/bin/bash
"""
Direct cURL Script for Data Product Statistics Endpoint

This shell script demonstrates how to make a direct cURL request to the
data product statistics endpoint.

Usage:
  ./curl_statistics.sh [DATA_PRODUCT_ID]

If no DATA_PRODUCT_ID is provided, it will use the default example ID.

Environment Variables Required:
  STARBURST_HOST     - Your Starburst host (e.g., my-cluster.starburst.io)
  STARBURST_AUTH_METHOD - Authentication method: 'basic' or 'oauth'

For Basic Authentication:
  STARBURST_USERNAME - Your username
  STARBURST_PASSWORD - Your password

For OAuth Authentication:
  STARBURST_JWT_TOKEN - Your OAuth JWT token

Optional Environment Variables:
  STARBURST_PROTOCOL   - Protocol (https or http, defaults to https)
  STARBURST_VERIFY_SSL - Set to 'false' to disable SSL verification

Examples:
  # Basic Authentication
  export STARBURST_HOST="my-cluster.starburst.io"
  export STARBURST_AUTH_METHOD="basic"
  export STARBURST_USERNAME="myuser"
  export STARBURST_PASSWORD="mypass"
  ./curl_statistics.sh 40b29ec1-9eeb-48a0-9799-c71c1651bb37

  # OAuth Authentication
  export STARBURST_HOST="my-cluster.starburst.io"
  export STARBURST_AUTH_METHOD="oauth"
  export STARBURST_JWT_TOKEN="your-jwt-token-here"
  ./curl_statistics.sh 40b29ec1-9eeb-48a0-9799-c71c1651bb37
  
  # OAuth Authentication (get token from your OAuth provider)
  export STARBURST_HOST="my-cluster.starburst.io"
  export STARBURST_AUTH_METHOD="oauth"
  export STARBURST_JWT_TOKEN="your-actual-jwt-token-here"
  ./curl_statistics.sh 40b29ec1-9eeb-48a0-9799-c71c1651bb37
"""

set -e  # Exit on any error

# Default values
DEFAULT_DATA_PRODUCT_ID="40b29ec1-9eeb-48a0-9799-c71c1651bb37"
PROTOCOL=${STARBURST_PROTOCOL:-https}
VERIFY_SSL=${STARBURST_VERIFY_SSL:-true}
AUTH_METHOD=${STARBURST_AUTH_METHOD:-basic}


# Function to display usage
usage() {
    echo "Usage: $0 [DATA_PRODUCT_ID]"
    echo ""
    echo "Environment Variables Required:"
    echo "  STARBURST_HOST        - Your Starburst host"
    echo "  STARBURST_AUTH_METHOD - Authentication method: 'basic' or 'oauth'"
    echo ""
    echo "For Basic Authentication:"
    echo "  STARBURST_USERNAME - Your username" 
    echo "  STARBURST_PASSWORD - Your password"
    echo ""
    echo "For OAuth Authentication:"
    echo "  STARBURST_JWT_TOKEN - Your OAuth JWT token"
    echo ""
    echo "Optional Environment Variables:"
    echo "  STARBURST_PROTOCOL   - Protocol (https or http, default: https)"
    echo "  STARBURST_VERIFY_SSL - Set to 'false' to disable SSL verification"
    echo ""
    echo "Examples:"
    echo "  # Basic Authentication"
    echo "  export STARBURST_HOST=\"my-cluster.starburst.io\""
    echo "  export STARBURST_AUTH_METHOD=\"basic\""
    echo "  export STARBURST_USERNAME=\"myuser\""
    echo "  export STARBURST_PASSWORD=\"mypass\""
    echo "  $0 40b29ec1-9eeb-48a0-9799-c71c1651bb37"
    echo ""
    echo "  # OAuth Authentication"
    echo "  export STARBURST_HOST=\"my-cluster.starburst.io\""
    echo "  export STARBURST_AUTH_METHOD=\"oauth\""
    echo "  export STARBURST_JWT_TOKEN=\"your-jwt-token-here\""
    echo "  $0 40b29ec1-9eeb-48a0-9799-c71c1651bb37"
    exit 1
}

# Check for required environment variables
if [[ -z "$STARBURST_HOST" ]]; then
    echo "❌ Error: STARBURST_HOST environment variable is required"
    usage
fi

# Validate authentication method and required variables
if [[ "$AUTH_METHOD" == "basic" ]]; then
    if [[ -z "$STARBURST_USERNAME" ]]; then
        echo "❌ Error: STARBURST_USERNAME environment variable is required for basic authentication"
        usage
    fi
    
    if [[ -z "$STARBURST_PASSWORD" ]]; then
        echo "❌ Error: STARBURST_PASSWORD environment variable is required for basic authentication"
        usage
    fi
elif [[ "$AUTH_METHOD" == "oauth" ]]; then
    if [[ -z "$STARBURST_JWT_TOKEN" ]]; then
        echo "❌ Error: STARBURST_JWT_TOKEN environment variable is required for OAuth authentication"
        usage
    fi
else
    echo "❌ Error: STARBURST_AUTH_METHOD must be 'basic' or 'oauth'"
    echo "Current value: '$AUTH_METHOD'"
    usage
fi

# Get data product ID from argument or use default
DATA_PRODUCT_ID=${1:-$DEFAULT_DATA_PRODUCT_ID}

# Construct URL
URL="$PROTOCOL://$STARBURST_HOST/api/v1/dataProduct/products/$DATA_PRODUCT_ID/statistics"

# Create authentication header based on method
if [[ "$AUTH_METHOD" == "basic" ]]; then
    AUTH_HEADER="Authorization: Basic $(echo -n "$STARBURST_USERNAME:$STARBURST_PASSWORD" | base64)"
elif [[ "$AUTH_METHOD" == "oauth" ]]; then
    AUTH_HEADER="Authorization: Bearer $STARBURST_JWT_TOKEN"
fi

# Build curl command
CURL_ARGS=()
CURL_ARGS+=("-H" "$AUTH_HEADER")
CURL_ARGS+=("-H" "Content-Type: application/json")
CURL_ARGS+=("-H" "Accept: application/json")

# Add SSL verification flag if disabled
if [[ "$VERIFY_SSL" == "false" ]]; then
    CURL_ARGS+=("-k")
fi

# Add the URL
CURL_ARGS+=("$URL")

echo "🌐 Data Product Statistics cURL Request"
echo "======================================"
echo "URL: $URL"
echo "Data Product ID: $DATA_PRODUCT_ID"
echo "Host: $STARBURST_HOST"
echo "Authentication Method: $AUTH_METHOD"
if [[ "$AUTH_METHOD" == "basic" ]]; then
    echo "Username: $STARBURST_USERNAME"
elif [[ "$AUTH_METHOD" == "oauth" ]]; then
    echo "JWT Token: ${STARBURST_JWT_TOKEN:0:20}...${STARBURST_JWT_TOKEN: -10}" # Show first 20 and last 10 chars
fi
echo "Protocol: $PROTOCOL"
echo "SSL Verification: $VERIFY_SSL"
echo ""

# Show the curl command that will be executed
echo "📝 Executing cURL command:"
echo "curl \\"
echo "  -H \"$AUTH_HEADER\" \\"
echo "  -H \"Content-Type: application/json\" \\"
echo "  -H \"Accept: application/json\" \\"
if [[ "$VERIFY_SSL" == "false" ]]; then
    echo "  -k \\"
fi
echo "  \"$URL\""
echo ""

# Execute the request
echo "🚀 Making request..."
response=$(curl -s -w "\n%{http_code}" "${CURL_ARGS[@]}")

# Parse response and status code
http_code=$(echo "$response" | tail -n1)
response_body=$(echo "$response" | sed '$d')

echo "📊 Response (HTTP $http_code):"
echo "=================================="

if [[ "$http_code" == "200" ]]; then
    echo "✅ Success!"
    echo ""
    echo "Raw JSON Response:"
    echo "$response_body"
    echo ""
    
    # Try to format JSON if jq is available
    if command -v jq &> /dev/null; then
        echo "📈 Formatted Response:"
        echo "$response_body" | jq '.'
        echo ""
        
        echo "📋 Summary:"
        echo "  Data Product ID: $(echo "$response_body" | jq -r '.dataProductId // "N/A"')"
        echo "  Queries (7 days): $(echo "$response_body" | jq -r '.sevenDayQueryCount // "N/A"')"
        echo "  Queries (30 days): $(echo "$response_body" | jq -r '.thirtyDayQueryCount // "N/A"')"
        echo "  Users (7 days): $(echo "$response_body" | jq -r '.sevenDayUserCount // "N/A"')"
        echo "  Users (30 days): $(echo "$response_body" | jq -r '.thirtyDayUserCount // "N/A"')"
        echo "  Updated At: $(echo "$response_body" | jq -r '.updatedAt // "N/A"')"
    else
        echo "💡 Install 'jq' for formatted JSON output: sudo apt-get install jq"
    fi
else
    echo "❌ Request failed with HTTP $http_code"
    echo "$response_body"
    exit 1
fi

echo ""
echo "🎉 Request completed successfully!"