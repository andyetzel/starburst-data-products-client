#!/usr/bin/env python3
"""
Direct cURL Example for Data Product Statistics Endpoint

This script demonstrates how to make a direct HTTP request to the data product
statistics endpoint using both Python requests and by generating equivalent
cURL commands that can be run from the command line.

Endpoint: GET /api/v1/dataProduct/products/{dataProductId}/statistics

The script shows:
- How to construct proper authentication headers (Basic Auth and OAuth)
- How to make the HTTP request using Python requests
- How to generate equivalent cURL commands for manual testing
- How to parse and display the JSON response

Supported Authentication Methods:
- Basic Authentication (username/password)
- OAuth2 JWT Bearer tokens

This is useful for:
- Testing the endpoint directly without the client library
- Debugging authentication issues
- Creating shell scripts or other automation
- Understanding the raw HTTP request/response flow
"""

import os
import sys
import base64
import json
import requests
from typing import Dict, Any, Optional

# Add the parent directory to sys.path to import the client
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from starburst_data_products_client.shared.auth_config import get_auth_info, AuthenticationError


def get_basic_auth_header(username: str, password: str) -> Dict[str, str]:
    """Generate basic authentication header."""
    credentials = f"{username}:{password}"
    encoded_credentials = base64.b64encode(credentials.encode()).decode()
    return {
        "Authorization": f"Basic {encoded_credentials}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }


def get_oauth_header(jwt_token: str) -> Dict[str, str]:
    """Generate OAuth Bearer token authentication header."""
    return {
        "Authorization": f"Bearer {jwt_token}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }


def generate_curl_command(host: str, data_product_id: str, headers: Dict[str, str], protocol: str = "https", verify_ssl: bool = True, verbose: bool = False) -> str:
    """Generate equivalent cURL command for the statistics request."""
    url = f"{protocol}://{host}/api/v1/dataProduct/products/{data_product_id}/statistics"
    
    curl_parts = ["curl"]
    
    # Add verbose flag if requested
    if verbose:
        curl_parts.append("-v")
    
    # Add SSL verification flag if disabled
    if not verify_ssl:
        curl_parts.append("-k")
    
    # Add headers with response headers shown
    curl_parts.append("-i")  # Include response headers in output
    
    # Add headers
    for key, value in headers.items():
        curl_parts.append(f'-H "{key}: {value}"')
    
    # Add URL
    curl_parts.append(f'"{url}"')
    
    return " \\\n  ".join(curl_parts)


def make_statistics_request(host: str, data_product_id: str, headers: Dict[str, str], protocol: str = "https", verify_ssl: bool = True) -> Dict[str, Any]:
    """Make the HTTP request to the statistics endpoint."""
    url = f"{protocol}://{host}/api/v1/dataProduct/products/{data_product_id}/statistics"
    
    print(f"🌐 Request Details:")
    print(f"  URL: {url}")
    print(f"  Method: GET")
    print(f"  SSL Verify: {verify_ssl}")
    print()
    
    print(f"📤 Request Headers:")
    for key, value in headers.items():
        if key.lower() == 'authorization':
            # Mask the token for security
            if value.startswith('Bearer '):
                print(f"  {key}: Bearer ***")
            elif value.startswith('Basic '):
                print(f"  {key}: Basic ***")
            else:
                print(f"  {key}: ***")
        else:
            print(f"  {key}: {value}")
    print()
    
    # Make the request
    try:
        response = requests.get(url, headers=headers, verify=verify_ssl)
    except requests.exceptions.SSLError as e:
        raise Exception(f'SSL Error: {e}\nTry setting SSL_VERIFY=false in your .env file if using self-signed certificates')
    except requests.exceptions.ConnectionError as e:
        raise Exception(f'Connection Error: {e}\nCheck if the host {host} is reachable')
    except requests.exceptions.Timeout as e:
        raise Exception(f'Timeout Error: {e}')
    except Exception as e:
        raise Exception(f'Request Error: {e}')
    
    print(f"📥 Response Details:")
    print(f"  Status Code: {response.status_code}")
    print(f"  Reason: {response.reason}")
    print(f"  Content-Type: {response.headers.get('content-type', 'Not specified')}")
    print()
    
    print(f"📥 Response Headers:")
    for key, value in response.headers.items():
        print(f"  {key}: {value}")
    print()
    
    if response.ok:
        try:
            return response.json()
        except ValueError as e:
            raise Exception(f'Response is not valid JSON: {e}\nResponse body: {response.text}')
    else:
        print(f"❌ Error Response Body:")
        print(f"  {response.text}")
        raise Exception(f'Request failed with status {response.status_code} ({response.reason})\nResponse body: {response.text}')


def display_statistics_response(stats_data: Dict[str, Any]):
    """Display the formatted statistics response."""
    print("📊 Statistics Response:")
    print("=" * 50)
    print(json.dumps(stats_data, indent=2, default=str))
    print("=" * 50)
    
    # Parse and display in a more readable format
    print("\n📈 Parsed Statistics:")
    print(f"  Data Product ID: {stats_data.get('dataProductId', 'N/A')}")
    print(f"  Queries (7 days): {stats_data.get('sevenDayQueryCount', 'N/A')}")
    print(f"  Queries (30 days): {stats_data.get('thirtyDayQueryCount', 'N/A')}")
    print(f"  Users (7 days): {stats_data.get('sevenDayUserCount', 'N/A')}")
    print(f"  Users (30 days): {stats_data.get('thirtyDayUserCount', 'N/A')}")
    print(f"  Updated At: {stats_data.get('updatedAt', 'N/A')}")


def main():
    """Main function to demonstrate direct HTTP requests to the statistics endpoint."""
    print("Direct cURL Example for Data Product Statistics")
    print("=" * 55)
    
    # Default data product ID (can be overridden)
    default_data_product_id = "40b29ec1-9eeb-48a0-9799-c71c1651bb37"
    
    # Get data product ID from user or use default
    print(f"\nData Product ID to query (press Enter for default):")
    print(f"Default: {default_data_product_id}")
    user_input = input("Enter Data Product ID: ").strip()
    data_product_id = user_input if user_input else default_data_product_id
    
    print(f"Using Data Product ID: {data_product_id}")
    
    # Load authentication configuration
    try:
        env_file = os.path.join(os.path.dirname(__file__), '.env')
        auth_info = get_auth_info(env_file)
        
        print("\n=== Authentication Configuration ===")
        print(f"Method: {auth_info['method']}")
        print(f"Host: {auth_info['host']}")
        print(f"Protocol: {auth_info['protocol']}")
        print(f"SSL Verify: {auth_info['verify_ssl']}")
        
        # Generate headers based on authentication method
        if auth_info['method'] == 'basic':
            headers = get_basic_auth_header(auth_info['username'], auth_info['password'])
        elif auth_info['method'] == 'oauth2_jwt':
            headers = get_oauth_header(auth_info['jwt_token'])
        else:
            print(f"\n❌ This example supports basic and OAuth2 JWT authentication only.")
            print(f"Current method: {auth_info['method']}")
            print(f"Supported methods: basic, oauth2_jwt")
            sys.exit(1)
        
        print(f"\n=== Generated cURL Commands ===")
        
        # Generate basic curl command
        basic_curl = generate_curl_command(
            auth_info['host'], 
            data_product_id, 
            headers, 
            auth_info['protocol'], 
            auth_info['verify_ssl'],
            verbose=False
        )
        
        # Generate verbose curl command
        verbose_curl = generate_curl_command(
            auth_info['host'], 
            data_product_id, 
            headers, 
            auth_info['protocol'], 
            auth_info['verify_ssl'],
            verbose=True
        )
        
        print("Basic cURL command (with response headers):")
        print(basic_curl)
        print()
        print("Verbose cURL command (with full debugging info):")
        print(verbose_curl)
        
        # Ask if user wants to execute the request
        print(f"\n=== Execute Request ===")
        print(f"Options:")
        print(f"  1. Test statistics endpoint (recommended)")
        print(f"  2. First verify data product exists")
        print(f"  3. List available data products to find valid IDs")
        print(f"  4. Skip execution")
        
        choice = input("Enter your choice (1-4): ").strip()
        
        if choice == '2':
            # Test if data product exists first
            try:
                print(f"\n🔍 Checking if data product exists...")
                dp_url = f"{auth_info['protocol']}://{auth_info['host']}/api/v1/dataProduct/products/{data_product_id}"
                print(f"Testing: {dp_url}")
                
                response = requests.get(dp_url, headers=headers, verify=auth_info['verify_ssl'])
                print(f"Response Status: {response.status_code}")
                
                if response.ok:
                    print(f"✅ Data product exists! Now testing statistics endpoint...")
                    stats_data = make_statistics_request(
                        auth_info['host'],
                        data_product_id,
                        headers,
                        auth_info['protocol'],
                        auth_info['verify_ssl']
                    )
                    display_statistics_response(stats_data)
                    print(f"\n✅ Statistics request completed successfully!")
                else:
                    print(f"❌ Data product not found (HTTP {response.status_code})")
                    print(f"Response: {response.text}")
                    print(f"\n💡 Try using a different data product ID that exists on your cluster.")
                    
            except Exception as e:
                print(f"\n❌ Request failed: {e}")
                
        elif choice == '1':
            # Go straight to statistics endpoint
            try:
                print(f"\n🌐 Making Statistics Request...")
                stats_data = make_statistics_request(
                    auth_info['host'],
                    data_product_id,
                    headers,
                    auth_info['protocol'],
                    auth_info['verify_ssl']
                )
                
                display_statistics_response(stats_data)
                print(f"\n✅ Request completed successfully!")
                
            except Exception as e:
                print(f"\n❌ Statistics request failed: {e}")
                
                # Suggest checking if data product exists
                print(f"\n💡 The statistics endpoint returned 404. This could mean:")
                print(f"   1. The data product ID doesn't exist")
                print(f"   2. The statistics endpoint is not available on this cluster")
                print(f"   3. Your user doesn't have permission to access statistics")
                
                suggest_test = input(f"\nWould you like to test if the data product exists? (y/N): ").strip().lower()
                if suggest_test in ['y', 'yes']:
                    try:
                        dp_url = f"{auth_info['protocol']}://{auth_info['host']}/api/v1/dataProduct/products/{data_product_id}"
                        response = requests.get(dp_url, headers=headers, verify=auth_info['verify_ssl'])
                        if response.ok:
                            print(f"✅ Data product exists, so the statistics endpoint may not be available on this cluster version.")
                        else:
                            print(f"❌ Data product not found (HTTP {response.status_code}). Try a different data product ID.")
                    except Exception as test_e:
                        print(f"❌ Could not test data product existence: {test_e}")
                        
        elif choice == '3':
            # List available data products
            try:
                print(f"\n📋 Listing available data products...")
                dp_list_url = f"{auth_info['protocol']}://{auth_info['host']}/api/v1/dataProduct/products"
                response = requests.get(dp_list_url, headers=headers, verify=auth_info['verify_ssl'])
                
                if response.ok:
                    products = response.json()
                    if products:
                        print(f"Found {len(products)} data products:")
                        for i, product in enumerate(products[:10]):  # Show first 10
                            name = product.get('name', 'Unknown')
                            product_id = product.get('id', 'Unknown')
                            status = product.get('status', 'Unknown')
                            print(f"  {i+1}. {name} (ID: {product_id}, Status: {status})")
                        
                        if len(products) > 10:
                            print(f"  ... and {len(products) - 10} more")
                            
                        # Ask if user wants to test statistics with one of these IDs
                        test_choice = input(f"\nEnter a data product ID to test statistics (or press Enter to skip): ").strip()
                        if test_choice:
                            try:
                                print(f"\n🌐 Testing statistics for data product: {test_choice}")
                                stats_data = make_statistics_request(
                                    auth_info['host'],
                                    test_choice,
                                    headers,
                                    auth_info['protocol'],
                                    auth_info['verify_ssl']
                                )
                                display_statistics_response(stats_data)
                                print(f"\n✅ Statistics request completed successfully!")
                            except Exception as stats_e:
                                print(f"❌ Statistics request failed: {stats_e}")
                    else:
                        print(f"No data products found on this cluster.")
                else:
                    print(f"❌ Could not list data products (HTTP {response.status_code})")
                    print(f"Response: {response.text}")
                    
            except Exception as e:
                print(f"❌ Could not list data products: {e}")
        else:
            print(f"\n📋 Request not executed. Use the cURL command above to test manually.")
        
        # Show additional examples
        print(f"\n=== Additional Examples ===")
        print(f"To test with different data product IDs, modify the URL:")
        print(f"  {auth_info['protocol']}://{auth_info['host']}/api/v1/dataProduct/products/YOUR_ID/statistics")
        print(f"\nTo save response to file:")
        print(f"  {curl_command} > statistics_response.json")
        print(f"\nTo see response headers:")
        print(f"  {curl_command} -i")
        print(f"\nTo see verbose output:")
        print(f"  {curl_command} -v")
        
        print(f"\n=== OAuth Configuration ===")
        print(f"For OAuth authentication, add to your .env file:")
        print(f"AUTH_METHOD=oauth2_jwt")
        print(f"SEP_JWT_TOKEN=your-jwt-token-here")
        print(f"")
        print(f"Example OAuth cURL command structure:")
        print(f"curl \\")
        print(f'  -H "Authorization: Bearer YOUR-JWT-TOKEN" \\')
        print(f'  -H "Content-Type: application/json" \\')
        print(f'  -H "Accept: application/json" \\')
        print(f'  "{auth_info["protocol"]}://{auth_info["host"]}/api/v1/dataProduct/products/{data_product_id}/statistics"')
        
    except AuthenticationError as e:
        print(f"❌ Authentication configuration error: {e}")
        print("Please check your .env file and fix the configuration.")
        print("Copy .env.example to .env and update with your values.")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()