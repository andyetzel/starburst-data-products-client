#!/usr/bin/env python3
"""
Data Product Usage Statistics Example

This script demonstrates how to retrieve usage statistics for data products 
using the Starburst Data Products API. It accesses statistics from two different
sources to provide comprehensive usage information.

Statistics Endpoint (/api/v1/dataProduct/products/{id}/statistics):
- Query count over the last 7 and 30 days
- Unique user count over the last 7 and 30 days  
- Statistics update timestamp

Access Metadata (from accessMetadata field in getDataProduct):
- Last queried timestamp (lastQueriedAt)
- Last user who queried the data product (lastQueriedBy)

The script attempts to gather data from both sources to provide the most
complete picture of data product usage patterns.

Note: This script demonstrates read-only operations and does not modify
any data products in your SEP cluster.
"""

import os
import sys
import json
import requests
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any

# Add the parent directory to sys.path to import the client
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from starburst_data_products_client.sep.api import Api
from starburst_data_products_client.sep.data import DataProduct, DataProductSearchResult, DataProductStatistics
from starburst_data_products_client.shared.auth_config import create_api_client_with_messages, get_auth_info, AuthenticationError


def display_auth_config():
    """Display current authentication configuration."""
    try:
        env_file = os.path.join(os.path.dirname(__file__), '.env')
        auth_info = get_auth_info(env_file)
        
        print("=== Authentication Configuration ===")
        print(f"Method: {auth_info['method']}")
        print(f"Host: {auth_info['host']}")
        print(f"Protocol: {auth_info['protocol']}")
        print(f"SSL Verify: {auth_info['verify_ssl']}")
        
        if auth_info['method'] == 'basic':
            print(f"Username: {auth_info.get('username', 'Not set')}")
            print(f"Password: {auth_info.get('password', 'Not set')}")
        elif auth_info['method'] == 'oauth2_jwt':
            print(f"JWT Token: {auth_info.get('jwt_token', 'Not set')}")
        elif auth_info['method'] == 'kerberos':
            print(f"Service Name: {auth_info.get('service_name', 'Not set')}")
        
        return auth_info
        
    except AuthenticationError as e:
        print(f"❌ Authentication configuration error: {e}")
        print("Please check your .env file and fix the configuration.")
        print("Copy .env.example to .env and update with your values.")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error loading configuration: {e}")
        sys.exit(1)


def select_data_products(api: Api) -> List[DataProduct]:
    """Allow user to select data products for usage statistics analysis."""
    print(f"\n=== Select Data Products for Usage Analysis ===")
    
    try:
        # Search for data products
        products = api.search_data_products()
        if not products:
            print("No data products found.")
            return []
        
        print("Available data products:")
        for i, product in enumerate(products[:20]):  # Show first 20
            print(f"  {i+1}. {product.name} (ID: {product.id})")
        
        if len(products) > 20:
            print(f"  ... and {len(products) - 20} more")
        
        print("\nSelection options:")
        print("  'a' or 'all' - Analyze all data products")
        print("  '1,3,5' - Analyze specific data products by numbers")
        print("  'q' - Quit")
        
        while True:
            choice = input(f"\nEnter your selection: ").strip()
            
            if choice.lower() == 'q':
                return []
            
            if choice.lower() in ['a', 'all']:
                print(f"✓ Selected all {len(products)} data products for analysis")
                # Get detailed information for all products
                detailed_products = []
                for product in products:
                    try:
                        print(f"  🌐 Getting detailed data for {product.name}...")
                        detailed = api.get_data_product(product.id)
                        
                        # Log raw JSON response for data product details
                        print(f"  📥 Raw data product response (accessMetadata only):")
                        if detailed.accessMetadata:
                            access_meta_json = {
                                'lastQueriedAt': detailed.accessMetadata.lastQueriedAt.isoformat() if detailed.accessMetadata.lastQueriedAt else None,
                                'lastQueriedBy': detailed.accessMetadata.lastQueriedBy
                            }
                            print(f"     {json.dumps(access_meta_json, indent=6)}")
                        else:
                            print(f"     No access metadata available")
                        
                        detailed_products.append(detailed)
                    except Exception as e:
                        print(f"  ❌ Could not get details for {product.name}: {e}")
                return detailed_products
            
            # Handle comma-separated list of numbers
            try:
                indices = [int(x.strip()) - 1 for x in choice.split(',')]
                selected_products = []
                
                for index in indices:
                    if 0 <= index < min(20, len(products)):
                        product = products[index]
                        try:
                            print(f"  🌐 Getting detailed data for {product.name}...")
                            detailed = api.get_data_product(product.id)
                            
                            # Log raw JSON response for data product details
                            print(f"  📥 Raw data product response (accessMetadata only):")
                            if detailed.accessMetadata:
                                access_meta_json = {
                                    'lastQueriedAt': detailed.accessMetadata.lastQueriedAt.isoformat() if detailed.accessMetadata.lastQueriedAt else None,
                                    'lastQueriedBy': detailed.accessMetadata.lastQueriedBy
                                }
                                print(f"     {json.dumps(access_meta_json, indent=6)}")
                            else:
                                print(f"     No access metadata available")
                            
                            selected_products.append(detailed)
                            print(f"  ✓ Selected: {product.name}")
                        except Exception as e:
                            print(f"  ❌ Could not get details for {product.name}: {e}")
                    else:
                        print(f"  Invalid selection: {index + 1}")
                
                if selected_products:
                    return selected_products
                else:
                    print("No valid products selected. Please try again.")
                    
            except ValueError:
                print("Invalid format. Please use numbers separated by commas (e.g., '1,3,5') or 'all'")
                
    except Exception as e:
        print(f"Error selecting data products: {e}")
        return []


def analyze_single_product_usage(api: Api, product: DataProduct, auth_info: Dict[str, Any]) -> Dict[str, Any]:
    """Analyze usage statistics for a single data product."""
    usage_stats = {
        'product_id': product.id,
        'product_name': product.name,
        'catalog': product.catalogName,
        'schema': product.schemaName,
        'status': product.status,
        'created_at': product.createdAt,
        'updated_at': product.updatedAt,
        'published_at': product.publishedAt,
        'last_queried_at': None,
        'last_queried_by': None,
        'days_since_last_query': None,
        'usage_status': 'Unknown',
        # Fields from statistics endpoint
        'seven_day_query_count': None,
        'thirty_day_query_count': None,
        'seven_day_user_count': None,
        'thirty_day_user_count': None,
        'statistics_updated_at': None,
        'statistics_available': False
    }
    
    # Try to get query count statistics from the statistics endpoint
    print(f"  🌐 Making direct HTTP call to statistics endpoint for {product.name}...")
    
    # Build the statistics URL
    stats_url = f"{auth_info['protocol']}://{auth_info['host']}/api/v1/dataProduct/products/{product.id}/statistics"
    print(f"  📍 URL: {stats_url}")
    
    # Prepare headers based on authentication method
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    
    if auth_info['method'] == 'basic':
        import base64
        credentials = f"{auth_info['username']}:{os.getenv('SEP_PASSWORD')}"
        encoded_credentials = base64.b64encode(credentials.encode()).decode()
        headers['Authorization'] = f"Basic {encoded_credentials}"
    elif auth_info['method'] == 'oauth2_jwt':
        jwt_token = os.getenv('SEP_JWT_TOKEN')
        if jwt_token:
            headers['Authorization'] = f"Bearer {jwt_token}"
        else:
            print(f"  ❌ JWT token not found in environment")
            usage_stats['statistics_available'] = False
            return usage_stats
    
    # Show request details (mask sensitive headers)
    print(f"  📤 Request headers:")
    for key, value in headers.items():
        if key.lower() == 'authorization':
            if value.startswith('Bearer '):
                print(f"     {key}: Bearer ***")
            elif value.startswith('Basic '):
                print(f"     {key}: Basic ***")
            else:
                print(f"     {key}: ***")
        else:
            print(f"     {key}: {value}")
    
    try:
        # Make the HTTP request
        response = requests.get(stats_url, headers=headers, verify=auth_info['verify_ssl'])
        
        # Log raw HTTP response details
        print(f"  📥 Raw HTTP Response:")
        print(f"     Status Code: {response.status_code}")
        print(f"     Reason: {response.reason}")
        print(f"     Content-Type: {response.headers.get('content-type', 'Not specified')}")
        
        # Always show the response body (JSON or error text)
        print(f"  📥 Response Body:")
        try:
            # Try to parse as JSON first
            response_data = response.json()
            print(f"     {json.dumps(response_data, indent=6)}")
            
            if response.ok:
                # Parse successful response
                stats = DataProductStatistics.load(response_data)
                usage_stats['seven_day_query_count'] = stats.sevenDayQueryCount
                usage_stats['thirty_day_query_count'] = stats.thirtyDayQueryCount
                usage_stats['seven_day_user_count'] = stats.sevenDayUserCount
                usage_stats['thirty_day_user_count'] = stats.thirtyDayUserCount
                usage_stats['statistics_updated_at'] = stats.updatedAt
                usage_stats['statistics_available'] = True
                
                # Update usage status based on query count statistics
                if stats.sevenDayQueryCount > 0:
                    usage_stats['usage_status'] = f'Very Active ({stats.sevenDayQueryCount} queries in 7 days)'
                elif stats.thirtyDayQueryCount > 0:
                    usage_stats['usage_status'] = f'Active ({stats.thirtyDayQueryCount} queries in 30 days)'
                else:
                    usage_stats['usage_status'] = 'No Recent Activity (0 queries in 30 days)'
            else:
                print(f"  ❌ HTTP Error {response.status_code}: {response.reason}")
                usage_stats['statistics_available'] = False
                
        except ValueError:
            # Not JSON, show as plain text
            print(f"     {response.text}")
            print(f"  ❌ Response is not valid JSON")
            usage_stats['statistics_available'] = False
            
    except requests.exceptions.SSLError as e:
        print(f"  ❌ SSL Error: {e}")
        print(f"     Try setting SSL_VERIFY=false in your .env file if using self-signed certificates")
        usage_stats['statistics_available'] = False
    except requests.exceptions.ConnectionError as e:
        print(f"  ❌ Connection Error: {e}")
        print(f"     Check if the host {auth_info['host']} is reachable")
        usage_stats['statistics_available'] = False
    except requests.exceptions.Timeout as e:
        print(f"  ❌ Timeout Error: {e}")
        usage_stats['statistics_available'] = False
    except Exception as e:
        print(f"  ❌ Unexpected Error: {e}")
        usage_stats['statistics_available'] = False
    
    # Extract access metadata from data product details
    if product.accessMetadata:
        usage_stats['last_queried_at'] = product.accessMetadata.lastQueriedAt
        usage_stats['last_queried_by'] = product.accessMetadata.lastQueriedBy
        
        if product.accessMetadata.lastQueriedAt:
            # Calculate days since last query
            now = datetime.now()
            # Handle timezone-aware datetime
            last_query = product.accessMetadata.lastQueriedAt
            if last_query.tzinfo is not None:
                # If last_query is timezone-aware, make now timezone-aware too
                from datetime import timezone
                now = now.replace(tzinfo=timezone.utc)
            
            days_since = (now - last_query).days
            usage_stats['days_since_last_query'] = days_since
            
            # Use access metadata for status if statistics are not available
            if not usage_stats['statistics_available']:
                if days_since <= 1:
                    usage_stats['usage_status'] = 'Very Active (within 1 day)'
                elif days_since <= 7:
                    usage_stats['usage_status'] = 'Active (within 1 week)'
                elif days_since <= 30:
                    usage_stats['usage_status'] = 'Moderate (within 1 month)'
                elif days_since <= 90:
                    usage_stats['usage_status'] = 'Low Activity (within 3 months)'
                else:
                    usage_stats['usage_status'] = f'Inactive ({days_since} days ago)'
        else:
            if not usage_stats['statistics_available']:
                usage_stats['usage_status'] = 'Never Queried'
    else:
        if not usage_stats['statistics_available']:
            usage_stats['usage_status'] = 'No Access Metadata Available'
    
    return usage_stats


def display_product_usage_statistics(usage_stats: Dict[str, Any]):
    """Display usage statistics for a single data product."""
    print(f"\n{'='*60}")
    print(f"📊 Usage Statistics: {usage_stats['product_name']}")
    print(f"{'='*60}")
    
    print(f"Product Details:")
    print(f"  ID: {usage_stats['product_id']}")
    print(f"  Location: {usage_stats['catalog']}.{usage_stats['schema']}")
    print(f"  Status: {usage_stats['status']}")
    print(f"  Created: {usage_stats['created_at']}")
    if usage_stats['published_at']:
        print(f"  Published: {usage_stats['published_at']}")
    
    print(f"\nUsage Information:")
    print(f"  Usage Status: {usage_stats['usage_status']}")
    
    # Display query count statistics if available
    if usage_stats['statistics_available']:
        print(f"\n📈 Query Count Statistics (from /statistics endpoint):")
        print(f"  🔄 Queries in Last 7 Days: {usage_stats['seven_day_query_count']}")
        print(f"  🔄 Queries in Last 30 Days: {usage_stats['thirty_day_query_count']}")
        print(f"  👥 Unique Users in Last 7 Days: {usage_stats['seven_day_user_count']}")
        print(f"  👥 Unique Users in Last 30 Days: {usage_stats['thirty_day_user_count']}")
        print(f"  📅 Statistics Last Updated: {usage_stats['statistics_updated_at']}")
    else:
        print(f"\n⚠️  Query count statistics not available (endpoint may not be accessible)")
    
    # Access metadata from data product details
    if usage_stats['last_queried_at']:
        print(f"\n🕐 Access Metadata (from data product details):")
        print(f"  Last Queried: {usage_stats['last_queried_at']}")
        print(f"  Last User: {usage_stats['last_queried_by']}")
        print(f"  Days Since Last Query: {usage_stats['days_since_last_query']}")
    else:
        if not usage_stats['statistics_available']:
            print(f"  ⚠️  This data product has never been queried")
    
    # Show information about available data sources
    if not usage_stats['statistics_available']:
        print(f"\n📋 Note: Using access metadata from data product details only")
        print(f"     Query count statistics available at: /api/v1/dataProduct/products/{usage_stats['product_id']}/statistics")


def generate_usage_summary(all_usage_stats: List[Dict[str, Any]]):
    """Generate a summary report across all analyzed data products."""
    print(f"\n{'='*80}")
    print(f"📋 USAGE SUMMARY REPORT")
    print(f"{'='*80}")
    
    if not all_usage_stats:
        print("No data products analyzed.")
        return
    
    print(f"Total Data Products Analyzed: {len(all_usage_stats)}")
    
    # Check how many have query count statistics
    stats_available_count = sum(1 for stats in all_usage_stats if stats['statistics_available'])
    print(f"Products with Query Count Statistics: {stats_available_count}/{len(all_usage_stats)}")
    
    # Query count statistics aggregation
    if stats_available_count > 0:
        total_7day_queries = sum(stats['seven_day_query_count'] or 0 for stats in all_usage_stats if stats['statistics_available'])
        total_30day_queries = sum(stats['thirty_day_query_count'] or 0 for stats in all_usage_stats if stats['statistics_available'])
        
        # Aggregate query counts across all products with statistics
        print(f"\n📈 Query Count Summary:")
        print(f"  🔄 Total Queries (7 days): {total_7day_queries}")
        print(f"  🔄 Total Queries (30 days): {total_30day_queries}")
        
        # Top active products by queries
        active_products = [stats for stats in all_usage_stats if stats['statistics_available'] and stats['seven_day_query_count'] > 0]
        active_products.sort(key=lambda x: x['seven_day_query_count'], reverse=True)
        
        print(f"\n🏆 Most Active Products (Last 7 Days):")
        for i, stats in enumerate(active_products[:5]):
            print(f"  {i+1}. {stats['product_name']}: {stats['seven_day_query_count']} queries, {stats['seven_day_user_count']} users")
        
        if len(active_products) == 0:
            print(f"    No products have queries in the last 7 days.")
    
    # Categorize by usage status
    status_counts = {}
    never_queried = 0
    recent_users = set()
    
    for stats in all_usage_stats:
        status = stats['usage_status']
        status_counts[status] = status_counts.get(status, 0) + 1
        
        if stats['last_queried_by']:
            recent_users.add(stats['last_queried_by'])
        if stats['last_queried_at'] is None and (not stats['statistics_available'] or stats['thirty_day_query_count'] == 0):
            never_queried += 1
    
    print(f"\n📊 Usage Status Distribution:")
    for status, count in sorted(status_counts.items()):
        percentage = (count / len(all_usage_stats)) * 100
        print(f"  {status}: {count} products ({percentage:.1f}%)")
    
    print(f"\n🔍 Key Insights:")
    print(f"  • Products with no recent activity: {never_queried}")
    print(f"  • Total unique users identified: {len(recent_users)}")
    
    # Most recently accessed products (legacy data)
    recent_products = [
        stats for stats in all_usage_stats 
        if stats['last_queried_at'] is not None
    ]
    recent_products.sort(
        key=lambda x: x['last_queried_at'], 
        reverse=True
    )
    
    print(f"\n🕐 Most Recently Accessed Products (from Access Metadata):")
    for i, stats in enumerate(recent_products[:5]):
        print(f"  {i+1}. {stats['product_name']} - {stats['last_queried_at']} by {stats['last_queried_by']}")
    
    if len(recent_products) == 0:
        print(f"    No recent access information available.")


def main():
    """Main function to analyze data product usage statistics."""
    print("Starburst Data Products Client - Usage Statistics Analysis")
    print("=" * 65)
    
    # Display authentication configuration
    auth_info = display_auth_config()
    
    if not auth_info['verify_ssl']:
        print("⚠️  SSL certificate verification is disabled")
    
    # Initialize API client using centralized authentication
    try:
        env_file = os.path.join(os.path.dirname(__file__), '.env')
        api = create_api_client_with_messages(env_file)
        print(f"✅ Successfully connected to SEP cluster using {auth_info['method']} authentication")
    except AuthenticationError as e:
        print(f"❌ Authentication error: {e}")
        print("Please check your .env file configuration.")
        sys.exit(1)
    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("Make sure required authentication libraries are installed.")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Failed to connect to SEP cluster: {e}")
        sys.exit(1)
    
    try:
        # Select data products to analyze
        selected_products = select_data_products(api)
        
        if not selected_products:
            print("No data products selected for analysis. Exiting.")
            sys.exit(0)
        
        print(f"\n🔍 Analyzing usage statistics for {len(selected_products)} data product(s)...")
        
        # Analyze each selected data product
        all_usage_stats = []
        for product in selected_products:
            try:
                usage_stats = analyze_single_product_usage(api, product, auth_info)
                all_usage_stats.append(usage_stats)
                display_product_usage_statistics(usage_stats)
            except Exception as e:
                print(f"Error analyzing {product.name}: {e}")
        
        # Generate summary report
        generate_usage_summary(all_usage_stats)
        
        print("\n" + "=" * 65)
        print("✅ Usage statistics analysis completed!")
        print("\nCurrent Capabilities:")
        print("✓ Query count statistics (7-day and 30-day windows)")
        print("✓ Unique user count analytics per data product")
        print("✓ Last query timestamp and user identification")
        print("✓ Usage recency categorization")
        print("✓ Cross-product usage summary and ranking")
        print("✓ Data collection from multiple API endpoints")
        
        print("\n🚀 Future Enhancement Possibilities:")
        print("• Query frequency trends and patterns over time")
        print("• Peak usage time identification")
        print("• User activity correlation across data products")
        print("• Usage forecasting and anomaly detection")
        
    except Exception as e:
        print(f"\n✗ Script failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()