#!/usr/bin/env python3
"""
Data Product Usage Statistics Example

This script demonstrates how to retrieve usage statistics for data products 
using the Starburst Data Products API. It accesses the accessMetadata field
from the getDataProduct API response which contains usage information.

Current Available Statistics:
- Last queried timestamp (lastQueriedAt)
- Last user who queried the data product (lastQueriedBy)

Future Enhancement Possibilities:
- Number of queries over the last 7 and 30 days
- Number of unique users over the last 7 and 30 days
- Query frequency trends and patterns

Note: This script demonstrates read-only operations and does not modify
any data products in your SEP cluster.
"""

import os
import sys
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any

# Add the parent directory to sys.path to import the client
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from starburst_data_products_client.sep.api import Api
from starburst_data_products_client.sep.data import DataProduct, DataProductSearchResult
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
                        detailed = api.get_data_product(product.id)
                        detailed_products.append(detailed)
                    except Exception as e:
                        print(f"  Warning: Could not get details for {product.name}: {e}")
                return detailed_products
            
            # Handle comma-separated list of numbers
            try:
                indices = [int(x.strip()) - 1 for x in choice.split(',')]
                selected_products = []
                
                for index in indices:
                    if 0 <= index < min(20, len(products)):
                        product = products[index]
                        try:
                            detailed = api.get_data_product(product.id)
                            selected_products.append(detailed)
                            print(f"  ✓ Selected: {product.name}")
                        except Exception as e:
                            print(f"  Warning: Could not get details for {product.name}: {e}")
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


def analyze_single_product_usage(product: DataProduct) -> Dict[str, Any]:
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
        'usage_status': 'Unknown'
    }
    
    # Extract access metadata if available
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
            
            # Categorize usage recency
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
            usage_stats['usage_status'] = 'Never Queried'
    else:
        usage_stats['usage_status'] = 'No Access Metadata'
    
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
    
    if usage_stats['last_queried_at']:
        print(f"  Last Queried: {usage_stats['last_queried_at']}")
        print(f"  Last User: {usage_stats['last_queried_by']}")
        print(f"  Days Since Last Query: {usage_stats['days_since_last_query']}")
    else:
        print(f"  ⚠️  This data product has never been queried")
    
    # Future enhancement placeholders
    print(f"\n📈 Extended Statistics (Future Enhancement):")
    print(f"  🔄 Queries in Last 7 Days: [API Enhancement Needed]")
    print(f"  🔄 Queries in Last 30 Days: [API Enhancement Needed]") 
    print(f"  👥 Unique Users in Last 7 Days: [API Enhancement Needed]")
    print(f"  👥 Unique Users in Last 30 Days: [API Enhancement Needed]")


def generate_usage_summary(all_usage_stats: List[Dict[str, Any]]):
    """Generate a summary report across all analyzed data products."""
    print(f"\n{'='*80}")
    print(f"📋 USAGE SUMMARY REPORT")
    print(f"{'='*80}")
    
    if not all_usage_stats:
        print("No data products analyzed.")
        return
    
    print(f"Total Data Products Analyzed: {len(all_usage_stats)}")
    
    # Categorize by usage status
    status_counts = {}
    never_queried = 0
    recent_users = set()
    
    for stats in all_usage_stats:
        status = stats['usage_status']
        status_counts[status] = status_counts.get(status, 0) + 1
        
        if stats['last_queried_by']:
            recent_users.add(stats['last_queried_by'])
        if stats['last_queried_at'] is None:
            never_queried += 1
    
    print(f"\n📊 Usage Status Distribution:")
    for status, count in sorted(status_counts.items()):
        percentage = (count / len(all_usage_stats)) * 100
        print(f"  {status}: {count} products ({percentage:.1f}%)")
    
    print(f"\n🔍 Key Insights:")
    print(f"  • Products never queried: {never_queried}")
    print(f"  • Total unique recent users: {len(recent_users)}")
    
    # Most recently accessed products
    recent_products = [
        stats for stats in all_usage_stats 
        if stats['last_queried_at'] is not None
    ]
    recent_products.sort(
        key=lambda x: x['last_queried_at'], 
        reverse=True
    )
    
    print(f"\n🕐 Most Recently Accessed Products:")
    for i, stats in enumerate(recent_products[:5]):
        print(f"  {i+1}. {stats['product_name']} - {stats['last_queried_at']} by {stats['last_queried_by']}")
    
    if len(recent_products) == 0:
        print(f"    No products have been queried yet.")


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
                usage_stats = analyze_single_product_usage(product)
                all_usage_stats.append(usage_stats)
                display_product_usage_statistics(usage_stats)
            except Exception as e:
                print(f"Error analyzing {product.name}: {e}")
        
        # Generate summary report
        generate_usage_summary(all_usage_stats)
        
        print("\n" + "=" * 65)
        print("✅ Usage statistics analysis completed!")
        print("\nCurrent Capabilities:")
        print("✓ Last query timestamp and user identification")
        print("✓ Usage recency categorization")
        print("✓ Cross-product usage summary")
        
        print("\n🚀 Future Enhancements:")
        print("• Query count statistics (7-day and 30-day windows)")
        print("• Unique user count analytics")
        print("• Usage trend analysis and patterns")
        print("• Peak usage time identification")
        
    except Exception as e:
        print(f"\n✗ Script failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()