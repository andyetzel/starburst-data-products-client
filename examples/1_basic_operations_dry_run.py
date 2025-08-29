#!/usr/bin/env python3
"""
Basic API Operations - Dry Run Examples

This script demonstrates basic operations for the Starburst Data Products API
using "dry run" actions that perform read-only operations without making 
any actual data changes.

Operations demonstrated:
- Connecting to SEP cluster
- Searching for data products
- Retrieving data product details
- Listing domains
- Getting tags for data products
- Checking workflow status
"""

import os
import sys
from typing import List

# Add the parent directory to sys.path to import the client
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from starburst_data_products_client.sep.api import Api
from starburst_data_products_client.sep.data import DataProductSearchResult
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
            print(f"Config: {auth_info.get('config', 'Not set')}")
        
        return auth_info
        
    except AuthenticationError as e:
        print(f"❌ Authentication configuration error: {e}")
        print("Please check your .env file and fix the configuration.")
        print("Copy .env.example to .env and update with your values.")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error loading configuration: {e}")
        sys.exit(1)


def demonstrate_search_operations(api: Api):
    """Demonstrate search operations (read-only)."""
    print("=== Search Operations (Dry Run) ===")
    
    try:
        # Search all data products
        print("\n1. Searching for all data products...")
        all_products = api.search_data_products()
        print(f"   Found {len(all_products)} data products")
        
        if all_products:
            print("   First 3 data products:")
            for i, product in enumerate(all_products[:3]):
                print(f"     {i+1}. {product.name} (ID: {product.id})")
        
        # Search with a specific term
        search_term = "sales"
        print(f"\n2. Searching for data products containing '{search_term}'...")
        sales_products = api.search_data_products(search_term)
        print(f"   Found {len(sales_products)} matching data products")
        
        for product in sales_products[:3]:
            print(f"     - {product.name} (ID: {product.id})")
            
    except Exception as e:
        print(f"   Error during search operations: {e}")


def demonstrate_data_product_details(api: Api):
    """Demonstrate retrieving data product details (read-only)."""
    print("\n=== Data Product Details (Dry Run) ===")
    
    try:
        # Get first available data product for demonstration
        all_products = api.search_data_products()
        if not all_products:
            print("   No data products found to demonstrate details retrieval")
            return
        
        first_product = all_products[0]
        print(f"\n1. Getting details for data product: {first_product.name}")
        
        # Get detailed information
        detailed_product = api.get_data_product(first_product.id)
        print(f"   Name: {detailed_product.name}")
        print(f"   Description: {detailed_product.description or 'No description'}")
        print(f"   Catalog: {detailed_product.catalogName}")
        print(f"   Schema: {detailed_product.schemaName}")
        print(f"   Status: {detailed_product.status}")
        print(f"   Created: {detailed_product.createdAt}")
        
        # Check if there are any views or materialized views
        if hasattr(detailed_product, 'views') and detailed_product.views:
            print(f"   Views: {len(detailed_product.views)}")
        if hasattr(detailed_product, 'materialized_views') and detailed_product.materialized_views:
            print(f"   Materialized Views: {len(detailed_product.materialized_views)}")
            
    except Exception as e:
        print(f"   Error retrieving data product details: {e}")


def demonstrate_domain_operations(api: Api):
    """Demonstrate domain operations (read-only)."""
    print("\n=== Domain Operations (Dry Run) ===")
    
    try:
        print("\n1. Listing all domains...")
        domains = api.list_domains()
        print(f"   Found {len(domains)} domains")
        
        for i, domain in enumerate(domains[:5]):  # Show first 5
            print(f"     {i+1}. {domain.name} (ID: {domain.id})")
            if domain.description:
                print(f"        Description: {domain.description}")
            print(f"        Schema Location: {domain.schemaLocation}")
            
    except Exception as e:
        print(f"   Error during domain operations: {e}")


def demonstrate_tag_operations(api: Api):
    """Demonstrate tag operations (read-only)."""
    print("\n=== Tag Operations (Dry Run) ===")
    
    try:
        # Get first available data product for demonstration
        all_products = api.search_data_products()
        if not all_products:
            print("   No data products found to demonstrate tag operations")
            return
        
        first_product = all_products[0]
        print(f"\n1. Getting tags for data product: {first_product.name}")
        
        # Get tags for the data product
        tags = api.get_tags(first_product.id)
        if tags:
            print(f"   Found {len(tags)} tags:")
            for tag in tags:
                print(f"     - {tag.value} (ID: {tag.id})")
        else:
            print("   No tags found for this data product")
            
    except Exception as e:
        print(f"   Error during tag operations: {e}")


def demonstrate_workflow_status(api: Api):
    """Demonstrate workflow status checking (read-only)."""
    print("\n=== Workflow Status (Dry Run) ===")
    
    try:
        # Get first available data product for demonstration
        all_products = api.search_data_products()
        if not all_products:
            print("   No data products found to check workflow status")
            return
        
        first_product = all_products[0]
        print(f"\n1. Checking publish status for: {first_product.name}")
        
        # Check publish status
        status = api.get_publish_data_product_status(first_product.id)
        print(f"   Publish Status: {status.status}")
        if hasattr(status, 'message') and status.message:
            print(f"   Message: {status.message}")
        if hasattr(status, 'last_updated') and status.last_updated:
            print(f"   Last Updated: {status.last_updated}")
            
    except Exception as e:
        print(f"   Error checking workflow status: {e}")


def main():
    """Main function to run all dry run demonstrations."""
    print("Starburst Data Products Client - Basic Operations (Dry Run)")
    print("=" * 60)
    
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
    
    # Run demonstrations
    demonstrate_search_operations(api)
    demonstrate_data_product_details(api)
    demonstrate_domain_operations(api)
    demonstrate_tag_operations(api)
    demonstrate_workflow_status(api)
    
    print("\n" + "=" * 60)
    print("Dry run completed successfully! No data was modified.")


if __name__ == "__main__":
    main()