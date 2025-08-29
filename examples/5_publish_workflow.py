#!/usr/bin/env python3
"""
Data Product Publish Workflow Example

This script demonstrates how to publish data products and monitor their
publish workflow status using the Starburst Data Products API.

Operations demonstrated:
- Publishing data products
- Monitoring publish workflow status
- Handling publish workflow errors
- Force republishing data products
- Checking workflow history

Note: This script will publish data products in your SEP cluster.
Make sure you have the necessary permissions and are connected to the 
correct environment.
"""

import os
import sys
import time
from typing import List, Optional

# Add the parent directory to sys.path to import the client
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from starburst_data_products_client.sep.api import Api
from starburst_data_products_client.shared.auth_config import create_api_client_with_messages, get_auth_info, AuthenticationError
from starburst_data_products_client.sep.data import (
    DataProduct, 
    DataProductSearchResult,
    DataProductWorkflowStatus
)


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


def load_configuration():
    """Load configuration from .env file."""
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))
    
    # Helper function to parse SSL_VERIFY setting
    def parse_ssl_verify(value):
        if value is None:
            return True  # Default to True for security
        return value.lower() in ('true', '1', 'yes', 'on')
    
    config = {
        'host': os.getenv('SEP_HOST'),
        'username': os.getenv('SEP_USERNAME'),
        'password': os.getenv('SEP_PASSWORD'),
        'protocol': os.getenv('SEP_PROTOCOL', 'https'),
        'ssl_verify': parse_ssl_verify(os.getenv('SSL_VERIFY')),
        'catalog_name': os.getenv('DEFAULT_CATALOG_NAME', 'hive'),
        'schema_name': os.getenv('DEFAULT_SCHEMA_NAME', 'example'),
        'domain_name': os.getenv('DEFAULT_DOMAIN_NAME', 'example_domain')
    }
    
    # Validate required configuration
    required_vars = ['host', 'username', 'password']
    missing_vars = [key for key in required_vars if not config[key]]
    if missing_vars:
        print(f"Error: Missing required environment variables: {', '.join(missing_vars)}")
        print("Please copy .env.example to .env and update with your values.")
        sys.exit(1)
    
    return config


def select_data_product(api: Api) -> Optional[DataProduct]:
    """Allow user to select a data product to publish."""
    print("=== Select Data Product to Publish ===")
    
    try:
        # Search for data products
        products = api.search_data_products()
        if not products:
            print("No data products found.")
            return None
        
        print("\nAvailable data products:")
        for i, product in enumerate(products[:10]):  # Show first 10
            print(f"  {i+1}. {product.name} (ID: {product.id})")
        
        if len(products) > 10:
            print(f"  ... and {len(products) - 10} more")
        
        # Get user selection
        while True:
            try:
                choice = input(f"\nSelect a data product (1-{min(10, len(products))}) or 'q' to quit: ")
                if choice.lower() == 'q':
                    return None
                
                index = int(choice) - 1
                if 0 <= index < min(10, len(products)):
                    selected_product = products[index]
                    # Get detailed information
                    detailed_product = api.get_data_product(selected_product.id)
                    print(f"\nSelected: {detailed_product.name}")
                    print(f"Status: {detailed_product.status}")
                    print(f"Description: {detailed_product.description or 'No description'}")
                    return detailed_product
                else:
                    print("Invalid selection. Please try again.")
            except ValueError:
                print("Please enter a valid number.")
                
    except Exception as e:
        print(f"Error selecting data product: {e}")
        return None


def publish_data_product(api: Api, product: DataProduct, force: bool = False):
    """Publish a data product and monitor the workflow."""
    print(f"\n=== Publishing Data Product ===")
    
    try:
        action = "Force republishing" if force else "Publishing"
        print(f"{action} data product: {product.name}")
        print(f"Product ID: {product.id}")
        print(f"Current Status: {product.status}")
        
        # Initiate publish
        api.publish_data_product(product.id, force=force)
        print("✓ Publish workflow initiated")
        
        # Monitor publish status with better error handling
        monitor_publish_workflow(api, product.id, product.name)
        
    except Exception as e:
        print(f"✗ Error publishing data product: {e}")


def monitor_publish_workflow(api: Api, product_id: str, product_name: str, max_attempts: int = 30):
    """Monitor the publish workflow status until completion."""
    print(f"\n=== Monitoring Publish Workflow ===")
    
    attempt = 0
    start_time = time.time()
    
    try:
        while attempt < max_attempts:
            try:
                status = api.get_publish_data_product_status(product_id)
                elapsed_time = time.time() - start_time
                
                print(f"  [{int(elapsed_time):03d}s] Status: {status.status}")
                
                # Handle successful completion
                if status.status in ["COMPLETED", "PUBLISHED", "SUCCESS"]:
                    print(f"✓ Data product '{product_name}' published successfully!")
                    print(f"  Total time: {int(elapsed_time)} seconds")
                    if hasattr(status, 'isFinalStatus') and status.isFinalStatus:
                        print("  Final status reached")
                    return True
                
                # Handle failure states
                elif status.status in ["FAILED", "ERROR"]:
                    print(f"✗ Publishing failed for '{product_name}'")
                    if hasattr(status, 'errors') and status.errors:
                        print("  Errors:")
                        for error in status.errors:
                            print(f"    - {error.entityType} '{error.entityName}': {error.message}")
                    return False
                
                # Handle in-progress states
                else:
                    progress_msg = f"  Publishing in progress... (attempt {attempt + 1}/{max_attempts})"
                    if hasattr(status, 'workflowType'):
                        progress_msg += f" | Type: {status.workflowType}"
                    print(progress_msg)
                    
                    time.sleep(3)  # Wait 3 seconds between checks
                    attempt += 1
                    
            except Exception as status_error:
                print(f"  Error checking status: {status_error}")
                # If we can't check status, wait a bit and try again
                time.sleep(5)
                attempt += 1
        
        # Timeout handling
        elapsed_time = time.time() - start_time
        print(f"⚠ Publishing status monitoring timed out after {int(elapsed_time)} seconds")
        print(f"  The publish workflow may still be in progress")
        print(f"  Check the SEP UI or run this script again to verify status")
        return None
        
    except KeyboardInterrupt:
        print(f"\n⚠ Monitoring interrupted by user")
        print(f"  The publish workflow may still be in progress")
        return None


def check_workflow_status(api: Api, product: DataProduct):
    """Check the current workflow status without initiating a new publish."""
    print(f"\n=== Current Workflow Status ===")
    
    try:
        print(f"Checking workflow status for: {product.name}")
        
        # Check publish status
        try:
            status = api.get_publish_data_product_status(product.id)
            print(f"  Publish Status: {status.status}")
            if hasattr(status, 'workflowType'):
                print(f"  Workflow Type: {status.workflowType}")
            if hasattr(status, 'isFinalStatus'):
                print(f"  Is Final Status: {status.isFinalStatus}")
            if hasattr(status, 'errors') and status.errors:
                print("  Errors:")
                for error in status.errors:
                    print(f"    - {error.entityType} '{error.entityName}': {error.message}")
        except Exception as e:
            if "404" in str(e) or "PUBLISH_OPERATION_NOT_FOUND" in str(e):
                print("  No active publish workflow found")
            else:
                print(f"  Error checking publish status: {e}")
        
        # Check delete status as well
        try:
            delete_status = api.get_delete_data_product_status(product.id)
            print(f"  Delete Status: {delete_status.status}")
        except Exception as e:
            if "404" in str(e) or "DELETE_OPERATION_NOT_FOUND" in str(e):
                print("  No active delete workflow found")
            else:
                print(f"  Error checking delete status: {e}")
        
        # Show current product status
        current_product = api.get_data_product(product.id)
        print(f"  Product Status: {current_product.status}")
        
    except Exception as e:
        print(f"✗ Error checking workflow status: {e}")


def demonstrate_publish_operations(api: Api):
    """Demonstrate various publish operations."""
    
    while True:
        print(f"\n=== Publish Workflow Operations Menu ===")
        print("1. Select and publish a data product")
        print("2. Force republish a data product")
        print("3. Check workflow status only")
        print("4. Monitor ongoing workflow")
        print("5. Exit")
        
        try:
            choice = input("\nSelect an option (1-5): ").strip()
            
            if choice == '1':
                # Regular publish
                product = select_data_product(api)
                if product:
                    if product.status == "PUBLISHED":
                        print(f"⚠ Data product '{product.name}' is already published")
                        republish = input("Do you want to force republish? (y/N): ")
                        if republish.lower() in ['y', 'yes']:
                            publish_data_product(api, product, force=True)
                    else:
                        publish_data_product(api, product, force=False)
                        
            elif choice == '2':
                # Force republish
                product = select_data_product(api)
                if product:
                    confirm = input(f"Force republish '{product.name}'? This will recreate all datasets. (y/N): ")
                    if confirm.lower() in ['y', 'yes']:
                        publish_data_product(api, product, force=True)
                    else:
                        print("Force republish cancelled")
                        
            elif choice == '3':
                # Check status only
                product = select_data_product(api)
                if product:
                    check_workflow_status(api, product)
                    
            elif choice == '4':
                # Monitor ongoing workflow
                product = select_data_product(api)
                if product:
                    print("Monitoring workflow status (this will not initiate a new publish)...")
                    try:
                        # Just monitor without publishing
                        monitor_publish_workflow(api, product.id, product.name, max_attempts=20)
                    except Exception as e:
                        print(f"Error monitoring workflow: {e}")
                        
            elif choice == '5':
                print("Exiting publish workflow operations")
                break
                
            else:
                print("Invalid option. Please select 1-5.")
                
        except KeyboardInterrupt:
            print(f"\nOperation interrupted")
            break
        except Exception as e:
            print(f"Error in operation: {e}")


def main():
    """Main function to demonstrate publish workflow operations."""
    print("Starburst Data Products Client - Publish Workflow")
    print("=" * 55)
    
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
        # Demonstrate publish workflow operations
        demonstrate_publish_operations(api)
        
        print("\n" + "=" * 55)
        print("Publish workflow operations completed!")
        
    except Exception as e:
        print(f"\n✗ Script failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    # Confirmation prompt
    response = input("\nThis script will publish data products in your SEP cluster.\nAre you sure you want to continue? (y/N): ")
    if response.lower() not in ['y', 'yes']:
        print("Operation cancelled.")
        sys.exit(0)
    
    main()