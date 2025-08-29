#!/usr/bin/env python3
"""
Domain Creation Example

This script demonstrates how to create and manage data product domains using
the Starburst Data Products API. Domains are logical groupings that organize 
related data products and define their storage locations.

Operations demonstrated:
- Creating new domains with metadata
- Listing existing domains
- Updating domain properties
- Setting up schema locations for data storage
- Organizing data products by business areas

Note: This script will create domains in your SEP cluster.
Make sure you have the necessary permissions and are connected to the 
correct environment.

Run this script before creating data products to ensure domains exist.
"""

import os
import sys
from typing import List, Optional

# Add the parent directory to sys.path to import the client
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from starburst_data_products_client.sep.api import Api
from starburst_data_products_client.sep.data import Domain
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


def load_domain_config():
    """Load domain-specific configuration from environment."""
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))
    
    config = {
        'catalog_name': os.getenv('DEFAULT_CATALOG_NAME', 'hive'),
        'schema_name': os.getenv('DEFAULT_SCHEMA_NAME', 'example'),
        'domain_name': os.getenv('DEFAULT_DOMAIN_NAME', 'example_domain')
    }
    
    return config


def list_existing_domains(api: Api):
    """List all existing domains."""
    print("=== Existing Domains ===")
    
    try:
        domains = api.list_domains()
        
        if not domains:
            print("No domains found in the system")
            return domains
        
        print(f"Found {len(domains)} domain(s):")
        for i, domain in enumerate(domains, 1):
            print(f"  {i}. {domain.name} (ID: {domain.id})")
            print(f"     Description: {domain.description or 'No description'}")
            print(f"     Schema Location: {domain.schemaLocation or 'Not specified'}")
            print(f"     Created: {domain.createdAt}")
            print(f"     Created By: {domain.createdBy}")
            if domain.assignedDataProducts:
                print(f"     Data Products: {len(domain.assignedDataProducts)} assigned")
            print()
        
        return domains
        
    except Exception as e:
        print(f"Error listing domains: {e}")
        return []


def create_domain(api: Api, name: str, description: str = None, schema_location: str = None) -> Domain:
    """Create a new domain."""
    print(f"\n=== Creating Domain: {name} ===")
    
    try:
        print(f"Creating domain with:")
        print(f"  Name: {name}")
        print(f"  Description: {description or 'No description provided'}")
        print(f"  Schema Location: {schema_location or 'Will be auto-generated'}")
        
        new_domain = api.create_domain(
            name=name,
            description=description,
            schema_location=schema_location
        )
        
        print(f"✓ Successfully created domain!")
        print(f"  ID: {new_domain.id}")
        print(f"  Name: {new_domain.name}")
        print(f"  Description: {new_domain.description}")
        print(f"  Schema Location: {new_domain.schemaLocation}")
        print(f"  Created At: {new_domain.createdAt}")
        print(f"  Created By: {new_domain.createdBy}")
        
        return new_domain
        
    except Exception as e:
        print(f"✗ Error creating domain '{name}': {e}")
        raise


def update_domain(api: Api, domain: Domain) -> Domain:
    """Update an existing domain."""
    print(f"\n=== Updating Domain: {domain.name} ===")
    
    try:
        print(f"Current domain properties:")
        print(f"  ID: {domain.id}")
        print(f"  Name: {domain.name}")
        print(f"  Description: {domain.description}")
        print(f"  Schema Location: {domain.schemaLocation}")
        
        # Example update - you can modify these as needed
        new_description = f"Updated domain description - Last modified on {domain.updatedAt or 'unknown date'}"
        
        print(f"\nUpdating domain description...")
        updated_domain = api.update_domain(
            domain_id=domain.id,
            description=new_description,
            schema_location=domain.schemaLocation  # Keep existing schema location
        )
        
        print(f"✓ Successfully updated domain!")
        print(f"  New Description: {updated_domain.description}")
        print(f"  Updated At: {updated_domain.updatedAt}")
        print(f"  Updated By: {updated_domain.updatedBy}")
        
        return updated_domain
        
    except Exception as e:
        print(f"✗ Error updating domain '{domain.name}': {e}")
        raise


def create_example_domains(api: Api, config: dict):
    """Create example domains for different business areas."""
    print(f"\n=== Creating Example Domains ===")
    
    # Define example domains for different business areas
    example_domains = [
        {
            'name': config['domain_name'],  # This is the main domain used by other scripts
            'description': 'Primary domain for example data products and demonstrations',
            'schema_location': f"s3://your-bucket/data-products/{config['domain_name']}/"
        },
        {
            'name': 'sales_analytics',
            'description': 'Domain for sales and revenue analytics data products',
            'schema_location': 's3://your-bucket/data-products/sales_analytics/'
        },
        {
            'name': 'customer_insights',
            'description': 'Domain for customer behavior and insights data products',
            'schema_location': 's3://your-bucket/data-products/customer_insights/'
        },
        {
            'name': 'marketing_campaigns',
            'description': 'Domain for marketing campaign performance and attribution data',
            'schema_location': 's3://your-bucket/data-products/marketing_campaigns/'
        }
    ]
    
    created_domains = []
    
    for domain_def in example_domains:
        try:
            # Check if domain already exists
            existing_domains = api.list_domains()
            existing_domain = next((d for d in existing_domains if d.name == domain_def['name']), None)
            
            if existing_domain:
                print(f"✓ Domain '{domain_def['name']}' already exists (ID: {existing_domain.id})")
                created_domains.append(existing_domain)
            else:
                new_domain = create_domain(
                    api=api,
                    name=domain_def['name'],
                    description=domain_def['description'],
                    schema_location=domain_def['schema_location']
                )
                created_domains.append(new_domain)
                
        except Exception as e:
            print(f"⚠ Failed to create domain '{domain_def['name']}': {e}")
            print("Continuing with remaining domains...")
    
    return created_domains


def demonstrate_domain_operations(api: Api, config: dict):
    """Demonstrate various domain operations."""
    
    # List existing domains first
    existing_domains = list_existing_domains(api)
    
    print(f"\n" + "=" * 55)
    print("Domain Operations Menu:")
    print("1. Create the main example_domain (required for other scripts)")
    print("2. Create multiple example domains for different business areas") 
    print("3. Update an existing domain")
    print("4. Create a custom domain")
    print("5. List domains again")
    print("6. Exit")
    
    while True:
        try:
            choice = input(f"\nSelect an option (1-6): ").strip()
            
            if choice == '1':
                # Create the main example domain
                main_domain_name = config['domain_name']
                existing_domain = next((d for d in existing_domains if d.name == main_domain_name), None)
                
                if existing_domain:
                    print(f"✓ Main domain '{main_domain_name}' already exists")
                    print(f"  The 2_create_data_product.py script can now be run successfully")
                else:
                    schema_location = f"s3://your-bucket/data-products/{main_domain_name}/"
                    create_domain(
                        api=api,
                        name=main_domain_name,
                        description=f"Primary domain for {main_domain_name} data products",
                        schema_location=schema_location
                    )
                    print(f"✓ Main domain created. You can now run 2_create_data_product.py")
                
            elif choice == '2':
                # Create example domains
                print("Creating multiple example domains...")
                created_domains = create_example_domains(api, config)
                print(f"\n✓ Domain creation process completed!")
                print(f"  Created/verified {len(created_domains)} domains")
                print(f"  You can now run other example scripts")
                
            elif choice == '3':
                # Update existing domain
                if not existing_domains:
                    print("No domains available to update. Create some domains first.")
                    continue
                    
                print("\nSelect a domain to update:")
                for i, domain in enumerate(existing_domains):
                    print(f"  {i+1}. {domain.name}")
                    
                try:
                    domain_choice = int(input("Select domain number: ")) - 1
                    if 0 <= domain_choice < len(existing_domains):
                        update_domain(api, existing_domains[domain_choice])
                    else:
                        print("Invalid selection")
                except ValueError:
                    print("Please enter a valid number")
                    
            elif choice == '4':
                # Create custom domain
                name = input("Enter domain name: ").strip()
                if not name:
                    print("Domain name cannot be empty")
                    continue
                    
                description = input("Enter description (optional): ").strip() or None
                schema_location = input("Enter schema location (optional, e.g., s3://bucket/path/): ").strip() or None
                
                try:
                    create_domain(api, name, description, schema_location)
                    # Refresh the existing domains list
                    existing_domains = api.list_domains()
                except Exception as e:
                    print(f"Failed to create custom domain: {e}")
                    
            elif choice == '5':
                # List domains again
                existing_domains = list_existing_domains(api)
                
            elif choice == '6':
                print("Exiting domain operations")
                break
                
            else:
                print("Invalid option. Please select 1-6.")
                
        except KeyboardInterrupt:
            print(f"\nOperation interrupted")
            break
        except Exception as e:
            print(f"Error in operation: {e}")


def main():
    """Main function to demonstrate domain creation and management."""
    print("Starburst Data Products Client - Create and Manage Domains")
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
    
    # Load domain specific configuration
    config = load_domain_config()
    
    try:
        # Demonstrate domain operations
        demonstrate_domain_operations(api, config)
        
        print("\n" + "=" * 60)
        print("Domain operations completed!")
        print(f"\n📝 Next Steps:")
        print(f"  1. Verify your domains are set up correctly")
        print(f"  2. Run 2_create_data_product.py to create data products in the domains")
        print(f"  3. Use 5_publish_workflow.py to publish your data products")
        print(f"  4. Manage data products with 3_update_data_product.py")
        
    except Exception as e:
        print(f"\n✗ Script failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    # Confirmation prompt
    response = input("\nThis script will create domains in your SEP cluster.\nAre you sure you want to continue? (y/N): ")
    if response.lower() not in ['y', 'yes']:
        print("Operation cancelled.")
        sys.exit(0)
    
    main()