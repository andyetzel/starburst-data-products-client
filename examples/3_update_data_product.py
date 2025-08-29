#!/usr/bin/env python3
"""
Data Product Update Example

This script demonstrates how to update various aspects of data products using 
the Starburst Data Products API. It includes the direct update API method 
and other specialized update operations:

- Direct data product update - Update core properties like description, summary, owners, and links
- Updating sample queries
- Updating tags  
- Updating domain information

Note: This script will modify existing data products in your SEP cluster.
Make sure you have the necessary permissions and are connected to the 
correct environment.
"""

import os
import sys
from datetime import datetime
from typing import List, Optional

# Add the parent directory to sys.path to import the client
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from starburst_data_products_client.sep.api import Api
from starburst_data_products_client.shared.auth_config import create_api_client_with_messages, get_auth_info, AuthenticationError
from starburst_data_products_client.sep.data import (
    DataProductParameters, 
    SampleQuery,
    DataProduct,
    View, 
    Column, 
    Owner, 
    RelevantLinks
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


def load_data_product_config():
    """Load data product specific configuration from environment."""
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))
    
    config = {
        'catalog_name': os.getenv('DEFAULT_CATALOG_NAME', 'hive'),
        'schema_name': os.getenv('DEFAULT_SCHEMA_NAME', 'example'),
        'domain_name': os.getenv('DEFAULT_DOMAIN_NAME', 'example_domain')
    }
    
    return config


def select_data_product(api: Api) -> Optional[DataProduct]:
    """Allow user to select a data product to update."""
    print("=== Select Data Product to Update ===")
    
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
                    print(f"Description: {detailed_product.description or 'No description'}")
                    return detailed_product
                else:
                    print("Invalid selection. Please try again.")
            except ValueError:
                print("Please enter a valid number.")
                
    except Exception as e:
        print(f"Error selecting data product: {e}")
        return None


def update_sample_queries(api: Api, product: DataProduct):
    """Update sample queries for the data product."""
    print(f"\n=== Updating Sample Queries ===")
    
    try:
        # Show current sample queries
        current_queries = api.list_sample_queries(product.id)
        print(f"Current sample queries: {len(current_queries)}")
        for i, query in enumerate(current_queries):
            print(f"  {i+1}. {query.name}: {query.description}")
        
        # Create new sample queries based on the iceberg.burst_bank.customer table schema
        new_queries = [
            SampleQuery(
                name="Customers by State",
                description="Query to find customer distribution by state",
                query=f"SELECT state, COUNT(*) as customer_count FROM {product.catalogName}.{product.schemaName}.{product.name}_view GROUP BY state ORDER BY customer_count DESC LIMIT 10"
            ),
            SampleQuery(
                name="High Income Customers",
                description="Query to find customers with estimated income above $100K",
                query=f"SELECT custkey, first_name, last_name, state, estimated_income FROM {product.catalogName}.{product.schemaName}.{product.name}_view WHERE estimated_income > 100000 ORDER BY estimated_income DESC LIMIT 20"
            ),
            SampleQuery(
                name="Customer Demographics by Gender",
                description="Query to analyze customer demographics by gender and marital status",
                query=f"SELECT gender, married, COUNT(*) as customer_count, AVG(estimated_income) as avg_income, AVG(fico) as avg_fico FROM {product.catalogName}.{product.schemaName}.{product.name}_view GROUP BY gender, married ORDER BY customer_count DESC"
            ),
            SampleQuery(
                name="Recent Registrations by Country",
                description="Query to find recent customer registrations by country",
                query=f"SELECT country, COUNT(*) as new_customers FROM {product.catalogName}.{product.schemaName}.{product.name}_view WHERE registration_date >= '2020-01-01' GROUP BY country ORDER BY new_customers DESC"
            ),
            SampleQuery(
                name="Credit Score Analysis",
                description="Query to analyze FICO credit score distribution",
                query=f"SELECT CASE WHEN fico >= 800 THEN 'Excellent (800+)' WHEN fico >= 740 THEN 'Very Good (740-799)' WHEN fico >= 670 THEN 'Good (670-739)' WHEN fico >= 580 THEN 'Fair (580-669)' ELSE 'Poor (<580)' END as credit_tier, COUNT(*) as customer_count, AVG(estimated_income) as avg_income FROM {product.catalogName}.{product.schemaName}.{product.name}_view GROUP BY CASE WHEN fico >= 800 THEN 'Excellent (800+)' WHEN fico >= 740 THEN 'Very Good (740-799)' WHEN fico >= 670 THEN 'Good (670-739)' WHEN fico >= 580 THEN 'Fair (580-669)' ELSE 'Poor (<580)' END ORDER BY AVG(estimated_income) DESC"
            )
        ]
        
        print(f"\nAdding {len(new_queries)} sample queries...")
        api.update_sample_queries(product.id, new_queries)
        
        # Verify the update
        updated_queries = api.list_sample_queries(product.id)
        print(f"✓ Successfully updated sample queries. New count: {len(updated_queries)}")
        for i, query in enumerate(updated_queries):
            print(f"  {i+1}. {query.name}: {query.description}")
            
    except Exception as e:
        print(f"✗ Error updating sample queries: {e}")


def update_tags(api: Api, product: DataProduct):
    """Update tags for the data product."""
    print(f"\n=== Updating Tags ===")
    
    try:
        # Show current tags
        current_tags = api.get_tags(product.id)
        current_values = [tag.value for tag in current_tags]
        print(f"Current tags: {current_values}")
        
        # Define new tags (including existing ones plus new ones) - relevant to customer data
        new_tag_values = current_values + [
            "customer-data",
            "banking", 
            "demographics",
            "financial-services",
            "pii-governance",
            "burst-bank",
            "analytics",
            "updated-example",
            f"updated-{datetime.now().strftime('%Y%m%d')}"
        ]
        
        # Remove duplicates while preserving order
        seen = set()
        unique_tags = []
        for tag in new_tag_values:
            if tag not in seen:
                unique_tags.append(tag)
                seen.add(tag)
        
        print(f"Updating with tags: {unique_tags}")
        api.update_tags(product.id, unique_tags)
        
        # Verify the update
        updated_tags = api.get_tags(product.id)
        updated_values = [tag.value for tag in updated_tags]
        print(f"✓ Successfully updated tags: {updated_values}")
        
    except Exception as e:
        print(f"✗ Error updating tags: {e}")


def update_domain_information(api: Api, product: DataProduct):
    """Update domain information for the data product's domain."""
    print(f"\n=== Updating Domain Information ===")
    
    try:
        # Get current domain information
        domain = api.get_domain(product.dataDomainId)
        print(f"Current domain: {domain.name}")
        print(f"Current description: {domain.description or 'No description'}")
        print(f"Current schema location: {domain.schema_location}")
        
        # Update domain with new description focused on customer data
        new_description = f"Updated description for {domain.name} - Contains customer data products from burst_bank including PII and financial information. Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        
        print(f"Updating domain description...")
        updated_domain = api.update_domain(
            domain_id=domain.id,
            description=new_description,
            schema_location=domain.schema_location  # Keep existing schema location
        )
        
        print(f"✓ Successfully updated domain:")
        print(f"  Name: {updated_domain.name}")
        print(f"  New Description: {updated_domain.description}")
        print(f"  Schema Location: {updated_domain.schema_location}")
        
    except Exception as e:
        print(f"✗ Error updating domain: {e}")


def update_data_product_directly(api: Api, product: DataProduct):
    """Update data product using the direct update API method."""
    print(f"\n=== Direct Data Product Update ===")
    print("Using the new update_data_product API method to update core properties.")
    
    try:
        print(f"Current data product details:")
        print(f"  Name: {product.name}")
        print(f"  Description: {product.description or 'No description'}")
        print(f"  Summary: {product.summary}")
        print(f"  Owners: {len(product.owners)} owner(s)")
        
        # Create updated parameters based on current product
        # Note: For views and materializedViews, we need to convert to dictionary format to avoid datetime serialization issues
        views_dict = []
        for view in product.views:
            view_dict = {
                'name': view.name,
                'description': view.description,
                'definitionQuery': view.definitionQuery,
                'columns': [{'name': col.name, 'type': col.type, 'description': col.description} for col in view.columns] if view.columns else [],
                'markedForDeletion': view.markedForDeletion
            }
            views_dict.append(view_dict)
        
        materializedViews_dict = []
        for mv in product.materializedViews:
            mv_dict = {
                'name': mv.name,
                'description': mv.description,
                'definitionQuery': mv.definitionQuery,
                'columns': [{'name': col.name, 'type': col.type, 'description': col.description} for col in mv.columns] if mv.columns else [],
                'markedForDeletion': mv.markedForDeletion
            }
            materializedViews_dict.append(mv_dict)
        
        updated_params = DataProductParameters(
            name=product.name,  # Keep the same name
            catalogName=product.catalogName,
            schemaName=product.schemaName,
            dataDomainId=product.dataDomainId,
            summary=f"Updated customer data product - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            description=f"Updated description: This customer data product (based on iceberg.burst_bank.customer) was updated using the direct API method on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}. Contains comprehensive customer information including demographics, financial data, and PII for governance demonstration. Original description: {product.description or 'None'}",
            views=views_dict,
            materializedViews=materializedViews_dict,
            owners=product.owners + [Owner(name="Customer Data Analyst", email="customer-analyst@example.com")],
            relevantLinks=product.relevantLinks + [RelevantLinks(label="Customer Data Governance Guide", url="https://docs.example.com/customer-data-governance")]
        )
        
        print(f"\nUpdating data product with new summary and description...")
        updated_product = api.update_data_product(product.id, updated_params)
        
        print(f"✓ Successfully updated data product:")
        print(f"  ID: {updated_product.id}")
        print(f"  Name: {updated_product.name}")
        print(f"  New Summary: {updated_product.summary}")
        print(f"  New Description: {updated_product.description[:100]}...")
        print(f"  Owners: {len(updated_product.owners)} owner(s)")
        print(f"  Relevant Links: {len(updated_product.relevantLinks)} link(s)")
        
        # Show the newly added owner and links
        if updated_product.owners:
            last_owner = updated_product.owners[-1]
            print(f"  Last Added Owner: {last_owner.name} ({last_owner.email})")
        
        if updated_product.relevantLinks:
            last_link = updated_product.relevantLinks[-1]
            print(f"  Last Added Link: {last_link.label} ({last_link.url})")
        
    except Exception as e:
        print(f"✗ Error updating data product directly: {e}")




def main():
    """Main function to demonstrate data product updates."""
    print("Starburst Data Products Client - Update Data Product")
    print("=" * 55)
    
    # Display authentication configuration
    auth_info = display_auth_config()
    
    if not auth_info['verify_ssl']:
        print("⚠️  SSL certificate verification is disabled")
    
    # Load data product specific configuration
    config = load_data_product_config()
    
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
    
    # Select data product to update
    product = select_data_product(api)
    if not product:
        print("No data product selected. Exiting.")
        sys.exit(0)
    
    try:
        # Demonstrate various update operations
        print(f"\nDemonstrating update operations for: {product.name}")
        
        # Update data product directly (new method)
        update_data_product_directly(api, product)
        
        # Update sample queries
        update_sample_queries(api, product)
        
        # Update tags
        update_tags(api, product)
        
        # Update domain information
        update_domain_information(api, product)
        
        print("\n" + "=" * 55)
        print("Customer data product update operations completed!")
        print("\nUpdate operations demonstrated on iceberg.burst_bank.customer data:")
        print("✓ Direct data product update (metadata and descriptions)")
        print("✓ Sample queries updated (5 customer-focused analytical queries)")
        print("✓ Tags updated (customer-data, banking, demographics, etc.)")  
        print("✓ Domain information updated (customer data governance context)")
        
    except Exception as e:
        print(f"\n✗ Script failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    # Confirmation prompt
    response = input("\nThis script will modify existing data products in your SEP cluster.\nAre you sure you want to continue? (y/N): ")
    if response.lower() not in ['y', 'yes']:
        print("Operation cancelled.")
        sys.exit(0)
    
    main()