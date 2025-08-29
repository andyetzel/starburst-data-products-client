#!/usr/bin/env python3
"""
Data Product Creation Example (with PII Data for Governance Demo)

This script demonstrates how to create a new data product using the 
Starburst Data Products API. The created data product contains comprehensive
PII (Personally Identifiable Information) data designed to showcase data
governance and privacy compliance capabilities.

Operations demonstrated:
- Interactive selection from existing domains
- Auto-generating schema names from data product titles (REQUIRED)
- Setting up data product parameters with PII-rich dataset
- Defining views with columns containing various PII sensitivity levels
- Setting privacy-aware owners and metadata
- Creating the data product in DRAFT status

IMPORTANT: Schema Name Generation:
- The Data Product's title automatically becomes the schema name
- Users cannot specify a different schema when creating a data product
- The system ensures unique schemas by generating valid names from titles

PII Data Types Included:
- High-sensitivity: SSN, passport, driver license, tax ID (encrypted)
- Medium-sensitivity: email, phone, addresses, birth dates
- Low-sensitivity: names, usernames
- Financial: salary, account numbers, credit scores
- Non-PII: business identifiers, order data, product information

Prerequisites:
- Run 0_create_domain.py first to create the required domain
- Ensure the domain specified in DEFAULT_DOMAIN_NAME exists

Note: This script will create actual data products in your SEP cluster.
Make sure you have the necessary permissions and are connected to the 
correct environment.

Next Steps After Creation:
1. Run 4_apply_pii_tags.py to demonstrate PII classification and tagging
2. Use 5_publish_workflow.py to publish the data product
3. Use 3_update_data_product.py for updates
"""

import os
import sys
import json
import re
from datetime import datetime

# Add the parent directory to sys.path to import the client
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from starburst_data_products_client.sep.api import Api
from starburst_data_products_client.sep.data import (
    DataProductParameters, 
    View, 
    Column, 
    Owner, 
    MaterializedView,
    RelevantLinks
)
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


def select_domain(api: Api) -> str:
    """Allow user to select from available domains."""
    print(f"\n=== Select Domain ===")
    
    try:
        # Get all available domains
        domains = api.list_domains()
        if not domains:
            print("✗ No domains found!")
            print("💡 To create a domain, run: python3 0_create_domain.py")
            raise Exception("No domains available. Please create a domain first using 0_create_domain.py")
        
        print("Available domains:")
        for i, domain in enumerate(domains):
            print(f"  {i+1}. {domain.name} (ID: {domain.id})")
            if domain.description:
                print(f"     Description: {domain.description}")
            print(f"     Schema Location: {domain.schemaLocation or 'Not specified'}")
            print()
        
        # Get user selection
        while True:
            try:
                choice = input(f"Select a domain (1-{len(domains)}) or 'q' to quit: ")
                if choice.lower() == 'q':
                    raise Exception("Domain selection cancelled by user")
                
                index = int(choice) - 1
                if 0 <= index < len(domains):
                    selected_domain = domains[index]
                    print(f"✓ Selected domain: {selected_domain.name} (ID: {selected_domain.id})")
                    return selected_domain.id
                else:
                    print("Invalid selection. Please try again.")
            except ValueError:
                print("Please enter a valid number.")
                
    except Exception as e:
        print(f"✗ Error selecting domain: {e}")
        raise


def generate_schema_name_from_title(product_name: str) -> str:
    """Generate a valid schema name from the data product title.
    
    Args:
        product_name: The data product name/title
        
    Returns:
        A valid schema name based on the product title
    """
    # Convert to lowercase and replace invalid characters with underscores
    schema_name = product_name.lower()
    # Replace any non-alphanumeric characters with underscores
    schema_name = re.sub(r'[^a-z0-9_]', '_', schema_name)
    # Remove multiple consecutive underscores
    schema_name = re.sub(r'_+', '_', schema_name)
    # Remove leading/trailing underscores
    schema_name = schema_name.strip('_')
    # Ensure it doesn't start with a number
    if schema_name and schema_name[0].isdigit():
        schema_name = f"dp_{schema_name}"
    
    return schema_name


def create_sample_data_product(api: Api, config: dict, domain_id: str):
    """Create a sample data product with views and columns."""
    print(f"\n=== Creating Data Product ===")
    
    # Generate unique name with timestamp - includes PII data for demonstration
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    product_name = f"customer_data_pii_demo_{timestamp}"
    
    # Generate schema name from the data product title (REQUIRED: schema = product name)
    schema_name = generate_schema_name_from_title(product_name)
    print(f"📝 Auto-generated schema name from product title: '{schema_name}'")
    print(f"   (Schema name is automatically derived from the Data Product title)")
    
    try:
        # Define columns based on the real iceberg.burst_bank.customer table schema
        # These columns showcase different PII sensitivity levels for the 4_apply_pii_tags.py script
        columns = [
            # Business identifiers and LOW_SENSITIVITY PII
            Column(name="custkey", type="varchar", description="Unique customer key identifier"),
            Column(name="first_name", type="varchar", description="Customer's first name"),
            Column(name="last_name", type="varchar", description="Customer's last name"),
            
            # MEDIUM_SENSITIVITY PII - Address and contact information  
            Column(name="street", type="varchar", description="Customer's street address"),
            Column(name="city", type="varchar", description="Customer's city"),
            Column(name="state", type="varchar", description="Customer's state or province"),
            Column(name="postcode", type="varchar", description="Customer's postal code"),
            Column(name="country", type="varchar", description="Customer's country"),
            Column(name="phone", type="varchar", description="Customer's phone number"),
            
            # MEDIUM_SENSITIVITY PII - Personal information
            Column(name="dob", type="varchar", description="Customer's date of birth"),
            Column(name="gender", type="varchar", description="Customer's gender"),
            Column(name="married", type="varchar", description="Customer's marital status"),
            Column(name="registration_date", type="varchar", description="Date when customer registered"),
            
            # HIGH_SENSITIVITY PII - Highly sensitive personal information
            Column(name="ssn", type="varchar", description="Customer's social security number"),
            Column(name="paycheck_dd", type="varchar", description="Customer's direct deposit information"),
            
            # FINANCIAL PII - Financial information
            Column(name="estimated_income", type="double", description="Customer's estimated annual income"),
            Column(name="fico", type="integer", description="Customer's FICO credit score"),
        ]
        
        # Define a view based on the real iceberg.burst_bank.customer table
        # Use dictionary structure for creation, not the full View dataclass
        view = {
            'name': f"{product_name}_view",
            'description': f"Customer data view in schema '{schema_name}' sourced from iceberg.burst_bank.customer containing PII information for governance demonstration",
            'definitionQuery': "SELECT * FROM iceberg.burst_bank.customer",
            'columns': [
                {
                    'name': col.name,
                    'type': col.type,
                    'description': col.description
                } for col in columns
            ],
            'markedForDeletion': False
        }
        
        # Define owners
        owners = [
            Owner(name="Data Product Owner", email="dataowner@example.com")
        ]
        
        # Define relevant links (optional)
        relevant_links = [
            RelevantLinks(
                label="Data Privacy Policy", 
                url="https://docs.example.com/data-privacy-policy"
            ),
            RelevantLinks(
                label="PII Governance Guide", 
                url="https://docs.example.com/pii-governance"
            )
        ] if hasattr(RelevantLinks, '__init__') else []
        
        # Create data product parameters
        data_product_params = DataProductParameters(
            name=product_name,
            catalogName="iceberg",
            schemaName=schema_name,  # Use auto-generated schema name based on product title
            dataDomainId=domain_id,
            summary="Customer data product containing PII for governance demonstration",
            description=(
                f"This data product creates its own dedicated schema '{schema_name}' and is based on the "
                "real iceberg.burst_bank.customer table. It contains comprehensive customer information with "
                "various levels of Personally Identifiable Information (PII) for demonstrating data "
                "governance and privacy compliance capabilities. It includes:\n"
                "• High-sensitivity PII: SSN, direct deposit information\n"
                "• Medium-sensitivity PII: addresses, phone, date of birth, gender, marital status\n" 
                "• Low-sensitivity PII: names, customer keys\n"
                "• Financial PII: estimated income, FICO credit scores\n"
                "• Non-PII business data: registration dates, geographic information\n\n"
                "The schema name is automatically generated from the data product title to ensure uniqueness. "
                "This dataset is designed to showcase PII classification, tagging, and "
                "governance workflows using the 4_apply_pii_tags.py script."
            ),
            views=[view],
            materializedViews=[],  # No materialized views in this example
            owners=owners,
            relevantLinks=relevant_links
        )
        
        print(f"Creating PII demonstration data product: {product_name}")
        print(f"  Catalog: iceberg")
        print(f"  Schema: {schema_name}")
        print(f"  Source Table: iceberg.burst_bank.customer")
        print(f"  Domain ID: {domain_id}")
        print(f"  Views: {len(data_product_params.views)}")
        print(f"  Total Columns: {len(columns)}")
        print(f"  PII Column Breakdown:")
        print(f"    • Low-sensitivity PII: 3 columns (custkey, first_name, last_name)")
        print(f"    • Medium-sensitivity PII: 8 columns (addresses, phone, dob, gender, etc.)") 
        print(f"    • High-sensitivity PII: 2 columns (ssn, paycheck_dd)")
        print(f"    • Financial PII: 2 columns (estimated_income, fico)")
        print(f"    • Non-PII: 2 columns (registration_date, country)")
        
        # Display the JSON payload that will be sent to the API
        print(f"\n=== API Request Payload ===")
        try:
            # Convert the DataProductParameters to a dictionary for JSON serialization
            payload_dict = {
                'name': data_product_params.name,
                'catalogName': data_product_params.catalogName,
                'schemaName': data_product_params.schemaName,
                'dataDomainId': data_product_params.dataDomainId,
                'summary': data_product_params.summary,
                'description': data_product_params.description,
                'views': data_product_params.views,
                'materializedViews': data_product_params.materializedViews,
                'owners': [{'name': owner.name, 'email': owner.email} for owner in data_product_params.owners],
                'relevantLinks': [{'label': link.label, 'url': link.url} for link in data_product_params.relevantLinks] if data_product_params.relevantLinks else []
            }
            
            # Pretty print the JSON payload
            json_payload = json.dumps(payload_dict, indent=2, default=str)
            print(json_payload)
            print("=" * 50)
            
        except Exception as json_error:
            print(f"Could not serialize payload for display: {json_error}")
        
        # Create the data product
        print(f"\nSending API request to create data product...")
        created_product = api.create_data_product(data_product_params)
        
        print(f"✓ Successfully created data product!")
        print(f"  ID: {created_product.id}")
        print(f"  Name: {created_product.name}")
        print(f"  Status: {created_product.status}")
        print(f"  Created At: {created_product.createdAt}")
        
        return created_product
        
    except Exception as e:
        print(f"✗ Error creating data product: {e}")
        raise



def main():
    """Main function to create a data product."""
    print("Starburst Data Products Client - Create Data Product")
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
    
    try:
        # Allow user to select from available domains
        domain_id = select_domain(api)
        
        # Create the data product
        created_product = create_sample_data_product(api, config, domain_id)
        
        print("\n" + "=" * 55)
        print("Data product creation completed!")
        print(f"Product ID: {created_product.id}")
        print(f"Product Name: {created_product.name}")
        print(f"Product Status: {created_product.status}")
        
        print(f"\n📝 Next Steps:")
        print(f"  1. Review the created data product in the SEP UI")
        print(f"  2. ⭐ DEMO PII GOVERNANCE: python3 4_apply_pii_tags.py")
        print(f"     This will analyze and tag the PII columns you just created!")
        print(f"  3. To publish this data product, use: python3 5_publish_workflow.py")
        print(f"  4. To update the data product, use: python3 3_update_data_product.py")
        print(f"  5. To create more domains, use: python3 0_create_domain.py")
        
    except Exception as e:
        print(f"\n✗ Script failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    # Confirmation prompt
    response = input("\nThis script will create a new data product in your SEP cluster.\nAre you sure you want to continue? (y/N): ")
    if response.lower() not in ['y', 'yes']:
        print("Operation cancelled.")
        sys.exit(0)
    
    main()