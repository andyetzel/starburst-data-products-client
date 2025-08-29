#!/usr/bin/env python3
"""
PII Tags Application Example

This script demonstrates how to apply PII (Personally Identifiable Information) 
tags to data products and their column descriptions using the Starburst Data 
Products API.

Operations demonstrated:
- Identifying data products that might contain PII data
- Applying PII-related tags to data products
- Updating column descriptions to include PII information
- Best practices for PII data classification

Note: This script will modify existing data products and their metadata.
Make sure you have the necessary permissions and are connected to the 
correct environment.
"""

import os
import sys
from datetime import datetime
from typing import List, Optional, Dict

# Add the parent directory to sys.path to import the client
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from starburst_data_products_client.sep.api import Api
from starburst_data_products_client.shared.auth_config import create_api_client_with_messages, get_auth_info, AuthenticationError
from starburst_data_products_client.sep.data import (
    DataProduct,
    DataProductParameters,
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


# PII classification mappings
PII_CLASSIFICATIONS = {
    'HIGH_SENSITIVITY': {
        'keywords': ['ssn', 'social_security', 'credit_card', 'passport', 'driver_license', 'tax_id'],
        'tags': ['pii-high', 'sensitive-data', 'restricted-access', 'compliance-required'],
        'description_suffix': '[PII-HIGH: Highly sensitive personal information]'
    },
    'MEDIUM_SENSITIVITY': {
        'keywords': ['email', 'phone', 'address', 'birth_date', 'birthdate', 'dob'],
        'tags': ['pii-medium', 'personal-data', 'gdpr-applicable'],
        'description_suffix': '[PII-MEDIUM: Personal contact information]'
    },
    'LOW_SENSITIVITY': {
        'keywords': ['name', 'first_name', 'last_name', 'username', 'user_id'],
        'tags': ['pii-low', 'personal-identifier'],
        'description_suffix': '[PII-LOW: Personal identifier]'
    },
    'FINANCIAL': {
        'keywords': ['salary', 'income', 'account_number', 'routing_number', 'iban'],
        'tags': ['pii-financial', 'financial-data', 'restricted-access'],
        'description_suffix': '[PII-FINANCIAL: Financial information]'
    }
}


def classify_pii_columns(columns: List[Column]) -> Dict[str, List[Column]]:
    """Classify columns based on potential PII content."""
    classified = {
        'HIGH_SENSITIVITY': [],
        'MEDIUM_SENSITIVITY': [],
        'LOW_SENSITIVITY': [],
        'FINANCIAL': [],
        'NON_PII': []
    }
    
    for column in columns:
        column_name_lower = column.name.lower()
        column_desc_lower = (column.description or '').lower()
        
        pii_found = False
        
        # Check against each PII classification
        for classification, config in PII_CLASSIFICATIONS.items():
            if any(keyword in column_name_lower or keyword in column_desc_lower 
                   for keyword in config['keywords']):
                classified[classification].append(column)
                pii_found = True
                break
        
        if not pii_found:
            classified['NON_PII'].append(column)
    
    return classified


def select_data_product(api: Api) -> Optional[DataProduct]:
    """Allow user to select a data product for PII tagging."""
    print("=== Select Data Product for PII Tagging ===")
    
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
                    return detailed_product
                else:
                    print("Invalid selection. Please try again.")
            except ValueError:
                print("Please enter a valid number.")
                
    except Exception as e:
        print(f"Error selecting data product: {e}")
        return None


def analyze_pii_content(product: DataProduct):
    """Analyze the data product for potential PII content."""
    print(f"\n=== PII Content Analysis ===")
    print(f"Analyzing data product: {product.name}")
    
    all_columns = []
    
    # Collect all columns from all views
    if product.views:
        for view in product.views:
            if view.columns:
                all_columns.extend(view.columns)
    
    if product.materializedViews:
        for mv in product.materializedViews:
            if hasattr(mv, 'columns') and mv.columns:
                all_columns.extend(mv.columns)
    
    print(f"Total columns to analyze: {len(all_columns)}")
    
    if not all_columns:
        print("No columns found for analysis.")
        return {}
    
    # Classify columns by PII sensitivity
    classified_columns = classify_pii_columns(all_columns)
    
    # Display analysis results
    print(f"\nPII Classification Results:")
    for classification, columns in classified_columns.items():
        if columns:
            print(f"  {classification}: {len(columns)} columns")
            for col in columns:
                print(f"    - {col.name} ({col.type}): {col.description or 'No description'}")
    
    return classified_columns


def apply_pii_tags_to_product(api: Api, product: DataProduct, classified_columns: Dict[str, List[Column]]):
    """Apply PII-related tags to the data product."""
    print(f"\n=== Applying PII Tags to Data Product ===")
    
    try:
        # Get current tags
        current_tags = api.get_tags(product.id)
        current_values = [tag.value for tag in current_tags]
        
        # Determine which PII tags to add based on column classification
        pii_tags_to_add = set()
        
        for classification, columns in classified_columns.items():
            if columns and classification != 'NON_PII':
                config = PII_CLASSIFICATIONS[classification]
                pii_tags_to_add.update(config['tags'])
        
        # Add general PII and compliance tags
        if any(classified_columns[cls] for cls in classified_columns if cls != 'NON_PII'):
            pii_tags_to_add.update([
                'contains-pii',
                'data-privacy',
                'requires-governance',
                f'pii-reviewed-{datetime.now().strftime("%Y%m%d")}'
            ])
        
        # Combine existing tags with new PII tags
        all_tags = list(set(current_values + list(pii_tags_to_add)))
        
        print(f"Current tags: {current_values}")
        print(f"Adding PII tags: {list(pii_tags_to_add)}")
        print(f"Final tags: {all_tags}")
        
        # Apply the updated tags
        api.update_tags(product.id, all_tags)
        
        # Verify the update
        updated_tags = api.get_tags(product.id)
        updated_values = [tag.value for tag in updated_tags]
        print(f"✓ Successfully applied PII tags: {updated_values}")
        
        return True
        
    except Exception as e:
        print(f"✗ Error applying PII tags: {e}")
        return False


def get_pii_category_tag(column: Column, classified_columns: Dict[str, List[Column]]) -> str:
    """Get the appropriate PII category tag for a column."""
    for classification, columns in classified_columns.items():
        if column in columns:
            if classification == 'HIGH_SENSITIVITY':
                return '<pii_high>'
            elif classification == 'MEDIUM_SENSITIVITY':
                return '<pii_medium>'
            elif classification == 'LOW_SENSITIVITY':
                return '<pii_low>'
            elif classification == 'FINANCIAL':
                return '<pii_financial>'
            else:  # NON_PII
                return '<non_pii>'
    return '<non_pii>'


def update_column_descriptions_with_pii(api: Api, product: DataProduct, classified_columns: Dict[str, List[Column]]):
    """Update column descriptions with PII classification tags."""
    print(f"\n=== Updating Column Descriptions with PII Classifications ===")
    
    try:
        # Ask user for confirmation
        response = input("Do you want to add PII classification tags to column descriptions? (y/N): ")
        if response.lower() not in ['y', 'yes']:
            print("Skipping column description updates.")
            return False
        
        print(f"Updating column descriptions with PII classification tags...")
        
        # Create updated views with PII-tagged column descriptions
        updated_views = []
        
        for view in product.views:
            updated_columns = []
            
            for column in view.columns:
                # Get the PII category tag for this column
                pii_tag = get_pii_category_tag(column, classified_columns)
                
                # Update the column description with PII tag
                original_description = column.description or "No description"
                
                # Check if PII tag already exists in description
                if not any(tag in original_description for tag in ['<pii_high>', '<pii_medium>', '<pii_low>', '<pii_financial>', '<non_pii>']):
                    updated_description = f"{original_description} {pii_tag}"
                else:
                    # Replace existing PII tag
                    for existing_tag in ['<pii_high>', '<pii_medium>', '<pii_low>', '<pii_financial>', '<non_pii>']:
                        if existing_tag in original_description:
                            updated_description = original_description.replace(existing_tag, pii_tag)
                            break
                    else:
                        updated_description = f"{original_description} {pii_tag}"
                
                # Create updated column dictionary
                updated_column = {
                    'name': column.name,
                    'type': column.type,
                    'description': updated_description
                }
                updated_columns.append(updated_column)
                
                print(f"  Updated {column.name}: {updated_description}")
            
            # Create updated view dictionary
            updated_view = {
                'name': view.name,
                'description': view.description,
                'definitionQuery': view.definitionQuery,
                'columns': updated_columns,
                'markedForDeletion': False
            }
            updated_views.append(updated_view)
        
        # Create updated materialized views (if any)
        updated_materialized_views = []
        if product.materializedViews:
            for mv in product.materializedViews:
                updated_columns = []
                
                if hasattr(mv, 'columns') and mv.columns:
                    for column in mv.columns:
                        # Get the PII category tag for this column
                        pii_tag = get_pii_category_tag(column, classified_columns)
                        
                        # Update the column description with PII tag
                        original_description = column.description or "No description"
                        
                        # Check if PII tag already exists in description
                        if not any(tag in original_description for tag in ['<pii_high>', '<pii_medium>', '<pii_low>', '<pii_financial>', '<non_pii>']):
                            updated_description = f"{original_description} {pii_tag}"
                        else:
                            # Replace existing PII tag
                            for existing_tag in ['<pii_high>', '<pii_medium>', '<pii_low>', '<pii_financial>', '<non_pii>']:
                                if existing_tag in original_description:
                                    updated_description = original_description.replace(existing_tag, pii_tag)
                                    break
                            else:
                                updated_description = f"{original_description} {pii_tag}"
                        
                        # Create updated column dictionary
                        updated_column = {
                            'name': column.name,
                            'type': column.type,
                            'description': updated_description
                        }
                        updated_columns.append(updated_column)
                
                # Create updated materialized view dictionary
                updated_mv = {
                    'name': mv.name,
                    'description': mv.description,
                    'definitionQuery': mv.definitionQuery,
                    'columns': updated_columns,
                    'markedForDeletion': False
                }
                updated_materialized_views.append(updated_mv)
        
        # Create updated data product parameters
        updated_params = DataProductParameters(
            name=product.name,
            catalogName=product.catalogName,
            schemaName=product.schemaName,
            dataDomainId=product.dataDomainId,
            summary=product.summary,
            description=product.description,
            views=updated_views,
            materializedViews=updated_materialized_views,
            owners=product.owners,
            relevantLinks=product.relevantLinks
        )
        
        # Update the data product
        print(f"\nApplying column description updates to data product...")
        updated_product = api.update_data_product(product.id, updated_params)
        
        print(f"✓ Successfully updated column descriptions with PII classifications!")
        print(f"  Updated {len(updated_views)} view(s)")
        if updated_materialized_views:
            print(f"  Updated {len(updated_materialized_views)} materialized view(s)")
        
        return True
        
    except Exception as e:
        print(f"✗ Error updating column descriptions: {e}")
        return False


def create_pii_aware_data_product_example(api: Api, config: dict):
    """Create a new data product with PII-aware column descriptions as an example."""
    print(f"\n=== Creating PII-Aware Data Product Example ===")
    
    try:
        response = input("Create a new example data product with PII-tagged columns? (y/N): ")
        if response.lower() not in ['y', 'yes']:
            print("Skipping PII-aware data product creation.")
            return
        
        # Get or create a domain
        domains = api.list_domains()
        if not domains:
            print("No domains found. Creating example domain...")
            domain = api.create_domain(
                name=config['domain_name'],
                description="Example domain for PII demonstration",
                schema_location=f"{config['catalog_name']}.{config['schema_name']}"
            )
            domain_id = domain.id
        else:
            domain_id = domains[0].id
        
        # Generate unique name with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        product_name = f"customer_data_pii_{timestamp}"
        
        # Define columns with PII classifications
        pii_columns = [
            Column(
                name="customer_id", 
                type="bigint", 
                description="Unique customer identifier [PII-LOW: Personal identifier]"
            ),
            Column(
                name="first_name", 
                type="varchar", 
                description="Customer's first name [PII-LOW: Personal identifier]"
            ),
            Column(
                name="last_name", 
                type="varchar", 
                description="Customer's last name [PII-LOW: Personal identifier]"
            ),
            Column(
                name="email", 
                type="varchar", 
                description="Customer's email address [PII-MEDIUM: Personal contact information]"
            ),
            Column(
                name="phone_number", 
                type="varchar", 
                description="Customer's phone number [PII-MEDIUM: Personal contact information]"
            ),
            Column(
                name="birth_date", 
                type="date", 
                description="Customer's date of birth [PII-MEDIUM: Personal contact information]"
            ),
            Column(
                name="social_security_number", 
                type="varchar", 
                description="Customer's SSN - ENCRYPTED [PII-HIGH: Highly sensitive personal information]"
            ),
            Column(
                name="home_address", 
                type="varchar", 
                description="Customer's home address [PII-MEDIUM: Personal contact information]"
            ),
            Column(
                name="annual_income", 
                type="decimal(15,2)", 
                description="Customer's annual income [PII-FINANCIAL: Financial information]"
            ),
            Column(
                name="account_balance", 
                type="decimal(15,2)", 
                description="Current account balance [PII-FINANCIAL: Financial information]"
            ),
            Column(
                name="registration_date", 
                type="timestamp", 
                description="Account registration date - Non-PII"
            ),
            Column(
                name="account_status", 
                type="varchar", 
                description="Account status (active, inactive, suspended) - Non-PII"
            )
        ]
        
        # Create view with PII-tagged columns
        view = View(
            name=f"{product_name}_view",
            description="Customer data view with PII classifications",
            createdBy="system",
            definitionQuery=f"SELECT * FROM {config['catalog_name']}.{config['schema_name']}.customer_data",
            status="DRAFT",
            columns=pii_columns,
            markedForDeletion=False,
            createdAt=datetime.now(),
            updatedAt=datetime.now(),
            updatedBy="system",
            publishedAt=None,
            publishedBy=None,
            matchesTrinoDefinition=None
        )
        
        # Define owners
        owners = [
            Owner(name="Data Product Owner", email="dataowner@example.com")
        ]
        
        # Create data product parameters
        data_product_params = DataProductParameters(
            name=product_name,
            catalogName=config['catalog_name'],
            schemaName=config['schema_name'],
            dataDomainId=domain_id,
            summary="Customer data with comprehensive PII classifications",
            description=(
                "This data product contains customer information with detailed PII "
                "classifications for each column. It demonstrates proper PII tagging "
                "and governance practices including:\n"
                "- High sensitivity PII (SSN) with encryption requirements\n"
                "- Medium sensitivity PII (email, phone, address) with access controls\n"
                "- Low sensitivity PII (names) with basic protections\n"
                "- Financial PII (income, balance) with special handling\n"
                "- Non-PII data (registration date, status) with standard access"
            ),
            views=[view],
            materializedViews=[],
            owners=owners,
            relevantLinks=[]
        )
        
        print(f"Creating PII-aware data product: {product_name}")
        
        # Create the data product
        created_product = api.create_data_product(data_product_params)
        
        print(f"✓ Successfully created PII-aware data product!")
        print(f"  ID: {created_product.id}")
        print(f"  Name: {created_product.name}")
        
        # Apply comprehensive PII tags
        pii_tags = [
            'contains-pii',
            'pii-high',
            'pii-medium', 
            'pii-low',
            'pii-financial',
            'sensitive-data',
            'gdpr-applicable',
            'data-privacy',
            'requires-governance',
            'compliance-required',
            'customer-data',
            f'pii-classified-{datetime.now().strftime("%Y%m%d")}'
        ]
        
        print(f"Applying comprehensive PII tags...")
        api.update_tags(created_product.id, pii_tags)
        print(f"✓ Applied PII tags: {pii_tags}")
        
        return created_product
        
    except Exception as e:
        print(f"✗ Error creating PII-aware data product: {e}")
        return None


def generate_pii_governance_report(product: DataProduct, classified_columns: Dict[str, List[Column]]):
    """Generate a PII governance report for the data product."""
    print(f"\n=== PII Governance Report ===")
    print(f"Data Product: {product.name} (ID: {product.id})")
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)
    
    total_pii_columns = sum(len(cols) for cls, cols in classified_columns.items() if cls != 'NON_PII')
    total_columns = sum(len(cols) for cols in classified_columns.values())
    
    print(f"Summary:")
    print(f"  Total Columns: {total_columns}")
    print(f"  PII Columns: {total_pii_columns}")
    print(f"  Non-PII Columns: {len(classified_columns.get('NON_PII', []))}")
    print(f"  PII Percentage: {(total_pii_columns/total_columns*100):.1f}%" if total_columns > 0 else "  PII Percentage: N/A")
    
    print(f"\nDetailed Classification:")
    for classification, columns in classified_columns.items():
        if columns:
            config = PII_CLASSIFICATIONS.get(classification, {})
            tags = config.get('tags', [])
            
            print(f"\n  {classification} ({len(columns)} columns):")
            print(f"    Recommended Tags: {tags}")
            print(f"    Columns:")
            for col in columns:
                print(f"      - {col.name} ({col.type})")
                print(f"        Description: {col.description or 'No description'}")
    
    print(f"\nRecommendations:")
    if classified_columns.get('HIGH_SENSITIVITY'):
        print("  ⚠️  HIGH SENSITIVITY PII detected - implement strong access controls")
    if classified_columns.get('FINANCIAL'):
        print("  💰 FINANCIAL PII detected - ensure compliance with financial regulations")
    if classified_columns.get('MEDIUM_SENSITIVITY'):
        print("  📧 MEDIUM SENSITIVITY PII detected - consider GDPR/privacy law compliance")
    if total_pii_columns > 0:
        print("  🔒 Consider implementing data masking or encryption for PII columns")
        print("  📋 Regular PII audits and access reviews recommended")
        print("  🏷️  Ensure all PII columns are properly tagged and documented")


def main():
    """Main function to demonstrate PII tagging operations."""
    print("Starburst Data Products Client - Apply PII Tags")
    print("=" * 50)
    
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
        print("\nPII Tagging Operations Menu:")
        print("1. Analyze existing data product for PII")
        print("2. Create PII-aware data product example")
        
        choice = input("\nSelect operation (1-2): ")
        
        if choice == '1':
            # Analyze existing data product
            product = select_data_product(api)
            if product:
                classified_columns = analyze_pii_content(product)
                
                if any(classified_columns[cls] for cls in classified_columns if cls != 'NON_PII'):
                    success = apply_pii_tags_to_product(api, product, classified_columns)
                    if success:
                        # Ask user if they want to update column descriptions with PII tags
                        column_update_success = update_column_descriptions_with_pii(api, product, classified_columns)
                        if column_update_success:
                            # Refresh the product data to show updated descriptions in the report
                            updated_product = api.get_data_product(product.id)
                            # Re-analyze to show updated column descriptions
                            updated_classified_columns = analyze_pii_content(updated_product)
                            generate_pii_governance_report(updated_product, updated_classified_columns)
                        else:
                            generate_pii_governance_report(product, classified_columns)
                else:
                    print("No PII content detected in this data product.")
        
        elif choice == '2':
            # Create PII-aware example
            created_product = create_pii_aware_data_product_example(api, config)
            if created_product:
                # Analyze the created product
                classified_columns = analyze_pii_content(created_product)
                generate_pii_governance_report(created_product, classified_columns)
        
        else:
            print("Invalid selection.")
            sys.exit(1)
        
        print("\n" + "=" * 50)
        print("PII tagging operations completed!")
        
    except Exception as e:
        print(f"\n✗ Script failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    # Confirmation prompt
    response = input("\nThis script will apply PII tags to data products in your SEP cluster.\nAre you sure you want to continue? (y/N): ")
    if response.lower() not in ['y', 'yes']:
        print("Operation cancelled.")
        sys.exit(0)
    
    main()