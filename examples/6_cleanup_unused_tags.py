#!/usr/bin/env python3
"""
Tag Cleanup Example

This script demonstrates how to identify and remove unused tags from the 
Starburst Data Products system. It analyzes all data products to identify
which tags are currently in use and presents options for cleaning up 
unused tags.

Operations demonstrated:
- Scanning all data products for tag usage
- Identifying unused tags in the system
- Selective or bulk deletion of unused tags
- Tag usage analytics and reporting

Note: This script will modify the tag system in your SEP cluster.
Make sure you have the necessary permissions and are connected to the 
correct environment.

Safety Features:
- Confirmation prompts before deletion
- Detailed reporting of tag usage
- Option to preview changes before applying
"""

import os
import sys
from datetime import datetime
from typing import List, Dict, Set, Tuple
from collections import Counter

# Add the parent directory to sys.path to import the client
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from starburst_data_products_client.sep.api import Api
from starburst_data_products_client.shared.auth_config import create_api_client_with_messages, get_auth_info, AuthenticationError
from starburst_data_products_client.sep.data import DataProduct


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


def scan_data_product_tags(api: Api) -> Tuple[Dict[str, Set[str]], Dict[str, int]]:
    """Scan all data products and collect tag usage information."""
    print("=== Scanning Data Products for Tag Usage ===")
    
    try:
        # Get all data products
        print("Fetching all data products...")
        products = api.search_data_products()
        
        if not products:
            print("No data products found.")
            return {}, {}
        
        print(f"Found {len(products)} data product(s)")
        
        tag_to_products = {}  # tag_value -> set of product names that use it
        tag_usage_count = {}  # tag_value -> count of products using it
        
        # Scan each data product for tags
        for i, product in enumerate(products, 1):
            print(f"  Scanning {i}/{len(products)}: {product.name}")
            
            try:
                # Get detailed product information including tags
                detailed_product = api.get_data_product(product.id)
                product_tags = api.get_tags(product.id)
                
                for tag in product_tags:
                    tag_value = tag.value
                    
                    # Initialize tracking for this tag if not seen before
                    if tag_value not in tag_to_products:
                        tag_to_products[tag_value] = set()
                        tag_usage_count[tag_value] = 0
                    
                    # Record which product uses this tag
                    tag_to_products[tag_value].add(product.name)
                    tag_usage_count[tag_value] += 1
                    
            except Exception as e:
                print(f"    ⚠️ Error scanning {product.name}: {e}")
                continue
        
        return tag_to_products, tag_usage_count
        
    except Exception as e:
        print(f"✗ Error scanning data products: {e}")
        return {}, {}


def get_all_available_tags(api: Api) -> List[str]:
    """Get all available tags in the system."""
    print("\n=== Getting All Available Tags ===")
    
    try:
        # Note: This assumes there's a method to get all tags in the system
        # If this method doesn't exist, we'll need to collect tags differently
        
        # Alternative approach: collect unique tags from all data products
        print("Collecting all unique tags from data products...")
        products = api.search_data_products()
        all_tags = set()
        
        for product in products:
            try:
                product_tags = api.get_tags(product.id)
                for tag in product_tags:
                    all_tags.add(tag.value)
            except Exception as e:
                print(f"    ⚠️ Error getting tags for {product.name}: {e}")
                continue
        
        all_tags_list = sorted(list(all_tags))
        print(f"Found {len(all_tags_list)} unique tags in the system")
        
        return all_tags_list
        
    except Exception as e:
        print(f"✗ Error getting all tags: {e}")
        return []


def analyze_tag_usage(tag_to_products: Dict[str, Set[str]], tag_usage_count: Dict[str, int], all_tags: List[str]) -> Dict[str, List[str]]:
    """Analyze tag usage and categorize tags."""
    print(f"\n=== Tag Usage Analysis ===")
    
    # Categorize tags
    used_tags = set(tag_usage_count.keys())
    all_tags_set = set(all_tags)
    unused_tags = all_tags_set - used_tags
    
    # Sort tags by usage frequency
    frequently_used = []
    occasionally_used = []
    rarely_used = []
    
    for tag, count in tag_usage_count.items():
        if count >= 5:
            frequently_used.append(tag)
        elif count >= 2:
            occasionally_used.append(tag)
        else:
            rarely_used.append(tag)
    
    # Sort each category
    frequently_used.sort()
    occasionally_used.sort()
    rarely_used.sort()
    unused_tags_list = sorted(list(unused_tags))
    
    print(f"Tag Usage Summary:")
    print(f"  Total unique tags in system: {len(all_tags)}")
    print(f"  Tags in use: {len(used_tags)}")
    print(f"  Unused tags: {len(unused_tags)}")
    print(f"  Frequently used (5+ products): {len(frequently_used)}")
    print(f"  Occasionally used (2-4 products): {len(occasionally_used)}")
    print(f"  Rarely used (1 product): {len(rarely_used)}")
    
    return {
        'frequently_used': frequently_used,
        'occasionally_used': occasionally_used,
        'rarely_used': rarely_used,
        'unused': unused_tags_list
    }


def display_detailed_tag_report(tag_to_products: Dict[str, Set[str]], tag_usage_count: Dict[str, int], categorized_tags: Dict[str, List[str]]):
    """Display a detailed report of tag usage."""
    print(f"\n=== Detailed Tag Usage Report ===")
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    # Show frequently used tags
    if categorized_tags['frequently_used']:
        print(f"\n🟢 FREQUENTLY USED TAGS ({len(categorized_tags['frequently_used'])} tags):")
        for tag in categorized_tags['frequently_used']:
            count = tag_usage_count[tag]
            print(f"  • {tag} (used by {count} products)")
    
    # Show occasionally used tags
    if categorized_tags['occasionally_used']:
        print(f"\n🟡 OCCASIONALLY USED TAGS ({len(categorized_tags['occasionally_used'])} tags):")
        for tag in categorized_tags['occasionally_used']:
            count = tag_usage_count[tag]
            products = list(tag_to_products[tag])[:3]  # Show first 3 products
            products_str = ", ".join(products)
            if len(tag_to_products[tag]) > 3:
                products_str += f", +{len(tag_to_products[tag]) - 3} more"
            print(f"  • {tag} (used by {count} products: {products_str})")
    
    # Show rarely used tags
    if categorized_tags['rarely_used']:
        print(f"\n🟠 RARELY USED TAGS ({len(categorized_tags['rarely_used'])} tags):")
        for tag in categorized_tags['rarely_used']:
            products = list(tag_to_products[tag])
            products_str = ", ".join(products)
            print(f"  • {tag} (used by: {products_str})")
    
    # Show unused tags
    if categorized_tags['unused']:
        print(f"\n🔴 UNUSED TAGS ({len(categorized_tags['unused'])} tags):")
        print("These tags exist but are not assigned to any data products:")
        for i, tag in enumerate(categorized_tags['unused']):
            if i % 5 == 0:
                print("  ", end="")
            print(f"{tag:<20}", end="")
            if (i + 1) % 5 == 0:
                print()
        if len(categorized_tags['unused']) % 5 != 0:
            print()


def select_tags_for_deletion(categorized_tags: Dict[str, List[str]]) -> List[str]:
    """Allow user to select which tags to delete."""
    print(f"\n=== Tag Deletion Selection ===")
    
    if not categorized_tags['unused'] and not categorized_tags['rarely_used']:
        print("No unused or rarely used tags found for cleanup.")
        return []
    
    tags_to_delete = []
    
    # Handle unused tags
    if categorized_tags['unused']:
        print(f"\nUnused tags ({len(categorized_tags['unused'])} tags):")
        for i, tag in enumerate(categorized_tags['unused']):
            print(f"  {i+1:2d}. {tag}")
        
        response = input(f"\nDelete all unused tags? (y/N): ")
        if response.lower() in ['y', 'yes']:
            tags_to_delete.extend(categorized_tags['unused'])
            print(f"✓ Selected {len(categorized_tags['unused'])} unused tags for deletion")
        else:
            # Allow individual selection
            while True:
                try:
                    selection = input("Enter tag numbers to delete (comma-separated) or 'done': ")
                    if selection.lower() == 'done':
                        break
                    
                    indices = [int(x.strip()) - 1 for x in selection.split(',')]
                    selected_tags = [categorized_tags['unused'][i] for i in indices if 0 <= i < len(categorized_tags['unused'])]
                    tags_to_delete.extend(selected_tags)
                    print(f"✓ Selected {len(selected_tags)} unused tags for deletion")
                    break
                    
                except (ValueError, IndexError):
                    print("Invalid selection. Please enter valid tag numbers.")
    
    # Handle rarely used tags
    if categorized_tags['rarely_used']:
        print(f"\nRarely used tags ({len(categorized_tags['rarely_used'])} tags):")
        for i, tag in enumerate(categorized_tags['rarely_used']):
            print(f"  {i+1:2d}. {tag} (used by 1 product)")
        
        response = input(f"\nReview rarely used tags for deletion? (y/N): ")
        if response.lower() in ['y', 'yes']:
            while True:
                try:
                    selection = input("Enter tag numbers to delete (comma-separated) or 'skip': ")
                    if selection.lower() == 'skip':
                        break
                    
                    indices = [int(x.strip()) - 1 for x in selection.split(',')]
                    selected_tags = [categorized_tags['rarely_used'][i] for i in indices if 0 <= i < len(categorized_tags['rarely_used'])]
                    tags_to_delete.extend(selected_tags)
                    print(f"✓ Selected {len(selected_tags)} rarely used tags for deletion")
                    break
                    
                except (ValueError, IndexError):
                    print("Invalid selection. Please enter valid tag numbers.")
    
    return tags_to_delete


def delete_tags_from_system(api: Api, tags_to_delete: List[str]) -> bool:
    """Delete specified tags from the system."""
    print(f"\n=== Deleting Tags ===")
    
    if not tags_to_delete:
        print("No tags selected for deletion.")
        return True
    
    print(f"Tags to delete: {len(tags_to_delete)}")
    for tag in tags_to_delete:
        print(f"  • {tag}")
    
    # Final confirmation
    response = input(f"\n⚠️ Are you sure you want to delete these {len(tags_to_delete)} tags? This action cannot be undone! (yes/N): ")
    if response.lower() != 'yes':
        print("Tag deletion cancelled.")
        return False
    
    # Note: The API might not have a direct delete tag method
    # This would need to be implemented based on the actual API capabilities
    # For now, we'll simulate the process
    
    print("Deleting tags...")
    deleted_count = 0
    failed_count = 0
    
    for tag in tags_to_delete:
        try:
            # This is a placeholder - actual implementation would depend on API
            # api.delete_tag(tag)  # Hypothetical method
            print(f"  ⚠️ Tag deletion not implemented in API: {tag}")
            # For now, we'll just track what would be deleted
            failed_count += 1
            
        except Exception as e:
            print(f"  ✗ Failed to delete tag '{tag}': {e}")
            failed_count += 1
    
    if deleted_count > 0:
        print(f"✓ Successfully deleted {deleted_count} tags")
    if failed_count > 0:
        print(f"⚠️ Note: Tag deletion functionality needs to be implemented in the API")
        print(f"   {failed_count} tags would be deleted when API supports it")
    
    return deleted_count > 0 or failed_count == 0


def demonstrate_tag_cleanup_operations(api: Api):
    """Demonstrate various tag cleanup operations."""
    
    print(f"Tag Cleanup Operations Menu:")
    print("1. Scan and analyze all tag usage")
    print("2. Generate detailed tag usage report")
    print("3. Clean up unused tags")
    print("4. Full cleanup workflow (recommended)")
    print("5. Exit")
    
    while True:
        try:
            choice = input(f"\nSelect an option (1-5): ").strip()
            
            if choice == '1':
                # Scan and analyze tags
                tag_to_products, tag_usage_count = scan_data_product_tags(api)
                all_tags = get_all_available_tags(api)
                categorized_tags = analyze_tag_usage(tag_to_products, tag_usage_count, all_tags)
                
            elif choice == '2':
                # Generate detailed report
                tag_to_products, tag_usage_count = scan_data_product_tags(api)
                all_tags = get_all_available_tags(api)
                categorized_tags = analyze_tag_usage(tag_to_products, tag_usage_count, all_tags)
                display_detailed_tag_report(tag_to_products, tag_usage_count, categorized_tags)
                
            elif choice == '3':
                # Clean up unused tags
                tag_to_products, tag_usage_count = scan_data_product_tags(api)
                all_tags = get_all_available_tags(api)
                categorized_tags = analyze_tag_usage(tag_to_products, tag_usage_count, all_tags)
                
                tags_to_delete = select_tags_for_deletion(categorized_tags)
                if tags_to_delete:
                    delete_tags_from_system(api, tags_to_delete)
                
            elif choice == '4':
                # Full cleanup workflow
                print("\n=== Full Tag Cleanup Workflow ===")
                
                # Step 1: Scan and analyze
                tag_to_products, tag_usage_count = scan_data_product_tags(api)
                all_tags = get_all_available_tags(api)
                categorized_tags = analyze_tag_usage(tag_to_products, tag_usage_count, all_tags)
                
                # Step 2: Show detailed report
                display_detailed_tag_report(tag_to_products, tag_usage_count, categorized_tags)
                
                # Step 3: Select and delete tags
                tags_to_delete = select_tags_for_deletion(categorized_tags)
                if tags_to_delete:
                    success = delete_tags_from_system(api, tags_to_delete)
                    if success:
                        print("\n✓ Tag cleanup completed successfully!")
                    else:
                        print("\n⚠️ Tag cleanup completed with some issues.")
                else:
                    print("\n✓ No tags selected for deletion. Cleanup completed.")
                
            elif choice == '5':
                print("Exiting tag cleanup operations")
                break
                
            else:
                print("Invalid option. Please select 1-5.")
                
        except KeyboardInterrupt:
            print(f"\nOperation interrupted")
            break
        except Exception as e:
            print(f"Error in operation: {e}")


def main():
    """Main function to demonstrate tag cleanup operations."""
    print("Starburst Data Products Client - Tag Cleanup")
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
        # Demonstrate tag cleanup operations
        demonstrate_tag_cleanup_operations(api)
        
        print("\n" + "=" * 50)
        print("Tag cleanup operations completed!")
        print(f"\n📝 Next Steps:")
        print(f"  1. Review tag usage regularly to maintain a clean tag ecosystem")
        print(f"  2. Consider implementing tag governance policies")
        print(f"  3. Use standardized tag naming conventions")
        print(f"  4. Regular cleanup helps improve data product discoverability")
        
    except Exception as e:
        print(f"\n✗ Script failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    # Confirmation prompt
    response = input("\nThis script will analyze and potentially delete tags in your SEP cluster.\nAre you sure you want to continue? (y/N): ")
    if response.lower() not in ['y', 'yes']:
        print("Operation cancelled.")
        sys.exit(0)
    
    main()