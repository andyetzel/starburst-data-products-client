#!/usr/bin/env python3
"""
OAuth Authentication Example

This script demonstrates how to use OAuth authentication with the 
Starburst Data Products Client, similar to how you would use OAuth
with pystarburst and the Trino client.

Operations demonstrated:
- Connecting to SEP cluster using OAuth2Authentication
- Performing basic data product operations with OAuth
- Comparing OAuth vs basic authentication approaches

Requirements:
- trino[all] package must be installed
- OAuth configuration in your Starburst cluster
"""

import os
import sys

# Add the parent directory to sys.path to import the client
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from starburst_data_products_client.shared.auth_config import create_api_client, get_auth_info, AuthenticationError, AuthConfig
from starburst_data_products_client.sep.api import Api


def demonstrate_centralized_auth_config():
    """Demonstrate using centralized authentication configuration."""
    print("=== Centralized Authentication Configuration ===")
    
    try:
        env_file = os.path.join(os.path.dirname(__file__), '.env')
        
        # Display current authentication configuration
        auth_info = get_auth_info(env_file)
        print(f"Current authentication method: {auth_info['method']}")
        print(f"Host: {auth_info['host']}")
        print(f"Protocol: {auth_info['protocol']}")
        
        # Create API client automatically based on .env configuration
        api = create_api_client(env_file)
        print(f"✅ Successfully created API client using {auth_info['method']} authentication")
        
        return api, auth_info
        
    except AuthenticationError as e:
        print(f"❌ Authentication configuration error: {e}")
        print("Please check your .env file. Available methods:")
        print("  - basic: Username/password authentication")
        print("  - oauth2: OAuth2 authentication")
        print("  - oauth2_jwt: OAuth2 JWT authentication")
        print("  - kerberos: Kerberos authentication")
        return None, None
    except Exception as e:
        print(f"❌ Error: {e}")
        return None, None


def demonstrate_different_auth_methods():
    """Demonstrate different authentication methods by temporarily changing config."""
    print("\n=== Demonstrating Different Authentication Methods ===")
    
    # This shows how you could programmatically create different auth configs
    methods_to_demo = [
        ("OAuth2", "oauth2", {}),
        ("OAuth2 JWT", "oauth2_jwt", {"JWT_TOKEN": "demo-token-here"}),
        ("Kerberos", "kerberos", {"KERBEROS_SERVICE_NAME": "starburst"}),
        ("Basic Auth", "basic", {"SEP_USERNAME": "admin", "SEP_PASSWORD": "password"})
    ]
    
    print("The following authentication methods are available:")
    for name, method, required_vars in methods_to_demo:
        print(f"\n{name} (AUTH_METHOD={method}):")
        print(f"  Required environment variables:")
        if not required_vars:
            print("    None (uses automatic configuration)")
        else:
            for var, example in required_vars.items():
                print(f"    {var}={example}")


def test_api_operations(api, auth_method):
    """Test basic API operations with the given API client."""
    print(f"\n--- Testing API operations with {auth_method} authentication ---")
    
    try:
        # Search for data products
        data_products = api.search_data_products()
        print(f"✅ Found {len(data_products)} data products")
        
        # List domains
        domains = api.list_domains()
        print(f"✅ Found {len(domains)} domains")
        
        if data_products:
            # Get details of first data product
            first_product = api.get_data_product(data_products[0].id)
            print(f"✅ Retrieved details for data product: {first_product.name}")
            
            # Get tags for the data product
            tags = api.get_tags(data_products[0].id)
            print(f"✅ Retrieved {len(tags)} tags for data product")
        else:
            print("ℹ️  No data products available for detailed testing")
        
        return True
        
    except Exception as e:
        print(f"❌ API operation failed: {e}")
        return False


def compare_authentication_methods():
    """Compare OAuth and basic authentication side by side."""
    print("\n=== Authentication Method Comparison ===")
    print("OAuth Benefits:")
    print("  - More secure (no password transmission)")
    print("  - Token-based authentication")
    print("  - Better integration with enterprise identity systems")
    print("  - Automatic token refresh")
    print()
    print("Basic Auth Benefits:")
    print("  - Simpler setup")
    print("  - No additional OAuth infrastructure required")
    print("  - Direct username/password authentication")


def main():
    """Main function demonstrating centralized authentication configuration."""
    print("Starburst Data Products Client - Centralized Authentication Example")
    print("=" * 70)
    
    # Demonstrate centralized authentication configuration
    api, auth_info = demonstrate_centralized_auth_config()
    
    if api is None:
        print("\n❌ Failed to create API client. Please check your .env file.")
        demonstrate_different_auth_methods()
        sys.exit(1)
    
    # Test API operations with the configured authentication
    success = test_api_operations(api, auth_info['method'])
    
    if success:
        print(f"\n✅ All operations completed successfully with {auth_info['method']} authentication!")
    else:
        print(f"\n⚠️  Some operations failed with {auth_info['method']} authentication.")
    
    # Show information about different authentication methods
    demonstrate_different_auth_methods()
    compare_authentication_methods()
    
    print("\n" + "=" * 70)
    print("Centralized authentication examples completed!")
    print("\n📝 How to use centralized authentication in your code:")
    print("```python")
    print("from starburst_data_products_client.shared.auth_config import create_api_client")
    print()
    print("# Create API client automatically using .env configuration")
    print("api = create_api_client()")
    print()
    print("# Or specify a custom .env file")
    print("api = create_api_client('/path/to/your/.env')")
    print()
    print("# Use API normally - authentication is handled automatically")
    print("data_products = api.search_data_products()")
    print("```")
    print()
    print("🔧 Configure authentication in your .env file:")
    print("```")
    print("AUTH_METHOD=oauth2")
    print("SEP_HOST=your-sep-host.com")
    print("SEP_PROTOCOL=https")
    print("```")


if __name__ == "__main__":
    main()