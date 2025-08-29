#!/usr/bin/env python3
"""
OAuth2 Troubleshooting Guide

This script helps troubleshoot OAuth2 authentication issues with the 
Starburst Data Products Client and provides guidance on resolving common problems.
"""

import os
import sys

# Add the parent directory to sys.path to import the client
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from starburst_data_products_client.shared.auth_config import create_api_client, get_auth_info


def check_oauth_configuration():
    """Check OAuth2 configuration and provide troubleshooting guidance."""
    print("=== OAuth2 Troubleshooting Guide ===")
    
    try:
        env_file = os.path.join(os.path.dirname(__file__), '.env')
        auth_info = get_auth_info(env_file)
        
        print(f"✓ Authentication method: {auth_info['method']}")
        print(f"✓ Host: {auth_info['host']}")
        print(f"✓ Protocol: {auth_info['protocol']}")
        
        if auth_info['method'] != 'oauth2':
            print(f"ℹ️  Note: Current authentication method is '{auth_info['method']}', not OAuth2")
            return
        
        print("\n=== OAuth2 Authentication Flow ===")
        print("OAuth2 authentication with Starburst requires several steps:")
        print("1. The client initiates an OAuth2 authorization request")
        print("2. A browser opens with the authorization URL")
        print("3. You authenticate with your identity provider")
        print("4. The authorization code is returned to the client")
        print("5. The client exchanges the code for an access token")
        
        print("\n=== Common OAuth2 Issues and Solutions ===")
        
        print("\n1. 'header WWW-Authenticate not available in the response'")
        print("   Cause: OAuth2 authentication flow is incomplete or token expired")
        print("   Solutions:")
        print("   - Complete the browser-based OAuth2 flow when prompted")
        print("   - Ensure your browser can open the OAuth2 authorization URL")
        print("   - Check that your identity provider is properly configured")
        print("   - Verify the OAuth2 configuration on your Starburst cluster")
        
        print("\n2. Browser cannot open OAuth2 URL (headless/remote environments)")
        print("   Solutions:")
        print("   - Copy the OAuth2 URL and open it manually in a browser")
        print("   - Use OAuth2 JWT authentication instead (if you have a token)")
        print("   - Use basic authentication for development/testing")
        print("   - Configure OAuth2 with a custom callback handler")
        
        print("\n3. OAuth2 authentication fails during API calls")
        print("   Causes:")
        print("   - OAuth2 token expired (tokens have limited lifetime)")
        print("   - Network connectivity issues")
        print("   - Different authentication requirements for specific endpoints")
        print("   Solutions:")
        print("   - Re-run the OAuth2 flow to refresh the token")
        print("   - Check network connectivity to the Starburst cluster")
        print("   - Contact your Starburst administrator about endpoint-specific auth")
        
        print("\n=== Alternative Authentication Methods ===")
        
        print("\nFor development and testing, consider using:")
        print("1. Basic Authentication:")
        print("   AUTH_METHOD=basic")
        print("   SEP_USERNAME=your_username")
        print("   SEP_PASSWORD=your_password")
        
        print("\n2. OAuth2 JWT (if you have a token):")
        print("   AUTH_METHOD=oauth2_jwt")
        print("   JWT_TOKEN=your_jwt_token_here")
        
    except Exception as e:
        print(f"❌ Configuration error: {e}")


def test_oauth_connection():
    """Test OAuth2 connection with basic error handling."""
    print("\n=== Testing OAuth2 Connection ===")
    
    try:
        print("Creating API client with OAuth2...")
        api = create_api_client()
        print("✓ API client created successfully")
        
        print("Testing basic API call (list domains)...")
        domains = api.list_domains()
        print(f"✓ Successfully retrieved {len(domains)} domains")
        print("✓ OAuth2 authentication is working correctly")
        
        return True
        
    except Exception as e:
        error_msg = str(e)
        print(f"❌ OAuth2 connection failed: {error_msg}")
        
        if "WWW-Authenticate not available" in error_msg:
            print("\n💡 This error indicates OAuth2 authentication is incomplete.")
            print("   Make sure to complete the browser-based OAuth2 flow.")
        elif "Authentication failed (401)" in error_msg:
            print("\n💡 This indicates an authentication problem.")
            print("   The OAuth2 token may be expired or invalid.")
        elif "OAuth authentication failed" in error_msg:
            print("\n💡 This indicates an OAuth2 setup issue.")
            print("   Check your Starburst cluster's OAuth2 configuration.")
        
        return False


def main():
    """Main troubleshooting function."""
    print("Starburst Data Products Client - OAuth2 Troubleshooting")
    print("=" * 60)
    
    check_oauth_configuration()
    
    # Only test connection if using OAuth2
    try:
        auth_info = get_auth_info()
        if auth_info['method'] == 'oauth2':
            success = test_oauth_connection()
            
            if not success:
                print("\n" + "=" * 60)
                print("OAuth2 Authentication Failed")
                print("=" * 60)
                print("Consider switching to basic authentication for testing:")
                print("1. Edit your .env file")
                print("2. Change AUTH_METHOD=basic")
                print("3. Add SEP_USERNAME and SEP_PASSWORD")
                print("4. Re-run your script")
        else:
            print(f"\nℹ️  Currently using {auth_info['method']} authentication.")
            print("   To test OAuth2, change AUTH_METHOD=oauth2 in your .env file.")
            
    except Exception as e:
        print(f"❌ Error: {e}")
    
    print("\n" + "=" * 60)
    print("OAuth2 troubleshooting completed!")


if __name__ == "__main__":
    main()