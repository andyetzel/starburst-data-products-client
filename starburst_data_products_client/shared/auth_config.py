"""
Authentication Configuration Module

This module provides centralized authentication configuration for the Starburst Data Products Client.
It reads authentication settings from environment variables and creates the appropriate API client
configuration automatically.

Supported authentication methods:
- basic: Username and password authentication (default)
- oauth2: OAuth2 authentication using trino.auth.OAuth2Authentication
- oauth2_jwt: OAuth2 JWT authentication using trino.auth.JWTAuthentication
- kerberos: Kerberos authentication using trino.auth.KerberosAuthentication

Environment Variables:
- AUTH_METHOD: Authentication method to use (basic, oauth2, oauth2_jwt, kerberos)
- SEP_HOST: Starburst Enterprise Platform hostname
- SEP_PROTOCOL: Protocol (http/https, defaults to https)
- SSL_VERIFY: Whether to verify SSL certificates (true/false, defaults to true)

For basic authentication:
- SEP_USERNAME: Username for basic authentication
- SEP_PASSWORD: Password for basic authentication

For OAuth2 authentication:
- No additional variables required (uses default OAuth2Authentication)

For OAuth2 JWT authentication:
- SEP_JWT_TOKEN: JWT token for authentication

For Kerberos authentication:
- KERBEROS_SERVICE_NAME: Kerberos service name
- KERBEROS_CONFIG: Path to Kerberos configuration file (optional)
- KERBEROS_KEYTAB: Path to Kerberos keytab file (optional)
- KERBEROS_PRINCIPAL: Kerberos principal (optional)
"""

import os
import warnings
from typing import Dict, Any, Optional, Tuple
from dotenv import load_dotenv


class AuthenticationError(Exception):
    """Raised when authentication configuration is invalid."""
    pass


class AuthConfig:
    """Centralized authentication configuration."""
    
    SUPPORTED_METHODS = ['basic', 'oauth2', 'oauth2_jwt', 'kerberos']
    
    def __init__(self, env_file: Optional[str] = None):
        """Initialize authentication configuration.
        
        Args:
            env_file: Path to .env file. If None, uses default .env discovery.
        """
        if env_file:
            load_dotenv(env_file)
        else:
            load_dotenv()
        
        self.method = self._get_auth_method()
        self.host = self._get_required_env('SEP_HOST', 'Starburst host')
        self.protocol = os.getenv('SEP_PROTOCOL', 'https')
        self.verify_ssl = self._parse_ssl_verify(os.getenv('SSL_VERIFY'))
        
        # Validate host format
        if '://' in self.host:
            raise AuthenticationError('SEP_HOST should not include protocol (use SEP_PROTOCOL instead)')
    
    def _get_auth_method(self) -> str:
        """Get and validate authentication method from environment."""
        method = os.getenv('AUTH_METHOD', 'basic').lower()
        if method not in self.SUPPORTED_METHODS:
            raise AuthenticationError(
                f"Unsupported authentication method '{method}'. "
                f"Supported methods: {', '.join(self.SUPPORTED_METHODS)}"
            )
        return method
    
    def _get_required_env(self, var_name: str, description: str) -> str:
        """Get required environment variable."""
        value = os.getenv(var_name)
        if not value:
            raise AuthenticationError(f"Missing required environment variable: {var_name} ({description})")
        return value
    
    def _parse_ssl_verify(self, value: Optional[str]) -> bool:
        """Parse SSL verification setting."""
        if value is None:
            return True  # Default to True for security
        return value.lower() in ('true', '1', 'yes', 'on')
    
    def get_api_kwargs(self) -> Dict[str, Any]:
        """Get API constructor arguments based on authentication method.
        
        Returns:
            Dictionary of arguments for Api constructor
            
        Raises:
            AuthenticationError: If authentication configuration is invalid
            ImportError: If required authentication library is not available
        """
        base_kwargs = {
            'host': self.host,
            'protocol': self.protocol,
            'verify_ssl': self.verify_ssl
        }
        
        if self.method == 'basic':
            return self._get_basic_auth_kwargs(base_kwargs)
        elif self.method == 'oauth2':
            return self._get_oauth2_auth_kwargs(base_kwargs)
        elif self.method == 'oauth2_jwt':
            return self._get_oauth2_jwt_auth_kwargs(base_kwargs)
        elif self.method == 'kerberos':
            return self._get_kerberos_auth_kwargs(base_kwargs)
        else:
            raise AuthenticationError(f"Authentication method '{self.method}' not implemented")
    
    def _get_basic_auth_kwargs(self, base_kwargs: Dict[str, Any]) -> Dict[str, Any]:
        """Get basic authentication arguments."""
        username = self._get_required_env('SEP_USERNAME', 'username for basic authentication')
        password = self._get_required_env('SEP_PASSWORD', 'password for basic authentication')
        
        return {
            **base_kwargs,
            'username': username,
            'password': password
        }
    
    def _get_oauth2_auth_kwargs(self, base_kwargs: Dict[str, Any]) -> Dict[str, Any]:
        """Get OAuth2 authentication arguments."""
        try:
            from trino.auth import OAuth2Authentication
        except ImportError:
            raise ImportError(
                "OAuth2 authentication requires trino package. "
                "Install with: pip install 'trino[all]'"
            )
        
        oauth_auth = OAuth2Authentication()
        return {
            **base_kwargs,
            'auth': oauth_auth
        }
    
    def _get_oauth2_jwt_auth_kwargs(self, base_kwargs: Dict[str, Any]) -> Dict[str, Any]:
        """Get OAuth2 JWT authentication arguments."""
        try:
            from trino.auth import JWTAuthentication
        except ImportError:
            raise ImportError(
                "OAuth2 JWT authentication requires trino package. "
                "Install with: pip install 'trino[all]'"
            )
        
        jwt_token = self._get_required_env('SEP_JWT_TOKEN', 'JWT token for OAuth2 JWT authentication')
        jwt_auth = JWTAuthentication(jwt_token)
        
        return {
            **base_kwargs,
            'auth': jwt_auth
        }
    
    def _get_kerberos_auth_kwargs(self, base_kwargs: Dict[str, Any]) -> Dict[str, Any]:
        """Get Kerberos authentication arguments."""
        try:
            from trino.auth import KerberosAuthentication
        except ImportError:
            raise ImportError(
                "Kerberos authentication requires trino package with kerberos support. "
                "Install with: pip install 'trino[all]'"
            )
        
        service_name = self._get_required_env('KERBEROS_SERVICE_NAME', 'Kerberos service name')
        
        # Optional kerberos parameters
        kerberos_kwargs = {'service_name': service_name}
        
        if os.getenv('KERBEROS_CONFIG'):
            kerberos_kwargs['config'] = os.getenv('KERBEROS_CONFIG')
        
        if os.getenv('KERBEROS_KEYTAB'):
            kerberos_kwargs['keytab'] = os.getenv('KERBEROS_KEYTAB')
        
        if os.getenv('KERBEROS_PRINCIPAL'):
            kerberos_kwargs['principal'] = os.getenv('KERBEROS_PRINCIPAL')
        
        kerberos_auth = KerberosAuthentication(**kerberos_kwargs)
        
        return {
            **base_kwargs,
            'auth': kerberos_auth
        }
    
    def create_api_client(self):
        """Create and return configured API client.
        
        Returns:
            Api: Configured API client instance
            
        Raises:
            AuthenticationError: If authentication configuration is invalid
            ImportError: If required authentication library is not available
        """
        from starburst_data_products_client.sep.api import Api
        
        kwargs = self.get_api_kwargs()
        api = Api(**kwargs)
        
        # For OAuth authentication methods, trigger authentication flow immediately
        # by making a simple test API call to ensure tokens are obtained upfront
        if self.method in ['oauth2', 'oauth2_jwt', 'kerberos']:
            try:
                # Make a simple API call to trigger OAuth flow immediately
                # This ensures the browser prompt appears right after "connecting" message
                api.list_domains()
            except Exception as e:
                # If the test call fails, it might be due to authentication issues
                # Let the calling code handle the specific error
                raise
        
        return api
    
    def get_auth_info(self) -> Dict[str, Any]:
        """Get information about the current authentication configuration.
        
        Returns:
            Dictionary with authentication information (without sensitive data)
        """
        info = {
            'method': self.method,
            'host': self.host,
            'protocol': self.protocol,
            'verify_ssl': self.verify_ssl
        }
        
        if self.method == 'basic':
            info['username'] = os.getenv('SEP_USERNAME', 'Not set')
            info['password'] = '***' if os.getenv('SEP_PASSWORD') else 'Not set'
        elif self.method == 'oauth2_jwt':
            info['jwt_token'] = '***' if os.getenv('SEP_JWT_TOKEN') else 'Not set'
        elif self.method == 'kerberos':
            info['service_name'] = os.getenv('KERBEROS_SERVICE_NAME', 'Not set')
            info['config'] = os.getenv('KERBEROS_CONFIG', 'Not set')
            info['keytab'] = os.getenv('KERBEROS_KEYTAB', 'Not set')
            info['principal'] = os.getenv('KERBEROS_PRINCIPAL', 'Not set')
        
        return info


def create_api_client(env_file: Optional[str] = None):
    """Convenience function to create API client with automatic authentication configuration.
    
    Args:
        env_file: Path to .env file. If None, uses default .env discovery.
        
    Returns:
        Api: Configured API client instance
        
    Raises:
        AuthenticationError: If authentication configuration is invalid
        ImportError: If required authentication library is not available
    """
    auth_config = AuthConfig(env_file)
    return auth_config.create_api_client()


def create_api_client_with_messages(env_file: Optional[str] = None):
    """Create API client with user-friendly authentication messages.
    
    Args:
        env_file: Path to .env file. If None, uses default .env discovery.
        
    Returns:
        Api: Configured API client instance
        
    Raises:
        AuthenticationError: If authentication configuration is invalid
        ImportError: If required authentication library is not available
    """
    auth_config = AuthConfig(env_file)
    
    # Show appropriate connecting message based on authentication method
    if auth_config.method in ['oauth2', 'oauth2_jwt', 'kerberos']:
        print(f"🔐 Connecting to SEP cluster with {auth_config.method} authentication...")
        if auth_config.method == 'oauth2':
            print("   (This will open a browser window for authentication)")
    else:
        print(f"🔐 Connecting to SEP cluster with {auth_config.method} authentication...")
    
    api = auth_config.create_api_client()
    return api


def get_auth_info(env_file: Optional[str] = None) -> Dict[str, Any]:
    """Get authentication configuration information.
    
    Args:
        env_file: Path to .env file. If None, uses default .env discovery.
        
    Returns:
        Dictionary with authentication information (without sensitive data)
    """
    auth_config = AuthConfig(env_file)
    return auth_config.get_auth_info()