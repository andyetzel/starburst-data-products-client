# Authentication Guide

The Starburst Data Products Client supports multiple authentication methods that can be configured through environment variables in your `.env` file. This provides a centralized way to manage authentication across all scripts and applications.

## Quick Start

1. Copy the example configuration file:
   ```bash
   cp examples/.env.example examples/.env
   ```

2. Edit the `.env` file and set your authentication method and credentials

3. Use the centralized authentication in your code:
   ```python
   from starburst_data_products_client.shared.auth_config import create_api_client
   
   # Automatically uses configuration from .env file
   api = create_api_client()
   
   # Use API normally
   data_products = api.search_data_products()
   ```

## Supported Authentication Methods

### 1. Basic Authentication (Default)

Username and password authentication.

**Configuration:**
```env
AUTH_METHOD=basic
SEP_HOST=your-sep-host.com
SEP_USERNAME=your_username
SEP_PASSWORD=your_password
SEP_PROTOCOL=https
SSL_VERIFY=true
```

**Use case:** Simple development and testing environments.

### 2. OAuth2 Authentication (Recommended for Production)

OAuth2 authentication using automatic configuration.

**Configuration:**
```env
AUTH_METHOD=oauth2
SEP_HOST=your-sep-host.com
SEP_PROTOCOL=https
SSL_VERIFY=true
```

**Requirements:**
- Install trino with OAuth support: `pip install 'trino[all]'`
- Starburst cluster must be configured for OAuth2

**Use case:** Production environments with OAuth2 infrastructure.

### 3. OAuth2 JWT Authentication

OAuth2 authentication using a pre-obtained JWT token.

**Configuration:**
```env
AUTH_METHOD=oauth2_jwt
SEP_HOST=your-sep-host.com
JWT_TOKEN=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
SEP_PROTOCOL=https
SSL_VERIFY=true
```

**Requirements:**
- Install trino with OAuth support: `pip install 'trino[all]'`
- Valid JWT token

**Use case:** Environments where JWT tokens are managed externally.

### 4. Kerberos Authentication

Kerberos authentication for enterprise environments.

**Configuration:**
```env
AUTH_METHOD=kerberos
SEP_HOST=your-sep-host.com
KERBEROS_SERVICE_NAME=starburst
SEP_PROTOCOL=https
SSL_VERIFY=true

# Optional Kerberos settings
KERBEROS_CONFIG=/etc/krb5.conf
KERBEROS_KEYTAB=/path/to/user.keytab
KERBEROS_PRINCIPAL=user@EXAMPLE.COM
```

**Requirements:**
- Install trino with Kerberos support: `pip install 'trino[all]'`
- Properly configured Kerberos environment

**Use case:** Enterprise environments using Kerberos for authentication.

## Environment Variables Reference

### Required Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `AUTH_METHOD` | Authentication method to use | `oauth2` |
| `SEP_HOST` | Starburst host (without protocol) | `sep.example.com` |
| `SEP_PROTOCOL` | Protocol (http/https) | `https` |
| `SSL_VERIFY` | Verify SSL certificates | `true` |

### Basic Authentication

| Variable | Description | Required |
|----------|-------------|----------|
| `SEP_USERNAME` | Username | Yes |
| `SEP_PASSWORD` | Password | Yes |

### OAuth2 JWT Authentication

| Variable | Description | Required |
|----------|-------------|----------|
| `JWT_TOKEN` | JWT token | Yes |

### Kerberos Authentication

| Variable | Description | Required |
|----------|-------------|----------|
| `KERBEROS_SERVICE_NAME` | Kerberos service name | Yes |
| `KERBEROS_CONFIG` | Path to krb5.conf | No |
| `KERBEROS_KEYTAB` | Path to keytab file | No |
| `KERBEROS_PRINCIPAL` | Kerberos principal | No |

## Usage Examples

### Using Centralized Authentication (Recommended)

```python
from starburst_data_products_client.shared.auth_config import create_api_client

# Create API client using .env configuration
api = create_api_client()

# Or specify a custom .env file
api = create_api_client('/path/to/custom/.env')

# Use API normally - authentication is handled automatically
data_products = api.search_data_products()
domains = api.list_domains()
```

### Manual Authentication Configuration

If you need to configure authentication manually:

```python
from starburst_data_products_client.sep.api import Api
from trino.auth import OAuth2Authentication

# Manual OAuth2 configuration
oauth_auth = OAuth2Authentication()
api = Api(
    host='sep.example.com',
    auth=oauth_auth,
    protocol='https'
)

# Manual basic authentication
api = Api(
    host='sep.example.com',
    username='admin',
    password='password',
    protocol='https'
)
```

### Getting Authentication Information

```python
from starburst_data_products_client.shared.auth_config import get_auth_info

# Get current authentication configuration
auth_info = get_auth_info()
print(f"Using {auth_info['method']} authentication")
print(f"Host: {auth_info['host']}")
```

## Testing

Set environment variables to control test authentication:

```bash
# Test with OAuth2
export AUTH_METHOD=oauth2
export SEP_HOST=localhost:8080

# Test with basic authentication (default)
export AUTH_METHOD=basic
export SEP_USERNAME=test_user
export SEP_PASSWORD=test_password

# Run tests
pytest tests/
```

## Troubleshooting

### Common Issues

1. **ImportError: No module named 'trino'**
   - Install trino: `pip install 'trino[all]'`

2. **AuthenticationError: Missing required environment variable**
   - Check your `.env` file has all required variables for your authentication method
   - Copy from `.env.example` and update with your values

3. **OAuth2 authentication failed**
   - Ensure your Starburst cluster is configured for OAuth2
   - Check network connectivity
   - Verify OAuth2 configuration

4. **SSL verification errors**
   - For development: Set `SSL_VERIFY=false` in your `.env` file
   - For production: Ensure proper SSL certificate configuration

### Getting Help

Check authentication configuration:
```python
from starburst_data_products_client.shared.auth_config import get_auth_info

try:
    auth_info = get_auth_info()
    print("Authentication configuration:")
    for key, value in auth_info.items():
        print(f"  {key}: {value}")
except Exception as e:
    print(f"Configuration error: {e}")
```

## Migration from Manual Authentication

If you're currently using manual authentication, you can easily migrate to centralized configuration:

### Before (Manual)
```python
from starburst_data_products_client.sep.api import Api

api = Api(
    host='sep.example.com',
    username='admin',
    password='password'
)
```

### After (Centralized)
```python
from starburst_data_products_client.shared.auth_config import create_api_client

# Set in .env file:
# AUTH_METHOD=basic
# SEP_HOST=sep.example.com
# SEP_USERNAME=admin
# SEP_PASSWORD=password

api = create_api_client()
```

This approach provides better security (no hardcoded credentials), easier configuration management, and consistent authentication across all scripts.