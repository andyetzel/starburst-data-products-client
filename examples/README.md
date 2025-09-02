# Starburst Data Products Client Examples

This directory contains Python scripts that demonstrate how to use the Starburst Data Products Client to interact with a Starburst Enterprise Platform (SEP) cluster. These examples cover common operations for managing data products, domains, tags, and PII governance.

## Prerequisites

1. **Python Environment**: Ensure you have Python 3.10+ installed
2. **Dependencies**: Install the required packages:
   ```bash
   pip install python-dotenv
   pip install starburst-data-products-client
   ```
3. **SEP Cluster Access**: You need access to a Starburst Enterprise Platform cluster with appropriate permissions

## Setup

1. **Environment Configuration**:
   ```bash
   cp .env.example .env
   ```
   
2. **Update the .env file** with your SEP cluster details:
   ```bash
   SEP_HOST=your-sep-cluster-host.example.com
   SEP_USERNAME=your-username
   SEP_PASSWORD=your-password
   SEP_PROTOCOL=https
   SSL_VERIFY=True
   DEFAULT_CATALOG_NAME=your-catalog
   DEFAULT_SCHEMA_NAME=your-schema
   DEFAULT_DOMAIN_NAME=your-domain
   ```

## Available Examples

### 0. Create Domain - `0_create_domain.py`

**Purpose**: Creates and manages data product domains, which are logical groupings for organizing related data products.

**Operations**:
- Create new domains with schema locations
- List existing domains with details
- Update domain properties
- Create multiple example domains for different business areas

**Usage**:
```bash
python 0_create_domain.py
```

**⚠️ Important**: Run this script first before creating data products. Other scripts depend on domains being created.

### 1. Basic Operations (Dry Run) - `1_basic_operations_dry_run.py`

**Purpose**: Demonstrates read-only API operations that don't modify any data.

**Operations**:
- Connect to SEP cluster
- Search for data products
- Retrieve data product details
- List domains
- Get tags for data products
- Check workflow status

**Usage**:
```bash
python 1_basic_operations_dry_run.py
```

**Safe to run**: ✅ This script only performs read operations and makes no changes to your data.

### 2. Create Data Product - `2_create_data_product.py`

**Purpose**: Demonstrates how to create a new data product with views, columns, and metadata.

**Prerequisites**: Must run `0_create_domain.py` first to create the required domain.

**Operations**:
- Look up existing domain by name
- Define data product with views and columns
- Set owners and metadata
- Create the data product in DRAFT status

**Usage**:
```bash
python 2_create_data_product.py
```

**⚠️ Warning**: This script will create new data products in your SEP cluster.

### 3. Update Data Product - `3_update_data_product.py`

**Purpose**: Demonstrates how to update various aspects of existing data products.

**Operations**:
- Direct data product updates (description, summary, owners, relevant links, etc.)
- Update sample queries
- Update tags
- Update domain information

**Usage**:
```bash
python 3_update_data_product.py
```

**⚠️ Warning**: This script will modify existing data products in your SEP cluster.

### 4. Apply PII Tags - `4_apply_pii_tags.py`

**Purpose**: Demonstrates PII (Personally Identifiable Information) classification and tagging.

### 5. Publish Workflow - `5_publish_workflow.py`

**Purpose**: Demonstrates how to publish data products and monitor publish workflows.

**Operations**:
- Publish data products
- Force republish data products (recreates all datasets)
- Monitor publish workflow status in real-time
- Handle publish workflow errors
- Check workflow history and status

**Usage**:
```bash
python 5_publish_workflow.py
```

**⚠️ Warning**: This script will publish data products and create actual datasets in your SEP cluster.

**Operations** (for PII Tags script):
- Analyze data products for PII content  
- Apply appropriate PII tags based on sensitivity levels
- Create PII-aware data products with classified columns
- Generate PII governance reports

**PII Classifications**:
- **HIGH_SENSITIVITY**: SSN, credit cards, passport numbers
- **MEDIUM_SENSITIVITY**: Email, phone, address, birth date
- **LOW_SENSITIVITY**: Names, usernames, user IDs
- **FINANCIAL**: Salary, account numbers, financial data

**Usage**:
```bash
python 4_apply_pii_tags.py
```

**⚠️ Warning**: This script will modify data products and apply PII-related tags.

### 6. Tag Cleanup - `6_cleanup_unused_tags.py`

**Purpose**: Identifies and removes unused tags across all data products to maintain a clean tag ecosystem.

**Operations**:
- Scan all data products for tag usage analysis
- Categorize tags by usage frequency (frequent, occasional, rare, unused)
- Generate comprehensive tag usage reports
- Selective or bulk deletion of unused tags
- Interactive tag cleanup workflow

**Tag Categories**:
- **Frequently used**: Tags used by 5+ data products
- **Occasionally used**: Tags used by 2-4 data products  
- **Rarely used**: Tags used by only 1 data product
- **Unused**: Tags that exist but aren't assigned to any data products

**Usage**:
```bash
python 6_cleanup_unused_tags.py
```

**⚠️ Warning**: This script will analyze and potentially delete tags in your SEP cluster. Use with caution.

### 7. Data Product Usage Statistics - `7_data_product_usage_statistics.py`

**Purpose**: Demonstrates how to retrieve comprehensive usage statistics for data products from multiple API sources.

**Operations**:
- Gather query count statistics (7-day and 30-day windows)
- Get unique user counts per data product
- Access last query timestamps and user identification
- Generate usage categorization and recency analysis
- Create cross-product usage summaries and rankings
- Collect data from multiple complementary API endpoints

**Data Sources**:
- **Statistics Endpoint**: `/api/v1/dataProduct/products/{id}/statistics`
  - Query counts over different time windows
  - Unique user counts per product
  - Statistics update timestamps
- **Access Metadata**: From `getDataProduct` response
  - Last queried timestamp and user
  - Historical access information

**Usage**:
```bash
python 7_data_product_usage_statistics.py
```

**Safe to run**: ✅ This script performs only read operations and makes no changes to your data.

### 8. Direct cURL Statistics Example - `8_direct_curl_statistics.py`

**Purpose**: Demonstrates how to make direct HTTP requests to the statistics endpoint without the client library.

**Operations**:
- Generate proper authentication headers for Basic Auth and OAuth
- Construct and execute HTTP requests using Python requests
- Generate equivalent cURL commands for manual testing
- Parse and display JSON responses in multiple formats
- Show debugging information for troubleshooting

**Supported Authentication**:
- Basic Authentication (username/password)
- OAuth2 JWT Bearer tokens

**Usage**:
```bash
python 8_direct_curl_statistics.py
```

**Safe to run**: ✅ This script performs only read operations.

### cURL Shell Script - `curl_statistics.sh`

**Purpose**: Pure bash/curl implementation for direct API testing without Python dependencies.

**Features**:
- Environment variable configuration
- Support for Basic Auth and OAuth authentication
- SSL verification options
- JSON response formatting (with jq if available)
- Comprehensive error handling

**Usage**:
```bash
# Basic Authentication
export STARBURST_HOST="my-cluster.starburst.io"
export STARBURST_AUTH_METHOD="basic"
export STARBURST_USERNAME="myuser"
export STARBURST_PASSWORD="mypass"
./curl_statistics.sh 40b29ec1-9eeb-48a0-9799-c71c1651bb37

# OAuth Authentication
export STARBURST_HOST="my-cluster.starburst.io"  
export STARBURST_AUTH_METHOD="oauth"
export STARBURST_JWT_TOKEN="your-jwt-token-here"
./curl_statistics.sh 40b29ec1-9eeb-48a0-9799-c71c1651bb37

# OAuth Authentication (obtain token from your OAuth provider)
export STARBURST_HOST="my-cluster.starburst.io"
export STARBURST_AUTH_METHOD="oauth"  
export STARBURST_JWT_TOKEN="your-actual-jwt-token-here"
./curl_statistics.sh 40b29ec1-9eeb-48a0-9799-c71c1651bb37
```

**Safe to run**: ✅ This script performs only read operations.

## Running the Examples

### Interactive Mode
Each script is designed to be run interactively and will prompt you for confirmations before making changes:

```bash
python 1_basic_operations_dry_run.py
```

### Safety Features
- **Confirmation prompts**: Scripts that modify data will ask for confirmation
- **Read-only operations**: The dry run script performs only read operations
- **Detailed logging**: All scripts provide detailed output about their operations
- **Error handling**: Comprehensive error handling with informative messages

## Common Use Cases

### Getting Started
1. Start with `1_basic_operations_dry_run.py` to explore your SEP cluster
2. **Create domains first**: Run `0_create_domain.py` to set up required domains  
3. Create data products: Use `2_create_data_product.py` to create test data products (creates in DRAFT status)
4. Publish data products: Use `5_publish_workflow.py` to publish your created data products
5. Practice updates with `3_update_data_product.py`
6. Implement PII governance with `4_apply_pii_tags.py`

### PII Governance Workflow
1. Run `4_apply_pii_tags.py` to analyze existing data products
2. Review the PII classification results
3. Apply appropriate tags and update column descriptions
4. Generate governance reports for compliance

### Development and Testing
1. Use the dry run script to understand your data landscape
2. Create test data products in a development environment
3. Practice update operations before applying to production
4. Validate PII tagging before rolling out governance policies

## Troubleshooting

### Connection Issues
- Verify your `.env` file has the correct SEP cluster details
- Ensure your network can reach the SEP cluster
- Check that your credentials have the necessary permissions

### Permission Errors
- Ensure your user has permissions to create/modify data products
- Check domain access permissions
- Verify catalog and schema access rights

### API Errors
- Check SEP cluster health and availability
- Verify API endpoints are accessible
- Review error messages for specific issues

## Best Practices

### Security
- Never commit the `.env` file with real credentials to version control
- Never hardcode JWT tokens or any secrets in scripts or documentation  
- Use service accounts for automation
- Store JWT tokens and other secrets only in `.env` file (which is in `.gitignore`)
- Implement proper access controls for PII data
- Regularly rotate OAuth tokens and passwords

### Development
- Always test in a development environment first
- Use the dry run script to understand existing data
- Back up important data products before modifications

### PII Governance
- Regularly audit PII classifications
- Keep PII tags up to date
- Document data handling procedures
- Train team members on PII sensitivity levels

## Additional Resources

- [Starburst Data Products Documentation](https://docs.starburst.io/)
- [Python Client API Reference](../docs/)
- [Starburst Enterprise Platform User Guide](https://docs.starburst.io/latest/)

## Support

If you encounter issues with these examples:

1. Check the error messages and logs
2. Verify your environment configuration
3. Review the troubleshooting section
4. Consult the Starburst documentation
5. Contact your Starburst administrator

---

**Note**: These examples are for demonstration purposes. Always review and adapt them to your specific environment and requirements before using in production.