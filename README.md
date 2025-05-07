# Starburst Data Products Client

A Python client library for interacting with the Starburst Enterprise Data Products API. This client provides a convenient interface for managing data products, domains, tags, and workflows in your Starburst Enterprise environment.

## Features

- Data Product Management
  - Create, clone, and delete data products
  - Search for data products
  - Manage sample queries
  - Handle materialized view refresh metadata
- Domain Management
  - Create, update, and delete domains
  - List and retrieve domain information
- Tag Management
  - Add, update, and remove tags from data products
  - Retrieve tags for data products
- Workflow Operations
  - Publish data products
  - Monitor workflow status
  - Delete data products with optional object cleanup

## Requirements

- Python 3.9 or higher
- Poetry for dependency management

## Installation

### Using pip

```bash
pip install starburst-data-products-client
```

### From source

1. Clone the repository:
```bash
git clone https://github.com/starburstdata/starburst-data-products-client.git
cd starburst-data-products-client
```

2. Install dependencies using Poetry:
```bash
poetry install
```

## Development Setup

1. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install development dependencies:
```bash
poetry install --with test
```

3. Run tests:
```bash
./test-sep-dp.sh
```

## Usage Examples

### Basic Setup

```python
from starburst_data_products_client.sep.api import Api

# Initialize the API client
api = Api(
    host="your-starburst-host",
    username="your-username",
    password="your-password"
)
```

### Working with Data Products

```python
# Search for data products
results = api.search_data_products(search_string="sales")

# Create a new data product
from starburst_data_products_client.sep.data import DataProductParameters
new_product = DataProductParameters(
    name="sales_analytics",
    description="Sales analytics data product",
    catalog_name="hive",
    schema_name="sales"
)
created_product = api.create_data_product(new_product)

# Clone an existing data product
cloned_product = api.clone_data_product(
    dp_id="original-product-id",
    catalog_name="hive",
    new_schema_name="sales_clone",
    new_name="sales_analytics_clone"
)
```

### Managing Domains

```python
# Create a new domain
domain = api.create_domain(
    name="sales_domain",
    description="Domain for sales-related data products",
    schema_location="hive.sales"
)

# List all domains
domains = api.list_domains()
```

### Working with Tags

```python
# Add tags to a data product
api.update_tags(
    dp_id="product-id",
    tag_values=["sales", "analytics", "quarterly"]
)

# Get tags for a data product
tags = api.get_tags(dp_id="product-id")
```

### Workflow Operations

```python
# Publish a data product
api.publish_data_product(dp_id="product-id")

# Check publish status
status = api.get_publish_data_product_status(dp_id="product-id")

# Delete a data product
api.delete_data_product(dp_id="product-id", skip_objects_delete=False)
```

## Contributing

1. Create a feature branch
2. Make your changes
3. Run tests
4. Submit a pull request


## Support

For support, please contact Starburst Data at info@starburstdata.com or visit our [support portal](https://support.starburst.io).

## Documentation

For more detailed documentation, please visit our [documentation site](https://docs.starburst.io).

TODO - generate docs for this project.
