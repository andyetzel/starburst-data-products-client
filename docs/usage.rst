Usage
=====

Installation
------------

To use first install it using pip:

.. code-block:: console

  (.venv) $ pip install starburst-data-products-client

Minimal supported Python version is 3.11.

Basic setup
-----------

.. code-block:: python

    from starburst_data_products_client.sep.api import Api

    # Initialize the API client
    api = Api(
       host="your-starburst-host",
       username="your-username",
       password="your-password"
    )

Working with data products
--------------------------

.. code-block:: python

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

Managing domains
----------------

.. code-block:: python

    # Create a new domain
    domain = api.create_domain(
        name="sales_domain",
        description="Domain for sales-related data products",
        schema_location="hive.sales"
    )

    # List all domains
    domains = api.list_domains()
