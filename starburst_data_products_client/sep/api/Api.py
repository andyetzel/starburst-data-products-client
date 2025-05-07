from starburst_data_products_client.sep.data import DataProductSearchResult
from starburst_data_products_client.sep.data import DataProduct, DataProductParameters
from starburst_data_products_client.sep.data import DataProductWorkflowStatus
from starburst_data_products_client.sep.data import Domain
from starburst_data_products_client.sep.data import MaterializedViewRefreshMetadata
from starburst_data_products_client.sep.data import SampleQuery
from starburst_data_products_client.sep.data import Tag

import requests
from typing import List
from json import dumps


class Api:
    """A client for interacting with the Starburst Enterprise Data Products API.

    This class provides methods to interact with various endpoints of the Starburst Data Products API,
    including data product management, domain management, tag management, and workflow operations.

    Attributes:
        DOMAIN_PATH (str): API endpoint path for domain operations
        DATA_PRODUCT_PATH (str): API endpoint path for data product operations
        DATA_PRODUCT_TAGS_PATH (str): API endpoint path for tag operations
    """

    DOMAIN_PATH = 'api/v1/dataProduct/domains'
    DATA_PRODUCT_PATH = 'api/v1/dataProduct/products'
    DATA_PRODUCT_TAGS_PATH = 'api/v1/dataProduct/tags'

    def __init__(self, host: str, username: str, password: str, protocol: str = 'https'):
        """Initialize the API client.

        Args:
            host (str): The hostname of the Starburst Data Products server
            username (str): Username for authentication
            password (str): Password for authentication
            protocol (str, optional): The protocol to use (http/https). Defaults to 'https'.

        Raises:
            ValueError: If the hostname includes a protocol
        """
        if '://' in host:
            raise ValueError(f'Hostname should not include protocol')
        self.host = host
        self.username = username
        self.password = password
        self.protocol = protocol


    # --- data product API methods ---
    def search_data_products(self, search_string: str=None) -> List[DataProductSearchResult]:
        """Search for data products matching the given search string.

        The search is performed against all data product attributes and is case-insensitive.
        The search string is bookended by '%' for partial matching.

        Args:
            search_string (str, optional): The string to search for. Defaults to None.

        Returns:
            List[DataProductSearchResult]: List of matching data product search results

        Raises:
            Exception: If the API request fails
        """
        #REQUEST searchOptions.searchString is bookended by '%' and compared against all
        #dp attributes https://github.com/starburstdata/starburst-enterprise/blob/807dbbbfb48b7e5ea87777fc3aae8cd360dea1e8/core/starburst-server-main/src/main/java/com/starburstdata/presto/dataproduct/search/SearchSqlBuilder.java#L213

        params = {'searchOptions': dumps({'searchString': search_string})} if search_string is not None else None
        response = requests.get(
            url=f'{self.protocol}://{self.host}/{self.DATA_PRODUCT_PATH}',
            auth=(self.username, self.password),
            params=params
        )
        if not response.ok:
            raise Exception(f'Request returned code {response.status_code}.\nResponse body: {response.text}')

        return [search_result for search_result in
                [DataProductSearchResult.load(result) for result in response.json()]
                if search_string is None or search_string in search_result.name]
    
    
    def create_data_product(self, data_product: DataProductParameters) -> DataProduct:
        """Create a new data product.

        Args:
            data_product (DataProductParameters): The parameters for the new data product

        Returns:
            DataProduct: The created data product

        Raises:
            Exception: If the API request fails
        """
        response = requests.post(
            url=f'{self.protocol}://{self.host}/{self.DATA_PRODUCT_PATH}',
            auth=(self.username, self.password),
            json=data_product.asdict()
        )
        if not response.ok:
            raise Exception(f'Request returned code {response.status_code}.\nResponse body: {response.text}')
        return DataProduct.load(response.json())

    
    def clone_data_product(
        self,
        dp_id: str,
        catalog_name: str,
        new_schema_name: str,
        new_name: str,
        domain_id: str=None
    ) -> DataProduct:
        """Clone an existing data product.

        Args:
            dp_id (str): ID of the data product to clone
            catalog_name (str): Name of the catalog for the cloned product
            new_schema_name (str): Name of the schema for the cloned product
            new_name (str): Name for the cloned product
            domain_id (str, optional): ID of the domain for the cloned product. Defaults to None.

        Returns:
            DataProduct: The cloned data product

        Raises:
            Exception: If the API request fails
        """
        body={
            'catalogName': catalog_name,
            'newSchemaName': new_schema_name,
            'newName': new_name
        }
        if domain_id is not None:
            body['dataDomainId'] = domain_id
        response = requests.post(
            url=f'{self.protocol}://{self.host}/{self.DATA_PRODUCT_PATH}/{dp_id}/clone',
            auth=(self.username, self.password),
            json=body
        )
        if not response.ok:
            raise Exception(f'Request returned code {response.status_code}.\nResponse body: {response.text}')
        return DataProduct.load(response.json())
    

    def get_data_product(self, dp_id: str) -> DataProduct:
        """Get details of a specific data product.

        Args:
            dp_id (str): ID of the data product to retrieve

        Returns:
            DataProduct: The requested data product

        Raises:
            Exception: If the API request fails
        """
        response = requests.get(
            url= f'{self.protocol}://{self.host}/{self.DATA_PRODUCT_PATH}/{dp_id}',
            auth=(self.username, self.password)
        )
        if not response.ok:
            raise Exception('bad request' + str(response))
        return DataProduct.load(response.json())
    
    
    def update_sample_queries(self, dp_id: str, sample_queries: List[SampleQuery]):
        """Update the sample queries for a data product.

        Args:
            dp_id (str): ID of the data product
            sample_queries (List[SampleQuery]): List of sample queries to set

        Raises:
            Exception: If the API request fails
        """
        response = requests.put(
            url=f'{self.protocol}://{self.host}/{self.DATA_PRODUCT_PATH}/{dp_id}/sampleQueries',
            auth=(self.username, self.password),
            json=[{'name':query.name,'description':query.description,'query':query.query} for query in sample_queries]
        )
        if not response.ok:
            raise Exception(f'Request returned code {response.status_code}.\nResponse body: {response.text}')
    
    
    def list_sample_queries(self, dp_id: str) -> List[SampleQuery]:
        """Get the list of sample queries for a data product.

        Args:
            dp_id (str): ID of the data product

        Returns:
            List[SampleQuery]: List of sample queries

        Raises:
            Exception: If the API request fails
        """
        response = requests.get(
            url=f'{self.protocol}://{self.host}/{self.DATA_PRODUCT_PATH}/{dp_id}/sampleQueries',
            auth=(self.username, self.password)
        )
        if not response.ok:
            raise Exception(f'Request returned code {response.status_code}.\nResponse body: {response.text}')
        return [SampleQuery.load(result) for result in response.json()]
    
    
    def get_materialized_view_refresh_metadata(self, dp_id: str, view_name: str) -> MaterializedViewRefreshMetadata:
        """Get refresh metadata for a materialized view.

        Args:
            dp_id (str): ID of the data product
            view_name (str): Name of the materialized view

        Returns:
            MaterializedViewRefreshMetadata: Metadata about the view's refresh status

        Raises:
            Exception: If the API request fails
        """
        response = requests.get(
            url=f'{self.protocol}://{self.host}/{self.DATA_PRODUCT_PATH}/{dp_id}/materializedViews/{view_name}/refreshMetadata',
            auth=(self.username, self.password)
        )
        if not response.ok:
            raise Exception(f'Request returned code {response.status_code}.\nResponse body: {response.text}')
        # response.json() will be None in scenario where no refresh has occurred yet
        if response.json() is None:
            return MaterializedViewRefreshMetadata(lastImport=None, incrementalColumn=None,refreshInterval=None,storageSchema=None,estimatedNextRefreshTime=None)
        return MaterializedViewRefreshMetadata.load(response.json())


    # --- domain API methods ---
    def create_domain(self, name: str, description: str=None, schema_location: str=None) -> Domain:
        """Create a new domain.

        Args:
            name (str): Name of the domain
            description (str, optional): Description of the domain. Defaults to None.
            schema_location (str, optional): Schema location for the domain. Defaults to None.

        Returns:
            Domain: The created domain

        Raises:
            Exception: If the API request fails
        """
        response = requests.post(
            url=f'{self.protocol}://{self.host}/{self.DOMAIN_PATH}',
            auth=(self.username, self.password),
            json={
                'name': name,
                'description': description,
                'schemaLocation': schema_location
            }
        )
        if not response.ok:
            raise Exception(f'Request returned code {response.status_code}.\nResponse body: {response.text}')
        return Domain.load(response.json())
    

    def delete_domain(self, domain_id: str):
        """Delete a domain.

        Args:
            domain_id (str): ID of the domain to delete

        Raises:
            Exception: If the API request fails
        """
        response = requests.delete(
            url=f'{self.protocol}://{self.host}/{self.DOMAIN_PATH}/{domain_id}',
            auth=(self.username, self.password)
        )
        if not response.ok:
            raise Exception(f'Request returned code {response.status_code}.\nResponse body: {response.text}')
    

    def update_domain(self, domain_id: str, description: str=None, schema_location: str=None) -> Domain:
        """Update a domain's properties.

        Args:
            domain_id (str): ID of the domain to update
            description (str, optional): New description. Defaults to None.
            schema_location (str, optional): New schema location. Defaults to None.

        Returns:
            Domain: The updated domain

        Raises:
            Exception: If the API request fails
        """
        response = requests.put(
            url=f'{self.protocol}://{self.host}/{self.DOMAIN_PATH}/{domain_id}',
            auth=(self.username, self.password),
            json={
                'description': description,
                'schemaLocation': schema_location
            }
        )
        if not response.ok:
            raise Exception(f'Request returned code {response.status_code}.\nResponse body: {response.text}')
        return Domain.load(response.json())
    

    def list_domains(self) -> List[Domain]:
        """Get a list of all domains.

        Returns:
            List[Domain]: List of all domains

        Raises:
            Exception: If the API request fails
        """
        response = requests.get(
            url=f'{self.protocol}://{self.host}/{self.DOMAIN_PATH}',
            auth=(self.username, self.password)
        )
        if not response.ok:
            raise Exception(f'Request returned code {response.status_code}.\nResponse body: {response.text}')
        return [Domain.load(result) for result in response.json()]


    def get_domain(self, domain_id: str) -> Domain:
        """Get details of a specific domain.

        Args:
            domain_id (str): ID of the domain to retrieve

        Returns:
            Domain: The requested domain

        Raises:
            Exception: If the API request fails
        """
        response = requests.get(
            url= f'{self.protocol}://{self.host}/{self.DOMAIN_PATH}/{domain_id}',
            auth=(self.username, self.password)
        )
        if not response.ok:
            raise Exception('bad request' + str(response))
        return Domain.load(response.json())


    # --- tags API methods ---
    def update_tags(self, dp_id: str, tag_values: List[str]) -> Tag:
        """Update tags for a data product.

        Args:
            dp_id (str): ID of the data product
            tag_values (List[str]): List of tag values to set

        Returns:
            Tag: The updated tags

        Raises:
            Exception: If the API request fails
        """
        response = requests.put(
            url=f'{self.protocol}://{self.host}/{self.DATA_PRODUCT_TAGS_PATH}/products/{dp_id}',
            auth=(self.username, self.password),
            json=[{"value": val} for val in tag_values]
        )
        if not response.ok:
            raise Exception(f'Request returned code {response.status_code}.\nResponse body: {response.text}')
        return [Tag.load(result) for result in response.json()]
        
        
    def get_tags(self, dp_id: str) -> List[Tag]:
        """Get tags for a data product.

        Args:
            dp_id (str): ID of the data product

        Returns:
            List[Tag]: List of tags

        Raises:
            Exception: If the API request fails
        """
        response = requests.get(
            url=f'{self.protocol}://{self.host}/{self.DATA_PRODUCT_TAGS_PATH}/products/{dp_id}',
            auth=(self.username, self.password)
        )
        if not response.ok:
            raise Exception(f'Request returned code {response.status_code}.\nResponse body: {response.text}')
        return [Tag.load(result) for result in response.json()]
    
    
    def delete_tag(self, tag_id: str, dp_id: str):
        """Delete a tag from a data product.

        Args:
            tag_id (str): ID of the tag to delete
            dp_id (str): ID of the data product

        Raises:
            Exception: If the API request fails
        """
        response = requests.delete(
            url=f'{self.protocol}://{self.host}/{self.DATA_PRODUCT_TAGS_PATH}/{tag_id}/products/{dp_id}',
            auth=(self.username, self.password)
        )
        if not response.ok:
            raise Exception(f'Request returned code {response.status_code}.\nResponse body: {response.text}')
    

    # --- workflow API methods ---
    def publish_data_product(self, dp_id: str, force: bool=False):
        """Publish a data product.

        Args:
            dp_id (str): ID of the data product to publish
            force (bool, optional): Whether to force publish. Defaults to False.

        Raises:
            Exception: If the API request fails
        """
        response = requests.post(
            url=f'{self.protocol}://{self.host}/{self.DATA_PRODUCT_PATH}/{dp_id}/workflows/publish',
            auth=(self.username, self.password),
            params={'force': force}
        )
        if not response.ok:
            raise Exception(f'Request returned code {response.status_code}.\nResponse body: {response.text}')
    

    def get_publish_data_product_status(self, dp_id: str) -> DataProductWorkflowStatus:
        """Get the status of a data product publish workflow.

        Args:
            dp_id (str): ID of the data product

        Returns:
            DataProductWorkflowStatus: Status of the publish workflow

        Raises:
            Exception: If the API request fails
        """
        response = requests.get(
            url=f'{self.protocol}://{self.host}/{self.DATA_PRODUCT_PATH}/{dp_id}/workflows/publish',
            auth=(self.username, self.password),
        )
        if not response.ok:
            raise Exception(f'Request returned code {response.status_code}.\nResponse body: {response.text}')
        
        return DataProductWorkflowStatus.load(response.json())
    

    def delete_data_product(self, dp_id: str, skip_objects_delete: bool=False):
        """Delete a data product.

        Args:
            dp_id (str): ID of the data product to delete
            skip_objects_delete (bool, optional): Whether to skip deleting Trino objects. Defaults to False.

        Raises:
            Exception: If the API request fails
        """
        response = requests.post(
            url=f'{self.protocol}://{self.host}/{self.DATA_PRODUCT_PATH}/{dp_id}/workflows/delete',
            auth=(self.username, self.password),
            params={'skipTrinoDelete': skip_objects_delete}
        )
        if not response.ok:
            raise Exception(f'Request returned code {response.status_code}.\nResponse body: {response.text}')
    

    def get_delete_data_product_status(self, dp_id: str) -> DataProductWorkflowStatus:
        """Get the status of a data product delete workflow.

        Args:
            dp_id (str): ID of the data product

        Returns:
            DataProductWorkflowStatus: Status of the delete workflow

        Raises:
            Exception: If the API request fails
        """
        response = requests.get(
            url=f'{self.protocol}://{self.host}/{self.DATA_PRODUCT_PATH}/{dp_id}/workflows/delete',
            auth=(self.username, self.password),
        )
        if not response.ok:
            raise Exception(f'Request returned code {response.status_code}.\nResponse body: {response.text}')
        
        return DataProductWorkflowStatus.load(response.json())
    