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

    DOMAIN_PATH = 'api/v1/dataProduct/domains'
    DATA_PRODUCT_PATH = 'api/v1/dataProduct/products'
    DATA_PRODUCT_TAGS_PATH = 'api/v1/dataProduct/tags'

    def __init__(self, host: str, username: str, password: str, protocol: str = 'https'):
        if '://' in host:
            raise ValueError(f'Hostname should not include protocol')
        self.host = host
        self.username = username
        self.password = password
        self.protocol = protocol


    # --- data product API methods ---
    def search_data_products(self, search_string: str=None) -> List[DataProductSearchResult]:
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
        response = requests.get(
            url= f'{self.protocol}://{self.host}/{self.DATA_PRODUCT_PATH}/{dp_id}',
            auth=(self.username, self.password)
        )
        if not response.ok:
            raise Exception('bad request' + str(response))
        return DataProduct.load(response.json())
    
    
    def update_sample_queries(self, dp_id: str, sample_queries: List[SampleQuery]):
        response = requests.put(
            url=f'{self.protocol}://{self.host}/{self.DATA_PRODUCT_PATH}/{dp_id}/sampleQueries',
            auth=(self.username, self.password),
            json=[{'name':query.name,'description':query.description,'query':query.query} for query in sample_queries]
        )
        if not response.ok:
            raise Exception(f'Request returned code {response.status_code}.\nResponse body: {response.text}')
    
    
    def list_sample_queries(self, dp_id: str) -> List[SampleQuery]:
        response = requests.get(
            url=f'{self.protocol}://{self.host}/{self.DATA_PRODUCT_PATH}/{dp_id}/sampleQueries',
            auth=(self.username, self.password)
        )
        if not response.ok:
            raise Exception(f'Request returned code {response.status_code}.\nResponse body: {response.text}')
        return [SampleQuery.load(result) for result in response.json()]
    
    
    def get_materialized_view_refresh_metadata(self, dp_id: str, view_name: str) -> MaterializedViewRefreshMetadata:
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
        response = requests.delete(
            url=f'{self.protocol}://{self.host}/{self.DOMAIN_PATH}/{domain_id}',
            auth=(self.username, self.password)
        )
        if not response.ok:
            raise Exception(f'Request returned code {response.status_code}.\nResponse body: {response.text}')
    

    def update_domain(self, domain_id: str, description: str=None, schema_location: str=None) -> Domain:
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
        response = requests.get(
            url=f'{self.protocol}://{self.host}/{self.DOMAIN_PATH}',
            auth=(self.username, self.password)
        )
        if not response.ok:
            raise Exception(f'Request returned code {response.status_code}.\nResponse body: {response.text}')
        return [Domain.load(result) for result in response.json()]


    def get_domain(self, domain_id: str) -> Domain:
        response = requests.get(
            url= f'{self.protocol}://{self.host}/{self.DOMAIN_PATH}/{domain_id}',
            auth=(self.username, self.password)
        )
        if not response.ok:
            raise Exception('bad request' + str(response))
        return Domain.load(response.json())


    # --- tags API methods ---
    def update_tags(self, dp_id: str, tag_values: List[str]) -> Tag:
        response = requests.put(
            url=f'{self.protocol}://{self.host}/{self.DATA_PRODUCT_TAGS_PATH}/products/{dp_id}',
            auth=(self.username, self.password),
            json=[{"value": val} for val in tag_values]
        )
        if not response.ok:
            raise Exception(f'Request returned code {response.status_code}.\nResponse body: {response.text}')
        return [Tag.load(result) for result in response.json()]
        
        
    def get_tags(self, dp_id: str) -> List[Tag]:
        response = requests.get(
            url=f'{self.protocol}://{self.host}/{self.DATA_PRODUCT_TAGS_PATH}/products/{dp_id}',
            auth=(self.username, self.password)
        )
        if not response.ok:
            raise Exception(f'Request returned code {response.status_code}.\nResponse body: {response.text}')
        return [Tag.load(result) for result in response.json()]
    
    
    def delete_tag(self, tag_id: str, dp_id: str):
        response = requests.delete(
            url=f'{self.protocol}://{self.host}/{self.DATA_PRODUCT_TAGS_PATH}/{tag_id}/products/{dp_id}',
            auth=(self.username, self.password)
        )
        if not response.ok:
            raise Exception(f'Request returned code {response.status_code}.\nResponse body: {response.text}')
    

    # --- workflow API methods ---
    def publish_data_product(self, dp_id: str, force: bool=False):
        response = requests.post(
            url=f'{self.protocol}://{self.host}/{self.DATA_PRODUCT_PATH}/{dp_id}/workflows/publish',
            auth=(self.username, self.password),
            params={'force': force}
        )
        if not response.ok:
            raise Exception(f'Request returned code {response.status_code}.\nResponse body: {response.text}')
    

    def get_publish_data_product_status(self, dp_id: str) -> DataProductWorkflowStatus:
        response = requests.get(
            url=f'{self.protocol}://{self.host}/{self.DATA_PRODUCT_PATH}/{dp_id}/workflows/publish',
            auth=(self.username, self.password),
        )
        if not response.ok:
            raise Exception(f'Request returned code {response.status_code}.\nResponse body: {response.text}')
        
        return DataProductWorkflowStatus.load(response.json())
    

    def delete_data_product(self, dp_id: str, skip_objects_delete: bool=False):
        response = requests.post(
            url=f'{self.protocol}://{self.host}/{self.DATA_PRODUCT_PATH}/{dp_id}/workflows/delete',
            auth=(self.username, self.password),
            params={'skipTrinoDelete': skip_objects_delete}
        )
        if not response.ok:
            raise Exception(f'Request returned code {response.status_code}.\nResponse body: {response.text}')
    

    def get_delete_data_product_status(self, dp_id: str) -> DataProductWorkflowStatus:
        response = requests.get(
            url=f'{self.protocol}://{self.host}/{self.DATA_PRODUCT_PATH}/{dp_id}/workflows/delete',
            auth=(self.username, self.password),
        )
        if not response.ok:
            raise Exception(f'Request returned code {response.status_code}.\nResponse body: {response.text}')
        
        return DataProductWorkflowStatus.load(response.json())
    