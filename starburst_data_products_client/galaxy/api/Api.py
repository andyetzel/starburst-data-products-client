import requests
from typing import List, cast
from starburst_data_products_client.galaxy.models import (CreateDataProductRequest, Cluster, User, Contact, Catalog, DataProduct, Tag,
                                        CreateDataProductResponse, TagIdentifier, TagResponse, SchemaMetadata)
from urllib.parse import quote_plus
from starburst_data_products_client.shared.api import ApiException


class Api:
    def __init__(self,
                 client_id: str,
                 client_secret: str,
                 username: str, password: str,
                 host: str,
                 cluster: str,
                 default_catalog: str,
                 api_key: str = None):
        self.client_id = client_id
        self.client_secret = client_secret
        self.username = username
        self.password = password
        self.url = host
        self.cluster = cluster
        self.default_catalog = default_catalog
        if api_key is not None:
            self.api_key = api_key
        else:
            self.api_key = self.__get_api_key__(url=host, client_id=client_id, client_secret=client_secret)

        self.__data_products_list__ = None
        self.__default_catalog_id__: str = None
        self.__users_list__ = None

    def get_default_catalog_id(self) -> str:
        if self.__default_catalog_id__ is None:
            self.__default_catalog_id__ = self.get_catalog_id_for_name(self.default_catalog)
        return self.__default_catalog_id__

    def __get_api_key__(self, url: str, client_id: str, client_secret: str) -> str:
        api_key_response = requests.post(url=f'{url}/oauth/v2/token',
                                         auth=(client_id, client_secret),
                                         headers={'Content-Type': 'application/x-www-form-urlencoded'},
                                         data='grant_type=client_credentials')
        return api_key_response.json()['access_token']

    def delete_data_product(self, data_product_name: str = None, data_product_id: str = None):
        if data_product_id is None:
            if data_product_name is None:
                raise ValueError('Either data product name or id must be provided')
            else:
                data_product_id = self.get_data_product_id_by_name(data_product_name)
        PATH = f'public/api/v1/dataProduct/{data_product_id}'
        response = requests.delete(
            f'{self.url}/{PATH}',
            headers={'Authorization': f'bearer {self.api_key}'}
        )

        if not response.ok:
            raise ApiException(
                f'Could not delete data product: {response.reason}. Status code {response.status_code}',
                reason=response.reason,
                status=response.status_code,
                body=response.text
            )

    def create_data_product(self, data_product_request: CreateDataProductRequest):
        PATH = 'public/api/v1/dataProduct'
        response = requests.post(
            f'{self.url}/{PATH}',
            headers={'Authorization': f'bearer {self.api_key}', "Content-type": "application/json"},
            data=data_product_request.to_json()
        )
        if not response.ok:
            raise ApiException(
                f'Could not create data product: {response.reason}. Status code {response.status_code}',
                reason=response.reason,
                status=response.status_code,
                body=response.text
            )
        create_data_product_response = CreateDataProductResponse.load(response.json())
        return create_data_product_response.dataProductId

    def update_data_product(self, data_product_request: CreateDataProductRequest):
        data_product_id = self.get_data_product_id_by_name(data_product_request.name)
        PATH = f'public/api/v1/dataProduct/{data_product_id}'
        response = requests.patch(
            f'{self.url}/{PATH}',
            headers={'Authorization': f'bearer {self.api_key}', "Content-type": "application/json"},
            data=data_product_request.to_json()
        )
        if not response.ok:
            raise ApiException(
                f'Could not create data product: {response.reason}. Status code {response.status_code}',
                reason=response.reason,
                status=response.status_code,
                body=response.text
            )

    def get_data_product_id_by_name(self, data_product_name: str) -> str:
        return next(iter([dp.dataProductId for dp in self.list_data_products() if dp.name == data_product_name]))

    def list_data_products(self, tag_name=None) -> List[DataProduct]:
        PATH = 'public/api/v1/dataProduct'
        if self.__data_products_list__ is None:
            self.__data_products_list__ = DataProduct.paginated_response_to_list(
                f'{self.url}/{PATH}',
                headers={'Authorization': f'bearer {self.api_key}'})

        if tag_name is not None:
            tagged_schema_names = [schema.schemaId for schema in
                                   self.get_tagged_schemas(self.get_default_catalog_id(), tag_name)]
            return [data_product for data_product in self.__data_products_list__
                    if data_product.schemaName in tagged_schema_names]
        return self.__data_products_list__

    def get_schema_tags(self, catalog_id: str, schema_name: str) -> List[TagIdentifier]:
        PATH = f'public/api/v1/catalog/{catalog_id}/schema'
        schema_metadata_response = SchemaMetadata.paginated_response_to_list(
            f'{self.url}/{PATH}',
            headers={'Authorization': f'bearer {self.api_key}'})
        return next(iter([schema.tags for schema in schema_metadata_response if schema.schemaId == schema_name]))

    def get_tagged_schemas(self, catalog_id: str, tag_name: str) -> List[SchemaMetadata]:
        PATH = f'public/api/v1/catalog/{catalog_id}/schema'
        schema_metadata_response = SchemaMetadata.paginated_response_to_list(
            f'{self.url}/{PATH}',
            headers={'Authorization': f'bearer {self.api_key}'})
        return [schema for schema in schema_metadata_response if tag_name in [tag.name for tag in schema.tags]]

    def __list_catalogs__(self) -> List[Catalog]:
        PATH = 'public/api/v1/catalog'
        return Catalog.paginated_response_to_list(
            f'{self.url}/{PATH}',
            headers={'Authorization': f'bearer {self.api_key}'})

    def get_catalog_id_for_name(self, name: str) -> str:
        catalogs = self.__list_catalogs__()
        return next(iter([catalog.catalogId for catalog in catalogs if catalog.catalogName == name]), None)

    def __list_clusters__(self) -> List[Cluster]:
        PATH = 'public/api/v1/cluster'
        return Cluster.paginated_response_to_list(
            f'{self.url}/{PATH}',
            headers={'Authorization': f'bearer {self.api_key}'})

    def get_cluster_by_name(self, name: str) -> Cluster:
        encoded_name = f'name={quote_plus(name)}'
        PATH = f'public/api/v1/cluster/{encoded_name}'
        cluster = Cluster.load(requests.get(f'{self.url}/{PATH}',
                                            headers={'Authorization': f'bearer {self.api_key}'}
                                            ).json())
        return cluster

    def create_schema_tag_str(self, tag_name: str):
        self.create_schema_tag(Tag(name=tag_name, color='green', description='Added by dp sync'))

    def create_schema_tag(self, tag: Tag):
        PATH = 'public/api/v1/tag'
        requests.post(f'{self.url}/{PATH}',
                      headers={'Authorization': f'bearer {self.api_key}'},
                      data=tag.to_json())

    def get_tag_by_name(self, tag_name: str) -> Tag:
        encoded_tag_name = f'name={quote_plus(tag_name)}'
        PATH = f'public/api/v1/tag/{encoded_tag_name}'
        return cast(Tag, TagResponse.load(requests.get(f'{self.url}/{PATH}',
                                                       headers={
                                                                'Authorization': f'bearer {self.api_key}'}).json()))

    def tag_schema(self, tag_id: str, catalog_id: str, schema_name: str):
        PATH = f'public/api/v1/tag/{tag_id}/catalog/{catalog_id}/schema/{quote_plus(schema_name)}'
        response = requests.put(f'{self.url}/{PATH}',
                                headers={'Authorization': f'bearer {self.api_key}'})

    def __list_users__(self) -> List[User]:
        PATH = 'public/api/v1/user'
        if self.__users_list__ is None:
            self.__users_list__ = User.paginated_response_to_list(
                f'{self.url}/{PATH}',
                headers={'Authorization': f'bearer {self.api_key}'})
        return self.__users_list__

    def emails_to_users(self, emails: List[str]) -> List[Contact]:
        return [Contact(userId=user.userId, email=user.email)
                for user in self.__list_users__() if user.email in emails]

    def __get_user_by_email(self, email: str) -> User:
        encoded_name = f'email={quote_plus(email)}'

        PATH = f'public/api/v1/user/{encoded_name}'
        return User.load(requests.get(f'{self.url}/{PATH}',
                                                 headers={'Authorization': f'bearer {self.api_key}'}
                                                 ).json())
