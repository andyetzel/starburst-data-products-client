from starburst_data_products_client.sep.api import Api as SepApi
from starburst_data_products_client.sep.data import DataProduct
import pytest
import time


class TestSepDataProducts:

    def setup_class(self):
        self.host = 'localhost'
        self.port = 8080
        self.sep_url = f'{self.host}:{self.port}'
        self.sep_user = 'sep_dp_api_user'
        self.sep_api = SepApi(
            host=self.sep_url,
            username=self.sep_user,
            password='',
            protocol='http'
        )


    def create_and_delete_data_product(self):
        domain = self.sep_api.create_domain('dpdomain')
        created_data_product = self.sep_api.create_data_product(
            self.create_data_product_obj(
                'dptest',
                'hive',
                'dptest',
                'this is a summary',
                domain.id
            )
        )
        assert created_data_product.name == 'dptest'
        self.sep_api.delete_data_product(created_data_product.id)
        # need to keep checking status until it is deleted
        data_product_status = self.sep_api.get_delete_data_product_status(created_data_product.id)
        while data_product_status.status != 'COMPLETED' or data_product_status.status != 'ERROR':
            time.sleep(10)
            data_product_status = self.sep_api.get_delete_data_product_status(created_data_product.id)
        with pytest.raises(Exception) as exc_info:
            self.sep_api.get_data_product(created_data_product.id)
        assert '404' in str(exc_info.value)
        self.sep_api.delete_domain(domain.id)

    
    def test_listing_data_products(self):
        data_product_names = ['data_product_1', 'data_product_2']
        available_dps = self.sep_api.search_data_products()
        for data_product in data_product_names:
            self.check_data_product(data_product, available_dps)


    def check_data_product(self, data_product_name, available_dps):
        for data_product in available_dps:
            if data_product.name == data_product_name:
                assert data_product.name == data_product_name
                assert data_product.catalogName == 'hive'
                assert data_product.schemaName == data_product_name
                assert data_product.createdBy == self.sep_user
    
    def create_data_product_obj(self, name, catalog_name, schema_name, summary, domain_id, views=None):
        data_product = DataProduct()
        data_product.name = name
        data_product.catalogName = catalog_name
        data_product.schemaName = schema_name
        data_product.summary = summary
        data_product.dataDomainId = domain_id
        data_product.views = views if views else []
        return data_product
