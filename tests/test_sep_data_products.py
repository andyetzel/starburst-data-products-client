from starburst_data_products_client.sep.api import Api as SepApi
from starburst_data_products_client.sep.data import DataProductParameters, Owner
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
        self.delete_data_product(created_data_product.id)
        self.sep_api.delete_domain(domain.id)

    
    def test_listing_data_products(self):
        domain = self.sep_api.create_domain('dpdomain')
        tpch_views = [
            {
                'name': 'nation_data_set',
                'definitionQuery': 'select name as nation_name from tpch.tiny.nation'
            },
            {
                'name': 'region_data_set',
                'definitionQuery': 'select name as region_name from tpch.tiny.region'
            }
        ]
        dp1 = self.sep_api.create_data_product(
            self.create_data_product_obj(
                'data_product_1',
                'hive',
                'data_product_1',
                'summary',
                domain.id,
                tpch_views
            )
        )
        dp2 = self.sep_api.create_data_product(
            self.create_data_product_obj(
                'data_product_2',
                'hive',
                'data_product_2',
                'summary',
                domain.id,
                tpch_views
            )
        )
        data_product_names = ['data_product_1', 'data_product_2']
        available_dps = self.sep_api.search_data_products()
        for data_product in data_product_names:
            self.check_data_product(data_product, available_dps)
        self.delete_data_product(dp1.id)
        self.delete_data_product(dp2.id)
        self.sep_api.delete_domain(domain.id)
    
    
    def test_data_product_tags(self):
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
        # add tags
        self.sep_api.update_tags(created_data_product.id, ['saoirse'])
        # verify tags
        tags = self.sep_api.get_tags(created_data_product.id)
        assert len(tags) == 1
        assert tags[0].value == 'saoirse'
        self.sep_api.delete_tag(tags[0].id, created_data_product.id)
        tags = self.sep_api.get_tags(created_data_product.id)
        assert len(tags) == 0
        self.delete_data_product(created_data_product.id)
        self.sep_api.delete_domain(domain.id)


    def test_publish_data_product(self):
        domain = self.sep_api.create_domain('dpdomain')
        tpch_views = [
            {
                'name': 'nation_data_set',
                'definitionQuery': 'select name as nation_name from tpch.tiny.nation'
            }
        ]
        created_data_product = self.sep_api.create_data_product(
            self.create_data_product_obj(
                'dptest',
                'hive',
                'dptest',
                'this is a summary',
                domain.id,
                tpch_views,
                [],
                [Owner(name='saoirse', email='saoirse@saoirse.com')]
            )
        )
        assert created_data_product.name == 'dptest'
        # add tags
        self.sep_api.update_tags(created_data_product.id, ['saoirse'])
        # verify tags
        tags = self.sep_api.get_tags(created_data_product.id)
        assert len(tags) == 1
        assert tags[0].value == 'saoirse'
        # publish data product
        self.sep_api.publish_data_product(created_data_product.id)
        # verify status
        data_product_status = self.sep_api.get_publish_data_product_status(created_data_product.id)
        attempts = 0
        while data_product_status.isFinalStatus is not True and attempts < 10:
            time.sleep(10)
            data_product_status = self.sep_api.get_publish_data_product_status(created_data_product.id)
            attempts += 1
        assert data_product_status.isFinalStatus == True
        print(data_product_status.errors)
        assert len(data_product_status.errors) == 0
        assert data_product_status.status == 'COMPLETED'
        self.sep_api.delete_tag(tags[0].id, created_data_product.id)
        tags = self.sep_api.get_tags(created_data_product.id)
        assert len(tags) == 0
        self.delete_data_product(created_data_product.id)
        self.sep_api.delete_domain(domain.id)
        

    def check_data_product(self, data_product_name, available_dps):
        for data_product in available_dps:
            if data_product.name == data_product_name:
                assert data_product.name == data_product_name
                assert data_product.catalogName == 'hive'
                assert data_product.schemaName == data_product_name
                assert data_product.createdBy == self.sep_user
    
    
    def create_data_product_obj(self, name, catalog_name, schema_name, summary, domain_id, views=None, mvs=None, owners=None):        
        return DataProductParameters(
            name=name,
            catalogName=catalog_name,
            schemaName=schema_name,
            dataDomainId=domain_id,
            summary=summary,
            description='dp created in unit tests',
            views=views if views else [],
            materializedViews=mvs if mvs else [],
            owners=owners if owners else [],
            relevantLinks=[]
        )
    
    
    def delete_data_product(self, id):
        self.sep_api.delete_data_product(id)
        # need to keep checking status until it is deleted
        data_product_status = self.sep_api.get_delete_data_product_status(id)
        attempts = 0
        while data_product_status.isFinalStatus is not True and attempts < 10:
            time.sleep(10)
            data_product_status = self.sep_api.get_delete_data_product_status(id)
            attempts += 1
        with pytest.raises(Exception) as exc_info:
            self.sep_api.get_data_product(id)
        assert '404' in str(exc_info.value)
        