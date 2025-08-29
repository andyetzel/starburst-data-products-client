from starburst_data_products_client.sep.api import Api as SepApi
from starburst_data_products_client.sep.data import DataProductParameters, Owner, SampleQuery
from starburst_data_products_client.shared.auth_config import AuthConfig, AuthenticationError
import pytest
import time
import os


class TestSepDataProducts:

    def setup_class(self):
        # Default test configuration
        self.host = 'localhost'
        self.port = 8080
        self.sep_url = f'{self.host}:{self.port}'
        self.sep_user = 'sep_dp_api_user'
        
        # Try to use centralized authentication configuration first
        try:
            # Set up test environment variables if not already set
            test_env = {
                'SEP_HOST': os.environ.get('SEP_HOST', self.sep_url),
                'SEP_PROTOCOL': os.environ.get('SEP_PROTOCOL', 'http'),
                'SSL_VERIFY': os.environ.get('SSL_VERIFY', 'false'),
                'AUTH_METHOD': os.environ.get('AUTH_METHOD', 'basic')
            }
            
            # For basic auth, set default test credentials
            if test_env['AUTH_METHOD'] == 'basic':
                test_env.update({
                    'SEP_USERNAME': os.environ.get('SEP_USERNAME', self.sep_user),
                    'SEP_PASSWORD': os.environ.get('SEP_PASSWORD', '')
                })
            
            # Temporarily set environment variables for auth config
            original_env = {}
            for key, value in test_env.items():
                original_env[key] = os.environ.get(key)
                os.environ[key] = value
            
            try:
                # Create auth config and API client
                auth_config = AuthConfig()
                self.sep_api = auth_config.create_api_client()
                auth_info = auth_config.get_auth_info()
                print(f"Using {auth_info['method']} authentication for tests")
                
            finally:
                # Restore original environment variables
                for key, original_value in original_env.items():
                    if original_value is None:
                        os.environ.pop(key, None)
                    else:
                        os.environ[key] = original_value
                        
        except (AuthenticationError, ImportError) as e:
            print(f"Centralized auth config failed: {e}")
            print("Falling back to basic authentication for tests")
            
            # Fallback to basic authentication
            self.sep_api = SepApi(
                host=self.sep_url,
                username=self.sep_user,
                password='',
                protocol='http',
                verify_ssl=False  # Disable SSL verification for test environment
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
        
    def test_clone_data_product(self):
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
        cloned_data_product = self.sep_api.clone_data_product(
            created_data_product.id,
            'hive',
            'dpclonetest',
            'dpclone'
        )
        assert cloned_data_product.name == 'dpclone'
        self.delete_data_product(created_data_product.id)
        self.delete_data_product(cloned_data_product.id)
        self.sep_api.delete_domain(domain.id)
    
    
    def test_data_product_sample_queries(self):
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
        dp_sample_queries = self.sep_api.list_sample_queries(created_data_product.id)
        assert len(dp_sample_queries) == 0
        self.sep_api.update_sample_queries(
            created_data_product.id,
            [SampleQuery(name='first',description='first description',query='select * from tpch.sf1.region')]
        )
        dp_sample_queries = self.sep_api.list_sample_queries(created_data_product.id)
        assert len(dp_sample_queries) == 1
        assert dp_sample_queries[0].name == 'first'
        self.delete_data_product(created_data_product.id)
        self.sep_api.delete_domain(domain.id)
    
    
    def test_update_data_product(self):
        """Test updating a data product using the new update_data_product method."""
        domain = self.sep_api.create_domain('dpdomain')
        
        # Create initial data product
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
        assert created_data_product.summary == 'this is a summary'
        assert created_data_product.description == 'dp created in unit tests'
        
        # Update the data product with new information
        updated_params = self.create_data_product_obj(
            'dptest',  # Keep same name
            'hive',
            'dptest',
            'this is an updated summary',  # Changed summary
            domain.id,
            owners=[Owner(name='Test Owner', email='test@example.com')]  # Added owner
        )
        updated_params.description = 'updated description for testing'  # Changed description
        
        updated_data_product = self.sep_api.update_data_product(created_data_product.id, updated_params)
        
        # Verify the updates
        assert updated_data_product.name == 'dptest'
        assert updated_data_product.summary == 'this is an updated summary'
        assert updated_data_product.description == 'updated description for testing'
        assert len(updated_data_product.owners) == 1
        assert updated_data_product.owners[0].name == 'Test Owner'
        assert updated_data_product.owners[0].email == 'test@example.com'
        
        # Verify by fetching the data product again
        fetched_data_product = self.sep_api.get_data_product(created_data_product.id)
        assert fetched_data_product.summary == 'this is an updated summary'
        assert fetched_data_product.description == 'updated description for testing'
        
        self.delete_data_product(created_data_product.id)
        self.sep_api.delete_domain(domain.id)
    

    def test_data_product_mv_refresh_data(self):
        domain = self.sep_api.create_domain('dpdomain')
        tpch_views = [
            {
                'name': 'nation_data_set',
                'definitionQuery': 'select name as nation_name from tpch.tiny.nation'
            }
        ]
        tpch_mvs = [
            {
                'name': 'region_data_set',
                'definitionQuery': 'select name as region_name from tpch.tiny.region'
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
                tpch_mvs
            )
        )
        assert created_data_product.name == 'dptest'
        mv_refresh_metadata = self.sep_api.get_materialized_view_refresh_metadata(created_data_product.id, 'region_data_set')
        assert mv_refresh_metadata.incrementalColumn == None
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
        