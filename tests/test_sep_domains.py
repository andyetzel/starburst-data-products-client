from starburst_data_products_client.sep.api import Api as SepApi
import pytest


class TestSepDomains:

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
    

    def test_listing_domains(self):
        created_domain = self.sep_api.create_domain('domain_1')
        assert created_domain.name == 'domain_1'
        domains = self.sep_api.list_domains()
        assert len(domains) == 1
        assert domains[0].name == 'domain_1'
        self.sep_api.delete_domain(created_domain.id)
        with pytest.raises(Exception) as exc_info:
            self.sep_api.get_domain(created_domain.id)
        assert '404' in str(exc_info.value)

    
    def test_create_and_delete_domain(self):
        created_domain = self.sep_api.create_domain('domaintest')
        assert created_domain.name == 'domaintest'
        domain = self.sep_api.get_domain(created_domain.id)
        assert domain.name == created_domain.name
        self.sep_api.delete_domain(created_domain.id)
        with pytest.raises(Exception) as exc_info:
            self.sep_api.get_domain(created_domain.id)
        assert '404' in str(exc_info.value)
    

    def test_update_domain(self):
        created_domain = self.sep_api.create_domain('domaintest')
        assert created_domain.name == 'domaintest'
        updated_domain = self.sep_api.update_domain(created_domain.id, 'this is a description')
        assert updated_domain.id == created_domain.id
        assert updated_domain.name == created_domain.name
        assert updated_domain.description == 'this is a description'
        updated_domain = self.sep_api.update_domain(created_domain.id, 'this is a description', 's3://bucketname/')
        assert updated_domain.id == created_domain.id
        assert updated_domain.name == created_domain.name
        assert updated_domain.description == 'this is a description'
        assert updated_domain.schemaLocation == 's3://bucketname/'
        self.sep_api.delete_domain(created_domain.id)
        with pytest.raises(Exception) as exc_info:
            self.sep_api.get_domain(created_domain.id)
        assert '404' in str(exc_info.value)
