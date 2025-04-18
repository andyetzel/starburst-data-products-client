from starburst_data_products_client.sep.api import Api as SepApi


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
