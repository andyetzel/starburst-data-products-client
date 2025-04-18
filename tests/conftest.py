import pytest
import time
import requests
from requests.exceptions import HTTPError
from starburst_data_products_client.sep.api import Api as SepApi

host='localhost'
port=8080
sep_user='sep_dp_api_user'


def create_data_product(name, catalog_name, schema_name, summary, domain_id, views=None):
    if views is None:
        views = []
    api_url = f'http://{host}:{port}/api/v1/dataProduct/products'
    try:
        response = requests.post(
            api_url,
            headers={
                'Accept': 'application/json',
                'Content-Type': 'application/json',
                'X-Trino-User': sep_user
            },
            json={
                'name': name,
                'catalogName': catalog_name,
                'schemaName': schema_name,
                'dataDomainId': domain_id,
                'summary': summary,
                'owners': [
                    {
                        'name': sep_user,
                        'email': 'email@starburst.io'
                    }
                ],
                'views': views
            }
        )
        response.raise_for_status()
        return response.json()
    except HTTPError as http_err:
        print(f'{http_err}')
        return None
    except Exception as err:
        print(f'{err}')
        return None


def publish_data_product(sep_api, data_product_id):
    sep_api.publish_data_product(data_product_id)


def publish_data_product_status(sep_api, data_product_id):
    return sep_api.get_publish_data_product_status(data_product_id)


def sync_publish_data_product(sep_api, data_product_id):
    publish_data_product(sep_api, data_product_id)
    data_product_status = publish_data_product_status(sep_api, data_product_id)
    while data_product_status.status == 'DRAFT' or data_product_status.status == 'IN_PROGRESS':
        time.sleep(10)
        data_product_status = publish_data_product_status(sep_api, data_product_id)


def main():
    sep_url = f'{host}:{port}'
    sep_api = SepApi(
        host=sep_url,
        username=sep_user,
        password='',
        protocol='http'
    )
    sep_api.create_domain('domain_1')
    domains = sep_api.list_domains()
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
    create_data_product(
        'data_product_1',
        'hive',
        'data_product_1',
        'summary',
        domains[0].id,
        tpch_views
    )
    create_data_product(
        'data_product_2',
        'hive',
        'data_product_2',
        'summary',
        domains[0].id,
        tpch_views
    )
    data_products = sep_api.search_data_products()
    for data_product in data_products:
        sync_publish_data_product(sep_api, data_product.id)


@pytest.fixture(scope="session", autouse=True)
def create_data_products():
    main()
