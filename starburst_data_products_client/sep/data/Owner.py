from dataclasses import dataclass
from starburst_data_products_client.shared.models import JsonDataClass


@dataclass
class Owner(JsonDataClass):
    name: str
    email: str
