from dataclasses import dataclass
from starburst_data_products_client.shared.models import JsonDataClass


@dataclass
class Column(JsonDataClass):
    name: str
    type: str
    description: str
