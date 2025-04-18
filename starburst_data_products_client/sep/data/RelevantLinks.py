from dataclasses import dataclass
from starburst_data_products_client.shared.models import JsonDataClass


@dataclass
class RelevantLinks(JsonDataClass):
    label: str
    url: str
