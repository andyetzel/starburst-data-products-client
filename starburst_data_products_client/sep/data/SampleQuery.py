from dataclasses import dataclass
from typing import Optional
from starburst_data_products_client.shared.models import JsonDataClass


@dataclass
class SampleQuery(JsonDataClass):
    name: str
    description: Optional[str]
    query: str
