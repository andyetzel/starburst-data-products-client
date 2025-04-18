from dataclasses import dataclass
from typing import Optional
from starburst_data_products_client.shared.models import PaginatedJsonDataClass

@dataclass
class Tag(PaginatedJsonDataClass):
    name: str
    color: str
    description: Optional[str]
