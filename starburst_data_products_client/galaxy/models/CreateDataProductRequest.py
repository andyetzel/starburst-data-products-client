from dataclasses import dataclass
from typing import List
from starburst_data_products_client.galaxy.models import Contact, Link
from starburst_data_products_client.shared.models import PaginatedJsonDataClass


@dataclass
class CreateDataProductRequest(PaginatedJsonDataClass):
    name: str
    summary: str
    description: str
    catalogId: str
    schemaName: str
    contacts: List[Contact]
    links: List[Link]
    defaultClusterId: str
