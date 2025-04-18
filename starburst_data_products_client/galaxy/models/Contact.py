from dataclasses import dataclass
from starburst_data_products_client.shared.models import PaginatedJsonDataClass


@dataclass
class Contact(PaginatedJsonDataClass):
    userId: str
    email: str
