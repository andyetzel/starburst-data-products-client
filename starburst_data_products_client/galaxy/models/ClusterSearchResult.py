from dataclasses import dataclass
from typing import List
from starburst_data_products_client.galaxy.models import Catalog
from starburst_data_products_client.shared.models import PaginatedJsonDataClass


@dataclass
class CatalogSearchResult(PaginatedJsonDataClass):
    clusters: List[Catalog]
