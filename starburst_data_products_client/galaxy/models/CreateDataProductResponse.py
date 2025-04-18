from dataclasses import dataclass
from typing import List
from starburst_data_products_client.galaxy.models import Contact, Link
from datetime import datetime
from starburst_data_products_client.shared.models import PaginatedJsonDataClass

@dataclass
class DataProductCatalogDetails(PaginatedJsonDataClass):
    catalogId: str
    catalogName: str
    catalogKind: str
    localRegions: List[str]


@dataclass
class CreateDataProductResponse(PaginatedJsonDataClass):
    syncToken: str
    dataProductId: str
    name: str
    summary: str
    description: str
    catalog: DataProductCatalogDetails
    schemaName: str
    contacts: List[Contact]
    links: List[Link]
    defaultClusterId: str
    createdOn: datetime
    modifiedOn: datetime
    createdBy: Contact
    modifiedBy: Contact
