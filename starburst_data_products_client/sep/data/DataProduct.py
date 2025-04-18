from dataclasses import dataclass

from starburst_data_products_client.sep.data import View
from starburst_data_products_client.sep.data import MaterializedView
from starburst_data_products_client.sep.data import Owner
from starburst_data_products_client.sep.data import AccessMetadata
from starburst_data_products_client.sep.data import UserData
from starburst_data_products_client.sep.data import RelevantLinks

from typing import List, Optional
from datetime import datetime
from starburst_data_products_client.shared.models import JsonDataClass


@dataclass
class DataProduct(JsonDataClass):
    id: Optional[str]
    name: str
    catalogName: str
    schemaName: str
    dataDomainId: str
    summary: str
    description: Optional[str]
    createdBy: Optional[str]
    status: Optional[str]
    views: List[View]
    materializedViews: List[MaterializedView]
    owners: List[Owner]
    productOwners: Optional[List[Owner]]
    relevantLinks: List[RelevantLinks]
    createdAt: Optional[datetime]
    updatedAt: Optional[datetime]
    updatedBy: Optional[str]
    publishedAt: Optional[datetime]
    publishedBy: Optional[str]
    accessMetadata: Optional[AccessMetadata]
    ratingsCount: Optional[int]
    userData: Optional[UserData]
    matchesTrinoDefinition: Optional[bool]
    bookmarkCount: Optional[int]

@dataclass
class DataProductParameters(JsonDataClass):
    name: str
    catalogName: str
    schemaName: str
    dataDomainId: str
    summary: str
    description: Optional[str]
    views: List[View]
    materializedViews: List[MaterializedView]
    owners: List[Owner]
    relevantLinks: List[RelevantLinks]
    