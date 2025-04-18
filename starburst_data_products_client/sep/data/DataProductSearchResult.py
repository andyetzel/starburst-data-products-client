from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from starburst_data_products_client.shared.models import JsonDataClass


@dataclass
class DataProductSearchResult(JsonDataClass):
    id: str
    name: str
    catalogName: str
    schemaName: str
    dataDomainId: str
    summary: str
    description: Optional[str]
    createdBy: str
    status: str
    createdAt: datetime
    updatedAt: datetime
    publishedAt: Optional[datetime]
    publishedBy: Optional[str]
    lastQueriedAt: Optional[datetime]
    lastQueriedBy: Optional[str]
    ratingsCount: int
    userData: dict
