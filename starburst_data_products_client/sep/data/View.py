from dataclasses import dataclass
from starburst_data_products_client.sep.data import Column
from typing import List, Optional
from datetime import datetime
from starburst_data_products_client.shared.models import JsonDataClass


@dataclass
class View(JsonDataClass):
    name: str
    description: Optional[str]
    createdBy: str
    definitionQuery: str
    status: str
    columns: List[Column]
    markedForDeletion: bool
    createdAt: datetime
    updatedAt: datetime
    updatedBy: str
    publishedAt: Optional[datetime]
    publishedBy: Optional[str]
    matchesTrinoDefinition: Optional[bool]
