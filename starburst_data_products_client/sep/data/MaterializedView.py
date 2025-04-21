from dataclasses import dataclass
from starburst_data_products_client.sep.data import Column
from starburst_data_products_client.sep.data import MaterializedViewProperties

from typing import List, Optional
from datetime import datetime
from starburst_data_products_client.shared.models import JsonDataClass


@dataclass
class MaterializedView(JsonDataClass):
    name: str
    description: Optional[str]
    createdBy: Optional[str]
    definitionQuery: str
    definitionProperties: Optional[MaterializedViewProperties]
    status: Optional[str]
    columns: Optional[List[Column]]
    markedForDeletion: Optional[bool]
    createdAt: Optional[datetime]
    updatedAt: Optional[datetime]
    updatedBy: Optional[str]
    publishedAt: Optional[datetime]
    publishedBy: Optional[str]
    matchesTrinoDefinition: Optional[bool]
