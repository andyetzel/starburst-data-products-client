from dataclasses import dataclass
from starburst_data_products_client.sep.data import Column
from starburst_data_products_client.sep.data import MaterializedViewProperties

from typing import List
from datetime import datetime
from starburst_data_products_client.shared.models import JsonDataClass


@dataclass
class MaterializedView(JsonDataClass):
    name: str
    description: str
    createdBy: str
    definitionQuery: str
    definitionProperties: MaterializedViewProperties
    status: str
    columns: List[Column]
    markedForDeletion: bool
    createdAt: datetime
    updatedAt: datetime
    updatedBy: str
    publishedAt: datetime
    publishedBy: str
    matchesTrinoDefinition: bool
