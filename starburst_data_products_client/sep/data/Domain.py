from dataclasses import dataclass

from starburst_data_products_client.sep.data import View
from starburst_data_products_client.sep.data import MaterializedView
from starburst_data_products_client.sep.data import Owner
from starburst_data_products_client.sep.data import AccessMetadata
from starburst_data_products_client.sep.data import UserData
from starburst_data_products_client.sep.data import RelevantLinks

from typing import List, Optional, Any
from datetime import datetime
from starburst_data_products_client.shared.models import JsonDataClass


@dataclass
class Domain(JsonDataClass):
    id: str
    name: str
    description: Optional[str]
    schemaLocation: Optional[str]
    assignedDataProducts: List[Any]
    createdBy: str
    createdAt: datetime
    updatedAt: datetime
    updatedBy: str
