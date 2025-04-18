from dataclasses import dataclass
from typing import List
from starburst_data_products_client.shared.models import JsonDataClass


@dataclass
class DataProductPublishError(JsonDataClass):
    entityType: str
    entityName: str
    message: str


@dataclass
class DataProductPublishStatus(JsonDataClass):
    workflowType: str
    status: str
    errors: List[DataProductPublishError]
    isFinalStatus: bool
