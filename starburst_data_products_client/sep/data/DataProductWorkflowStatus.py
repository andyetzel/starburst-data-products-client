from dataclasses import dataclass
from typing import List
from starburst_data_products_client.shared.models import JsonDataClass


@dataclass
class DataProductWorkflowError(JsonDataClass):
    entityType: str
    entityName: str
    message: str


@dataclass
class DataProductWorkflowStatus(JsonDataClass):
    workflowType: str
    status: str
    errors: List[DataProductWorkflowError]
    isFinalStatus: bool
