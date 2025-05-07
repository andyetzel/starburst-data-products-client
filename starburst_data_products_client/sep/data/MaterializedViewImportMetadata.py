from dataclasses import dataclass
from typing import List, Optional
from datetime import datetime
from starburst_data_products_client.shared.models import JsonDataClass


@dataclass
class MaterializedViewImportMetadata(JsonDataClass):
    status: Optional[str]
    scheduledTime: Optional[datetime]
    startTime: Optional[datetime]
    finishTime: Optional[datetime]
    rowCount: Optional[int]
    error: Optional[str]
