from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from starburst_data_products_client.shared.models import JsonDataClass


@dataclass
class AccessMetadata(JsonDataClass):
    lastQueriedAt: Optional[datetime]
    lastQueriedBy: Optional[str]
