from dataclasses import dataclass
from typing import Optional
from starburst_data_products_client.shared.models import JsonDataClass


@dataclass
class MaterializedViewProperties(JsonDataClass):
    refresh_interval: str
    grace_period: Optional[str]
