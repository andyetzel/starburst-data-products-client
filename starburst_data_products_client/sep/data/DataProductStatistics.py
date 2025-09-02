from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from starburst_data_products_client.shared.models import JsonDataClass


@dataclass
class DataProductStatistics(JsonDataClass):
    """Data Product usage statistics.
    
    Contains metrics about data product usage including query counts and unique user counts
    over different time windows.
    """
    dataProductId: str
    sevenDayQueryCount: int
    thirtyDayQueryCount: int
    sevenDayUserCount: int
    thirtyDayUserCount: int
    updatedAt: datetime