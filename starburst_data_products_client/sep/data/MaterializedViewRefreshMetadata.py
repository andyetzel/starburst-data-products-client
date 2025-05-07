from dataclasses import dataclass
from starburst_data_products_client.sep.data import MaterializedViewImportMetadata
from typing import List, Optional
from datetime import datetime
from starburst_data_products_client.shared.models import JsonDataClass


@dataclass
class MaterializedViewRefreshMetadata(JsonDataClass):
    lastImport: Optional[MaterializedViewImportMetadata]
    incrementalColumn: Optional[str]
    refreshInterval: Optional[str]
    storageSchema: Optional[str]
    estimatedNextRefreshTime: Optional[datetime]
