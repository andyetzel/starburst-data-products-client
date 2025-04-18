from dataclasses import dataclass
from starburst_data_products_client.shared.models import JsonDataClass


@dataclass
class UserData(JsonDataClass):
    isBookmarked: bool
    # TODO: check docs for additional fields
