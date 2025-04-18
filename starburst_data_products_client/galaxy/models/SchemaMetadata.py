from dataclasses import dataclass
from typing import List, Optional
from starburst_data_products_client.galaxy.models import Contact, Link
from starburst_data_products_client.shared.models import PaginatedJsonDataClass

@dataclass
class TagIdentifier(PaginatedJsonDataClass):
    tagId: str
    name: str


@dataclass
class RoleIdentifier(PaginatedJsonDataClass):
    roleId: str
    roleName: str


@dataclass
class SchemaMetadata(PaginatedJsonDataClass):
    schemaId: str
    description: Optional[str]
    owner: RoleIdentifier
    tags: List[TagIdentifier]
    contacts: List[Contact]
    links: List[Link]
