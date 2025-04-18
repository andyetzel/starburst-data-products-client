from dataclasses import dataclass
from typing import List
from datetime import datetime
from starburst_data_products_client.shared.models import PaginatedJsonDataClass


@dataclass
class UserPrincipal(PaginatedJsonDataClass):
    id: str
    type: str


@dataclass
class UserRole(PaginatedJsonDataClass):
    roleName: str
    roleId: str
    principal: UserPrincipal
    adminOption: bool


@dataclass
class User(PaginatedJsonDataClass):
    syncToken: str
    userId: str
    email: str
    defaultRoleId: str
    createdOn: datetime
    scimManaged: bool
    directlyGrantedRoles: List[UserRole]
    allRoles: List[UserRole]
