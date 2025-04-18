from typing import cast, Self, List, Any
import marshmallow_dataclass
from dataclasses import asdict, is_dataclass
from json import JSONEncoder, dumps
from datetime import datetime
from functools import singledispatchmethod
from dataclasses import dataclass
from requests import get


class DateTimeEncoder(JSONEncoder):
    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()
        return super().default(o)


class JsonDataClass:

    @singledispatchmethod
    @classmethod
    def load(cls, arg):
        raise NotImplemented('JsonDataClass.load() only accepts dicts and json strings')

    @load.register
    @classmethod
    def _(cls, data: str) -> Self:
        if is_dataclass(cls):
            schema = marshmallow_dataclass.class_schema(cls)()
            return cast(cls, schema.loads(data))
        else:
            raise NotImplemented('Only dataclasses should inherit from JsonDataClass!')

    @load.register
    @classmethod
    def _(cls, data: dict) -> Self:
        if is_dataclass(cls):
            schema = marshmallow_dataclass.class_schema(cls)()
            return cast(cls, schema.load(data))
        else:
            raise NotImplemented('Only dataclasses should inherit from JsonDataClass!')

    def asdict(self) -> dict:
        if is_dataclass(self):
            return asdict(self)
        else:
            raise NotImplemented('Only dataclasses should inherit from JsonDataClass!')

    def to_json(self) -> str:
        return dumps(self.asdict(), cls=DateTimeEncoder)


@dataclass
class PaginatedResponse(JsonDataClass):
    nextPageToken: str
    result: List[Any]


class PaginatedJsonDataClass(JsonDataClass):
    @classmethod
    def paginated_response_to_list(cls, url: str, headers: {}) -> List[Self]:
        response = get(url=url, headers=headers)
        paginated_response = PaginatedResponse.load(response.json())
        results = [cls.load(r) for r in paginated_response.result]
        while paginated_response.nextPageToken is not None and paginated_response.nextPageToken.strip() != '':
            response = get(url=url, headers=headers, params={'pageToken': paginated_response.nextPageToken})
            paginated_response = PaginatedResponse.load(response.json())
            results += [cls.load(r) for r in paginated_response.result]
        return results
