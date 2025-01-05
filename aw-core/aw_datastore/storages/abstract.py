from abc import ABCMeta, abstractmethod
from datetime import datetime
from typing import Dict, List, Optional

from aw_core.models import Event


class AbstractStorage(metaclass=ABCMeta):
    """
    Interface for storage methods.
    """

    sid = "Storage id not set, fix me"

    @abstractmethod
    def __init__(self, testing: bool) -> None:
        self.testing = True
        raise NotImplementedError

    @abstractmethod
    def buckets(self) -> Dict[str, dict]:
        raise NotImplementedError

    @abstractmethod
    def create_bucket(
        self,
        bucket_id: str,
        type_id: str,
        client: str,
        hostname: str,
        created: str,
        name: Optional[str] = None,
        data: Optional[dict] = None,
    ) -> str:
        raise NotImplementedError

    @abstractmethod
    def update_bucket(
        self,
        bucket_hash_key: str,
        type_id: Optional[str] = None,
        client: Optional[str] = None,
        hostname: Optional[str] = None,
        name: Optional[str] = None,
        data: Optional[dict] = None,
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def delete_bucket(self, bucket_hash_key: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def get_metadata(self, bucket_hash_key: str) -> dict:
        raise NotImplementedError

    @abstractmethod
    def get_event(
        self,
        bucket_hash_key: str,
        event_id: int,
    ) -> Optional[Event]:
        raise NotImplementedError

    @abstractmethod
    def get_events(
        self,
        bucket_hash_key: str,
        limit: int,
        starttime: Optional[datetime] = None,
        endtime: Optional[datetime] = None,
    ) -> List[Event]:
        raise NotImplementedError

    def get_eventcount(
        self,
        bucket_hash_key: str,
        starttime: Optional[datetime] = None,
        endtime: Optional[datetime] = None,
    ) -> int:
        raise NotImplementedError

    @abstractmethod
    def insert_one(self, bucket_hash_key: str, event: Event) -> Event:
        raise NotImplementedError

    def insert_many(self, bucket_hash_key: str, events: List[Event]) -> None:
        for event in events:
            self.insert_one(bucket_hash_key, event)

    @abstractmethod
    def delete(self, bucket_hash_key: str, event_id: int) -> bool:
        raise NotImplementedError

    @abstractmethod
    def replace(self, bucket_hash_key: str, event_id: int, event: Event) -> bool:
        raise NotImplementedError

    @abstractmethod
    def replace_last(self, bucket_hash_key: str, event: Event) -> None:
        raise NotImplementedError

    @abstractmethod
    def get_user_by_uuid(self, user_uuid: int) -> Optional[dict]:
        raise NotImplementedError

    @abstractmethod
    def update_user(self, uuid, data):
        raise NotImplementedError

    @abstractmethod
    def create_user(self, data):
        raise NotImplementedError

    @abstractmethod
    def get_users(self):
        raise NotImplementedError

    @abstractmethod
    def get_buckets_for_user(self, user):
        raise NotImplementedError