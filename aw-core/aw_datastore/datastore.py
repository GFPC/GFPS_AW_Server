import hashlib
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Union

import iso8601


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def safe_json_loads(s: str):
    try:
        return json.loads(s)
    except Exception:
        try:
            # Fallback: escape all backslashes to neutralize broken \uXXXX fragments
            return json.loads(s.replace("\\", "\\\\"))
        except Exception:
            return {}
from aw_core.models import Event
from playhouse.migrate import MySQLMigrator, migrate
from playhouse.mysql_ext import MySQLDatabase
from peewee import (
    AutoField,
    CharField,
    DateTimeField,
    DecimalField,
    ForeignKeyField,
    IntegerField,
    TextField,
    Model,
)

logger = logging.getLogger(__name__)

_db = MySQLDatabase(None)


def auto_migrate(host: str, port: int, user: str, password: str, database: str) -> None:
    db = MySQLDatabase(
        database,
        host=host,
        port=port,
        user=user,
        password=password,
        charset="utf8mb4",
    )
    migrator = MySQLMigrator(db)
    try:
        info = db.execute_sql("DESCRIBE bucketmodel")
        has_datastr = any(row[0] == "datastr" for row in info)
    except Exception:
        has_datastr = False
    if not has_datastr:
        datastr_field = CharField(default="{}")
        with db.atomic():
            migrate(migrator.add_column("bucketmodel", "datastr", datastr_field))
    db.close()


class BaseModel(Model):
    class Meta:
        database = _db


class UserModel(BaseModel):

    id = AutoField(primary_key=True,null=False)
    username = CharField()
    uuid = CharField(unique=True)
    team = CharField(null=True)
    data = TextField(null=True)
    created = DateTimeField(default=datetime.now)

    def json(self):
        created_str = None
        if self.created:
            if isinstance(self.created, str):
                created_str = iso8601.parse_date(self.created).astimezone(timezone.utc).isoformat()
            else:
                if self.created.tzinfo is None:
                    created_dt = self.created.replace(tzinfo=timezone.utc)
                else:
                    created_dt = self.created.astimezone(timezone.utc)
                created_str = created_dt.isoformat()
        return {
            "id": self.id,
            "username": self.username,
            "uuid": self.uuid,
            "team": self.team,
            "created": created_str,
            "data": safe_json_loads(self.data) if self.data else {},
        }


class BucketModel(BaseModel):
    key = AutoField(primary_key=True,null=False)
    id = CharField(null=False)
    created = DateTimeField(default=datetime.now)
    type = CharField()
    client = CharField()
    hostname = CharField()
    datastr = TextField(null=True)
    user = ForeignKeyField(UserModel, field="id", null=True)
    hash_key = CharField()

    def json(self):
        created_str = None
        if self.created:
            if isinstance(self.created, str):
                created_str = iso8601.parse_date(self.created).astimezone(timezone.utc).isoformat()
            else:
                if self.created.tzinfo is None:
                    created_dt = self.created.replace(tzinfo=timezone.utc)
                else:
                    created_dt = self.created.astimezone(timezone.utc)
                created_str = created_dt.isoformat()
        return {
            "id": self.id,
            "created": created_str,
            "type": self.type,
            "client": self.client,
            "hostname": self.hostname,
            "data": safe_json_loads(self.datastr) if self.datastr else {},
            "user": self.user,
            "hash_key": self.hash_key,
        }


class EventModel(BaseModel):
    id = AutoField()
    bucket = ForeignKeyField(BucketModel, backref="events", index=True)
    timestamp = DateTimeField(index=True, default=datetime.now)
    duration = DecimalField(max_digits=16, decimal_places=6)
    datastr = TextField()

    @classmethod
    def from_event(cls, bucket_key, event: Event):
        return cls(
            bucket=bucket_key,
            id=event.id,
            timestamp=event.timestamp,
            duration=event.duration.total_seconds(),
            datastr=json.dumps(event.data, ensure_ascii=False),
        )

    def json(self):
        timestamp_str = None
        if self.timestamp:
            if isinstance(self.timestamp, str):
                timestamp_str = iso8601.parse_date(self.timestamp).astimezone(timezone.utc).isoformat()
            else:
                if self.timestamp.tzinfo is None:
                    timestamp_dt = self.timestamp.replace(tzinfo=timezone.utc)
                else:
                    timestamp_dt = self.timestamp.astimezone(timezone.utc)
                timestamp_str = timestamp_dt.isoformat()
        return {
            "id": self.id,
            "timestamp": timestamp_str,
            "duration": float(self.duration),
            "data": safe_json_loads(self.datastr),
        }


def calculate_bucket_hash_key(name, user):
    return hashlib.md5((str(name) + str(user)).encode("utf-8")).hexdigest()


class AuthCacheModel(BaseModel):
    key = AutoField(primary_key=True)
    token = CharField()
    u_hash = CharField()
    expires_at = DateTimeField()
    created = DateTimeField(default=datetime.now)

    class Meta:
        indexes = (
            (("token", "u_hash"), True),  # unique composite index
        )


class Datastore:
    def __init__(
        self,
        testing: bool = False,
        host: str = "localhost",
        port: int = 3306,
        user: str = "root",
        password: str = "",
        database: str = "activitywatch",
        **_: Dict,
    ) -> None:
        self.logger = logger.getChild("Datastore")
        self.bucket_instances: Dict[str, Bucket] = {}

        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database

        self.db = _db
        self.db.init(
            database,
            host=host,
            port=port,
            user=user,
            password=password,
            charset="utf8mb4",
        )
        logger.info(f"Using MySQL database: {database}@{host}:{port}")

        self._create_database_if_not_exists(host, port, user, password, database)
        self.db.connect()

        self.bucket_hash_keys: Dict[str, int] = {}
        UserModel.create_table(safe=True)
        BucketModel.create_table(safe=True)
        EventModel.create_table(safe=True)
        AuthCacheModel.create_table(safe=True)

        self.db.close()
        auto_migrate(host, port, user, password, database)
        self.db.connect()

        self.update_bucket_hash_keys()

    def __repr__(self):
        return f"<Datastore object using MySQL>"

    def _create_database_if_not_exists(self, host, port, user, password, database):
        try:
            import pymysql

            connection = pymysql.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                charset="utf8mb4",
            )
            with connection.cursor() as cursor:
                cursor.execute(
                    f"CREATE DATABASE IF NOT EXISTS `{database}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
                )
            connection.close()
        except Exception as e:
            logger.warning(f"Could not create database '{database}': {e}")

    def update_bucket_hash_keys(self) -> None:
        buckets = BucketModel.select()
        self.bucket_hash_keys = {bucket.hash_key: bucket.key for bucket in buckets}

    def __getitem__(self, bucket_hash_key: str) -> "Bucket":
        if bucket_hash_key not in self.bucket_instances:
            if bucket_hash_key in self.buckets():
                bucket = Bucket(self, bucket_hash_key)
                self.bucket_instances[bucket_hash_key] = bucket
            else:
                self.logger.error(
                    f"Cannot create a Bucket object for {bucket_hash_key} because it doesn't exist in the database"
                )
                raise KeyError
        return self.bucket_instances[bucket_hash_key]

    def create_bucket(
        self,
        bucket_id: str,
        type: str,
        client: str,
        hostname: str,
        created: Optional[datetime] = None,
        name: Optional[str] = None,
        data: Optional[dict] = None,
        user: Optional[int] = None,
    ) -> "Bucket":
        created = created or datetime.now(timezone.utc)
        self.logger.info(f"Creating bucket '{bucket_id}'")
        user_id = user
        BucketModel.create(
            id=bucket_id,
            type=type,
            client=client,
            hostname=hostname,
            created=created.isoformat(),
            name=name,
            datastr=json.dumps(data or {}),
            user=user_id,
            hash_key=calculate_bucket_hash_key(bucket_id, user_id),
        )
        self.update_bucket_hash_keys()
        bucket_hash_key = calculate_bucket_hash_key(bucket_id, user_id)
        return self[bucket_hash_key]

    def update_bucket(self, bucket_hash_key: str, **kwargs):
        self.logger.info(f"Updating bucket '{bucket_hash_key}'")
        if bucket_hash_key in self.bucket_hash_keys:
            bucket = BucketModel.get(BucketModel.key == self.bucket_hash_keys[bucket_hash_key])
            if "type_id" in kwargs and kwargs["type_id"] is not None:
                bucket.type = kwargs["type_id"]
            if "client" in kwargs and kwargs["client"] is not None:
                bucket.client = kwargs["client"]
            if "hostname" in kwargs and kwargs["hostname"] is not None:
                bucket.hostname = kwargs["hostname"]
            if "name" in kwargs and kwargs["name"] is not None:
                bucket.name = kwargs["name"]
            if "data" in kwargs and kwargs["data"] is not None:
                bucket.datastr = json.dumps(kwargs["data"])
            bucket.save()
        else:
            raise ValueError("Bucket did not exist, could not update")

    def delete_bucket(self, bucket_hash_key: str):
        self.logger.info(f"Deleting bucket '{bucket_hash_key}'")
        if bucket_hash_key in self.bucket_instances:
            del self.bucket_instances[bucket_hash_key]
        if bucket_hash_key in self.bucket_hash_keys:
            bucket_key = self.bucket_hash_keys[bucket_hash_key]
            bucket = BucketModel.get(BucketModel.key == bucket_key)
            bucket.delete_instance(recursive=True)
            del self.bucket_hash_keys[bucket_hash_key]

    def buckets(self):
        return {bucket.hash_key: bucket.json() for bucket in BucketModel.select()}

    def bucketsv2(self, users: List[int]):
        return {
            bucket.hash_key: bucket.json()
            for bucket in BucketModel.select().where(BucketModel.user.in_(users))
        }

    def get_user_by_uuid(self, user_uuid: int) -> Optional[dict]:
        try:
            user = UserModel.get(UserModel.uuid == user_uuid)
            return {"user": user.json()}
        except UserModel.DoesNotExist:
            return None

    def update_user(self, uuid, data):
        print(uuid,data)
        try:
            user = UserModel.get(UserModel.uuid == uuid)
            if "team" in data:
                user.team = data["team"]
            if "username" in data:
                user.username = data["username"]
            if "data" in data:
                user.data = json.dumps(data)
            user.save()
        except UserModel.DoesNotExist:
            pass

    def create_user(self, data):
        print(data)
        user = UserModel.create(
           uuid=data["uuid"], username=data["username"], team=data.get("team"), data=json.dumps(data.get("data", {}))
        )
        return user.json()

    def get_users(self):
        return [user.json() for user in UserModel.select()]

    def get_buckets_for_user(self, user):
        buckets: Dict[str, dict] = {}
        for bucket in BucketModel.select().where(BucketModel.user == user):
            buckets[bucket.hash_key] = bucket.json()
        return buckets

    def get_workers(self, team_id):
        users = [user.json() for user in UserModel.select().where(UserModel.team == team_id)]
        for i in range(len(users)):
            users[i].pop("team")
        return users

    def get_buckets_for_users(self, users):
        buckets: Dict[str, dict] = {}
        for bucket in BucketModel.select().where(BucketModel.user.in_(users)):
            buckets[bucket.hash_key] = bucket.json()
        return buckets

    # --- Auth cache helpers ---
    def is_user_authorized(self, token: str, u_hash: str) -> bool:
        try:
            entry = (
                AuthCacheModel
                .select()
                .where((AuthCacheModel.token == token) & (AuthCacheModel.u_hash == u_hash))
                .get()
            )
            return entry.expires_at > datetime.utcnow()
        except AuthCacheModel.DoesNotExist:
            return False

    def set_user_authorized(self, token: str, u_hash: str, ttl_hours: int = 24) -> None:
        expires = datetime.utcnow() + timedelta(hours=ttl_hours)
        try:
            entry = (
                AuthCacheModel
                .select()
                .where((AuthCacheModel.token == token) & (AuthCacheModel.u_hash == u_hash))
                .get()
            )
            entry.expires_at = expires
            entry.save()
        except AuthCacheModel.DoesNotExist:
            AuthCacheModel.create(token=token, u_hash=u_hash, expires_at=expires)

    def get_events_for_buckets(self, bucket_models: List[BucketModel], limit, start, end) -> Dict[str, list]:
        events: Dict[str, list] = {}
        for bm in bucket_models:
            # Ensure key is a real string (not a Peewee CharField descriptor)
            bucket_hash_key: str = str(bm.bucket_hash_key)
            events[bucket_hash_key] = [event.to_json_dict() for event in self[bucket_hash_key].get(limit=limit, starttime=start, endtime=end)]
        return events

    


class Bucket:
    def __init__(self, datastore: "Datastore", bucket_hash_key: str) -> None:
        self.logger = logger.getChild("Bucket")
        self.ds = datastore
        self.bucket_hash_key = bucket_hash_key

    def metadata(self) -> dict:
        if self.bucket_hash_key in self.ds.bucket_hash_keys:
            bucket = BucketModel.get(BucketModel.key == self.ds.bucket_hash_keys[self.bucket_hash_key])
            return bucket.json()
        else:
            raise ValueError("Bucket did not exist")

    def get(
        self,
        limit: int = -1,
        starttime: Optional[datetime] = None,
        endtime: Optional[datetime] = None,
    ) -> List[Event]:

        # any timezone to utc
        def _to_utc(dt: Optional[datetime]):
            if dt is None:
                return None
            if isinstance(dt, str):
                try:
                    dt = iso8601.parse_date(dt)
                except Exception:
                    return None
            if dt.tzinfo is None:
                return dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)

        starttime = _to_utc(starttime)
        endtime = _to_utc(endtime)

        if starttime:
            starttime = starttime.replace(microsecond=1000 * int(starttime.microsecond / 1000))
        if endtime:
            milliseconds = 1 + int(endtime.microsecond / 1000)
            second_offset = int(milliseconds / 1000)
            microseconds = (1000 * milliseconds) % 1000000
            endtime = endtime.replace(microsecond=microseconds) + timedelta(seconds=second_offset)

        if self.bucket_hash_key in self.ds.bucket_hash_keys:
            bucket_key = self.ds.bucket_hash_keys[self.bucket_hash_key]
            query = EventModel.select().where(EventModel.bucket == bucket_key)
            if starttime:
                query = query.where(EventModel.timestamp >= starttime)
            if endtime:
                query = query.where(EventModel.timestamp <= endtime)
            if limit and limit > 0:
                query = query.order_by(EventModel.timestamp.desc()).limit(limit)
            else:
                query = query.order_by(EventModel.timestamp.desc())
            events: List[Event] = []
            for event in query:
                try:
                    json.loads(event.datastr)
                except Exception:
                    print("Invalid event data: ", event.datastr)
                events.append(
                    Event(
                        id=event.id,
                        timestamp=event.timestamp,
                        duration=timedelta(seconds=float(event.duration)),
                        data=safe_json_loads(event.datastr),
                    )
                )
            return events
        else:
            raise ValueError("Bucket did not exist")

    def get_by_id(self, event_id) -> Optional[Event]:
        if self.bucket_hash_key in self.ds.bucket_hash_keys:
            bucket_key = self.ds.bucket_hash_keys[self.bucket_hash_key]
            try:
                event = EventModel.get((EventModel.bucket == bucket_key) & (EventModel.id == event_id))
                return Event(
                    id=event.id,
                    timestamp=event.timestamp,
                    duration=float(event.duration),
                    data=json.loads(event.datastr),
                )
            except EventModel.DoesNotExist:
                return None
        else:
            raise ValueError("Bucket did not exist")

    def get_eventcount(self, starttime: Optional[datetime] = None, endtime: Optional[datetime] = None) -> int:
        if self.bucket_hash_key in self.ds.bucket_hash_keys:
            bucket_key = self.ds.bucket_hash_keys[self.bucket_hash_key]
            query = EventModel.select().where(EventModel.bucket == bucket_key)
            if starttime:
                query = query.where(EventModel.timestamp >= starttime)
            if endtime:
                query = query.where(EventModel.timestamp <= endtime)
            return query.count()
        else:
            raise ValueError("Bucket did not exist")

    def get_last_before_or_equal(self, ts: datetime) -> Optional[Event]:
        if self.bucket_hash_key in self.ds.bucket_hash_keys:
            bucket_key = self.ds.bucket_hash_keys[self.bucket_hash_key]
            try:
                em = (
                    EventModel.select()
                    .where((EventModel.bucket == bucket_key) & (EventModel.timestamp <= ts))
                    .order_by(EventModel.timestamp.desc())
                    .limit(1)
                    .get()
                )
                return Event(
                    id=em.id,
                    timestamp=em.timestamp,
                    duration=timedelta(seconds=float(em.duration)),
                    data=safe_json_loads(em.datastr),
                )
            except EventModel.DoesNotExist:
                return None
        else:
            raise ValueError("Bucket did not exist")

    def get_last_inserted(self) -> Optional[Event]:
        if self.bucket_hash_key in self.ds.bucket_hash_keys:
            bucket_key = self.ds.bucket_hash_keys[self.bucket_hash_key]
            try:
                em = (
                    EventModel.select()
                    .where(EventModel.bucket == bucket_key)
                    .order_by(EventModel.id.desc())
                    .limit(1)
                    .get()
                )
                return Event(
                    id=em.id,
                    timestamp=em.timestamp,
                    duration=timedelta(seconds=float(em.duration)),
                    data=safe_json_loads(em.datastr),
                )
            except EventModel.DoesNotExist:
                return None
        else:
            raise ValueError("Bucket did not exist")

    def insert(self, events: Union[Event, List[Event]]) -> Optional[Event]:
        """
        Inserts one or several events.
        If a single event is inserted, return the event with its id assigned.
        If several events are inserted, returns None. (This is due to there being no efficient way of getting ids out when doing bulk inserts with some datastores such as peewee/SQLite)
        """

        # NOTE: Should we keep the timestamp checking?
        warn_older_event = False

        # Get last event for timestamp check after insert
        if warn_older_event:
            last_event_list = self.get(1)
            last_event = None
            if last_event_list:
                last_event = last_event_list[0]

        now = datetime.now(tz=timezone.utc)

        inserted: Optional[Event] = None

        # Call insert
        if isinstance(events, Event):
            oldest_event: Optional[Event] = events
            if events.timestamp + events.duration > now:
                self.logger.warning(
                    f"Event inserted into bucket {self.bucket_hash_key} reaches into the future. Current UTC time: {str(now)}. Event data: {str(events)}"
                )
            # MySQL insert_one implementation with de-dup by (bucket, timestamp, datastr)
            if self.bucket_hash_key in self.ds.bucket_hash_keys:
                bucket_key = self.ds.bucket_hash_keys[self.bucket_hash_key]
                datastr_value = json.dumps(events.data, ensure_ascii=False)
                existing = (
                    EventModel.select()
                    .where(
                        (EventModel.bucket == bucket_key)
                        & (EventModel.timestamp == events.timestamp)
                        & (EventModel.datastr == datastr_value)
                    )
                    .first()
                )
                if existing:
                    new_duration = events.duration.total_seconds() if hasattr(events.duration, 'total_seconds') else float(events.duration)
                    existing.duration = max(float(existing.duration), new_duration)
                    existing.save()
                    events.id = existing.id
                    event_model = existing
                else:
                    event_model = EventModel.from_event(bucket_key, events)
                    event_model.save()
                    events.id = event_model.id
                inserted = Event(
                    id=event_model.id,
                    timestamp=event_model.timestamp,
                    duration=event_model.duration,
                    data=json.loads(event_model.datastr),
                )
            else:
                raise ValueError("Bucket did not exist")
            # assert inserted
        elif isinstance(events, list):
            if events:
                oldest_event = sorted(events, key=lambda k: k["timestamp"])[0]
            else:  # pragma: no cover
                oldest_event = None
            for event in events:
                if event.timestamp + event.duration > now:
                    self.logger.warning(
                        f"Event inserted into bucket {self.bucket_hash_key} reaches into the future. Current UTC time: {str(now)}. Event data: {str(event)}"
                    )
            # MySQL insert_many implementation
            if self.bucket_hash_key in self.ds.bucket_hash_keys:
                bucket_key = self.ds.bucket_hash_keys[self.bucket_hash_key]
                # Upsert one-by-one to dedup identical (timestamp, datastr)
                for e in events:
                    datastr_value = json.dumps(e.data, ensure_ascii=False)
                    existing = (
                        EventModel.select()
                        .where(
                            (EventModel.bucket == bucket_key)
                            & (EventModel.timestamp == e.timestamp)
                            & (EventModel.datastr == datastr_value)
                        )
                        .first()
                    )
                    if existing:
                        new_duration = e.duration.total_seconds() if hasattr(e.duration, 'total_seconds') else float(e.duration)
                        existing.duration = max(float(existing.duration), new_duration)
                        existing.save()
                        e.id = existing.id
                    else:
                        EventModel.create(
                            bucket=bucket_key,
                            timestamp=e.timestamp,
                            duration=e.duration.total_seconds() if hasattr(e.duration, 'total_seconds') else float(e.duration),
                            datastr=datastr_value,
                        )
            else:
                raise ValueError("Bucket did not exist")
        else:
            raise TypeError

        # Warn if timestamp is older than last event
        if warn_older_event and last_event and oldest_event:
            if oldest_event.timestamp < last_event.timestamp:  # pragma: no cover
                self.logger.warning(
                    f"""Inserting event that has a older timestamp than previous event!
Previous: {last_event}
Inserted: {oldest_event}"""
                )

        return inserted

    def delete(self, event_id):
        if self.bucket_hash_key in self.ds.bucket_hash_keys:
            bucket_key = self.ds.bucket_hash_keys[self.bucket_hash_key]
            try:
                event = EventModel.get((EventModel.bucket == bucket_key) & (EventModel.id == event_id))
                event.delete_instance()
                return True
            except EventModel.DoesNotExist:
                return False
        else:
            raise ValueError("Bucket did not exist")

    def replace_last(self, event):
        if self.bucket_hash_key in self.ds.bucket_hash_keys:
            bucket_key = self.ds.bucket_hash_keys[self.bucket_hash_key]
            try:
                last_event = (
                    EventModel.select()
                    .where(EventModel.bucket == bucket_key)
                    .order_by(EventModel.id.desc())
                    .limit(1)
                    .get()
                )
                # Only extend duration and update data; keep original start timestamp
                new_duration_sec = (
                    event.duration.total_seconds() if hasattr(event.duration, 'total_seconds') else float(event.duration)
                )
                if new_duration_sec < 0:
                    new_duration_sec = 0.0
                last_event.duration = new_duration_sec
                last_event.datastr = json.dumps(event.data, ensure_ascii=False)
                last_event.save()
            except EventModel.DoesNotExist:
                EventModel.create(
                    id=event.id,
                    bucket=bucket_key,
                    timestamp=event.timestamp,
                    duration=event.duration.total_seconds() if hasattr(event.duration, 'total_seconds') else event.duration,
                    datastr=json.dumps(event.data, ensure_ascii=False),
                )
        else:
            raise ValueError("Bucket did not exist")

    def replace(self, event_id, event):
        if self.bucket_hash_key in self.ds.bucket_hash_keys:
            bucket_key = self.ds.bucket_hash_keys[self.bucket_hash_key]
            try:
                event_model = EventModel.get((EventModel.bucket == bucket_key) & (EventModel.id == event_id))
                # Keep original start timestamp; only adjust duration and data
                new_duration_sec = event.duration.total_seconds() if hasattr(event.duration, 'total_seconds') else float(event.duration)
                if new_duration_sec < 0:
                    new_duration_sec = 0.0
                event_model.duration = new_duration_sec
                event_model.datastr = json.dumps(event.data, ensure_ascii=False)
                event_model.save()
                return True
            except EventModel.DoesNotExist:
                return False
        else:
            raise ValueError("Bucket did not exist")
