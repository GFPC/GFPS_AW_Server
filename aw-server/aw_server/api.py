import functools
import hashlib
import json
import logging
from datetime import datetime
from pathlib import Path
from socket import gethostname
from typing import (
    Any,
    Dict,
    List,
    Optional,
)
from uuid import uuid4

import iso8601
from aw_core.dirs import get_data_dir
from aw_core.log import get_log_file_path
from aw_core.models import Event
from aw_query import query2
from aw_transform import heartbeat_merge

from .__about__ import __version__
from .bronevik_api.api import GetBroveikUserProfile, GetBroveikUserTeam
from .exceptions import NotFound
from .settings import Settings

logger = logging.getLogger(__name__)


def get_device_id() -> str:
    path = Path(get_data_dir("aw-server")) / "device_id"
    if path.exists():
        with open(path) as f:
            return f.read()
    else:
        uuid = str(uuid4())
        with open(path, "w") as f:
            f.write(uuid)
        return uuid


def check_bucket_exists(f):
    @functools.wraps(f)
    def g(self, bucket_hash_key, *args, **kwargs):
        if bucket_hash_key not in self.db.buckets():
            raise NotFound("NoSuchBucket", f"There's no bucket with hash key {bucket_hash_key}")
        return f(self, bucket_hash_key, *args, **kwargs)

    return g

def check_bucket_exists_over_hash(self,bucket_id, uuid):
    user_id = self.db.get_user_by_uuid(uuid)
    if user_id == {}:
        raise NotFound("NoSuchUser", f"There's no user with uuid {uuid}")
    user_id = user_id["user"]["id"]
    bucket_hash_key = hashlib.md5((str(bucket_id) + str(user_id)).encode("utf-8")).hexdigest()
    if bucket_hash_key not in self.db.buckets():
        raise NotFound("NoSuchBucket", f"There's no bucket with hash key {bucket_hash_key}")
    return True
class ServerAPI:
    def __init__(self, db, testing, bronevik_url) -> None:
        self.db = db
        self.settings = Settings(testing)
        self.testing = testing
        self.last_event = {}  # type: dict
        self.bronevik_url = bronevik_url

        print("INIT BRONEVIK URL", self.bronevik_url)

    def get_info(self) -> Dict[str, Any]:
        """Get server info"""
        payload = {
            "hostname": gethostname(),
            "version": __version__,
            "testing": self.testing,
            "device_id": get_device_id(),
        }
        return payload

    def get_buckets(self) -> Dict[str, Dict]:
        """Get dict {bucket_name: Bucket} of all buckets"""
        logger.debug("Received get request for buckets")
        buckets = self.db.buckets()
        for b in buckets:
            # TODO: Move this code to aw-core?
            last_events = self.db[b].get(limit=1)
            if len(last_events) > 0:
                last_event = last_events[0]
                last_updated = last_event.timestamp + last_event.duration
                buckets[b]["last_updated"] = last_updated.isoformat()
        return buckets

    @check_bucket_exists
    def get_bucket_metadata(self, bucket_id: str) -> Dict[str, Any]:
        """Get metadata about bucket."""
        bucket = self.db[bucket_id]
        return bucket.metadata()

    @check_bucket_exists
    def export_bucket(self, bucket_id: str) -> Dict[str, Any]:
        """Export a bucket to a dataformat consistent across versions, including all events in it."""
        bucket = self.get_bucket_metadata(bucket_id)
        bucket["user"] = bucket["user"].json()["id"]
        bucket["events"] = self.get_events(bucket_id, limit=-1)
        # Scrub event IDs
        for event in bucket["events"]:
            del event["id"]
        return bucket

    def export_all(self, user_id) -> Dict[str, Any]:
        """Exports all buckets and their events to a format consistent across versions"""
        buckets = self.get_buckets_for_user(user_id)
        exported_buckets = {}
        for bid in buckets.keys():
            exported_buckets[bid] = self.export_bucket(bid)
            exported_buckets[bid]["user"] = buckets[bid]["user"]
        return exported_buckets

    def import_bucket(self, bucket_data: Any):
        bucket_id = bucket_data["id"]
        bucket_hash_key = bucket_data["hash_key"]
        logger.info(f"Importing bucket {bucket_id}")

        if not self.db[bucket_hash_key]:
            self.db.create_bucket(
                bucket_id,
                type=bucket_data["type"],
                client=bucket_data["client"],
                hostname=bucket_data["hostname"],
                created=(
                    bucket_data["created"]
                    if isinstance(bucket_data["created"], datetime)
                    else iso8601.parse_date(bucket_data["created"])
                ),
                user=bucket_data["user"],
            )

        # scrub IDs from events
        # (otherwise causes weird bugs with no events seemingly imported when importing events exported from aw-server-rust, which contains IDs)
        for event in bucket_data["events"]:
            if "id" in event:
                del event["id"]

        self.create_events(
            bucket_hash_key,
            [Event(**e) if isinstance(e, dict) else e for e in bucket_data["events"]],
        )

    def import_all(self, buckets: Dict[str, Any]):
        for b_hash_key, bucket in buckets.items():
            self.import_bucket(bucket)

    def create_bucket(
        self,
        bucket_id: str,
        event_type: str,
        client: str,
        hostname: str,
        created: Optional[datetime] = None,
        data: Optional[Dict[str, Any]] = None,
        user: Optional[int] = None
    ) -> bool:
        """
        Create a bucket.

        If hostname is "!local", the hostname and device_id will be set from the server info.
        This is useful for watchers which are known/assumed to run locally but might not know their hostname (like aw-watcher-web).

        Returns True if successful, otherwise false if a bucket with the given ID already existed.
        """
        user_id = self.db.get_user_by_uuid(user)
        if user_id == {}:
            return False
        user_id = user_id["user"]["id"]
        bucket_hash_key = hashlib.md5((str(bucket_id)+str(user_id)).encode("utf-8")).hexdigest()
        if bucket_hash_key in self.db.buckets():
            return False

        if created is None:
            created = datetime.now()
        if bucket_id in self.db.buckets():
            return False
        if hostname == "!local":
            info = self.get_info()
            if data is None:
                data = {}
            hostname = info["hostname"]
            data["device_id"] = info["device_id"]
        self.db.create_bucket(
            bucket_id,
            type=event_type,
            client=client,
            hostname=hostname,
            created=created,
            data=data,
            user=user_id
        )
        return True

    @check_bucket_exists
    def update_bucket(
        self,
        bucket_hash_key: str,
        event_type: Optional[str] = None,
        client: Optional[str] = None,
        hostname: Optional[str] = None,
        data: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Update bucket metadata"""
        self.db.update_bucket(
            bucket_hash_key=bucket_hash_key,
            type=event_type,
            client=client,
            hostname=hostname,
            data=data,
        )
        return None

    @check_bucket_exists
    def delete_bucket(self, bucket_hash_key: str) -> None:
        """Delete a bucket"""
        self.db.delete_bucket(bucket_hash_key)
        logger.debug(f"Deleted bucket '{bucket_hash_key}'")
        return None

    @check_bucket_exists
    def get_event(
        self,
        bucket_id: str,
        event_id: int,
    ) -> Optional[Event]:
        """Get a single event from a bucket"""
        logger.debug(
            f"Received get request for event {event_id} in bucket '{bucket_id}'"
        )
        event = self.db[bucket_id].get_by_id(event_id)
        return event.to_json_dict() if event else None

    @check_bucket_exists
    def get_events(
        self,
        bucket_hash_key: str,
        limit: int = -1,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
    ) -> List[Event]:
        """Get events from a bucket"""
        logger.debug(f"Received get request for events in bucket '{bucket_hash_key}'")
        if limit is None:  # Let limit = None also mean "no limit"
            limit = -1
        events = [
            event.to_json_dict() for event in self.db[bucket_hash_key].get(limit, start, end)
        ]
        return events

    @check_bucket_exists
    def create_events(self, bucket_hash_key: str, events: List[Event]) -> Optional[Event]:
        """Create events for a bucket. Can handle both single events and multiple ones.

        Returns the inserted event when a single event was inserted, otherwise None."""
        return self.db[bucket_hash_key].insert(events)

    @check_bucket_exists
    def get_eventcount(
        self,
        bucket_id: str,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
    ) -> int:
        """Get eventcount from a bucket"""
        logger.debug(f"Received get request for eventcount in bucket '{bucket_id}'")
        return self.db[bucket_id].get_eventcount(start, end)

    @check_bucket_exists
    def delete_event(self, bucket_id: str, event_id) -> bool:
        """Delete a single event from a bucket"""
        return self.db[bucket_id].delete(event_id)

    #@check_bucket_exists
    def heartbeat(self, bucket_id: str, heartbeat: Event, pulsetime: float, uuid: str) -> Event:
        check_bucket_exists_over_hash(self, bucket_id, uuid)
        """
        Heartbeats are useful when implementing watchers that simply keep
        track of a state, how long it's in that state and when it changes.
        A single heartbeat always has a duration of zero.

        If the heartbeat was identical to the last (apart from timestamp), then the last event has its duration updated.
        If the heartbeat differed, then a new event is created.

        Such as:
         - Active application and window title
           - Example: aw-watcher-window
         - Currently open document/browser tab/playing song
           - Example: wakatime
           - Example: aw-watcher-web
           - Example: aw-watcher-spotify
         - Is the user active/inactive?
           Send an event on some interval indicating if the user is active or not.
           - Example: aw-watcher-afk

        Inspired by: https://wakatime.com/developers#heartbeats
        """
        logger.debug(
            "Received heartbeat in bucket '{}'\n\ttimestamp: {}, duration: {}, pulsetime: {}\n\tdata: {}".format(
                bucket_id,
                heartbeat.timestamp,
                heartbeat.duration,
                pulsetime,
                heartbeat.data,
            )
        )

        # The endtime here is set such that in the event that the heartbeat is older than an
        # existing event we should try to merge it with the last event before the heartbeat instead.
        # FIXME: This (the endtime=heartbeat.timestamp) gets rid of the "heartbeat was older than last event"
        #        warning and also causes a already existing "newer" event to be overwritten in the
        #        replace_last call below. This is problematic.
        # Solution: This could be solved if we were able to replace arbitrary events.
        #           That way we could double check that the event has been applied
        #           and if it hasn't we simply replace it with the updated counterpart.

        user_id = self.db.get_user_by_uuid(uuid)["user"]["id"]
        bucket_hash_key = hashlib.md5((str(bucket_id) + str(user_id)).encode("utf-8")).hexdigest()

        last_event = None
        if bucket_hash_key not in self.last_event:
            last_events = self.db[bucket_hash_key].get(limit=1)
            if len(last_events) > 0:
                last_event = last_events[0]
        else:
            last_event = self.last_event[bucket_hash_key]
        if last_event:
            if last_event.data == heartbeat.data:
                merged = heartbeat_merge(last_event, heartbeat, pulsetime)
                if merged is not None:
                    # Heartbeat was merged into last_event
                    logger.debug(
                        "Received valid heartbeat, merging. (bucket: {})".format(
                            bucket_hash_key
                        )
                    )
                    self.last_event[bucket_hash_key] = merged
                    try:
                        self.db[bucket_hash_key].replace_last(merged)
                    except:
                        logger.error("Could not replace last event in bucket: {}".format(bucket_hash_key))
                    return merged
                else:
                    logger.info(
                        "Received heartbeat after pulse window, inserting as new event. (bucket: {})".format(
                            bucket_hash_key
                        )
                    )
            else:
                logger.debug(
                    "Received heartbeat with differing data, inserting as new event. (bucket: {})".format(
                        bucket_hash_key
                    )
                )
        else:
            logger.info(
                "Received heartbeat, but bucket was previously empty, inserting as new event. (bucket: {})".format(
                    bucket_hash_key
                )
            )
        self.db[bucket_hash_key].insert(heartbeat)
        self.last_event[bucket_hash_key] = heartbeat
        return heartbeat

    def query2(self, name, query, timeperiods, cache):
        result = []
        for timeperiod in timeperiods:
            period = timeperiod.split("/")[
                :2
            ]  # iso8601 timeperiods are separated by a slash
            starttime = iso8601.parse_date(period[0])
            endtime = iso8601.parse_date(period[1])
            query = "".join(query)
            result.append(query2.query(name, query, starttime, endtime, self.db))
        return result

    # TODO: Right now the log format on disk has to be JSON, this is hard to read by humans...
    def get_log(self):
        """Get the server log in json format"""
        payload = []
        with open(get_log_file_path()) as log_file:
            for line in log_file.readlines()[::-1]:
                payload.append(json.loads(line))
        return payload, 200

    def get_setting(self, key):
        """Get a setting"""
        return self.settings.get(key, None)

    def set_setting(self, key, value):
        """Set a setting"""
        self.settings[key] = value
        return value

    def get_user_by_uuid(self,uuid):
        return self.db.get_user_by_uuid(uuid)

    def update_user(self, uuid, data):
        return self.db.update_user(uuid, data)

    def create_user(self, data):
        return self.db.create_user(data)

    def get_users(self):
        return self.db.get_users()

    def get_buckets_for_user(self, user):
        buckets = self.db.get_buckets_for_user(user)
        for b in buckets:
            # TODO: Move this code to aw-core?
            last_events = self.db[b].get(limit=1)
            if len(last_events) > 0:
                last_event = last_events[0]
                last_updated = last_event.timestamp + last_event.duration
                buckets[b]["last_updated"] = last_updated.isoformat()
            buckets[b]['user'] = buckets[b]['user'].json()['id']
        print(buckets)

        return buckets

    def get_team_members(self, token, u_hash, team_id):

        requesting_profile = GetBroveikUserProfile(self.bronevik_url, token, u_hash)
        #print(json.dumps(requesting_profile, indent=4))
        if requesting_profile["status"] != "success":
            return {"status": "error", "message": "unauthorized access"}
        team = GetBroveikUserTeam(self.bronevik_url, token, u_hash)
        print(json.dumps(team, indent=4))


        return {
            "status": "success",
            "members": self.db.get_team_members(team_id),
        }