import datetime
import json
import traceback
from functools import wraps
from threading import Lock
from typing import Dict, List, Union

import iso8601
from colorama import init
from flask import (
    Blueprint,
    current_app,
    jsonify,
    make_response,
    request,
)
from flask_restx import Api, Resource, fields

from aw_core import schema
from aw_core.models import Event
from aw_query.exceptions import QueryException

from . import logger
from .__about__ import __version__
from .api import ServerAPI
from .exceptions import BadRequest, Unauthorized

init(autoreset=True)


def host_header_check(f):
    """
    Protects against DNS rebinding attacks (see https://github.com/ActivityWatch/activitywatch/security/advisories/GHSA-v9fg-6g9j-h4x4)

    Some discussion in Syncthing how they do it: https://github.com/syncthing/syncthing/issues/4819
    """

    @wraps(f)
    def decorator(*args, **kwargs):
        server_host = current_app.config["HOST"]
        req_host = request.headers.get("host", None)
        if server_host == "0.0.0.0":
            logger.warning(
                "Server is listening on 0.0.0.0, host header check is disabled (potential security issue)."
            )
        elif req_host is None:
            return {"message": "host header is missing"}, 400
        else:
            if req_host.split(":")[0] not in ["localhost", "127.0.0.1", server_host]:
                return {"message": f"host header is invalid (was {req_host})"}, 400
        return f(*args, **kwargs)

    return decorator


def missing_fields(required, data):
    missing = []
    for key in required:
        if key not in data:
            missing.append(key)
    return missing


blueprint = Blueprint("api", __name__, url_prefix="/api")

api = Api(
    blueprint,
    doc="/",
    decorators=[host_header_check],
    title="GFP TIM Server API",
    version=__version__.lstrip("v") if __version__.startswith("v") else __version__,
    description=(
        "HTTP API compatible with ActivityWatch clients, with GFP extensions "
        "(MySQL storage, Bronevik-backed manager endpoints under `/api/1/manager/`). "
        "Requires a valid `Host` header matching the bound address (except when bound to 0.0.0.0)."
    ),
    license="MPL-2.0",
    license_url="https://www.mozilla.org/MPL/2.0/",
    contact_email="",
    default="v0",
    default_label="ActivityWatch API v0",
)

# --- JSONSchema-backed models (event, bucket, export) ---
event = api.schema_model("Event", schema.get_json_schema("event"))
bucket = api.schema_model("Bucket", schema.get_json_schema("bucket"))
buckets_export = api.schema_model("Export", schema.get_json_schema("export"))

# --- Explicit Swagger models ---
server_info = api.model(
    "ServerInfo",
    {
        "name": fields.String(description="Server product name", example="GFP TIM Server"),
        "version": fields.String(description="Server version string"),
        "testing": fields.Boolean(description="Whether server runs in --testing mode"),
        "author": fields.String(description="Vendor / author label"),
    },
)

error_msg = api.model(
    "ErrorMessage",
    {"message": fields.String(description="Human-readable error")},
)

buckets_list_post = api.model(
    "BucketsListPost",
    {
        "user": fields.Raw(
            required=True,
            description="Numeric user id whose buckets to list",
        ),
    },
)

create_bucket = api.model(
    "CreateBucket",
    {
        "client": fields.String(required=True, description="Watcher client id"),
        "type": fields.String(required=True, description="Bucket event type"),
        "hostname": fields.String(required=True, description='Machine hostname or "!local"'),
    },
)

create_bucket_with_uuid = api.inherit(
    "CreateBucketWithUuid",
    create_bucket,
    {
        "uuid": fields.String(
            required=True,
            description="External user UUID (must exist in `/api/0/user`)",
        ),
    },
)

update_bucket = api.model(
    "UpdateBucket",
    {
        "client": fields.String(description="Client id"),
        "type": fields.String(description="Bucket type"),
        "hostname": fields.String(description="Hostname"),
        "data": fields.Raw(description="Arbitrary JSON metadata"),
    },
)

query_body = api.model(
    "QueryRequest",
    {
        "timeperiods": fields.List(
            fields.String,
            required=True,
            description='ISO8601 intervals, e.g. ["2024-01-01T00:00:00+00:00/2024-02-01T00:00:00+00:00"]',
        ),
        "query": fields.List(
            fields.String,
            required=True,
            description="aw-query statements (strings)",
        ),
    },
)

v1_bronevik_auth = api.model(
    "V1BronevikAuth",
    {
        "token": fields.String(required=True, description="Bronevik API token"),
        "u_hash": fields.String(required=True, description="Bronevik user hash"),
    },
)

v1_buckets_post = api.inherit(
    "V1ManagerBucketsPost",
    v1_bronevik_auth,
    {
        "users": fields.Raw(
            required=True,
            description='List of numeric user ids, or the string "all"',
        ),
    },
)

v1_events_post = api.inherit(
    "V1ManagerEventsPost",
    v1_bronevik_auth,
    {
        "buckets": fields.List(
            fields.String,
            required=True,
            description="List of bucket hash keys",
        ),
        "limit": fields.Integer(description="Max events per bucket (-1 = all)"),
        "start": fields.String(description="ISO8601 start (optional)"),
        "end": fields.String(description="ISO8601 end (optional)"),
    },
)

v1_events_count_post = api.inherit(
    "V1ManagerEventsCountPost",
    v1_bronevik_auth,
    {
        "buckets": fields.List(
            fields.String,
            required=True,
            description="List of bucket hash keys",
        ),
    },
)

v1_workers_post = api.inherit(
    "V1ManagerWorkersPost",
    v1_bronevik_auth,
    {
        "team_id": fields.String(
            required=False,
            description="Team id; empty = resolve from Bronevik",
        ),
    },
)

v1_error = api.model(
    "V1ErrorResponse",
    {
        "status": fields.String(example="error"),
        "message": fields.String(),
        "errorIn": fields.Raw(description="Present on validation errors"),
    },
)

user_post_body = api.model(
    "UserPost",
    {
        "uuid": fields.String(required=True, description="Stable external user id"),
        "username": fields.String(description="Login / display handle"),
        "firstName": fields.String(description="Given name (also accepted: first_name)"),
        "lastName": fields.String(description="Family name (also accepted: last_name)"),
        "middleName": fields.String(description="Patronymic / middle name (also accepted: middle_name)"),
        "team": fields.List(
            fields.String(),
            description="Team ids (user may belong to several teams)",
        ),
        "email": fields.String(),
        "role_id": fields.String(description="External role id"),
        "client_version": fields.String(description="Client build/version (optional)"),
        "created": fields.String(description="ISO8601 (auto if omitted)"),
        "data": fields.Raw(description="Arbitrary JSON"),
    },
)

user_put_body = api.model(
    "UserPut",
    {
        "uuid": fields.String(required=True),
        "username": fields.String(),
        "firstName": fields.String(),
        "lastName": fields.String(),
        "middleName": fields.String(),
        "team": fields.List(fields.String()),
        "email": fields.String(),
        "role_id": fields.String(),
        "client_version": fields.String(),
        "data": fields.Raw(),
    },
)

delete_success = api.model(
    "DeleteEventResponse",
    {"success": fields.Boolean(description="Whether the row was deleted")},
)

health_ok = api.model("Health", {"status": fields.String(example="ok")})

invitation_row_in = api.model(
    "InvitationCreateRow",
    {
        "email": fields.String(required=True, description="Invitee email"),
        "role_id": fields.String(description="Role id string (managed on Bronevik/other backend)"),
        "firstName": fields.String(description="Preload given name (snake_case first_name also accepted)"),
        "lastName": fields.String(description="Preload family name"),
        "middleName": fields.String(description="Preload middle / patronymic"),
    },
)

v1_invitations_post = api.inherit(
    "V1ManagerInvitationsPost",
    v1_bronevik_auth,
    {
        "team_id": fields.String(
            description="Optional team filter when listing, or team for created batch",
        ),
        "invitations": fields.List(
            fields.Nested(invitation_row_in),
            required=False,
            description=(
                "If omitted or empty array: return invitation list (same POST). "
                "If non-empty: create batch; each row gets a unique installer token."
            ),
        ),
    },
)

invitation_row_out = api.model(
    "InvitationStatus",
    {
        "id": fields.Integer(description="Invitation row id"),
        "token": fields.String(
            description="Secret installer token (Base58); null in list responses — only returned when creating",
        ),
        "team_id": fields.String(),
        "email": fields.String(),
        "role_id": fields.String(description="External role id"),
        "firstName": fields.String(),
        "lastName": fields.String(),
        "middleName": fields.String(),
        "status": fields.String(
            description="pending | claimed | superseded (lost race: email already registered elsewhere)",
        ),
        "installed": fields.Boolean(),
        "installed_at": fields.String(description="ISO8601 UTC when claimed"),
        "superseded_at": fields.String(
            description="ISO8601 UTC when status became superseded (e.g. email taken)",
        ),
        "revoked_at": fields.String(
            description="ISO8601 UTC when revoked (e.g. linked user deleted); row kept for audit",
        ),
        "user_id": fields.Integer(description="Linked user id after install"),
        "user": fields.Raw(
            description=(
                "Full user object when linked (same shape as /manager/workers row for this team "
                "via json_scoped_for_team); null if not yet claimed"
            ),
        ),
        "created": fields.String(description="ISO8601 UTC"),
    },
)

manager_user_update = api.model(
    "ManagerUserUpdate",
    {
        "token": fields.String(required=True),
        "u_hash": fields.String(required=True),
        "team_id": fields.String(
            description=(
                "Team scope for role_id: updates only that membership in user_team_role "
                "(same semantics as POST /manager/workers). Omit only if not sending role_id."
            ),
        ),
        "username": fields.String(),
        "firstName": fields.String(),
        "lastName": fields.String(),
        "middleName": fields.String(),
        "email": fields.String(),
        "role_id": fields.String(
            description="External role id; with team_id changes role only in that team",
        ),
        "team": fields.List(
            fields.String(),
            description="Team ids (replaces stored list when set)",
        ),
        "client_version": fields.String(),
        "data": fields.Raw(description="Replaces user data JSON when set"),
    },
)

claim_invitation_body = api.model(
    "ClaimInvitation",
    {
        "token": fields.String(required=True, description="Invitation token"),
        "uuid": fields.String(
            required=True,
            description="Stable client id (v4); used everywhere for identification — not username",
        ),
        "username": fields.String(
            description=(
                "Optional display label only (ActivityWatch field); not used for authentication. "
                "Omit or empty → filled from invitation FIO or email local-part."
            ),
        ),
    },
)


def copy_doc(api_method):
    """Copy docstring from ServerAPI onto the Resource method for Swagger."""

    def decorator(f):
        f.__doc__ = api_method.__doc__
        return f

    return decorator


def v1_preprocess_headers() -> Union[dict, Dict[str, str]]:
    if request.headers.get("Content-Type") == "application/json":
        data = request.get_json()
    elif request.headers.get("Content-Type") == "application/x-www-form-urlencoded":
        data = request.form.to_dict()
        parsed_data = json.loads(data["data"])
        data = {**data, **parsed_data}
    else:
        return {"status": "error", "message": "Unsupported Content-Type"}
    return data


# ---------------------------------------------------------------------------
# SERVER INFO
# ---------------------------------------------------------------------------


@api.route("/0/info")
class InfoResource(Resource):
    @api.doc(
        "get_server_info",
        tags=["v0"],
        summary="Server metadata",
        description="Returns product name, version, and whether testing mode is active.",
    )
    @api.marshal_with(server_info)
    @api.response(200, "OK")
    @copy_doc(ServerAPI.get_info)
    def get(self) -> Dict[str, Dict]:
        return current_app.api.get_info()


# ---------------------------------------------------------------------------
# BUCKETS
# ---------------------------------------------------------------------------


@api.route("/0/buckets", endpoint="buckets_no_slash")
@api.route("/0/buckets/")
class BucketsResource(Resource):
    @api.doc(
        "list_all_buckets",
        tags=["v0"],
        summary="List all buckets",
        description="Returns every bucket in the datastore with optional last_updated.",
    )
    @api.response(200, "Bucket id → metadata map")
    @copy_doc(ServerAPI.get_buckets)
    def get(self) -> Dict[str, Dict]:
        return current_app.api.get_buckets()

    @api.doc(
        "list_buckets_for_user",
        tags=["v0"],
        summary="List buckets for one user",
        description="POST body must include numeric `user` id.",
    )
    @api.expect(buckets_list_post, validate=True)
    @api.response(200, "Buckets for user")
    def post(self):
        data = request.get_json()
        user = data["user"]
        return current_app.api.get_buckets_for_user(user)


@api.route("/1/manager/buckets/")
class BucketsManagerV1Resource(Resource):
    @api.doc(
        "manager_buckets_v1",
        tags=["v1-manager"],
        summary="Buckets for users (Bronevik auth)",
        description=(
            "Requires valid Bronevik `token` and `u_hash`. "
            "`users` is a list of user ids or `'all'` (see product limits). "
            "Supports JSON or `application/x-www-form-urlencoded` with a `data` JSON field."
        ),
    )
    @api.expect(v1_buckets_post, validate=False)
    @api.response(200, "Success envelope with status and data.buckets")
    @api.response(400, "Bad request", v1_error)
    def post(self):
        data = v1_preprocess_headers()
        if data.get("status") == "error" and data.get("message") == "Unsupported Content-Type":
            return {"status": "error", "message": "Unsupported Content-Type"}
        required = ["users", "token", "u_hash"]
        if not all(key in data for key in required):
            return {"status": "error", "message": "Missing required fields"}
        return current_app.api.get_buckets_v2(data["users"], data["token"], data["u_hash"])


@api.route("/0/buckets/<string:bucket_id>")
class BucketResource(Resource):
    @api.doc(
        "get_bucket_metadata",
        tags=["v0"],
        summary="Get bucket metadata",
        description="`bucket_id` is the bucket **hash key** (MD5), not the logical bucket name.",
        params={"bucket_id": "Bucket hash key"},
    )
    @api.response(200, "Bucket metadata", model=bucket)
    @api.response(404, "Unknown bucket")
    @copy_doc(ServerAPI.get_bucket_metadata)
    def get(self, bucket_id):
        return current_app.api.get_bucket_metadata(bucket_id)

    @api.doc(
        "create_bucket",
        tags=["v0"],
        summary="Create bucket",
        description="Logical bucket id is the URL segment; body ties it to user `uuid`.",
        params={"bucket_id": "Logical bucket id (name)"},
    )
    @api.expect(create_bucket_with_uuid, validate=False)
    @api.response(200, "Created")
    @api.response(304, "Already exists or unknown user")
    @api.response(400, "Missing uuid")
    @copy_doc(ServerAPI.create_bucket)
    def post(self, bucket_id):
        data = request.get_json()
        if "uuid" not in data:
            return {"message": "user is missing"}, 400
        bucket_created = current_app.api.create_bucket(
            bucket_id,
            event_type=data["type"],
            client=data["client"],
            hostname=data["hostname"],
            user=data["uuid"],
        )
        if bucket_created:
            return {}, 200
        else:
            return {}, 304

    @api.doc(
        "update_bucket",
        tags=["v0"],
        summary="Update bucket metadata",
        params={"bucket_id": "Bucket hash key"},
    )
    @api.expect(update_bucket, validate=False)
    @api.response(200, "Updated")
    @copy_doc(ServerAPI.update_bucket)
    def put(self, bucket_id):
        data = request.get_json()
        current_app.api.update_bucket(
            bucket_id,
            event_type=data["type"],
            client=data["client"],
            hostname=data["hostname"],
            data=data["data"],
        )
        return {}, 200

    @api.doc(
        "delete_bucket",
        tags=["v0"],
        summary="Delete bucket",
        description="In production, requires `?force=1` unless `--testing` mode.",
        params={
            "bucket_id": "Bucket hash key",
            "force": "Must be `1` to allow delete outside testing mode",
        },
    )
    @api.response(200, "Deleted")
    @api.response(401, "Unauthorized without force/testing")
    @copy_doc(ServerAPI.delete_bucket)
    def delete(self, bucket_id):
        args = request.args
        if not current_app.api.testing:
            if "force" not in args or args["force"] != "1":
                msg = "Deleting buckets is only permitted if aw-server is running in testing mode or if ?force=1"
                raise Unauthorized("DeleteBucketUnauthorized", msg)

        current_app.api.delete_bucket(bucket_id)
        return {}, 200


# ---------------------------------------------------------------------------
# EVENTS
# ---------------------------------------------------------------------------


@api.route("/0/buckets/<string:bucket_hash_key>/events")
class BucketsEventsV0Resource(Resource):
    @api.doc(
        "list_events",
        tags=["v0"],
        summary="List events in a bucket",
        params={
            "bucket_hash_key": "Bucket hash key",
            "limit": "Max rows (-1 = all)",
            "start": "ISO8601 lower bound (optional)",
            "end": "ISO8601 upper bound (optional)",
        },
    )
    @api.response(200, "List of events")
    @copy_doc(ServerAPI.get_events)
    def get(self, bucket_hash_key):
        args = request.args
        limit = int(args["limit"]) if "limit" in args else -1
        start = iso8601.parse_date(args["start"]) if "start" in args else None
        end = iso8601.parse_date(args["end"]) if "end" in args else None

        events = current_app.api.get_events(
            bucket_hash_key, limit=limit, start=start, end=end
        )
        return events, 200

    @api.doc(
        "create_events",
        tags=["v0"],
        summary="Insert one or more events",
        description="Body is a single event object or an array of events.",
        params={"bucket_hash_key": "Bucket hash key"},
    )
    @api.expect(event, validate=False)
    @api.response(200, "Last inserted event (if single insert)")
    @copy_doc(ServerAPI.create_events)
    def post(self, bucket_hash_key):
        data = request.get_json()
        logger.debug(
            "Received post request for event in bucket '{}' and data: {}".format(
                bucket_hash_key, data
            )
        )

        if isinstance(data, dict):
            events = [Event(**data)]
        elif isinstance(data, list):
            events = [Event(**e) for e in data]
        else:
            raise BadRequest("Invalid POST data", "")

        event = current_app.api.create_events(bucket_hash_key, events)
        return event.to_json_dict() if event else None, 200


@api.route("/1/manager/buckets/events")
class ManagerBucketsEventsV1Resource(Resource):
    @api.doc(
        "manager_events_v1",
        tags=["v1-manager"],
        summary="Fetch events for multiple buckets",
        description="Bronevik-authenticated bulk export.",
    )
    @api.expect(v1_events_post, validate=False)
    @api.response(200, "Success")
    @api.response(400, "Validation / auth error", v1_error)
    def post(self):
        data = v1_preprocess_headers()
        if data.get("status") == "error" and data.get("message") == "Unsupported Content-Type":
            return {"status": "error", "message": "Unsupported Content-Type"}
        required = ["buckets", "token", "u_hash"]
        if not all(key in data for key in required):
            return {"status": "error", "message": "Missing required fields"}
        limit = int(data["limit"]) if "limit" in data else -1
        start = iso8601.parse_date(data["start"]) if "start" in data else None
        end = iso8601.parse_date(data["end"]) if "end" in data else None

        return current_app.api.get_events_for_buckets(
            data["buckets"], limit=limit, start=start, end=end, token=data["token"], u_hash=data["u_hash"]
        )


@api.route("/1/manager/buckets/events/count")
class ManagerBucketsEventCountV1Resource(Resource):
    @api.doc(
        "manager_event_counts_v1",
        tags=["v1-manager"],
        summary="Event counts per bucket hash",
        description="Bronevik-authenticated; returns counts per bucket key.",
    )
    @api.expect(v1_events_count_post, validate=False)
    @api.response(200, "Success")
    def post(self):
        data = v1_preprocess_headers()
        if data.get("status") == "error" and data.get("message") == "Unsupported Content-Type":
            return {"status": "error", "message": "Unsupported Content-Type"}
        required = ["buckets", "token", "u_hash"]
        if not all(key in data for key in required):
            return {"status": "error", "message": "Missing required fields"}

        return current_app.api.get_eventcount_for_buckets(data["buckets"], data["token"], data["u_hash"])


@api.route("/0/buckets/<string:bucket_id>/events/count")
class BucketEventCountV0Resource(Resource):
    @api.doc(
        "event_count",
        tags=["v0"],
        summary="Count events in interval",
        params={
            "bucket_id": "Bucket hash key",
            "start": "ISO8601 (optional)",
            "end": "ISO8601 (optional)",
        },
    )
    @api.response(200, "Integer count")
    @copy_doc(ServerAPI.get_eventcount)
    def get(self, bucket_id):
        args = request.args
        start = iso8601.parse_date(args["start"]) if "start" in args else None
        end = iso8601.parse_date(args["end"]) if "end" in args else None

        events = current_app.api.get_eventcount(bucket_id, start=start, end=end)
        return events, 200


@api.route("/0/buckets/<string:bucket_id>/events/<int:event_id>")
class SingleEventResource(Resource):
    @api.doc(
        "get_event",
        tags=["v0"],
        summary="Get one event by id",
        params={"bucket_id": "Bucket hash key", "event_id": "Database event id"},
    )
    @api.response(200, "Event", model=event)
    @api.response(404, "Not found")
    @copy_doc(ServerAPI.get_event)
    def get(self, bucket_id: str, event_id: int):
        logger.debug(
            f"Received get request for event with id '{event_id}' in bucket '{bucket_id}'"
        )
        event = current_app.api.get_event(bucket_id, event_id)
        if event:
            return event, 200
        else:
            return None, 404

    @api.doc(
        "delete_event",
        tags=["v0"],
        summary="Delete one event",
        params={"bucket_id": "Bucket hash key", "event_id": "Database event id"},
    )
    @api.marshal_with(delete_success)
    @copy_doc(ServerAPI.delete_event)
    def delete(self, bucket_id: str, event_id: int):
        logger.debug(
            "Received delete request for event with id '{}' in bucket '{}'".format(
                event_id, bucket_id
            )
        )
        success = current_app.api.delete_event(bucket_id, event_id)
        return {"success": success}, 200


@api.route("/0/buckets/<string:bucket_id>/heartbeat")
class HeartbeatResource(Resource):
    def __init__(self, *args, **kwargs):
        self.lock = Lock()
        super().__init__(*args, **kwargs)

    @api.doc(
        "heartbeat",
        tags=["v0"],
        summary="Merge or append heartbeat event",
        description=(
            "Watcher heartbeat: body is an Event JSON plus `uuid` (user). "
            "Query param `pulsetime` is required (merge window in seconds)."
        ),
        params={
            "bucket_id": "Logical bucket id (not hash key)",
            "pulsetime": "Seconds; heartbeats with same data within this window merge",
        },
    )
    @api.expect(event, validate=False)
    @api.response(200, "Resulting event")
    @api.response(400, "Missing pulsetime or uuid")
    @copy_doc(ServerAPI.heartbeat)
    def post(self, bucket_id):
        raw = request.get_json()
        uuid = raw.get("uuid", None)
        data = {k: v for k, v in raw.items() if k != "uuid"}
        heartbeat = Event(**data)

        if "pulsetime" in request.args:
            pulsetime = float(request.args["pulsetime"])
        else:
            raise BadRequest("MissingParameter", "Missing required parameter pulsetime")

        if not uuid:
            raise BadRequest("MissingParameter", "Missing required parameter uuid")

        aquired = self.lock.acquire(timeout=1)
        if not aquired:
            logger.warning(
                "Heartbeat lock could not be aquired within a reasonable time, this likely indicates a bug."
            )
        try:
            event = current_app.api.heartbeat(bucket_id, heartbeat, pulsetime, uuid)
        finally:
            self.lock.release()
        return event.to_json_dict(), 200


# ---------------------------------------------------------------------------
# QUERY
# ---------------------------------------------------------------------------


@api.route("/0/query/")
class QueryResource(Resource):
    @api.doc(
        "query_v0",
        tags=["v0"],
        summary="Run aw-query",
        description="Executes query language over the configured time periods.",
        params={"name": "Optional query name (for caching)"},
    )
    @api.expect(query_body, validate=True)
    @api.response(200, "Query result rows")
    @api.response(400, "Query error")
    def post(self):
        name = ""
        if "name" in request.args:
            name = request.args["name"]
        query = request.get_json()
        try:
            result = current_app.api.query2(
                name, query["query"], query["timeperiods"], False
            )
            return jsonify(result)
        except QueryException as qe:
            traceback.print_exc()
            return {"type": type(qe).__name__, "message": str(qe)}, 400


# ---------------------------------------------------------------------------
# EXPORT / IMPORT
# ---------------------------------------------------------------------------


@api.route("/0/export/<string:user_id>")
class ExportAllResource(Resource):
    @api.doc(
        "export_all",
        tags=["v0"],
        summary="Export all buckets for a user",
        description="Returns JSON attachment with all buckets and events.",
        params={"user_id": "Numeric user id"},
    )
    @api.response(200, "application/json attachment")
    @copy_doc(ServerAPI.export_all)
    def get(self, user_id):
        buckets_export = current_app.api.export_all(user_id)
        payload = {"buckets": buckets_export}
        response = make_response(json.dumps(payload, ensure_ascii=False))
        filename = "aw-buckets-export.json"
        response.headers["Content-Disposition"] = "attachment; filename={}".format(
            filename
        )
        return response


@api.route("/0/buckets/<string:bucket_id>/export")
class BucketExportResource(Resource):
    @api.doc(
        "export_bucket",
        tags=["v0"],
        summary="Export one bucket",
        params={"bucket_id": "Bucket hash key"},
    )
    @api.response(200, "application/json attachment")
    @copy_doc(ServerAPI.export_bucket)
    def get(self, bucket_id):
        bucket_export = current_app.api.export_bucket(bucket_id)
        payload = {"buckets": {bucket_export["hash_key"]: bucket_export}}
        response = make_response(json.dumps(payload))
        filename = "aw-bucket-export_{}.json".format(bucket_export["id"])
        response.headers["Content-Disposition"] = "attachment; filename={}".format(
            filename
        )
        return response


@api.route("/0/import")
class ImportAllResource(Resource):
    @api.doc(
        "import_all",
        tags=["v0"],
        summary="Import buckets from export JSON",
        description="Accepts JSON body or multipart file upload (`buckets` root key).",
    )
    @api.expect(buckets_export, validate=False)
    @api.response(200, "Imported")
    @copy_doc(ServerAPI.import_all)
    def post(self):
        if len(request.files) > 0:
            for filename, f in request.files.items():
                buckets = json.loads(f.stream.read())["buckets"]
                current_app.api.import_all(buckets)
        else:
            buckets = request.get_json()["buckets"]
            current_app.api.import_all(buckets)
        return None, 200


# ---------------------------------------------------------------------------
# LOGGING / SETTINGS
# ---------------------------------------------------------------------------


@api.route("/0/log")
class LogResource(Resource):
    @api.doc(
        "server_log",
        tags=["v0"],
        summary="Tail server log (JSON lines)",
    )
    @api.response(200, "JSON lines")
    @copy_doc(ServerAPI.get_log)
    def get(self):
        return current_app.api.get_log(), 200


@api.route("/0/settings", defaults={"key": ""})
@api.route("/0/settings/<string:key>")
class SettingsResource(Resource):
    @api.doc(
        "get_settings",
        tags=["v0"],
        summary="Read settings object or one key",
        params={"key": "Setting key (empty = all)"},
    )
    def get(self, key: str):
        data = current_app.api.get_setting(key)
        return jsonify(data)

    @api.doc(
        "set_settings",
        tags=["v0"],
        summary="Write setting value",
        params={"key": "Setting key (required in path)"},
    )
    @api.response(200, "Stored value")
    def post(self, key: str):
        if not key:
            raise BadRequest("MissingParameter", "Missing required parameter key")
        data = current_app.api.set_setting(key, request.get_json())
        return data


# ---------------------------------------------------------------------------
# GFP EXTENSIONS
# ---------------------------------------------------------------------------


@api.route("/0/gfps/bucket/upload")
class GFPBucketUpload(Resource):
    @api.doc(
        "gfps_bucket_upload",
        tags=["gfp"],
        summary="Placeholder upload endpoint",
    )
    @api.response(200, "Stub OK")
    def post(self) -> Dict[str, Dict]:
        logger.debug("GFP bucket upload stub invoked")
        return {"status": "ok"}

    def get(self) -> Dict[str, Dict]:
        return {"WARNING": "TEST ENDPOINT"}


@api.route("/0/user")
class UserResource(Resource):
    @api.doc(
        "user_post",
        tags=["gfp"],
        summary="Register or check user by uuid",
        description="If uuid exists → ok; else create user (no `id` in body).",
    )
    @api.expect(user_post_body, validate=False)
    def post(self):
        data = request.get_json()
        if "uuid" in data:
            user = current_app.api.get_user_by_uuid(data["uuid"])
            if user:
                return {"status": "ok"}, 200
            else:
                if "id" in data:
                    return {"status": "error",
                            "error": "id is not allowed in POST request"}, 200
                if "created" not in data:
                    data["created"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
                if "data" not in data:
                    data["data"] = {}
                user = current_app.api.create_user(data)
                if isinstance(user, dict) and user.get("error") == "email_taken":
                    return {
                        "status": "error",
                        "message": "email already in use",
                        "error": "email_taken",
                    }, 409
                return {"status": "ok"}, 200

    @api.doc(
        "user_put",
        tags=["gfp"],
        summary="Update user by uuid (incl. client_version)",
        description=(
            "Public GFP endpoint (no Bronevik). Body must include `uuid`. "
            "Send `client_version` (string, e.g. app semver/build) for periodic version reporting; "
            "other optional fields match UserPut model."
        ),
    )
    @api.expect(user_put_body, validate=False)
    def put(self):
        data = request.get_json()
        if "uuid" not in data:
            return {"error": "uuid is missing"}
        uuid = data["uuid"]
        err = current_app.api.update_user(uuid, data)
        if isinstance(err, dict) and err.get("error") == "email_taken":
            return {
                "status": "error",
                "message": "email already in use",
                "error": "email_taken",
            }, 409
        return {"status": "ok"}, 200


@api.route("/0/user/<string:user_uuid>")
class UserByUuidResource(Resource):
    @api.doc(
        "user_get",
        tags=["gfp"],
        summary="Get user by uuid",
        params={"user_uuid": "External user uuid"},
    )
    def get(self, user_uuid):
        user = current_app.api.get_user_by_uuid(user_uuid)
        if user:
            return user
        else:
            return {"error": "User not found"}


@api.route("/0/users")
class UsersListResource(Resource):
    @api.doc(
        "users_list",
        tags=["gfp"],
        summary="List all users",
    )
    def get(self):
        users = current_app.api.get_users()
        return users


@api.route("/0/status")
class HealthResource(Resource):
    @api.doc(
        "health",
        tags=["gfp"],
        summary="Liveness probe",
    )
    @api.marshal_with(health_ok)
    def get(self):
        return {"status": "ok"}


@api.route("/1/manager/workers")
class ManagerWorkersV1Resource(Resource):
    @api.doc(
        "manager_workers_v1",
        tags=["v1-manager"],
        summary="List workers in a team",
        description=(
            "Bronevik auth; optional `team_id` or resolve from API. "
            "Each worker object includes `team` and `role_id` only for that team — "
            "memberships in other teams are not exposed."
        ),
    )
    @api.expect(v1_workers_post, validate=False)
    @api.response(200, "Success")
    @api.response(400, "Error")
    def post(self):
        data = v1_preprocess_headers()
        if data.get("status") == "error" and data.get("message") == "Unsupported Content-Type":
            return {"status": "error", "message": "Unsupported Content-Type"}
        if "token" not in data or "u_hash" not in data:
            return {"error": f"fields {missing_fields(['token', 'u_hash'], data)} are missing"}

        return current_app.api.get_workers(data["token"], data["u_hash"], str(data.get("team_id", "")))


# ---------------------------------------------------------------------------
# INVITATIONS & MANAGER USERS (GFP)
# ---------------------------------------------------------------------------


def _invitations_post_is_list(data: dict) -> bool:
    """List when `invitations` is absent, null, or empty list."""
    inv = data.get("invitations")
    if inv is None:
        return True
    if isinstance(inv, list) and len(inv) == 0:
        return True
    return False


@api.route("/1/manager/invitations")
class ManagerInvitationsV1Resource(Resource):
    @api.doc(
        "manager_invitations_post",
        tags=["v1-manager"],
        summary="List or create invitations",
        description=(
            "Bronevik auth (`token`, `u_hash` in JSON). "
            "If `invitations` is omitted or an empty array, returns the invitation list "
            "(optional `team_id` filter). "
            "If `invitations` is a non-empty array, creates a batch; optional `team_id` applies to new rows."
        ),
    )
    @api.expect(v1_invitations_post, validate=False)
    @api.response(200, "Success")
    @api.response(401, "Unauthorized", v1_error)
    def post(self):
        data = v1_preprocess_headers()
        if data.get("status") == "error" and data.get("message") == "Unsupported Content-Type":
            return {"status": "error", "message": "Unsupported Content-Type"}, 400
        if "token" not in data or "u_hash" not in data:
            return {"status": "error", "message": "token and u_hash required"}, 400
        if _invitations_post_is_list(data):
            return current_app.api.manager_list_invitations(
                data.get("team_id"),
                data["token"],
                data["u_hash"],
            )
        inv = data["invitations"]
        if not isinstance(inv, list):
            return {"status": "error", "message": "invitations must be an array"}, 400
        return current_app.api.manager_create_invitations(
            inv,
            data.get("team_id"),
            data["token"],
            data["u_hash"],
        )


@api.route("/1/manager/users/<int:user_id>")
class ManagerUserByIdV1Resource(Resource):
    @api.doc(
        "manager_user_update",
        tags=["v1-manager"],
        summary="Update employee (manager)",
        description=(
            "Bronevik auth. Updates profile fields; does not change uuid. "
            "To change the employee's role in a team, send `role_id` together with `team_id` "
            "(only that team's membership is updated; other teams unchanged). "
            "If you send `role_id` without `team_id` or `team`, the request is rejected."
        ),
    )
    @api.expect(manager_user_update, validate=False)
    @api.response(200, "Success")
    @api.response(404, "User not found")
    def put(self, user_id: int):
        data = v1_preprocess_headers()
        if data.get("status") == "error" and data.get("message") == "Unsupported Content-Type":
            return {"status": "error", "message": "Unsupported Content-Type"}, 400
        if "token" not in data or "u_hash" not in data:
            return {"status": "error", "message": "Missing token or u_hash"}, 400
        body = {k: data[k] for k in data if k not in ("token", "u_hash")}
        return current_app.api.manager_update_user(user_id, body, data["token"], data["u_hash"])

    @api.doc(
        "manager_user_delete",
        tags=["v1-manager"],
        summary="Delete employee and their buckets",
        description=(
            "Bronevik auth: `token` and `u_hash` in the JSON body (same as PUT on this path). "
            "Removes buckets, events, invitation links."
        ),
    )
    @api.expect(v1_bronevik_auth, validate=False)
    @api.response(200, "Deleted")
    def delete(self, user_id: int):
        data = v1_preprocess_headers()
        if data.get("status") == "error" and data.get("message") == "Unsupported Content-Type":
            return {"status": "error", "message": "Unsupported Content-Type"}, 400
        if not isinstance(data, dict):
            return {"status": "error", "message": "JSON body required"}, 400
        if "token" not in data or "u_hash" not in data:
            return {"status": "error", "message": "Missing token or u_hash"}, 400
        return current_app.api.manager_delete_user(user_id, data["token"], data["u_hash"])


@api.route("/0/gfps/invitations/claim")
class ClaimInvitationResource(Resource):
    @api.doc(
        "claim_invitation",
        tags=["gfp"],
        summary="Complete invitation after install",
        description=(
            "Public (no Bronevik). Creates user from invitation preload, binds uuid, "
            "marks invitation installed. Call once per client."
        ),
    )
    @api.expect(claim_invitation_body, validate=False)
    @api.response(200, "Success")
    @api.response(400, "Invalid or already used")
    def post(self):
        data = request.get_json() or {}
        for k in ("token", "uuid"):
            if k not in data:
                return {"status": "error", "message": f"missing {k}"}, 400
        return current_app.api.claim_invitation(
            data["token"],
            data["uuid"],
            data.get("username") or "",
        )
