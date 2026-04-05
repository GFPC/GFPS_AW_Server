import hashlib
import json
import logging
import secrets
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set, Union

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


def normalize_team_value(val: Any) -> Optional[List[str]]:
    """Build a list of team ids for JSON storage; None = omit / not set for partial updates."""
    if val is None:
        return None
    if isinstance(val, list):
        seen: Set[str] = set()
        out: List[str] = []
        for x in val:
            if x is None:
                continue
            s = str(x).strip()
            if not s or s in seen:
                continue
            seen.add(s)
            out.append(s)
        return out
    if isinstance(val, str):
        s = val.strip()
        return [s] if s else None
    s = str(val).strip()
    return [s] if s else None


def pick_optional_str(d: Dict[str, Any], *keys: str) -> Optional[str]:
    """First non-empty string among keys (camelCase or snake_case)."""
    for k in keys:
        if k not in d:
            continue
        v = d[k]
        if v is None:
            continue
        s = str(v).strip()
        if s:
            return s
    return None


def strip_or_none(val: Any) -> Optional[str]:
    if val is None:
        return None
    s = str(val).strip()
    return s if s else None


def normalize_email_for_uniqueness(val: Any) -> Optional[str]:
    """Lowercase trimmed email for storage and uniqueness; empty → None."""
    if val is None:
        return None
    s = str(val).strip().lower()
    return s if s else None


def _is_email_unique_violation(exc: BaseException) -> bool:
    """True only for duplicate email (1062 + '@' in message); avoids masking uuid/other keys."""
    msg = str(exc)
    if "Duplicate entry" not in msg or "@" not in msg:
        return False
    if isinstance(exc, IntegrityError):
        return True
    try:
        import pymysql.err

        if isinstance(exc, pymysql.err.IntegrityError):
            return True
    except ImportError:
        pass
    args = getattr(exc, "args", ())
    return bool(args) and args[0] == 1062


def person_name_updates_from_payload(data: Dict[str, Any]) -> Dict[str, Optional[str]]:
    """Partial update: only keys present in payload (either camelCase or snake_case)."""
    out: Dict[str, Optional[str]] = {}
    if any(k in data for k in ("firstName", "first_name")):
        v = data.get("firstName", data.get("first_name"))
        out["first_name"] = None if v is None else strip_or_none(v)
    if any(k in data for k in ("lastName", "last_name")):
        v = data.get("lastName", data.get("last_name"))
        out["last_name"] = None if v is None else strip_or_none(v)
    if any(k in data for k in ("middleName", "middle_name")):
        v = data.get("middleName", data.get("middle_name"))
        out["middle_name"] = None if v is None else strip_or_none(v)
    return out


def person_names_for_create(data: Dict[str, Any]) -> Dict[str, Optional[str]]:
    return {
        "first_name": strip_or_none(data.get("firstName", data.get("first_name"))),
        "last_name": strip_or_none(data.get("lastName", data.get("last_name"))),
        "middle_name": strip_or_none(data.get("middleName", data.get("middle_name"))),
    }


from aw_core.models import Event
from playhouse.migrate import MySQLMigrator, migrate
from playhouse.mysql_ext import JSONField, MySQLDatabase
from peewee import (
    AutoField,
    BooleanField,
    CharField,
    DateTimeField,
    DecimalField,
    ForeignKeyField,
    IntegerField,
    IntegrityError,
    SQL,
    TextField,
    Model,
    fn,
)
from playhouse.shortcuts import prefetch

logger = logging.getLogger(__name__)

_db = MySQLDatabase(None)

from .invitation_tokens import (  # noqa: E402  — after logger, uses stdlib + base58 only
    generate_invitation_secret,
    invitation_token_hash_from_client,
)


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

    # UserModel: email, role_id, client_version
    try:
        info_u = db.execute_sql("DESCRIBE usermodel")
        ucols = {row[0] for row in info_u}
    except Exception:
        ucols = set()
    with db.atomic():
        if "email" not in ucols:
            migrate(migrator.add_column("usermodel", "email", CharField(null=True)))
        if "role_id" not in ucols:
            migrate(migrator.add_column("usermodel", "role_id", CharField(null=True)))
        if "client_version" not in ucols:
            migrate(migrator.add_column("usermodel", "client_version", CharField(null=True)))
        if "first_name" not in ucols:
            migrate(migrator.add_column("usermodel", "first_name", CharField(null=True)))
        if "last_name" not in ucols:
            migrate(migrator.add_column("usermodel", "last_name", CharField(null=True)))
        if "middle_name" not in ucols:
            migrate(migrator.add_column("usermodel", "middle_name", CharField(null=True)))

    # team: legacy VARCHAR -> JSON array of team id strings
    try:
        team_rows = list(db.execute_sql("SHOW COLUMNS FROM usermodel WHERE Field = 'team'"))
    except Exception:
        team_rows = []
    if team_rows:
        col_type = team_rows[0][1].lower()
        if "json" not in col_type:
            with db.atomic():
                db.execute_sql("ALTER TABLE `usermodel` ADD COLUMN `team_json` JSON NULL")
                db.execute_sql(
                    """
                    UPDATE `usermodel` SET `team_json` = CASE
                        WHEN `team` IS NULL OR `team` = '' THEN NULL
                        ELSE JSON_ARRAY(`team`)
                    END
                    """
                )
                db.execute_sql("ALTER TABLE `usermodel` DROP COLUMN `team`")
                db.execute_sql("ALTER TABLE `usermodel` CHANGE `team_json` `team` JSON NULL")

    try:
        info_inv = db.execute_sql("DESCRIBE invitationmodel")
        icols = {row[0] for row in info_inv}
    except Exception:
        icols = set()
    if icols:
        if "middle_name" not in icols:
            with db.atomic():
                migrate(migrator.add_column("invitationmodel", "middle_name", CharField(null=True)))
        if "token_hash" not in icols:
            with db.atomic():
                migrate(
                    migrator.add_column(
                        "invitationmodel",
                        "token_hash",
                        CharField(null=True, max_length=64),
                    )
                )
        if "token" in icols:
            rows = list(
                db.execute_sql(
                    "SELECT `id`, `token` FROM `invitationmodel` "
                    "WHERE `token` IS NOT NULL AND `token` != ''"
                )
            )
            for row in rows:
                rid, tok = row[0], row[1]
                if isinstance(tok, bytes):
                    tok = tok.decode("utf-8", errors="replace")
                th = hashlib.sha256(tok.encode("utf-8")).hexdigest()
                db.execute_sql(
                    "UPDATE `invitationmodel` SET `token_hash` = %s WHERE `id` = %s",
                    (th, rid),
                )
            with db.atomic():
                migrate(migrator.drop_column("invitationmodel", "token"))

    try:
        info_inv2 = db.execute_sql("DESCRIBE invitationmodel")
        icols2 = {row[0] for row in info_inv2}
    except Exception:
        icols2 = set()
    if icols2:
        if "status" not in icols2:
            with db.atomic():
                migrate(
                    migrator.add_column(
                        "invitationmodel",
                        "status",
                        CharField(max_length=32, default="pending"),
                    )
                )
                migrate(
                    migrator.add_column(
                        "invitationmodel",
                        "superseded_at",
                        DateTimeField(null=True),
                    )
                )
            db.execute_sql(
                "UPDATE `invitationmodel` SET `status` = 'claimed' WHERE `installed` = 1"
            )
            db.execute_sql(
                "UPDATE `invitationmodel` SET `status` = 'pending' WHERE `installed` = 0 OR `installed` IS NULL"
            )
        elif "superseded_at" not in icols2:
            with db.atomic():
                migrate(
                    migrator.add_column(
                        "invitationmodel",
                        "superseded_at",
                        DateTimeField(null=True),
                    )
                )

    try:
        info_inv3 = db.execute_sql("DESCRIBE invitationmodel")
        icols3 = {row[0] for row in info_inv3}
    except Exception:
        icols3 = set()
    if icols3 and "revoked_at" not in icols3:
        with db.atomic():
            migrate(
                migrator.add_column(
                    "invitationmodel",
                    "revoked_at",
                    DateTimeField(null=True),
                )
            )

    # user_team_role + drop legacy usermodel.team / role_id
    try:
        ucols_final = {row[0] for row in db.execute_sql("DESCRIBE usermodel")}
    except Exception:
        ucols_final = set()
    if "team" in ucols_final or "role_id" in ucols_final:
        sel_cols = ["id"]
        if "team" in ucols_final:
            sel_cols.append("team")
        if "role_id" in ucols_final:
            sel_cols.append("role_id")
        q = "SELECT " + ", ".join(f"`{c}`" for c in sel_cols) + " FROM `usermodel`"
        rows_um = list(db.execute_sql(q))
        for row in rows_um:
            uid = row[0]
            team_raw = None
            rid = None
            if len(sel_cols) == 3:
                team_raw, rid = row[1], row[2]
            elif len(sel_cols) == 2 and "team" in sel_cols:
                team_raw = row[1]
            elif len(sel_cols) == 2 and "role_id" in sel_cols:
                rid = row[1]
            teams: List[str] = []
            if team_raw is not None:
                if isinstance(team_raw, (bytes, bytearray)):
                    team_raw = team_raw.decode("utf-8", errors="replace")
                if isinstance(team_raw, str):
                    try:
                        parsed = json.loads(team_raw)
                    except Exception:
                        parsed = []
                else:
                    parsed = team_raw
                if isinstance(parsed, list):
                    teams = [str(t).strip() for t in parsed if t and str(t).strip()]
                elif parsed:
                    teams = [str(parsed).strip()]
            for tid in teams:
                db.execute_sql(
                    "INSERT IGNORE INTO `user_team_role` "
                    "(`user_id`, `team_id`, `role`, `created_at`) "
                    "VALUES (%s, %s, %s, UTC_TIMESTAMP())",
                    (uid, tid, rid),
                )
        if "team" in ucols_final:
            with db.atomic():
                migrate(migrator.drop_column("usermodel", "team"))
        if "role_id" in ucols_final:
            with db.atomic():
                migrate(migrator.drop_column("usermodel", "role_id"))

    # Unique non-null emails: normalize, dedupe, add unique index
    try:
        db.execute_sql(
            "UPDATE `usermodel` SET `email` = LOWER(TRIM(`email`)) "
            "WHERE `email` IS NOT NULL AND TRIM(`email`) != ''"
        )
        rows_em = list(
            db.execute_sql(
                "SELECT `id`, `email` FROM `usermodel` "
                "WHERE `email` IS NOT NULL AND `email` != ''"
            )
        )
        by_lower: Dict[str, List[int]] = {}
        for row in rows_em:
            uid, em = int(row[0]), row[1]
            if isinstance(em, bytes):
                em = em.decode("utf-8", errors="replace")
            key = str(em).strip().lower()
            if not key:
                continue
            by_lower.setdefault(key, []).append(uid)
        for _key, ids in by_lower.items():
            if len(ids) > 1:
                keep = min(ids)
                for uid in ids:
                    if uid != keep:
                        db.execute_sql("UPDATE `usermodel` SET `email` = NULL WHERE `id` = %s", (uid,))
        idx_rows = list(
            db.execute_sql(
                "SELECT COUNT(*) FROM information_schema.statistics "
                "WHERE table_schema = DATABASE() AND table_name = 'usermodel' "
                "AND column_name = 'email' AND non_unique = 0"
            )
        )
        if idx_rows and int(idx_rows[0][0]) == 0:
            db.execute_sql(
                "CREATE UNIQUE INDEX `usermodel_email_unique` ON `usermodel` (`email`)"
            )
    except Exception as ex:
        logger.warning("usermodel email unique migration skipped or failed: %s", ex)
    db.close()


class BaseModel(Model):
    class Meta:
        database = _db


class UserModel(BaseModel):

    id = AutoField(primary_key=True,null=False)
    username = CharField()
    uuid = CharField(unique=True)
    email = CharField(null=True, unique=True)
    first_name = CharField(null=True)
    last_name = CharField(null=True)
    middle_name = CharField(null=True)
    client_version = CharField(null=True)
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
        rows = (
            UserTeamRoleModel.select()
            .where(UserTeamRoleModel.user == self.id)
            .order_by(UserTeamRoleModel.team_id)
        )
        team_out = [r.team_id for r in rows if r.team_id]
        role_id_out = rows[0].role if rows else None
        return {
            "id": self.id,
            "username": self.username,
            "uuid": self.uuid,
            "team": team_out,
            "email": self.email,
            "firstName": self.first_name,
            "lastName": self.last_name,
            "middleName": self.middle_name,
            "role_id": role_id_out,
            "client_version": self.client_version,
            "created": created_str,
            "data": safe_json_loads(self.data) if self.data else {},
        }

    def json_scoped_for_team(self, team_id: str) -> Dict[str, Any]:
        """
        Same shape as json(), but `team` and `role_id` reflect only the given team.
        Used for manager /workers so other team memberships stay hidden.
        """
        d = self.json()
        tid = (team_id or "").strip()
        if not tid:
            d["team"] = []
            d["role_id"] = None
            return d
        try:
            m = UserTeamRoleModel.get(
                (UserTeamRoleModel.user == self.id) & (UserTeamRoleModel.team_id == tid)
            )
            d["team"] = [m.team_id] if m.team_id else []
            d["role_id"] = m.role
        except UserTeamRoleModel.DoesNotExist:
            d["team"] = []
            d["role_id"] = None
        return d


class UserTeamRoleModel(BaseModel):
    """Per-team role for a user; `role` is an opaque string (SaaS validates)."""

    id = AutoField(primary_key=True)
    user = ForeignKeyField(UserModel, field="id", backref="team_roles", on_delete="CASCADE")
    team_id = CharField(index=True)
    role = CharField(null=True)
    created_at = DateTimeField(default=datetime.now)

    class Meta:
        table_name = "user_team_role"
        indexes = (
            (("user", "team_id"), True),
        )


def _replace_user_team_roles(
    user: "UserModel",
    teams: Optional[List[str]],
    role: Optional[str],
) -> None:
    UserTeamRoleModel.delete().where(UserTeamRoleModel.user == user).execute()
    if not teams:
        return
    now = datetime.now(tz=timezone.utc)
    for tid in teams:
        s = str(tid).strip()
        if not s:
            continue
        UserTeamRoleModel.create(user=user, team_id=s, role=role, created_at=now)


def _sync_user_team_roles_from_update(user: "UserModel", data: Dict[str, Any]) -> None:
    if "team" not in data and "role_id" not in data:
        return
    old_map = {r.team_id: r.role for r in UserTeamRoleModel.select().where(UserTeamRoleModel.user == user)}
    if "team" in data:
        teams = normalize_team_value(data["team"]) or []
    else:
        teams = list(old_map.keys())
    now = datetime.now(tz=timezone.utc)
    UserTeamRoleModel.delete().where(UserTeamRoleModel.user == user).execute()
    if "role_id" in data:
        new_role = data["role_id"]
        for tid in teams:
            s = str(tid).strip()
            if not s:
                continue
            UserTeamRoleModel.create(user=user, team_id=s, role=new_role, created_at=now)
    else:
        for tid in teams:
            s = str(tid).strip()
            if not s:
                continue
            UserTeamRoleModel.create(user=user, team_id=s, role=old_map.get(s), created_at=now)


class InvitationModel(BaseModel):
    """`status`: pending | claimed | superseded | revoked (user removed; row kept for audit)."""

    id = AutoField(primary_key=True)
    token_hash = CharField(unique=True, max_length=64, index=True)
    team_id = CharField(null=True, index=True)
    email = CharField()
    role_id = CharField(null=True)
    first_name = CharField(null=True)
    last_name = CharField(null=True)
    middle_name = CharField(null=True)
    status = CharField(max_length=32, default="pending")
    superseded_at = DateTimeField(null=True)
    revoked_at = DateTimeField(null=True)
    installed = BooleanField(default=False)
    installed_at = DateTimeField(null=True)
    user = ForeignKeyField(UserModel, field="id", null=True, backref="invitations")
    created = DateTimeField(default=datetime.now)

    def json(self) -> Dict[str, Any]:
        installed_at_str = None
        if self.installed_at:
            dt = self.installed_at
            if isinstance(dt, str):
                installed_at_str = iso8601.parse_date(dt).astimezone(timezone.utc).isoformat()
            else:
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                else:
                    dt = dt.astimezone(timezone.utc)
                installed_at_str = dt.isoformat()
        created_str = None
        if self.created:
            c = self.created
            if isinstance(c, str):
                created_str = iso8601.parse_date(c).astimezone(timezone.utc).isoformat()
            else:
                if c.tzinfo is None:
                    c = c.replace(tzinfo=timezone.utc)
                else:
                    c = c.astimezone(timezone.utc)
                created_str = c.isoformat()
        uid = self.user_id
        display = getattr(self, "_display_token", None)
        st = (self.status or "pending").strip() or "pending"
        superseded_at_str = None
        if self.superseded_at:
            sat = self.superseded_at
            if isinstance(sat, str):
                superseded_at_str = iso8601.parse_date(sat).astimezone(timezone.utc).isoformat()
            else:
                if sat.tzinfo is None:
                    sat = sat.replace(tzinfo=timezone.utc)
                else:
                    sat = sat.astimezone(timezone.utc)
                superseded_at_str = sat.isoformat()
        revoked_at_str = None
        if self.revoked_at:
            rv = self.revoked_at
            if isinstance(rv, str):
                revoked_at_str = iso8601.parse_date(rv).astimezone(timezone.utc).isoformat()
            else:
                if rv.tzinfo is None:
                    rv = rv.replace(tzinfo=timezone.utc)
                else:
                    rv = rv.astimezone(timezone.utc)
                revoked_at_str = rv.isoformat()
        user_payload: Optional[Dict[str, Any]] = None
        if uid is not None:
            try:
                u = self.user
                tid = (self.team_id or "").strip()
                if tid:
                    user_payload = u.json_scoped_for_team(tid)
                else:
                    user_payload = u.json()
            except UserModel.DoesNotExist:
                user_payload = None

        return {
            "id": self.id,
            "token": display,
            "team_id": self.team_id,
            "email": self.email,
            "role_id": self.role_id,
            "firstName": self.first_name,
            "lastName": self.last_name,
            "middleName": self.middle_name,
            "status": st,
            "installed": bool(self.installed),
            "installed_at": installed_at_str,
            "superseded_at": superseded_at_str,
            "revoked_at": revoked_at_str,
            "user_id": uid,
            "user": user_payload,
            "created": created_str,
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
        UserTeamRoleModel.create_table(safe=True)
        InvitationModel.create_table(safe=True)
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

    def _email_is_taken(self, email_norm: str, exclude_user_id: Optional[int]) -> bool:
        # Align with MySQL UNIQUE + typical collations: compare case-insensitively.
        lowered = fn.LOWER(fn.TRIM(UserModel.email))
        q = UserModel.select().where(lowered == email_norm)
        if exclude_user_id is not None:
            q = q.where(UserModel.id != exclude_user_id)
        return q.exists()

    def update_user(self, uuid, data):
        print(uuid,data)
        try:
            user = UserModel.get(UserModel.uuid == uuid)
            if "username" in data:
                user.username = data["username"]
            if "email" in data:
                em = normalize_email_for_uniqueness(data.get("email"))
                if em is not None and self._email_is_taken(em, user.id):
                    return {"error": "email_taken"}
                user.email = em
            if "client_version" in data:
                user.client_version = data["client_version"]
            for k, v in person_name_updates_from_payload(data).items():
                setattr(user, k, v)
            if "data" in data:
                user.data = json.dumps(data)
            try:
                user.save()
                _sync_user_team_roles_from_update(user, data)
            except Exception as e:
                if _is_email_unique_violation(e):
                    return {"error": "email_taken"}
                raise
        except UserModel.DoesNotExist:
            pass
        return None

    def create_user(self, data):
        print(data)
        pn = person_names_for_create(data)
        em = normalize_email_for_uniqueness(data.get("email"))
        if em is not None and self._email_is_taken(em, None):
            return {"error": "email_taken"}
        try:
            user = UserModel.create(
                uuid=data["uuid"],
                username=data["username"],
                email=em,
                first_name=pn["first_name"],
                last_name=pn["last_name"],
                middle_name=pn["middle_name"],
                client_version=data.get("client_version"),
                data=json.dumps(data.get("data", {})),
            )
        except Exception as e:
            if _is_email_unique_violation(e):
                return {"error": "email_taken"}
            raise
        _replace_user_team_roles(
            user,
            normalize_team_value(data.get("team")),
            data.get("role_id"),
        )
        return user.json()

    def get_user_by_id(self, user_id: int) -> Optional[dict]:
        try:
            user = UserModel.get(UserModel.id == user_id)
            return {"user": user.json()}
        except UserModel.DoesNotExist:
            return None

    def update_user_by_id(self, user_id: int, data: Dict[str, Any]) -> str:
        """Returns ``ok``, ``not_found``, or ``email_taken``."""
        try:
            user = UserModel.get(UserModel.id == user_id)
            if "username" in data:
                user.username = data["username"]
            if "email" in data:
                em = normalize_email_for_uniqueness(data.get("email"))
                if em is not None and self._email_is_taken(em, user.id):
                    return "email_taken"
                user.email = em
            if "client_version" in data:
                user.client_version = data["client_version"]
            for k, v in person_name_updates_from_payload(data).items():
                setattr(user, k, v)
            if "data" in data and isinstance(data["data"], dict):
                user.data = json.dumps(data["data"])
            try:
                user.save()
                _sync_user_team_roles_from_update(user, data)
            except Exception as e:
                if _is_email_unique_violation(e):
                    return "email_taken"
                raise
            return "ok"
        except UserModel.DoesNotExist:
            return "not_found"

    def update_user_team_role(self, user_id: int, team_id: str, role_id: Optional[str]) -> bool:
        """Update `user_team_role.role` for one (user, team); no other rows touched."""
        try:
            user = UserModel.get(UserModel.id == user_id)
        except UserModel.DoesNotExist:
            return False
        tid = (team_id or "").strip()
        if not tid:
            return False
        try:
            m = UserTeamRoleModel.get(
                (UserTeamRoleModel.user == user) & (UserTeamRoleModel.team_id == tid)
            )
        except UserTeamRoleModel.DoesNotExist:
            return False
        m.role = role_id
        m.save()
        return True

    def delete_user_by_id(self, user_id: int) -> bool:
        """Deletes user buckets (and events), then the user row.

        Invitations linked to this user are soft-revoked (status=revoked, revoked_at, FK cleared).
        Other invitations with the same email that were superseded become pending again (email free).

        Reopen uses every normalized email we can derive *before* revoke: the user row and each
        invitation still pointing at this user. That way superseded rows still match even if
        ``user.email`` was changed after registration (superseded rows keep the original invite text).
        """
        try:
            user = UserModel.get(UserModel.id == user_id)
        except UserModel.DoesNotExist:
            return False
        email_norms: Set[str] = set()
        u_em = normalize_email_for_uniqueness(user.email)
        if u_em:
            email_norms.add(u_em)
        for inv in InvitationModel.select(InvitationModel.email).where(
            InvitationModel.user_id == user_id
        ):
            inv_em = normalize_email_for_uniqueness(inv.email)
            if inv_em:
                email_norms.add(inv_em)
        now = datetime.now(tz=timezone.utc)
        (
            InvitationModel.update(
                status="revoked",
                revoked_at=now,
                user_id=None,
                installed=False,
            )
            .where(InvitationModel.user_id == user_id)
            .execute()
        )
        UserTeamRoleModel.delete().where(UserTeamRoleModel.user == user).execute()
        for bucket in BucketModel.select().where(BucketModel.user == user):
            hk = bucket.hash_key
            if hk in self.bucket_instances:
                del self.bucket_instances[hk]
            if hk in self.bucket_hash_keys:
                del self.bucket_hash_keys[hk]
            bucket.delete_instance(recursive=True)
        user.delete_instance()
        if email_norms:
            lowered = fn.LOWER(fn.TRIM(InvitationModel.email))
            pieces = [(lowered == em) for em in email_norms]
            expr = pieces[0]
            for p in pieces[1:]:
                expr = expr | p
            (
                InvitationModel.update(
                    status="pending",
                    superseded_at=None,
                )
                .where(expr & (InvitationModel.status == "superseded"))
                .execute()
            )
        self.update_bucket_hash_keys()
        return True

    def _pending_invitation_exists_for_team_email(
        self,
        email_norm: str,
        team_id: Optional[str],
    ) -> bool:
        """True if a non-installed invitation already exists for this team + email."""
        tid = (team_id or "").strip()
        team_expr = (
            InvitationModel.team_id == tid
            if tid
            else ((InvitationModel.team_id.is_null(True)) | (InvitationModel.team_id == ""))
        )
        lowered = fn.LOWER(fn.TRIM(InvitationModel.email))
        return (
            InvitationModel.select()
            .where(
                (InvitationModel.status == "pending")
                & (lowered == email_norm)
                & team_expr
            )
            .exists()
        )

    def _mark_invitation_superseded_email_race(self, inv: InvitationModel) -> None:
        """Another registration consumed this email first; this invite is closed for the team."""
        inv.status = "superseded"
        inv.superseded_at = datetime.now(tz=timezone.utc)
        inv.save()

    def _supersede_other_pending_invitations_same_email(self, email_norm: str) -> None:
        """
        After a successful claim, mark every other still-pending invitation with the same
        normalized email as superseded (same person cannot install twice).
        Call only after the winning invitation row is saved with status=claimed.
        """
        if not email_norm:
            return
        lowered = fn.LOWER(fn.TRIM(InvitationModel.email))
        now = datetime.now(tz=timezone.utc)
        InvitationModel.update(
            status="superseded",
            superseded_at=now,
        ).where(
            (lowered == email_norm) & (InvitationModel.status == "pending")
        ).execute()

    def create_invitations_batch(
        self,
        items: List[Dict[str, Any]],
        team_id: Optional[str],
    ) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
        """Each item: email (required), role_id, firstName/lastName/middleName (or snake_case).

        Returns an error dict (and creates nothing) when:

        - ``duplicate_emails_in_batch`` — same normalized email appears in more than one row.
        - ``email_already_registered`` — a user already has that email.
        - ``pending_invitation_exists`` — an unclaimed invitation already exists for the same
          ``team_id`` + email (second invite for the same team is rejected; another team may
          still invite the same address).
        """
        seen_in_batch: Set[str] = set()
        batch_duplicates: Set[str] = set()
        for item in items:
            email_raw = (item.get("email") or "").strip()
            if not email_raw:
                continue
            em = normalize_email_for_uniqueness(email_raw)
            if em is None:
                continue
            if em in seen_in_batch:
                batch_duplicates.add(em)
            seen_in_batch.add(em)
        if batch_duplicates:
            return {
                "error": "duplicate_emails_in_batch",
                "emails": sorted(batch_duplicates),
            }

        conflicts: Set[str] = set()
        for item in items:
            email_raw = (item.get("email") or "").strip()
            if not email_raw:
                continue
            em = normalize_email_for_uniqueness(email_raw)
            if em is not None and self._email_is_taken(em, None):
                conflicts.add(em)
        if conflicts:
            return {
                "error": "email_already_registered",
                "emails": sorted(conflicts),
            }

        pending: Set[str] = set()
        for item in items:
            email_raw = (item.get("email") or "").strip()
            if not email_raw:
                continue
            em = normalize_email_for_uniqueness(email_raw)
            if em is not None and self._pending_invitation_exists_for_team_email(em, team_id):
                pending.add(em)
        if pending:
            return {
                "error": "pending_invitation_exists",
                "emails": sorted(pending),
            }
        out: List[Dict[str, Any]] = []
        for item in items:
            email = (item.get("email") or "").strip()
            if not email:
                continue
            while True:
                _raw, display, th = generate_invitation_secret()
                if InvitationModel.select().where(InvitationModel.token_hash == th).count() == 0:
                    break
            inv = InvitationModel.create(
                token_hash=th,
                team_id=team_id,
                email=email,
                role_id=item.get("role_id"),
                first_name=pick_optional_str(item, "firstName", "first_name"),
                last_name=pick_optional_str(item, "lastName", "last_name"),
                middle_name=pick_optional_str(item, "middleName", "middle_name"),
                status="pending",
                superseded_at=None,
                installed=False,
                installed_at=None,
                user=None,
            )
            inv._display_token = display  # noqa: SLF001 — only for create response JSON
            out.append(inv.json())
        return out

    def list_invitations(self, team_id: Optional[str] = None) -> List[Dict[str, Any]]:
        q = InvitationModel.select()
        if team_id is not None and team_id != "":
            q = q.where(InvitationModel.team_id == team_id)
        q = q.order_by(InvitationModel.created.desc())
        try:
            invs = list(prefetch(q, UserModel))
        except Exception:
            invs = list(q)
        return [inv.json() for inv in invs]

    def get_invitation_by_token(self, token: str) -> Optional[Dict[str, Any]]:
        th = invitation_token_hash_from_client(token)
        if not th:
            return None
        try:
            inv = InvitationModel.get(InvitationModel.token_hash == th)
            return inv.json()
        except InvitationModel.DoesNotExist:
            return None

    def claim_invitation(
        self,
        token: str,
        uuid: str,
        username: str,
    ) -> Optional[Dict[str, Any]]:
        """Create user from invitation preload, mark installed, link FK."""
        th = invitation_token_hash_from_client(token)
        if not th:
            return None
        try:
            inv = InvitationModel.get(InvitationModel.token_hash == th)
        except InvitationModel.DoesNotExist:
            return None
        if (inv.status or "pending") != "pending":
            return {"error": "invitation_already_used", "invitation": inv.json()}
        if UserModel.select().where(UserModel.uuid == uuid).count() > 0:
            return {"error": "uuid_already_registered"}
        fn = inv.first_name
        ln = inv.last_name
        mn = inv.middle_name
        uname = (username or "").strip()
        if not uname:
            parts = [fn, mn, ln]
            uname = " ".join(str(p).strip() for p in parts if p and str(p).strip())
        if not uname:
            em = (inv.email or "").strip()
            uname = em.split("@")[0] if em else "user"
        tid = (inv.team_id or "").strip()
        inv_email_norm = normalize_email_for_uniqueness(inv.email)
        if inv_email_norm is not None and self._email_is_taken(inv_email_norm, None):
            self._mark_invitation_superseded_email_race(inv)
            return {"error": "email_taken", "invitation": inv.json()}
        try:
            user = UserModel.create(
                uuid=uuid,
                username=uname,
                email=inv_email_norm if inv_email_norm else None,
                first_name=fn,
                last_name=ln,
                middle_name=mn,
                client_version=None,
                data=json.dumps({}),
            )
        except Exception as e:
            if _is_email_unique_violation(e):
                self._mark_invitation_superseded_email_race(inv)
                return {"error": "email_taken", "invitation": inv.json()}
            raise
        if tid:
            UserTeamRoleModel.create(
                user=user,
                team_id=tid,
                role=inv.role_id,
                created_at=datetime.now(tz=timezone.utc),
            )
        inv.user = user
        inv.status = "claimed"
        inv.installed = True
        inv.installed_at = datetime.now(tz=timezone.utc)
        inv.save()
        if inv_email_norm:
            self._supersede_other_pending_invitations_same_email(inv_email_norm)
        return {"status": "ok", "user": user.json(), "invitation": inv.json()}

    def get_users(self):
        return [user.json() for user in UserModel.select()]

    def get_buckets_for_user(self, user):
        buckets: Dict[str, dict] = {}
        for bucket in BucketModel.select().where(BucketModel.user == user):
            buckets[bucket.hash_key] = bucket.json()
        return buckets

    def get_workers(self, team_id: str) -> List[Dict[str, Any]]:
        tid = (team_id or "").strip()
        return [
            user.json_scoped_for_team(tid)
            for user in UserModel.select()
            .join(UserTeamRoleModel, on=(UserModel.id == UserTeamRoleModel.user_id))
            .where(UserTeamRoleModel.team_id == tid)
        ]

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
