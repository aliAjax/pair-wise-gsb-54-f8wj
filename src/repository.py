"""SQLite 表结构与事务访问。"""
import json
import sqlite3
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

from .domain import Conflict, NotFound


QUEUE_ACTIVE_STATUSES = ("trial", "committed", "departed")
DEFAULT_POOLS = (("vessel", "CS-1", 1.0), ("vessel", "CS-2", 1.0), ("crew", "TEAM-A", 1.0), ("crew", "TEAM-B", 1.0), ("spare_cable", "MAIN", 40.0))


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class Repository:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path, timeout=15)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 15000")
        return connection

    def _init_schema(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    reference TEXT NOT NULL UNIQUE,
                    state TEXT NOT NULL,
                    version INTEGER NOT NULL DEFAULT 1,
                    payload TEXT NOT NULL,
                    created_by TEXT NOT NULL,
                    updated_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS audit_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    record_id INTEGER NOT NULL REFERENCES records(id) ON DELETE CASCADE,
                    action TEXT NOT NULL,
                    actor_id TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    details TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_records_state ON records(state);
                CREATE INDEX IF NOT EXISTS idx_audit_record ON audit_events(record_id, id);
                CREATE TABLE IF NOT EXISTS queue_items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    record_id INTEGER NOT NULL REFERENCES records(id),
                    idempotency_key TEXT NOT NULL UNIQUE,
                    impact_score REAL NOT NULL,
                    window_start TEXT NOT NULL,
                    window_end TEXT NOT NULL,
                    spare_required_km REAL NOT NULL,
                    position INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    allocation TEXT NOT NULL,
                    shortfall TEXT NOT NULL,
                    basis_version INTEGER NOT NULL,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE UNIQUE INDEX IF NOT EXISTS idx_queue_active_record ON queue_items(record_id) WHERE status IN ('trial','committed','departed');
                CREATE TABLE IF NOT EXISTS queue_versions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    version INTEGER NOT NULL UNIQUE,
                    origin TEXT NOT NULL,
                    ref TEXT UNIQUE,
                    actor_id TEXT NOT NULL,
                    note TEXT NOT NULL DEFAULT '',
                    snapshot TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS queue_candidates (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    actor_id TEXT NOT NULL,
                    proposed_order TEXT NOT NULL,
                    base_version INTEGER NOT NULL,
                    current_version INTEGER NOT NULL,
                    note TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS queue_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id INTEGER,
                    record_id INTEGER,
                    action TEXT NOT NULL,
                    actor_id TEXT NOT NULL,
                    details TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS resource_pools (
                    kind TEXT NOT NULL,
                    name TEXT NOT NULL,
                    capacity REAL NOT NULL,
                    PRIMARY KEY (kind, name)
                );
                CREATE TABLE IF NOT EXISTS queue_meta (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    version INTEGER NOT NULL,
                    dirty INTEGER NOT NULL DEFAULT 0
                );
                """
            )
            connection.execute("INSERT OR IGNORE INTO queue_meta(id,version,dirty) VALUES (1,0,0)")
            connection.executemany("INSERT OR IGNORE INTO resource_pools(kind,name,capacity) VALUES (?,?,?)", DEFAULT_POOLS)
            row = connection.execute("SELECT dirty FROM queue_meta WHERE id=1").fetchone()
            if row is not None and int(row["dirty"]):
                self._recover_locked(connection, "system")

    @staticmethod
    def _row(row: sqlite3.Row) -> Dict[str, Any]:
        item = dict(row)
        item["payload"] = json.loads(item["payload"])
        return item

    def create(self, reference: str, state: str, payload: Dict[str, Any], actor_id: str) -> Dict[str, Any]:
        now = _now()
        try:
            with self._connect() as connection:
                cursor = connection.execute(
                    "INSERT INTO records(reference,state,version,payload,created_by,updated_by,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)",
                    (reference, state, 1, json.dumps(payload, ensure_ascii=False, sort_keys=True), actor_id, actor_id, now, now),
                )
                record_id = int(cursor.lastrowid)
                connection.execute(
                    "INSERT INTO audit_events(record_id,action,actor_id,version,details,created_at) VALUES(?,?,?,?,?,?)",
                    (record_id, "created", actor_id, 1, json.dumps({"state": state}, ensure_ascii=False, sort_keys=True), now),
                )
                row = connection.execute("SELECT * FROM records WHERE id=?", (record_id,)).fetchone()
        except sqlite3.IntegrityError as exc:
            raise Conflict("reference已存在") from exc
        return self._row(row)

    def get(self, record_id: int) -> Dict[str, Any]:
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM records WHERE id=?", (record_id,)).fetchone()
        if row is None:
            raise NotFound("记录不存在")
        return self._row(row)

    def list_records(self, state: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        limit = max(1, min(int(limit), 500))
        with self._connect() as connection:
            if state:
                rows = connection.execute("SELECT * FROM records WHERE state=? ORDER BY id DESC LIMIT ?", (state, limit)).fetchall()
            else:
                rows = connection.execute("SELECT * FROM records ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        return [self._row(row) for row in rows]

    def mutate(self, record_id: int, expected_version: int, state: str, payload: Dict[str, Any], actor_id: str, action: str, details: Dict[str, Any]) -> Dict[str, Any]:
        now = _now()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT version FROM records WHERE id=?", (record_id,)).fetchone()
            if row is None:
                connection.rollback()
                raise NotFound("记录不存在")
            if int(row["version"]) != int(expected_version):
                connection.rollback()
                raise Conflict("版本冲突，请刷新后重试")
            version = int(expected_version) + 1
            connection.execute(
                "UPDATE records SET state=?,version=?,payload=?,updated_by=?,updated_at=? WHERE id=?",
                (state, version, json.dumps(payload, ensure_ascii=False, sort_keys=True), actor_id, now, record_id),
            )
            connection.execute(
                "INSERT INTO audit_events(record_id,action,actor_id,version,details,created_at) VALUES(?,?,?,?,?,?)",
                (record_id, action, actor_id, version, json.dumps(details, ensure_ascii=False, sort_keys=True), now),
            )
            result = connection.execute("SELECT * FROM records WHERE id=?", (record_id,)).fetchone()
            connection.commit()
        return self._row(result)

    def add_audit(self, record_id: int, actor_id: str, action: str, details: Dict[str, Any]) -> None:
        with self._connect() as connection:
            row = connection.execute("SELECT version FROM records WHERE id=?", (record_id,)).fetchone()
            if row is None:
                raise NotFound("记录不存在")
            connection.execute(
                "INSERT INTO audit_events(record_id,action,actor_id,version,details,created_at) VALUES(?,?,?,?,?,?)",
                (record_id, action, actor_id, int(row["version"]), json.dumps(details, ensure_ascii=False, sort_keys=True), _now()),
            )

    def audit_timeline(self, record_id: int) -> List[Dict[str, Any]]:
        self.get(record_id)
        with self._connect() as connection:
            rows = connection.execute("SELECT * FROM audit_events WHERE record_id=? ORDER BY id", (record_id,)).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item["details"] = json.loads(item["details"])
            result.append(item)
        return result

    def stats(self) -> Dict[str, int]:
        with self._connect() as connection:
            rows = connection.execute("SELECT state, COUNT(*) AS total FROM records GROUP BY state").fetchall()
        return {str(row["state"]): int(row["total"]) for row in rows}

    def health(self) -> bool:
        try:
            with self._connect() as connection:
                connection.execute("SELECT 1").fetchone()
            return True
        except sqlite3.Error:
            return False

    # ---- 调度队列 ----

    @staticmethod
    def _queue_item_row(row: sqlite3.Row) -> Dict[str, Any]:
        item = dict(row)
        item["allocation"] = json.loads(item["allocation"])
        item["shortfall"] = json.loads(item["shortfall"])
        return item

    @staticmethod
    def _meta_version_locked(connection: sqlite3.Connection) -> int:
        row = connection.execute("SELECT version FROM queue_meta WHERE id=1").fetchone()
        return int(row["version"])

    def _active_items_locked(self, connection: sqlite3.Connection) -> List[Dict[str, Any]]:
        rows = connection.execute(
            "SELECT q.*, r.reference AS record_reference, r.state AS record_state FROM queue_items q JOIN records r ON r.id=q.record_id WHERE q.status IN ('trial','committed','departed') ORDER BY q.position"
        ).fetchall()
        return [self._queue_item_row(row) for row in rows]

    @staticmethod
    def _pools_locked(connection: sqlite3.Connection) -> List[Dict[str, Any]]:
        rows = connection.execute("SELECT * FROM resource_pools ORDER BY kind, name").fetchall()
        return [dict(row) for row in rows]

    def _bump_locked(self, connection: sqlite3.Connection, origin: str, ref: Optional[str], actor_id: str, note: str) -> int:
        version = self._meta_version_locked(connection) + 1
        snapshot = {"version": version, "origin": origin, "items": self._active_items_locked(connection), "pools": self._pools_locked(connection)}
        connection.execute(
            "INSERT INTO queue_versions(version,origin,ref,actor_id,note,snapshot,created_at) VALUES(?,?,?,?,?,?,?)",
            (version, origin, ref, actor_id, note, json.dumps(snapshot, ensure_ascii=False, sort_keys=True), _now()),
        )
        connection.execute("UPDATE queue_meta SET version=?, dirty=0 WHERE id=1", (version,))
        return version

    @staticmethod
    def _queue_event_locked(connection: sqlite3.Connection, item_id: Optional[int], record_id: Optional[int], action: str, actor_id: str, details: Dict[str, Any]) -> None:
        connection.execute(
            "INSERT INTO queue_events(item_id,record_id,action,actor_id,details,created_at) VALUES(?,?,?,?,?,?)",
            (item_id, record_id, action, actor_id, json.dumps(details, ensure_ascii=False, sort_keys=True), _now()),
        )

    @staticmethod
    def _record_audit_locked(connection: sqlite3.Connection, record_id: int, actor_id: str, action: str, details: Dict[str, Any]) -> None:
        row = connection.execute("SELECT version FROM records WHERE id=?", (record_id,)).fetchone()
        if row is None:
            raise NotFound("记录不存在")
        connection.execute(
            "INSERT INTO audit_events(record_id,action,actor_id,version,details,created_at) VALUES(?,?,?,?,?,?)",
            (record_id, action, actor_id, int(row["version"]), json.dumps(details, ensure_ascii=False, sort_keys=True), _now()),
        )

    def _mark_dirty(self) -> None:
        with self._connect() as connection:
            connection.execute("UPDATE queue_meta SET dirty=1 WHERE id=1")

    def _clear_dirty(self) -> None:
        with self._connect() as connection:
            connection.execute("UPDATE queue_meta SET dirty=0 WHERE id=1")

    def _recover_locked(self, connection: sqlite3.Connection, actor_id: str) -> Optional[int]:
        row = connection.execute("SELECT version, snapshot FROM queue_versions ORDER BY version DESC LIMIT 1").fetchone()
        if row is None:
            connection.execute("UPDATE queue_meta SET dirty=0 WHERE id=1")
            return None
        snapshot = json.loads(row["snapshot"])
        connection.execute("DELETE FROM queue_items WHERE status IN ('trial','committed','departed')")
        for item in snapshot.get("items", []):
            connection.execute(
                "INSERT OR REPLACE INTO queue_items(id,record_id,idempotency_key,impact_score,window_start,window_end,spare_required_km,position,status,allocation,shortfall,basis_version,created_by,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    item["id"], item["record_id"], item["idempotency_key"], item["impact_score"], item["window_start"], item["window_end"],
                    item["spare_required_km"], item["position"], item["status"],
                    json.dumps(item["allocation"], ensure_ascii=False, sort_keys=True),
                    json.dumps(item["shortfall"], ensure_ascii=False, sort_keys=True),
                    item["basis_version"], item["created_by"], item["created_at"], item["updated_at"],
                ),
            )
        connection.execute("UPDATE queue_meta SET version=?, dirty=0 WHERE id=1", (int(row["version"]),))
        self._queue_event_locked(connection, None, None, "queue_recover", actor_id, {"recovered_version": int(row["version"]), "restored_items": len(snapshot.get("items", []))})
        return int(row["version"])

    def queue_version(self) -> int:
        with self._connect() as connection:
            return self._meta_version_locked(connection)

    def queue_state(self) -> Tuple[int, List[Dict[str, Any]], List[Dict[str, Any]]]:
        with self._connect() as connection:
            return self._meta_version_locked(connection), self._active_items_locked(connection), self._pools_locked(connection)

    def get_queue_item(self, item_id: int) -> Dict[str, Any]:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT q.*, r.reference AS record_reference, r.state AS record_state FROM queue_items q JOIN records r ON r.id=q.record_id WHERE q.id=?",
                (item_id,),
            ).fetchone()
        if row is None:
            raise NotFound("队列项不存在")
        return self._queue_item_row(row)

    def active_item_for_record(self, record_id: int) -> Optional[Dict[str, Any]]:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT q.*, r.reference AS record_reference, r.state AS record_state FROM queue_items q JOIN records r ON r.id=q.record_id WHERE q.record_id=? AND q.status IN ('trial','committed','departed')",
                (record_id,),
            ).fetchone()
        return self._queue_item_row(row) if row is not None else None

    def enqueue_item(self, record_id: int, key: str, impact_score: float, window_start: str, window_end: str, spare_required: float, confirm: bool, actor_id: str, plan_callback: Callable) -> Tuple[Dict[str, Any], bool, int]:
        self._mark_dirty()
        try:
            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                existing = connection.execute(
                    "SELECT q.*, r.reference AS record_reference, r.state AS record_state FROM queue_items q JOIN records r ON r.id=q.record_id WHERE q.idempotency_key=?",
                    (key,),
                ).fetchone()
                if existing is not None:
                    item = self._queue_item_row(existing)
                    if int(item["record_id"]) != int(record_id):
                        connection.rollback()
                        raise Conflict("幂等键已被其他故障占用")
                    version = self._meta_version_locked(connection)
                    connection.commit()
                    return item, False, version
                if connection.execute("SELECT id FROM records WHERE id=?", (record_id,)).fetchone() is None:
                    connection.rollback()
                    raise NotFound("记录不存在")
                duplicate = connection.execute(
                    "SELECT id FROM queue_items WHERE record_id=? AND status IN ('trial','committed','departed')", (record_id,)
                ).fetchone()
                if duplicate is not None:
                    connection.rollback()
                    raise Conflict("该故障已在调度队列中")
                items = self._active_items_locked(connection)
                pools = self._pools_locked(connection)
                allocation, shortfall = plan_callback(items, pools, {"spare_required_km": spare_required})
                position = max([int(it["position"]) for it in items], default=0) + 1
                status = "committed" if confirm else "trial"
                now = _now()
                new_version = self._meta_version_locked(connection) + 1
                cursor = connection.execute(
                    "INSERT INTO queue_items(record_id,idempotency_key,impact_score,window_start,window_end,spare_required_km,position,status,allocation,shortfall,basis_version,created_by,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        record_id, key, impact_score, window_start, window_end, spare_required, position, status,
                        json.dumps(allocation, ensure_ascii=False, sort_keys=True),
                        json.dumps(shortfall, ensure_ascii=False, sort_keys=True),
                        new_version, actor_id, now, now,
                    ),
                )
                item_id = int(cursor.lastrowid)
                version = self._bump_locked(connection, "enqueue", None, actor_id, "")
                self._record_audit_locked(connection, record_id, actor_id, "queue_enqueue", {"summary": "加入调度队列", "queue_item_id": item_id, "position": position, "status": status, "allocation": allocation, "shortfall": shortfall, "queue_version": version})
                self._queue_event_locked(connection, item_id, record_id, "enqueue", actor_id, {"position": position, "status": status, "queue_version": version})
                connection.commit()
            return self.get_queue_item(item_id), True, version
        finally:
            self._clear_dirty()

    def confirm_item(self, item_id: int, actor_id: str, plan_callback: Callable) -> Tuple[Dict[str, Any], int]:
        self._mark_dirty()
        try:
            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                row = connection.execute("SELECT * FROM queue_items WHERE id=?", (item_id,)).fetchone()
                if row is None:
                    connection.rollback()
                    raise NotFound("队列项不存在")
                item = self._queue_item_row(row)
                if item["status"] != "trial":
                    connection.rollback()
                    raise Conflict("仅试占中的队列项可确认")
                items = self._active_items_locked(connection)
                pools = self._pools_locked(connection)
                allocation, shortfall = plan_callback(items, pools, item)
                connection.execute(
                    "UPDATE queue_items SET status='committed', allocation=?, shortfall=?, basis_version=?, updated_at=? WHERE id=?",
                    (
                        json.dumps(allocation, ensure_ascii=False, sort_keys=True),
                        json.dumps(shortfall, ensure_ascii=False, sort_keys=True),
                        self._meta_version_locked(connection) + 1, _now(), item_id,
                    ),
                )
                version = self._bump_locked(connection, "confirm", None, actor_id, "")
                self._record_audit_locked(connection, item["record_id"], actor_id, "queue_confirm", {"summary": "确认资源占用", "queue_item_id": item_id, "allocation": allocation, "shortfall": shortfall, "queue_version": version})
                self._queue_event_locked(connection, item_id, item["record_id"], "confirm", actor_id, {"queue_version": version})
                connection.commit()
            return self.get_queue_item(item_id), version
        finally:
            self._clear_dirty()

    def depart_item(self, item_id: int, actor_id: str) -> Tuple[Dict[str, Any], int]:
        self._mark_dirty()
        try:
            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                row = connection.execute("SELECT * FROM queue_items WHERE id=?", (item_id,)).fetchone()
                if row is None:
                    connection.rollback()
                    raise NotFound("队列项不存在")
                item = self._queue_item_row(row)
                if item["status"] != "committed":
                    connection.rollback()
                    raise Conflict("仅已确认的队列项可离港")
                shortfall = item["shortfall"]
                if shortfall.get("vessel") or shortfall.get("crew") or float(shortfall.get("spare_km", 0) or 0) > 0:
                    connection.rollback()
                    raise Conflict("存在未解决的资源差量，不能离港")
                connection.execute("UPDATE queue_items SET status='departed', updated_at=? WHERE id=?", (_now(), item_id))
                version = self._bump_locked(connection, "depart", None, actor_id, "")
                self._record_audit_locked(connection, item["record_id"], actor_id, "queue_depart", {"summary": "抢修船已离港", "queue_item_id": item_id, "basis_version": item["basis_version"], "allocation": item["allocation"], "queue_version": version})
                self._queue_event_locked(connection, item_id, item["record_id"], "depart", actor_id, {"basis_version": item["basis_version"], "queue_version": version})
                connection.commit()
            return self.get_queue_item(item_id), version
        finally:
            self._clear_dirty()

    def _release_locked(self, connection: sqlite3.Connection, item: Dict[str, Any], actor_id: str, reason: str) -> int:
        connection.execute("UPDATE queue_items SET status='released', updated_at=? WHERE id=?", (_now(), item["id"]))
        version = self._bump_locked(connection, "release", None, actor_id, reason)
        self._record_audit_locked(connection, item["record_id"], actor_id, "queue_release", {"summary": "释放队列资源", "queue_item_id": item["id"], "reason": reason, "freed": item["allocation"], "queue_version": version})
        self._queue_event_locked(connection, item["id"], item["record_id"], "release", actor_id, {"reason": reason, "queue_version": version})
        return version

    def release_item(self, item_id: int, actor_id: str, reason: str) -> Tuple[Dict[str, Any], int]:
        self._mark_dirty()
        try:
            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                row = connection.execute("SELECT * FROM queue_items WHERE id=?", (item_id,)).fetchone()
                if row is None:
                    connection.rollback()
                    raise NotFound("队列项不存在")
                item = self._queue_item_row(row)
                if item["status"] not in QUEUE_ACTIVE_STATUSES:
                    connection.rollback()
                    raise Conflict("队列项已结束")
                version = self._release_locked(connection, item, actor_id, reason)
                connection.commit()
            return self.get_queue_item(item_id), version
        finally:
            self._clear_dirty()

    def release_active_item_for_record(self, record_id: int, actor_id: str, reason: str) -> Optional[Dict[str, Any]]:
        self._mark_dirty()
        try:
            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                row = connection.execute(
                    "SELECT * FROM queue_items WHERE record_id=? AND status IN ('trial','committed','departed')", (record_id,)
                ).fetchone()
                if row is None:
                    connection.commit()
                    return None
                item = self._queue_item_row(row)
                self._release_locked(connection, item, actor_id, reason)
                connection.commit()
            return self.get_queue_item(int(item["id"]))
        finally:
            self._clear_dirty()

    def submit_reorder(self, expected_version: int, order: List[int], reorder_id: str, actor_id: str, note: str, plan_callback: Callable, candidate_id: Optional[int] = None) -> Dict[str, Any]:
        self._mark_dirty()
        try:
            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                if reorder_id:
                    replay = connection.execute("SELECT version FROM queue_versions WHERE ref=?", (reorder_id,)).fetchone()
                    if replay is not None:
                        if candidate_id is not None:
                            connection.execute("UPDATE queue_candidates SET status='applied' WHERE id=? AND status='pending'", (candidate_id,))
                        version = int(replay["version"])
                        connection.commit()
                        return {"result": "replayed", "queue_version": version, "reorder_id": reorder_id}
                current = self._meta_version_locked(connection)
                if int(expected_version) != current:
                    cursor = connection.execute(
                        "INSERT INTO queue_candidates(actor_id,proposed_order,base_version,current_version,note,status,created_at) VALUES(?,?,?,?,?,'pending',?)",
                        (actor_id, json.dumps(order), int(expected_version), current, note, _now()),
                    )
                    new_candidate_id = int(cursor.lastrowid)
                    self._queue_event_locked(connection, None, None, "reorder_candidate", actor_id, {"candidate_id": new_candidate_id, "base_version": int(expected_version), "current_version": current})
                    connection.commit()
                    return {"result": "candidate", "candidate_id": new_candidate_id, "queue_version": current}
                items = self._active_items_locked(connection)
                pools = self._pools_locked(connection)
                changes = plan_callback(items, pools, order, current + 1)
                now = _now()
                for item in items:
                    change = changes[int(item["id"])]
                    connection.execute(
                        "UPDATE queue_items SET position=?, allocation=?, shortfall=?, basis_version=?, updated_at=? WHERE id=?",
                        (
                            change["position"],
                            json.dumps(change["allocation"], ensure_ascii=False, sort_keys=True),
                            json.dumps(change["shortfall"], ensure_ascii=False, sort_keys=True),
                            change["basis_version"], now, item["id"],
                        ),
                    )
                version = self._bump_locked(connection, "reorder", reorder_id, actor_id, note)
                for item in items:
                    change = changes[int(item["id"])]
                    self._record_audit_locked(connection, item["record_id"], actor_id, "queue_reorder", {
                        "summary": "队列重排", "reorder_id": reorder_id, "queue_version": version,
                        "old_position": item["position"], "new_position": change["position"],
                        "allocation_before": item["allocation"], "allocation_after": change["allocation"],
                        "shortfall": change["shortfall"], "basis_version": change["basis_version"], "kept_basis": change["kept_basis"],
                    })
                self._queue_event_locked(connection, None, None, "reorder", actor_id, {"reorder_id": reorder_id, "queue_version": version, "order": order, "note": note})
                if candidate_id is not None:
                    connection.execute("UPDATE queue_candidates SET status='applied' WHERE id=?", (candidate_id,))
                connection.commit()
                return {"result": "committed", "queue_version": version, "reorder_id": reorder_id, "changes": changes}
        finally:
            self._clear_dirty()

    def get_candidate(self, candidate_id: int) -> Dict[str, Any]:
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM queue_candidates WHERE id=?", (candidate_id,)).fetchone()
        if row is None:
            raise NotFound("候选变更不存在")
        item = dict(row)
        item["proposed_order"] = json.loads(item["proposed_order"])
        return item

    def list_candidates(self) -> List[Dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute("SELECT * FROM queue_candidates ORDER BY id DESC").fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item["proposed_order"] = json.loads(item["proposed_order"])
            result.append(item)
        return result

    def discard_candidate(self, candidate_id: int, actor_id: str) -> Dict[str, Any]:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT status FROM queue_candidates WHERE id=?", (candidate_id,)).fetchone()
            if row is None:
                connection.rollback()
                raise NotFound("候选变更不存在")
            if row["status"] != "pending":
                connection.rollback()
                raise Conflict("候选变更已处理")
            connection.execute("UPDATE queue_candidates SET status='discarded' WHERE id=?", (candidate_id,))
            self._queue_event_locked(connection, None, None, "candidate_discard", actor_id, {"candidate_id": candidate_id})
            connection.commit()
        return self.get_candidate(candidate_id)

    def upsert_pool(self, kind: str, name: str, capacity: float, actor_id: str) -> int:
        self._mark_dirty()
        try:
            with self._connect() as connection:
                connection.execute("BEGIN IMMEDIATE")
                connection.execute(
                    "INSERT INTO resource_pools(kind,name,capacity) VALUES(?,?,?) ON CONFLICT(kind,name) DO UPDATE SET capacity=excluded.capacity",
                    (kind, name, capacity),
                )
                version = self._bump_locked(connection, "pool", None, actor_id, "%s:%s" % (kind, name))
                self._queue_event_locked(connection, None, None, "pool_upsert", actor_id, {"kind": kind, "name": name, "capacity": capacity, "queue_version": version})
                connection.commit()
                return version
        finally:
            self._clear_dirty()

    def list_queue_versions(self) -> List[Dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute("SELECT version,origin,ref,actor_id,note,snapshot,created_at FROM queue_versions ORDER BY version DESC").fetchall()
        result = []
        for row in rows:
            item = dict(row)
            snapshot = json.loads(item.pop("snapshot"))
            item["item_count"] = len(snapshot.get("items", []))
            result.append(item)
        return result

    def list_queue_events(self, limit: int = 100) -> List[Dict[str, Any]]:
        limit = max(1, min(int(limit), 500))
        with self._connect() as connection:
            rows = connection.execute("SELECT * FROM queue_events ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item["details"] = json.loads(item["details"])
            result.append(item)
        return result

    def recover_queue(self, actor_id: str) -> int:
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            if connection.execute("SELECT version FROM queue_versions LIMIT 1").fetchone() is None:
                connection.rollback()
                raise NotFound("没有可恢复的队列快照")
            version = self._recover_locked(connection, actor_id)
            connection.commit()
        return int(version)
