"""SQLite 表结构与事务访问。"""
import json
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from .domain import Conflict, NotFound


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
                CREATE TABLE IF NOT EXISTS dispatch_meta (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    version INTEGER NOT NULL,
                    dirty INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS dispatch_resources (
                    name TEXT PRIMARY KEY,
                    kind TEXT NOT NULL,
                    capacity REAL NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS dispatch_entries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    record_id INTEGER NOT NULL REFERENCES records(id) ON DELETE CASCADE,
                    impact_score REAL NOT NULL,
                    window_start TEXT NOT NULL,
                    window_end TEXT NOT NULL,
                    spare_need_km REAL NOT NULL,
                    status TEXT NOT NULL DEFAULT 'queued',
                    position INTEGER,
                    vessel TEXT,
                    crew TEXT,
                    spare_depot TEXT,
                    spare_reserved_km REAL NOT NULL DEFAULT 0,
                    delta TEXT NOT NULL DEFAULT '{}',
                    queue_version INTEGER NOT NULL,
                    operation_id TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE UNIQUE INDEX IF NOT EXISTS idx_dispatch_active_record ON dispatch_entries(record_id) WHERE status IN ('queued','departed');
                CREATE TABLE IF NOT EXISTS dispatch_operations (
                    operation_id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL,
                    actor_id TEXT NOT NULL,
                    queue_version INTEGER NOT NULL,
                    result TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS dispatch_candidates (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    operation_id TEXT NOT NULL UNIQUE,
                    actor_id TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    base_version INTEGER NOT NULL,
                    current_version INTEGER NOT NULL,
                    status TEXT NOT NULL DEFAULT 'candidate',
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS dispatch_snapshots (
                    version INTEGER PRIMARY KEY,
                    state TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS dispatch_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event TEXT NOT NULL,
                    actor_id TEXT NOT NULL,
                    queue_version INTEGER,
                    operation_id TEXT,
                    details TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_records_state ON records(state);
                CREATE INDEX IF NOT EXISTS idx_audit_record ON audit_events(record_id, id);
                """
            )
            connection.execute("INSERT OR IGNORE INTO dispatch_meta(id,version,dirty) VALUES(1,1,0)")
            if connection.execute("SELECT COUNT(*) AS total FROM dispatch_resources").fetchone()["total"] == 0:
                now = _now()
                for name, kind, capacity in [("CS-1", "vessel", 1), ("CS-2", "vessel", 1), ("TEAM-A", "crew", 1), ("TEAM-B", "crew", 1), ("DEPOT-MAIN", "spare", 50.0)]:
                    connection.execute(
                        "INSERT INTO dispatch_resources(name,kind,capacity,created_at) VALUES(?,?,?,?)",
                        (name, kind, capacity, now),
                    )

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

    # ---------- 调度队列 ----------

    ENTRY_COLUMNS = "id,record_id,impact_score,window_start,window_end,spare_need_km,status,position,vessel,crew,spare_depot,spare_reserved_km,delta,queue_version,operation_id,created_at,updated_at"

    @staticmethod
    def _entry_row(row: sqlite3.Row) -> Dict[str, Any]:
        item = dict(row)
        item["delta"] = json.loads(item["delta"])
        return item

    def _entries_conn(self, connection: sqlite3.Connection) -> List[Dict[str, Any]]:
        rows = connection.execute(
            "SELECT e.*, r.reference AS reference, r.state AS record_state FROM dispatch_entries e JOIN records r ON r.id=e.record_id "
            "ORDER BY CASE e.status WHEN 'queued' THEN 0 WHEN 'departed' THEN 1 ELSE 2 END, e.position, e.id"
        ).fetchall()
        return [self._entry_row(row) for row in rows]

    def _entry_by_id_conn(self, connection: sqlite3.Connection, entry_id: int) -> Optional[Dict[str, Any]]:
        row = connection.execute(
            "SELECT e.*, r.reference AS reference, r.state AS record_state FROM dispatch_entries e JOIN records r ON r.id=e.record_id WHERE e.id=?",
            (entry_id,),
        ).fetchone()
        return self._entry_row(row) if row is not None else None

    def dispatch_meta(self) -> Dict[str, Any]:
        with self._connect() as connection:
            row = connection.execute("SELECT * FROM dispatch_meta WHERE id=1").fetchone()
        return dict(row)

    def list_dispatch_entries(self) -> List[Dict[str, Any]]:
        with self._connect() as connection:
            return self._entries_conn(connection)

    def get_entry_by_record(self, record_id: int) -> Optional[Dict[str, Any]]:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT e.*, r.reference AS reference, r.state AS record_state FROM dispatch_entries e JOIN records r ON r.id=e.record_id WHERE e.record_id=? ORDER BY e.id DESC LIMIT 1",
                (record_id,),
            ).fetchone()
        return self._entry_row(row) if row is not None else None

    def get_active_entry_by_record(self, record_id: int) -> Optional[Dict[str, Any]]:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM dispatch_entries WHERE record_id=? AND status IN ('queued','departed') ORDER BY id DESC LIMIT 1",
                (record_id,),
            ).fetchone()
        return self._entry_row(row) if row is not None else None

    def list_dispatch_resources(self) -> List[Dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute("SELECT * FROM dispatch_resources ORDER BY kind, name").fetchall()
        return [dict(row) for row in rows]

    def insert_dispatch_resource(self, name: str, kind: str, capacity: float) -> Dict[str, Any]:
        try:
            with self._connect() as connection:
                connection.execute(
                    "INSERT INTO dispatch_resources(name,kind,capacity,created_at) VALUES(?,?,?,?)",
                    (name, kind, float(capacity), _now()),
                )
        except sqlite3.IntegrityError as exc:
            raise Conflict("资源已存在") from exc
        return {"name": name, "kind": kind, "capacity": float(capacity)}

    def list_candidates(self) -> List[Dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute("SELECT * FROM dispatch_candidates ORDER BY id DESC").fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item["payload"] = json.loads(item["payload"])
            result.append(item)
        return result

    def list_dispatch_events(self, limit: int = 100) -> List[Dict[str, Any]]:
        limit = max(1, min(int(limit), 500))
        with self._connect() as connection:
            rows = connection.execute("SELECT * FROM dispatch_events ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item["details"] = json.loads(item["details"])
            result.append(item)
        return result

    def _store_candidate_conn(self, connection: sqlite3.Connection, operation_id: str, actor_id: str, payload: Dict[str, Any], base_version: int, current_version: int) -> int:
        try:
            cursor = connection.execute(
                "INSERT INTO dispatch_candidates(operation_id,actor_id,payload,base_version,current_version,status,created_at) VALUES(?,?,?,?,?,?,?)",
                (operation_id, actor_id, json.dumps(payload, ensure_ascii=False, sort_keys=True), int(base_version), int(current_version), "candidate", _now()),
            )
            return int(cursor.lastrowid)
        except sqlite3.IntegrityError:
            row = connection.execute("SELECT id FROM dispatch_candidates WHERE operation_id=?", (operation_id,)).fetchone()
            return int(row["id"])

    def _finalize(self, connection: sqlite3.Connection, new_version: int, actor_id: str, event: str, operation_id: Optional[str], details: Dict[str, Any]) -> None:
        now = _now()
        connection.execute("UPDATE dispatch_meta SET version=?, dirty=0 WHERE id=1", (int(new_version),))
        state = {"version": int(new_version), "entries": self._entries_conn(connection), "captured_at": now}
        connection.execute(
            "INSERT INTO dispatch_snapshots(version,state,created_at) VALUES(?,?,?)",
            (int(new_version), json.dumps(state, ensure_ascii=False), now),
        )
        connection.execute(
            "INSERT INTO dispatch_events(event,actor_id,queue_version,operation_id,details,created_at) VALUES(?,?,?,?,?,?)",
            (event, actor_id, int(new_version), operation_id, json.dumps(details, ensure_ascii=False, sort_keys=True), now),
        )

    def _queue_txn(self, precheck, apply):
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            meta = connection.execute("SELECT * FROM dispatch_meta WHERE id=1").fetchone()
            if int(meta["dirty"]):
                connection.rollback()
                raise Conflict("队列存在未完成写入，请先执行恢复")
            if precheck is not None:
                pre = precheck(connection, meta)
                if isinstance(pre, _Replay):
                    connection.commit()
                    return pre.result
            connection.execute("UPDATE dispatch_meta SET dirty=1 WHERE id=1")
            connection.commit()
            connection.execute("BEGIN IMMEDIATE")
            result = apply(connection, meta)
            connection.commit()
            return result
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def enqueue_entry(self, *, record_id: int, impact_score: float, window_start: str, window_end: str, spare_need_km: float, operation_id: Optional[str], actor_id: str, allocate) -> Dict[str, Any]:
        def precheck(connection, meta):
            if operation_id:
                row = connection.execute("SELECT result FROM dispatch_operations WHERE operation_id=?", (operation_id,)).fetchone()
                if row is not None:
                    result = json.loads(row["result"])
                    result["replayed"] = True
                    return _Replay(result)
            row = connection.execute(
                "SELECT id FROM dispatch_entries WHERE record_id=? AND status IN ('queued','departed')",
                (record_id,),
            ).fetchone()
            if row is not None:
                raise Conflict("该故障已在调度队列中")
            return None

        def apply(connection, meta):
            now = _now()
            new_version = int(meta["version"]) + 1
            entries = self._entries_conn(connection)
            resources = [dict(row) for row in connection.execute("SELECT * FROM dispatch_resources ORDER BY kind, name").fetchall()]
            allocation = allocate(entries, resources)
            positions = [int(e["position"]) for e in entries if e["status"] == "queued" and e["position"]]
            position = (max(positions) if positions else 0) + 1
            cursor = connection.execute(
                "INSERT INTO dispatch_entries(record_id,impact_score,window_start,window_end,spare_need_km,status,position,vessel,crew,spare_depot,spare_reserved_km,delta,queue_version,operation_id,created_at,updated_at) VALUES(?,?,?,?,?,'queued',?,?,?,?,?,?,?,?,?,?)",
                (
                    record_id, float(impact_score), window_start, window_end, float(spare_need_km), position,
                    allocation["vessel"], allocation["crew"], allocation["spare_depot"], allocation["spare_reserved_km"],
                    json.dumps(allocation["delta"], ensure_ascii=False, sort_keys=True), new_version, operation_id, now, now,
                ),
            )
            entry_id = int(cursor.lastrowid)
            connection.execute(
                "INSERT INTO audit_events(record_id,action,actor_id,version,details,created_at) VALUES(?,?,?,(SELECT version FROM records WHERE id=?),?,?)",
                (
                    record_id, "dispatch_enqueue", actor_id, record_id,
                    json.dumps({"summary": "故障加入调度队列", "operation_id": operation_id, "queue_version": new_version, "position": position, "allocation": allocation, "delta": allocation["delta"]}, ensure_ascii=False, sort_keys=True),
                    now,
                ),
            )
            self._finalize(connection, new_version, actor_id, "enqueue", operation_id, {"entry_id": entry_id, "record_id": record_id, "position": position})
            entry = self._entry_by_id_conn(connection, entry_id)
            result = {"entry": entry, "queue_version": new_version, "replayed": False}
            if operation_id:
                connection.execute(
                    "INSERT INTO dispatch_operations(operation_id,kind,actor_id,queue_version,result,created_at) VALUES(?,?,?,?,?,?)",
                    (operation_id, "enqueue", actor_id, new_version, json.dumps(result, ensure_ascii=False, sort_keys=True), now),
                )
            return result

        return self._queue_txn(precheck, apply)

    def apply_reorder(self, *, operation_id: str, expected_version: int, ordered_ids: List[int], plan: List[Dict[str, Any]], actor_id: str, reason: str) -> Dict[str, Any]:
        def precheck(connection, meta):
            row = connection.execute("SELECT result FROM dispatch_operations WHERE operation_id=?", (operation_id,)).fetchone()
            if row is not None:
                result = json.loads(row["result"])
                result["replayed"] = True
                return _Replay(result)
            current = int(meta["version"])
            queued = {int(row["id"]) for row in connection.execute("SELECT id FROM dispatch_entries WHERE status='queued'").fetchall()}
            if current != int(expected_version) or queued != {int(item) for item in ordered_ids}:
                candidate_id = self._store_candidate_conn(connection, operation_id, actor_id, {"order": [int(item) for item in ordered_ids], "reason": reason}, int(expected_version), current)
                connection.commit()
                raise Conflict("队列已被他人调整，本次顺位调整已保留为候选变更", details={"candidate_id": candidate_id, "current_version": current})
            return None

        def apply(connection, meta):
            return self._reorder_apply(connection, meta, operation_id, ordered_ids, plan, actor_id, reason)

        return self._queue_txn(precheck, apply)

    def _reorder_apply(self, connection: sqlite3.Connection, meta: sqlite3.Row, operation_id: str, ordered_ids: List[int], plan: List[Dict[str, Any]], actor_id: str, reason: str) -> Dict[str, Any]:
        now = _now()
        new_version = int(meta["version"]) + 1
        for item in plan:
            old = connection.execute("SELECT * FROM dispatch_entries WHERE id=?", (item["entry_id"],)).fetchone()
            connection.execute(
                "UPDATE dispatch_entries SET position=?, vessel=?, crew=?, spare_depot=?, spare_reserved_km=?, delta=?, queue_version=?, operation_id=?, updated_at=? WHERE id=?",
                (
                    item["position"], item["vessel"], item["crew"], item["spare_depot"], item["spare_reserved_km"],
                    json.dumps(item["delta"], ensure_ascii=False, sort_keys=True), new_version, operation_id, now, item["entry_id"],
                ),
            )
            details = {
                "summary": "调度顺位重排",
                "operation_id": operation_id,
                "queue_version": new_version,
                "reason": reason,
                "old_position": old["position"],
                "new_position": item["position"],
                "old_allocation": {"vessel": old["vessel"], "crew": old["crew"], "spare_depot": old["spare_depot"], "spare_reserved_km": old["spare_reserved_km"]},
                "new_allocation": {"vessel": item["vessel"], "crew": item["crew"], "spare_depot": item["spare_depot"], "spare_reserved_km": item["spare_reserved_km"]},
                "delta": item["delta"],
            }
            connection.execute(
                "INSERT INTO audit_events(record_id,action,actor_id,version,details,created_at) VALUES(?,?,?,(SELECT version FROM records WHERE id=?),?,?)",
                (old["record_id"], "dispatch_reorder", actor_id, old["record_id"], json.dumps(details, ensure_ascii=False, sort_keys=True), now),
            )
        departed = [
            {"entry_id": row["id"], "record_id": row["record_id"], "vessel": row["vessel"], "crew": row["crew"], "spare_depot": row["spare_depot"], "spare_reserved_km": row["spare_reserved_km"]}
            for row in connection.execute("SELECT id, record_id, vessel, crew, spare_depot, spare_reserved_km FROM dispatch_entries WHERE status='departed'").fetchall()
        ]
        self._finalize(connection, new_version, actor_id, "reorder", operation_id, {"order": [int(item) for item in ordered_ids], "reason": reason})
        result = {"operation_id": operation_id, "mode": "confirm", "queue_version": new_version, "replayed": False, "entries": plan, "departed": departed, "reason": reason}
        connection.execute(
            "INSERT INTO dispatch_operations(operation_id,kind,actor_id,queue_version,result,created_at) VALUES(?,?,?,?,?,?)",
            (operation_id, "reorder", actor_id, new_version, json.dumps(result, ensure_ascii=False, sort_keys=True), now),
        )
        return result

    def depart_entry(self, entry_id: int, actor_id: str, reason: str = "") -> Dict[str, Any]:
        def precheck(connection, meta):
            row = connection.execute("SELECT * FROM dispatch_entries WHERE id=?", (entry_id,)).fetchone()
            if row is None:
                raise NotFound("调度项不存在")
            if row["status"] == "departed":
                return _Replay({"entry": self._entry_by_id_conn(connection, entry_id), "queue_version": int(meta["version"]), "changed": False})
            if row["status"] != "queued":
                raise Conflict("已结束的调度项不能离港")
            return None

        def apply(connection, meta):
            row = connection.execute("SELECT * FROM dispatch_entries WHERE id=?", (entry_id,)).fetchone()
            now = _now()
            new_version = int(meta["version"]) + 1
            connection.execute("UPDATE dispatch_entries SET status='departed', queue_version=?, updated_at=? WHERE id=?", (new_version, now, entry_id))
            connection.execute(
                "INSERT INTO audit_events(record_id,action,actor_id,version,details,created_at) VALUES(?,?,?,(SELECT version FROM records WHERE id=?),?,?)",
                (
                    row["record_id"], "dispatch_depart", actor_id, row["record_id"],
                    json.dumps({"summary": "抢修船已离港，预占转为执行依据", "queue_version": new_version, "reason": reason, "allocation": {"vessel": row["vessel"], "crew": row["crew"], "spare_depot": row["spare_depot"], "spare_reserved_km": row["spare_reserved_km"]}}, ensure_ascii=False, sort_keys=True),
                    now,
                ),
            )
            self._finalize(connection, new_version, actor_id, "depart", None, {"entry_id": entry_id, "record_id": row["record_id"], "reason": reason})
            return {"entry": self._entry_by_id_conn(connection, entry_id), "queue_version": new_version, "changed": True}

        return self._queue_txn(precheck, apply)

    def release_entry(self, record_id: int, target_status: str, actor_id: str, reason: str = "") -> Optional[Dict[str, Any]]:
        def precheck(connection, meta):
            row = connection.execute(
                "SELECT id FROM dispatch_entries WHERE record_id=? AND status IN ('queued','departed') ORDER BY id DESC LIMIT 1",
                (record_id,),
            ).fetchone()
            if row is None:
                return _Replay(None)
            return None

        def apply(connection, meta):
            row = connection.execute(
                "SELECT * FROM dispatch_entries WHERE record_id=? AND status IN ('queued','departed') ORDER BY id DESC LIMIT 1",
                (record_id,),
            ).fetchone()
            now = _now()
            new_version = int(meta["version"]) + 1
            connection.execute("UPDATE dispatch_entries SET status=?, queue_version=?, updated_at=? WHERE id=?", (target_status, new_version, now, row["id"]))
            connection.execute(
                "INSERT INTO audit_events(record_id,action,actor_id,version,details,created_at) VALUES(?,?,?,(SELECT version FROM records WHERE id=?),?,?)",
                (
                    record_id, "dispatch_release", actor_id, record_id,
                    json.dumps({"summary": "调度项结束，资源预占释放", "queue_version": new_version, "reason": reason, "freed": {"vessel": row["vessel"], "crew": row["crew"], "spare_depot": row["spare_depot"], "spare_reserved_km": row["spare_reserved_km"]}}, ensure_ascii=False, sort_keys=True),
                    now,
                ),
            )
            self._finalize(connection, new_version, actor_id, "release", None, {"entry_id": row["id"], "record_id": record_id, "status": target_status, "reason": reason})
            return {"entry_id": row["id"], "queue_version": new_version, "status": target_status}

        return self._queue_txn(precheck, apply)

    def recover_queue(self, actor_id: str) -> Dict[str, Any]:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            snapshot = connection.execute("SELECT * FROM dispatch_snapshots ORDER BY version DESC LIMIT 1").fetchone()
            if snapshot is None:
                connection.rollback()
                raise Conflict("没有可用的队列快照")
            state = json.loads(snapshot["state"])
            connection.execute("DELETE FROM dispatch_entries")
            now = _now()
            for entry in state["entries"]:
                connection.execute(
                    "INSERT INTO dispatch_entries(id,record_id,impact_score,window_start,window_end,spare_need_km,status,position,vessel,crew,spare_depot,spare_reserved_km,delta,queue_version,operation_id,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        entry["id"], entry["record_id"], entry["impact_score"], entry["window_start"], entry["window_end"], entry["spare_need_km"],
                        entry["status"], entry["position"], entry["vessel"], entry["crew"], entry["spare_depot"], entry["spare_reserved_km"],
                        json.dumps(entry["delta"], ensure_ascii=False, sort_keys=True) if isinstance(entry["delta"], (dict, list)) else entry["delta"],
                        entry["queue_version"], entry["operation_id"], entry["created_at"], entry["updated_at"],
                    ),
                )
            connection.execute("UPDATE dispatch_meta SET version=?, dirty=0 WHERE id=1", (int(snapshot["version"]),))
            connection.execute(
                "INSERT INTO dispatch_events(event,actor_id,queue_version,operation_id,details,created_at) VALUES(?,?,?,?,?,?)",
                ("recover", actor_id, int(snapshot["version"]), None, json.dumps({"restored_version": int(snapshot["version"]), "entries": len(state["entries"])}, ensure_ascii=False, sort_keys=True), now),
            )
            connection.commit()
            return {"queue_version": int(snapshot["version"]), "restored_entries": len(state["entries"]), "dirty": False}
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()


class _Replay:
    __slots__ = ("result",)

    def __init__(self, result: Dict[str, Any]) -> None:
        self.result = result
