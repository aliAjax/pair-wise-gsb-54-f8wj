"""调度队列：资源试占、顺位重排、候选变更与快照恢复。

队列把故障影响量、抢修时段和备缆余量连成一体：
- 默认顺位按影响量降序、抢修窗口开始升序排列；
- 确认前先试占（trial），差量直接记在原项上；
- 确认重排时未离港项作废旧预占并按新顺位重算，已离港项保持原依据；
- 并发确认时后到者只留候选变更，不重复占用资源；
- 每次确认写入完整快照，写入失败后可从最近一次完整队列恢复；
- 同一插队动作（operation_id）重放不会二次占用。
"""
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

from .audit import AuditRecorder
from .domain import Actor, Conflict, PermissionDenied, ValidationError, choice, number, optional_text, text
from .repository import Repository
from .rules import DomainRules


DISPATCH_ROLES = {"dispatcher"}
ACTIVE_STATUSES = ("queued", "departed")
RESOURCE_KINDS = ("vessel", "crew", "spare")


def default_order(entries: List[Dict[str, Any]]) -> List[int]:
    """未离港项默认顺位：影响量降序、抢修窗口开始升序、入队先后兜底。"""
    queued = [entry for entry in entries if entry["status"] == "queued"]
    ordered = sorted(queued, key=lambda entry: (-float(entry["impact_score"]), str(entry["window_start"]), int(entry["id"])))
    return [int(entry["id"]) for entry in ordered]


def _pools(resources: List[Dict[str, Any]], kind: str) -> List[Dict[str, Any]]:
    return sorted((resource for resource in resources if resource["kind"] == kind), key=lambda resource: resource["name"])


def _holds(entries: List[Dict[str, Any]]):
    vessels: Dict[str, float] = {}
    crews: Dict[str, float] = {}
    spare: Dict[str, float] = {}
    for entry in entries:
        if entry["status"] not in ACTIVE_STATUSES:
            continue
        if entry.get("vessel"):
            vessels[entry["vessel"]] = vessels.get(entry["vessel"], 0.0) + 1.0
        if entry.get("crew"):
            crews[entry["crew"]] = crews.get(entry["crew"], 0.0) + 1.0
        if entry.get("spare_depot"):
            spare[entry["spare_depot"]] = spare.get(entry["spare_depot"], 0.0) + float(entry["spare_reserved_km"])
    return vessels, crews, spare


def remaining_resources(resources: List[Dict[str, Any]], entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """各资源池的已占/余量视图，备缆余量由此体现。"""
    vessels, crews, spare = _holds(entries)
    view = []
    for resource in sorted(resources, key=lambda item: (item["kind"], item["name"])):
        if resource["kind"] == "vessel":
            used = vessels.get(resource["name"], 0.0)
        elif resource["kind"] == "crew":
            used = crews.get(resource["name"], 0.0)
        else:
            used = spare.get(resource["name"], 0.0)
        view.append({
            "name": resource["name"],
            "kind": resource["kind"],
            "capacity": resource["capacity"],
            "used": round(used, 2),
            "remaining": round(float(resource["capacity"]) - used, 2),
        })
    return view


def _allocate_one(spare_need_km: float, vessel_pool, crew_pool, spare_pool, vessels, crews, spare) -> Dict[str, Any]:
    vessel = None
    for resource in vessel_pool:
        if float(resource["capacity"]) - vessels.get(resource["name"], 0.0) >= 1.0:
            vessel = resource["name"]
            vessels[vessel] = vessels.get(vessel, 0.0) + 1.0
            break
    crew = None
    for resource in crew_pool:
        if float(resource["capacity"]) - crews.get(resource["name"], 0.0) >= 1.0:
            crew = resource["name"]
            crews[crew] = crews.get(crew, 0.0) + 1.0
            break
    depot = None
    reserved = 0.0
    best_name = None
    best_remaining = 0.0
    for resource in spare_pool:
        remaining = float(resource["capacity"]) - spare.get(resource["name"], 0.0)
        if remaining > best_remaining:
            best_name, best_remaining = resource["name"], remaining
    if best_name is not None and best_remaining > 0:
        depot = best_name
        reserved = min(float(spare_need_km), best_remaining)
        spare[depot] = spare.get(depot, 0.0) + reserved
    delta = {
        "vessel": 0 if vessel else 1,
        "crew": 0 if crew else 1,
        "spare_km": round(float(spare_need_km) - reserved, 2),
    }
    return {"vessel": vessel, "crew": crew, "spare_depot": depot, "spare_reserved_km": round(reserved, 2), "delta": delta}


def compute_plan(entries: List[Dict[str, Any]], resources: List[Dict[str, Any]], ordered_ids: List[int]) -> List[Dict[str, Any]]:
    """按新顺位重算未离港项预占；已离港项的占用保持原样并先从池中扣除。"""
    by_id = {int(entry["id"]): entry for entry in entries}
    departed = [entry for entry in entries if entry["status"] == "departed"]
    vessels, crews, spare = _holds(departed)
    vessel_pool = _pools(resources, "vessel")
    crew_pool = _pools(resources, "crew")
    spare_pool = _pools(resources, "spare")
    plan = []
    for position, entry_id in enumerate(ordered_ids, start=1):
        entry = by_id[int(entry_id)]
        allocation = _allocate_one(entry["spare_need_km"], vessel_pool, crew_pool, spare_pool, vessels, crews, spare)
        plan.append({"entry_id": int(entry_id), "record_id": entry["record_id"], "position": position, **allocation})
    return plan


def allocate_single(entries: List[Dict[str, Any]], resources: List[Dict[str, Any]], spare_need_km: float) -> Dict[str, Any]:
    """新项入队时在现有占用基础上试占一份资源。"""
    vessels, crews, spare = _holds(entries)
    return _allocate_one(spare_need_km, _pools(resources, "vessel"), _pools(resources, "crew"), _pools(resources, "spare"), vessels, crews, spare)


def _parse_window(payload: Dict[str, Any]):
    start = text(payload, "window_start")
    end = text(payload, "window_end")
    try:
        start_at = datetime.fromisoformat(start)
        end_at = datetime.fromisoformat(end)
    except ValueError as exc:
        raise ValidationError("抢修时段必须是ISO时间") from exc
    if end_at <= start_at:
        raise ValidationError("抢修时段结束必须晚于开始")
    return start, end


class DispatchService:
    def __init__(self, repository: Repository, rules: DomainRules, audit: AuditRecorder = None) -> None:
        self.repository = repository
        self.rules = rules
        self.audit = audit or AuditRecorder(repository)

    @staticmethod
    def _actor(actor: Actor) -> Actor:
        if actor is None or not actor.user_id.strip() or not actor.role.strip():
            raise PermissionDenied("缺少调用身份")
        return actor

    def _ensure_known_role(self, actor: Actor) -> None:
        if not self.rules.known_role(actor.role):
            raise PermissionDenied("角色无权访问该服务")

    def _require_dispatcher(self, actor: Actor) -> None:
        if actor.role != "admin" and actor.role not in DISPATCH_ROLES:
            raise PermissionDenied("角色无权调整调度队列")

    def queue_view(self, actor: Actor) -> Dict[str, Any]:
        actor = self._actor(actor)
        self._ensure_known_role(actor)
        entries = self.repository.list_dispatch_entries()
        resources = self.repository.list_dispatch_resources()
        meta = self.repository.dispatch_meta()
        return {
            "version": int(meta["version"]),
            "dirty": bool(meta["dirty"]),
            "resources": remaining_resources(resources, entries),
            "entries": entries,
        }

    def resources_view(self, actor: Actor) -> List[Dict[str, Any]]:
        actor = self._actor(actor)
        self._ensure_known_role(actor)
        entries = self.repository.list_dispatch_entries()
        return remaining_resources(self.repository.list_dispatch_resources(), entries)

    def add_resource(self, actor: Actor, payload: Dict[str, Any]) -> Dict[str, Any]:
        actor = self._actor(actor)
        self._ensure_known_role(actor)
        self._require_dispatcher(actor)
        payload = dict(payload or {})
        name = text(payload, "name")
        kind = choice(payload, "kind", list(RESOURCE_KINDS))
        capacity = number(payload, "capacity", 0)
        if capacity <= 0:
            raise ValidationError("capacity必须大于0")
        return self.repository.insert_dispatch_resource(name, kind, capacity)

    def enqueue(self, actor: Actor, payload: Dict[str, Any]) -> Dict[str, Any]:
        actor = self._actor(actor)
        self._ensure_known_role(actor)
        self._require_dispatcher(actor)
        payload = dict(payload or {})
        record_id = payload.get("record_id")
        if isinstance(record_id, bool) or not isinstance(record_id, int) or record_id < 1:
            raise ValidationError("record_id必须是正整数")
        record = self.repository.get(record_id)
        if record["state"] in ("restored", "cancelled"):
            raise ValidationError("故障已结束，不能加入调度队列")
        impact = number(payload, "impact_score", 0)
        window_start, window_end = _parse_window(payload)
        if payload.get("spare_need_km") is None:
            spare_need = float(record["payload"].get("required_spare_km", 0))
        else:
            spare_need = number(payload, "spare_need_km", 0)
        operation_id = optional_text(payload, "operation_id") or None
        allocate: Callable[[List[Dict[str, Any]], List[Dict[str, Any]]], Dict[str, Any]] = lambda entries, resources: allocate_single(entries, resources, spare_need)
        return self.repository.enqueue_entry(
            record_id=record_id,
            impact_score=impact,
            window_start=window_start,
            window_end=window_end,
            spare_need_km=spare_need,
            operation_id=operation_id,
            actor_id=actor.user_id,
            allocate=allocate,
        )

    def reorder(self, actor: Actor, payload: Dict[str, Any]) -> Dict[str, Any]:
        actor = self._actor(actor)
        self._ensure_known_role(actor)
        self._require_dispatcher(actor)
        payload = dict(payload or {})
        mode = optional_text(payload, "mode", "trial") or "trial"
        if mode not in ("trial", "confirm"):
            raise ValidationError("mode只能是trial/confirm")
        reason = optional_text(payload, "reason")
        entries = self.repository.list_dispatch_entries()
        resources = self.repository.list_dispatch_resources()
        queued_ids = {int(entry["id"]) for entry in entries if entry["status"] == "queued"}
        if payload.get("auto"):
            ordered_ids = default_order(entries)
        else:
            raw = payload.get("order")
            if not isinstance(raw, list) or any(isinstance(item, bool) or not isinstance(item, int) for item in raw):
                raise ValidationError("order必须是调度项整数编号列表")
            ordered_ids = [int(item) for item in raw]
            if len(ordered_ids) != len(queued_ids) or set(ordered_ids) != queued_ids:
                raise ValidationError("顺位列表必须覆盖全部未离港调度项")
        plan = compute_plan(entries, resources, ordered_ids)
        departed = [
            {"entry_id": entry["id"], "record_id": entry["record_id"], "vessel": entry["vessel"], "crew": entry["crew"], "spare_depot": entry["spare_depot"], "spare_reserved_km": entry["spare_reserved_km"]}
            for entry in entries
            if entry["status"] == "departed"
        ]
        if mode == "trial":
            meta = self.repository.dispatch_meta()
            return {"mode": "trial", "queue_version": int(meta["version"]), "entries": plan, "departed": departed, "reason": reason}
        operation_id = text(payload, "operation_id")
        expected = payload.get("expected_version")
        if isinstance(expected, bool) or not isinstance(expected, int):
            raise ValidationError("expected_version必须是整数")
        return self.repository.apply_reorder(
            operation_id=operation_id,
            expected_version=expected,
            ordered_ids=ordered_ids,
            plan=plan,
            actor_id=actor.user_id,
            reason=reason,
        )

    def depart(self, actor: Actor, entry_id: int) -> Dict[str, Any]:
        actor = self._actor(actor)
        self._ensure_known_role(actor)
        self._require_dispatcher(actor)
        return self.repository.depart_entry(int(entry_id), actor.user_id, reason="调度员确认离港")

    def recover(self, actor: Actor) -> Dict[str, Any]:
        actor = self._actor(actor)
        self._ensure_known_role(actor)
        self._require_dispatcher(actor)
        return self.repository.recover_queue(actor.user_id)

    def candidates(self, actor: Actor) -> List[Dict[str, Any]]:
        actor = self._actor(actor)
        self._ensure_known_role(actor)
        return self.repository.list_candidates()

    def events(self, actor: Actor, limit: int = 100) -> List[Dict[str, Any]]:
        actor = self._actor(actor)
        self._ensure_known_role(actor)
        return self.repository.list_dispatch_events(limit=limit)

    def entry_for_record(self, record_id: int) -> Optional[Dict[str, Any]]:
        return self.repository.get_entry_by_record(record_id)

    def on_record_action(self, actor: Actor, record: Dict[str, Any], action: str) -> None:
        """记录流程联动：动员即离港，恢复/取消即释放预占。"""
        entry = self.repository.get_active_entry_by_record(record["id"])
        if entry is None:
            return
        if action == "mobilize" and entry["status"] == "queued":
            self.repository.depart_entry(entry["id"], actor.user_id, reason="记录动作mobilize联动")
        elif action == "restore":
            self.repository.release_entry(record["id"], "completed", actor.user_id, reason="记录动作restore联动")
        elif action == "cancel":
            self.repository.release_entry(record["id"], "cancelled", actor.user_id, reason="记录动作cancel联动")
