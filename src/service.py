"""业务用例编排、权限检查与审计。"""
from typing import Any, Dict, List, Optional

from .audit import AuditRecorder
from .domain import Actor, Conflict, PermissionDenied, integer, text
from .repository import Repository
from .rules import DomainRules


class Service:
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

    def create(self, actor: Actor, reference: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        actor = self._actor(actor)
        self._ensure_known_role(actor)
        if not self.rules.role_can_create(actor.role):
            raise PermissionDenied("角色无权创建记录")
        reference = text({"reference": reference}, "reference")
        prepared = self.rules.prepare_create(payload or {})
        self.rules.check_create_conflicts(prepared, self.repository.list_records(limit=500))
        return self.repository.create(reference, self.rules.INITIAL_STATE, prepared, actor.user_id)

    def list_records(self, actor: Actor, state: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        actor = self._actor(actor)
        self._ensure_known_role(actor)
        return self.repository.list_records(state=state, limit=limit)

    def get_record(self, actor: Actor, record_id: int) -> Dict[str, Any]:
        actor = self._actor(actor)
        self._ensure_known_role(actor)
        record = self.repository.get(record_id)
        record["queue"] = self.repository.active_item_for_record(record_id)
        return record

    def act(self, actor: Actor, record_id: int, expected_version: int, action: str, data: Dict[str, Any]) -> Dict[str, Any]:
        actor = self._actor(actor)
        self._ensure_known_role(actor)
        action = text({"action": action}, "action")
        if not self.rules.role_can_action(actor.role, action):
            raise PermissionDenied("角色无权执行该操作")
        record = self.repository.get(record_id)
        self.rules.require_transition(record, action)
        new_state, new_payload, summary = self.rules.apply_action(record, action, data or {})
        result = self.repository.mutate(
            record_id=record_id,
            expected_version=int(expected_version),
            state=new_state,
            payload=new_payload,
            actor_id=actor.user_id,
            action=action,
            details={"summary": summary, "input": data or {}, "from": record["state"], "to": new_state},
        )
        if action in ("cancel", "restore"):
            reason = "记录已取消" if action == "cancel" else "记录已恢复"
            self.repository.release_active_item_for_record(record_id, actor.user_id, reason)
        return result

    def timeline(self, actor: Actor, record_id: int) -> List[Dict[str, Any]]:
        actor = self._actor(actor)
        self._ensure_known_role(actor)
        return self.audit.timeline(record_id)

    def stats(self, actor: Actor) -> Dict[str, int]:
        actor = self._actor(actor)
        self._ensure_known_role(actor)
        return self.repository.stats()

    # ---- 调度队列 ----

    def _ensure_queue_role(self, actor: Actor) -> None:
        if not self.rules.role_can_queue(actor.role):
            raise PermissionDenied("角色无权操作调度队列")

    def _queue_actor(self, actor: Actor) -> Actor:
        actor = self._actor(actor)
        self._ensure_known_role(actor)
        self._ensure_queue_role(actor)
        return actor

    def _holders(self, items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return [it["allocation"] for it in items if it["status"] in ("committed", "departed")]

    def get_queue(self, actor: Actor) -> Dict[str, Any]:
        actor = self._actor(actor)
        self._ensure_known_role(actor)
        version, items, pools = self.repository.queue_state()
        return {"version": version, "items": items, "resources": self.rules.resources_view(pools, items)}

    def enqueue(self, actor: Actor, data: Dict[str, Any]) -> Dict[str, Any]:
        actor = self._queue_actor(actor)
        record_id = integer(data or {}, "record_id", 1)
        record = self.repository.get(record_id)
        if record["state"] in ("restored", "cancelled"):
            raise Conflict("记录已结束，不能加入调度队列")
        params = self.rules.validate_enqueue(data, record["payload"])

        def plan(items, pools, draft):
            return self.rules.plan_single(draft, self.rules.pools_dict(pools), self._holders(items))

        item, created, version = self.repository.enqueue_item(
            record_id, params["idempotency_key"], params["impact_score"], params["window_start"],
            params["window_end"], params["spare_required_km"], params["confirm"], actor.user_id, plan,
        )
        return {"item": item, "created": created, "replayed": not created, "queue_version": version}

    def confirm_item(self, actor: Actor, item_id: int) -> Dict[str, Any]:
        actor = self._queue_actor(actor)

        def plan(items, pools, item):
            return self.rules.plan_single(item, self.rules.pools_dict(pools), self._holders(items))

        item, version = self.repository.confirm_item(item_id, actor.user_id, plan)
        return {"item": item, "queue_version": version}

    def depart_item(self, actor: Actor, item_id: int) -> Dict[str, Any]:
        actor = self._queue_actor(actor)
        item, version = self.repository.depart_item(item_id, actor.user_id)
        return {"item": item, "queue_version": version}

    def release_item(self, actor: Actor, item_id: int, reason: str = "") -> Dict[str, Any]:
        actor = self._queue_actor(actor)
        item, version = self.repository.release_item(item_id, actor.user_id, reason or "调度释放")
        return {"item": item, "queue_version": version}

    def reorder(self, actor: Actor, data: Dict[str, Any]) -> Dict[str, Any]:
        actor = self._queue_actor(actor)
        params = self.rules.validate_reorder(data)
        if params["dry_run"]:
            version, items, pools = self.repository.queue_state()
            changes = self.rules.plan_reorder(items, self.rules.pools_dict(pools), params["order"], version + 1)
            return {"result": "dry_run", "queue_version": version, "changes": changes}

        def plan(items, pools, order, new_version):
            return self.rules.plan_reorder(items, self.rules.pools_dict(pools), order, new_version)

        return self.repository.submit_reorder(params["expected_version"], params["order"], params["reorder_id"], actor.user_id, params["note"], plan)

    def list_candidates(self, actor: Actor) -> List[Dict[str, Any]]:
        actor = self._queue_actor(actor)
        return self.repository.list_candidates()

    def apply_candidate(self, actor: Actor, candidate_id: int) -> Dict[str, Any]:
        actor = self._queue_actor(actor)
        candidate = self.repository.get_candidate(candidate_id)
        if candidate["status"] != "pending":
            raise Conflict("候选变更已处理")
        current = self.repository.queue_version()

        def plan(items, pools, order, new_version):
            return self.rules.plan_reorder(items, self.rules.pools_dict(pools), order, new_version)

        return self.repository.submit_reorder(current, candidate["proposed_order"], "candidate:%d" % candidate_id, actor.user_id, candidate.get("note", ""), plan, candidate_id=candidate_id)

    def discard_candidate(self, actor: Actor, candidate_id: int) -> Dict[str, Any]:
        actor = self._queue_actor(actor)
        return {"candidate": self.repository.discard_candidate(candidate_id, actor.user_id)}

    def list_resources(self, actor: Actor) -> Dict[str, Any]:
        actor = self._actor(actor)
        self._ensure_known_role(actor)
        version, items, pools = self.repository.queue_state()
        return {"items": pools, "remaining": self.rules.resources_view(pools, items), "queue_version": version}

    def upsert_resource(self, actor: Actor, data: Dict[str, Any]) -> Dict[str, Any]:
        actor = self._queue_actor(actor)
        kind, name, capacity = self.rules.validate_pool(data)
        version = self.repository.upsert_pool(kind, name, capacity, actor.user_id)
        return {"kind": kind, "name": name, "capacity": capacity, "queue_version": version}

    def list_queue_versions(self, actor: Actor) -> List[Dict[str, Any]]:
        actor = self._queue_actor(actor)
        return self.repository.list_queue_versions()

    def queue_events(self, actor: Actor, limit: int = 100) -> List[Dict[str, Any]]:
        actor = self._queue_actor(actor)
        return self.repository.list_queue_events(limit)

    def recover_queue(self, actor: Actor) -> Dict[str, Any]:
        actor = self._queue_actor(actor)
        version = self.repository.recover_queue(actor.user_id)
        return {"recovered": True, "queue_version": version}

    def suggest_order(self, actor: Actor) -> Dict[str, Any]:
        actor = self._queue_actor(actor)
        version, items, pools = self.repository.queue_state()
        order = self.rules.suggest_order(items)
        changes = self.rules.plan_reorder(items, self.rules.pools_dict(pools), order, version + 1)
        return {"queue_version": version, "order": order, "changes": changes}
