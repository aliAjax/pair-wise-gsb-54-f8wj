"""跨海光缆故障与抢修协调领域规则与状态转换。"""
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Tuple

from .domain import Actor, Conflict, ValidationError, boolean, choice, integer, number, optional_text, text, text_list


INITIAL_STATE = "detected"
CREATE_ROLES = {'noc_operator'}
ACTION_ROLES = {'approve': {'repair_manager'}, 'mobilize': {'vessel_master'}, 'survey': {'cable_engineer'}, 'splice': {'cable_engineer'}, 'test': {'noc_operator'}, 'restore': {'noc_operator', 'repair_manager'}, 'cancel': {'repair_manager'}}
TRANSITIONS = {'approve': {'detected': 'approved'}, 'mobilize': {'approved': 'mobilized'}, 'survey': {'mobilized': 'surveyed'}, 'splice': {'surveyed': 'spliced'}, 'test': {'spliced': 'tested'}, 'restore': {'tested': 'restored'}, 'cancel': {'detected': 'cancelled', 'approved': 'cancelled', 'mobilized': 'cancelled'}}

QUEUE_ROLES = {'dispatcher'}
QUEUE_ACTIVE_STATUSES = ('trial', 'committed', 'departed')
POOL_KINDS = ['vessel', 'crew', 'spare_cable']


def parse_window_time(value: Any, key: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ValidationError("%s不能为空" % key)
    raw = value.strip()
    try:
        moment = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValidationError("%s必须是ISO时间" % key) from exc
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return moment


def plan_allocations(items: List[Dict[str, Any]], pools: Dict[str, Any], holders: List[Dict[str, Any]]) -> List[Tuple[Dict[str, Any], Dict[str, Any]]]:
    """按顺位依次为队列项分配船机、班组和备缆，差量随项返回。"""
    used_vessels = {str(a.get("vessel")) for a in holders if a.get("vessel")}
    used_crews = {str(a.get("crew")) for a in holders if a.get("crew")}
    used_spare = sum(float(a.get("spare_km", 0) or 0) for a in holders)
    vessels = [name for name in pools.get("vessels", []) if name not in used_vessels]
    crews = [name for name in pools.get("crews", []) if name not in used_crews]
    spare = max(0.0, round(float(pools.get("spare_km", 0)) - used_spare, 2))
    plans: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []
    for item in items:
        allocation = {"vessel": None, "crew": None, "spare_km": 0.0}
        shortfall = {"vessel": False, "crew": False, "spare_km": 0.0}
        if vessels:
            allocation["vessel"] = vessels.pop(0)
        else:
            shortfall["vessel"] = True
        if crews:
            allocation["crew"] = crews.pop(0)
        else:
            shortfall["crew"] = True
        required = max(0.0, float(item.get("spare_required_km", 0)))
        granted = round(min(required, spare), 2)
        allocation["spare_km"] = granted
        lack = round(required - granted, 2)
        if lack > 0:
            shortfall["spare_km"] = lack
        spare = round(spare - granted, 2)
        plans.append((allocation, shortfall))
    return plans


class DomainRules:
    INITIAL_STATE = INITIAL_STATE

    def known_role(self, role: str) -> bool:
        all_roles = set(CREATE_ROLES) | set(QUEUE_ROLES)
        for roles in ACTION_ROLES.values():
            all_roles.update(roles)
        return role == "admin" or role in all_roles

    def role_can_create(self, role: str) -> bool:
        return role == "admin" or role in CREATE_ROLES

    def role_can_action(self, role: str, action: str) -> bool:
        return role == "admin" or role in ACTION_ROLES.get(action, set())

    def validate_create(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        p = dict(payload)
        text(p, "cable")
        text(p, "segment")
        start = number(p, "start_km", 0)
        end = number(p, "end_km", 0)
        number(p, "depth_m", 1)
        integer(p, "sea_state", 0, 9)
        boolean(p, "vessel_available")
        number(p, "spare_length_km", 0)
        boolean(p, "permit_valid")
        integer(p, "capacity_gbps", 1)
        if end <= start:
            raise ValidationError("结束里程必须大于开始里程")
        return p

    def prepare_create(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        p = self.validate_create(payload)
        distance = float(p["end_km"]) - float(p["start_km"])
        p["repair_distance_km"] = round(distance, 2)
        p["required_spare_km"] = round(distance * 1.05, 2)
        p["estimated_repair_hours"] = round(distance / 2.0 + float(p["depth_m"]) / 100.0 + int(p["sea_state"]) * 2.0, 2)
        p["repair_feasible"] = bool(p["vessel_available"] and p["permit_valid"] and p["spare_length_km"] >= p["required_spare_km"] and int(p["sea_state"]) <= 5)
        return p

    def check_create_conflicts(self, payload: Dict[str, Any], existing: Iterable[Dict[str, Any]]) -> None:
        for item in existing:
            if item["state"] in {"restored", "cancelled"} or item["payload"].get("cable") != payload.get("cable") or item["payload"].get("segment") != payload.get("segment"):
                continue
            if float(payload["start_km"]) < float(item["payload"].get("end_km", 0)) and float(payload["end_km"]) > float(item["payload"].get("start_km", 0)):
                raise Conflict("同一光缆区段已有未结束抢修")

    def require_transition(self, record: Dict[str, Any], action: str) -> str:
        allowed = TRANSITIONS.get(action, {}).get(record["state"])
        if allowed is None:
            raise Conflict("当前状态不允许执行%s" % action)
        return allowed

    def apply_action(self, record: Dict[str, Any], action: str, data: Dict[str, Any]) -> Tuple[str, Dict[str, Any], str]:
        new_state = self.require_transition(record, action)
        data = dict(data or {})
        p = dict(record["payload"])
        changes: Dict[str, Any] = {}
        summary = ""
        if action == "approve":
            if not bool(p["permit_valid"]) or not bool(p["vessel_available"]):
                raise ValidationError("许可或船舶条件不满足")
            changes["repair_manager"] = text(data, "repair_manager")
            summary = "抢修方案已批准"
        elif action == "mobilize":
            if float(data.get("weather_window_hours", 0)) < float(p["estimated_repair_hours"]):
                raise ValidationError("海况窗口不足以完成抢修")
            if float(data.get("available_spare_km", 0)) < float(p["required_spare_km"]):
                raise ValidationError("船上备缆不足")
            changes["weather_window_hours"] = float(data["weather_window_hours"])
            changes["vessel_name"] = text(data, "vessel_name")
            summary = "抢修船已动员"
        elif action == "survey":
            if not boolean(data, "survey_complete"):
                raise ValidationError("勘察尚未完成")
            fault_km = number(data, "fault_location_km", 0)
            if not (float(p["start_km"]) <= fault_km <= float(p["end_km"])):
                raise ValidationError("故障点不在申报区段")
            changes["fault_location_km"] = fault_km
            summary = "故障点勘察完成"
        elif action == "splice":
            loss = number(data, "splice_loss_db", 0)
            if loss > 0.2:
                raise ValidationError("接续损耗超过阈值")
            if float(data.get("spare_used_km", 0)) < float(p["repair_distance_km"]):
                raise ValidationError("备缆使用长度不足")
            changes["splice_loss_db"] = loss
            changes["spare_used_km"] = float(data["spare_used_km"])
            summary = "光缆接续完成"
        elif action == "test":
            end_loss = number(data, "end_to_end_loss_db", 0)
            if end_loss > 0.5:
                raise ValidationError("端到端损耗不合格")
            changes["end_to_end_loss_db"] = end_loss
            changes["test_passed"] = True
            summary = "系统测试通过"
        elif action == "restore":
            if not boolean(data, "traffic_restored"):
                raise ValidationError("业务流量尚未恢复")
            changes["traffic_restored"] = True
            changes["restore_capacity_gbps"] = integer(data, "restore_capacity_gbps", 1)
            summary = "通信恢复"
        elif action == "cancel":
            changes["cancel_reason"] = text(data, "cancel_reason")
            summary = "抢修取消"
        p.update(changes)
        return new_state, p, summary or ("已执行%s" % action)

    # ---- 调度队列规则 ----

    def role_can_queue(self, role: str) -> bool:
        return role == "admin" or role in QUEUE_ROLES

    @staticmethod
    def pools_dict(pool_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        return {
            "vessels": sorted(str(r["name"]) for r in pool_rows if r["kind"] == "vessel"),
            "crews": sorted(str(r["name"]) for r in pool_rows if r["kind"] == "crew"),
            "spare_km": round(sum(float(r["capacity"]) for r in pool_rows if r["kind"] == "spare_cable"), 2),
        }

    def resources_view(self, pool_rows: List[Dict[str, Any]], items: List[Dict[str, Any]]) -> Dict[str, Any]:
        holders = [it["allocation"] for it in items if it["status"] in ("committed", "departed")]
        used_vessels = {str(a.get("vessel")) for a in holders if a.get("vessel")}
        used_crews = {str(a.get("crew")) for a in holders if a.get("crew")}
        used_spare = round(sum(float(a.get("spare_km", 0) or 0) for a in holders), 2)
        spare_total = round(sum(float(r["capacity"]) for r in pool_rows if r["kind"] == "spare_cable"), 2)
        return {
            "vessels": [{"name": r["name"], "available": r["name"] not in used_vessels} for r in pool_rows if r["kind"] == "vessel"],
            "crews": [{"name": r["name"], "available": r["name"] not in used_crews} for r in pool_rows if r["kind"] == "crew"],
            "spare_cable": {"total": spare_total, "used": used_spare, "remaining": round(spare_total - used_spare, 2)},
        }

    def validate_window(self, data: Dict[str, Any]) -> Tuple[str, str]:
        start_raw = text(data, "window_start")
        end_raw = text(data, "window_end")
        if parse_window_time(start_raw, "window_start") >= parse_window_time(end_raw, "window_end"):
            raise ValidationError("抢修时段结束必须晚于开始")
        return start_raw, end_raw

    def validate_enqueue(self, data: Dict[str, Any], record_payload: Dict[str, Any]) -> Dict[str, Any]:
        p = dict(data or {})
        key = text(p, "idempotency_key")
        window_start, window_end = self.validate_window(p)
        impact = number({"impact_score": p.get("impact_score", record_payload.get("capacity_gbps"))}, "impact_score", 0)
        spare = number({"spare_required_km": p.get("spare_required_km", record_payload.get("required_spare_km"))}, "spare_required_km", 0)
        return {
            "idempotency_key": key,
            "window_start": window_start,
            "window_end": window_end,
            "impact_score": round(impact, 2),
            "spare_required_km": round(spare, 2),
            "confirm": boolean(p, "confirm", False),
        }

    def validate_reorder(self, data: Dict[str, Any]) -> Dict[str, Any]:
        p = dict(data or {})
        expected = integer(p, "expected_version", 0)
        dry_run = boolean(p, "dry_run", False)
        order_raw = p.get("order")
        if not isinstance(order_raw, list) or any(isinstance(i, bool) or not isinstance(i, int) for i in order_raw):
            raise ValidationError("order必须是队列项ID列表")
        reorder_id = optional_text(p, "reorder_id")
        if not dry_run and not reorder_id:
            raise ValidationError("reorder_id不能为空")
        return {
            "expected_version": expected,
            "order": [int(i) for i in order_raw],
            "reorder_id": reorder_id,
            "note": optional_text(p, "note"),
            "dry_run": dry_run,
        }

    def validate_pool(self, data: Dict[str, Any]) -> Tuple[str, str, float]:
        p = dict(data or {})
        kind = choice(p, "kind", POOL_KINDS)
        name = text(p, "name")
        capacity = number(p, "capacity", 0)
        if kind in ("vessel", "crew"):
            capacity = 1.0
        return kind, name, round(capacity, 2)

    def plan_single(self, item: Dict[str, Any], pools: Dict[str, Any], holders: List[Dict[str, Any]]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        return plan_allocations([item], pools, holders)[0]

    def plan_reorder(self, items: List[Dict[str, Any]], pools: Dict[str, Any], order: List[int], new_version: int) -> Dict[int, Dict[str, Any]]:
        """按新顺位重算：未离港项作废旧预占重新分配，已离港项保留原依据。"""
        by_id = {int(it["id"]): it for it in items}
        order_ids = [int(i) for i in order]
        if len(set(order_ids)) != len(order_ids):
            raise ValidationError("顺位列表存在重复项")
        if set(order_ids) != set(by_id.keys()):
            raise ValidationError("顺位列表必须覆盖全部未结束队列项")
        departed = [by_id[i] for i in order_ids if by_id[i]["status"] == "departed"]
        committed = [by_id[i] for i in order_ids if by_id[i]["status"] == "committed"]
        fixed = [it["allocation"] for it in departed]
        committed_plans = plan_allocations(committed, pools, fixed)
        holders = fixed + [allocation for allocation, _ in committed_plans]
        committed_iter = iter(committed_plans)
        changes: Dict[int, Dict[str, Any]] = {}
        for position, item_id in enumerate(order_ids, start=1):
            item = by_id[item_id]
            if item["status"] == "departed":
                changes[item_id] = {"position": position, "allocation": item["allocation"], "shortfall": item["shortfall"], "basis_version": item["basis_version"], "kept_basis": True}
            elif item["status"] == "committed":
                allocation, shortfall = next(committed_iter)
                changes[item_id] = {"position": position, "allocation": allocation, "shortfall": shortfall, "basis_version": new_version, "kept_basis": False}
            else:
                allocation, shortfall = plan_allocations([item], pools, holders)[0]
                changes[item_id] = {"position": position, "allocation": allocation, "shortfall": shortfall, "basis_version": new_version, "kept_basis": False}
        return changes

    def suggest_order(self, items: List[Dict[str, Any]]) -> List[int]:
        """建议顺位：已离港项位置固定，其余按影响量降序、抢修时段升序排列。"""
        ordered = sorted(items, key=lambda it: it["position"])
        pinned = {idx: it["id"] for idx, it in enumerate(ordered) if it["status"] == "departed"}
        movable = sorted(
            (it for it in ordered if it["status"] != "departed"),
            key=lambda it: (-float(it["impact_score"]), parse_window_time(it["window_start"], "window_start"), int(it["id"])),
        )
        iterator = iter(movable)
        result: List[int] = []
        for idx in range(len(ordered)):
            if idx in pinned:
                result.append(int(pinned[idx]))
            else:
                result.append(int(next(iterator)["id"]))
        return result
