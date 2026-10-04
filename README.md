# 跨海光缆故障与抢修协调

纯Python标准库实现的跨海光缆故障与抢修协调原型，使用SQLite持久化，HTTP接口由`http.server`提供。

## 模块结构

- `app.py`：命令行参数、依赖组装和服务启动。
- `src/domain.py`：领域数据类型、错误和基础校验。
- `src/rules.py`：状态转换、海况、船机许可、备缆窗口和接续质量和冲突检查。
- `src/repository.py`：SQLite建表、事务和查询。
- `src/service.py`：用例编排、权限检查、乐观并发和审计。
- `src/http_api.py`：HTTP路由与统一错误响应。
- `src/audit.py`：事件时间线。
- `static/index.html`：最小演示页面。
- `tests/`：完整流程、规则计算和失败场景测试。

## 启动

```bash
python3 app.py --db ./data.db --port 8330
```

默认端口为`8330`，默认数据库位于项目目录。服务启动时自动建表。

## 主要接口

- `GET /health`：健康检查。
- `GET /`：演示页面。
- `GET /api/records`：记录列表，可带`state`和`limit`参数。
- `GET /api/records/{id}`：记录详情，附带该故障的队列项信息。
- `GET /api/records/{id}/audit`：审计时间线，含队列重排来源事件。
- `GET /api/stats`：状态统计。
- `POST /api/records`：创建记录，请求体为`{"reference":"...","data":{...}}`。
- `POST /api/records/{id}/actions/{action}`：执行业务动作，请求体为`{"expected_version":1,"data":{...}}`。

除`/health`和`/`外，请求需提供`X-User-Id`、`X-Role`，可选`X-Org`。

## 调度队列

多段同时告警时，故障影响量、抢修时段和备缆需求组成可重排的调度队列，船机、班组和备缆按顺位分配。队列操作需要`dispatcher`角色（`admin`亦可）。

- `GET /api/queue`：当前队列版本、队列项（顺位、状态、占用、差量、依据版本）和资源余量。
- `POST /api/queue/items`：故障入队，请求体`{"record_id":1,"idempotency_key":"k-1","impact_score":400,"window_start":"...","window_end":"...","spare_required_km":15.75,"confirm":false}`。先入队试占，差量直接记在该项上；`confirm:true`直接确认占用。同一`idempotency_key`重放返回原项，不会二次占用。
- `POST /api/queue/items/{id}/confirm`：试占项确认占用资源。
- `POST /api/queue/items/{id}/depart`：确认离港，存在未解决差量的项不能离港；离港项按原依据执行。
- `POST /api/queue/items/{id}/release`：释放资源，请求体可带`reason`。
- `POST /api/queue/reorder`：顺位调整，请求体`{"expected_version":3,"order":[5,3,4],"reorder_id":"r-1","note":"...","dry_run":false}`。`dry_run:true`先试算不落库。版本匹配时未离港项作废旧预占按新顺位重算、已离港项保留原依据；版本过期时只登记候选变更（返回202），不重复占资源；同一`reorder_id`重放不二次生效。
- `GET /api/queue/candidates`：候选变更列表。
- `POST /api/queue/candidates/{id}/apply`：按当前版本应用候选变更。
- `POST /api/queue/candidates/{id}/discard`：废弃候选变更。
- `GET /api/queue/resources`：资源池与余量。
- `POST /api/queue/resources`：新增或调整资源，请求体`{"kind":"vessel|crew|spare_cable","name":"...","capacity":40}`。
- `GET /api/queue/suggestion`：按影响量降序、抢修时段升序给出建议顺位及试算结果。
- `GET /api/queue/versions`：队列版本历史（每次变更的完整快照）。
- `GET /api/queue/events`：队列事件流。
- `POST /api/queue/recover`：从最近一次完整队列快照恢复；服务启动时发现未完成的写入也会自动恢复。

故障记录取消或恢复时，其队列项自动释放资源。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

测试覆盖完整流程、规则计算、重复引用、权限拒绝、版本冲突，以及调度队列的试占差量、重排重算、已离港项保留依据、并发候选、幂等重放、快照恢复和审计追溯。
