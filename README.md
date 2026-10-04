# 跨海光缆故障与抢修协调

纯Python标准库实现的跨海光缆故障与抢修协调原型，使用SQLite持久化，HTTP接口由`http.server`提供。

## 模块结构

- `app.py`：命令行参数、依赖组装和服务启动。
- `src/domain.py`：领域数据类型、错误和基础校验。
- `src/rules.py`：状态转换、海况、船机许可、备缆窗口和接续质量和冲突检查。
- `src/repository.py`：SQLite建表、事务和查询。
- `src/service.py`：用例编排、权限检查、乐观并发和审计。
- `src/dispatch.py`：调度队列、资源试占、顺位重排、候选变更与快照恢复。
- `src/http_api.py`：HTTP路由与统一错误响应。
- `src/audit.py`：事件时间线。
- `static/index.html`：最小演示页面。
- `tests/`：完整流程、规则计算、调度队列和失败场景测试。

## 启动

```bash
python3 app.py --db ./data.db --port 8330
```

默认端口为`8330`，默认数据库位于项目目录。服务启动时自动建表。

## 主要接口

- `GET /health`：健康检查。
- `GET /`：演示页面。
- `GET /api/records`：记录列表，可带`state`和`limit`参数。
- `GET /api/records/{id}`：记录详情。
- `GET /api/records/{id}/audit`：审计时间线。
- `GET /api/stats`：状态统计。
- `POST /api/records`：创建记录，请求体为`{"reference":"...","data":{...}}`。
- `POST /api/records/{id}/actions/{action}`：执行业务动作，请求体为`{"expected_version":1,"data":{...}}`。

## 调度队列

多段同时告警时，调度员（`dispatcher`角色）把故障影响量、抢修时段和备缆余量接成可重排队列，按顺位分配船机、班组和备缆：

- `GET /api/dispatch/queue`：队列视图，含版本、脏标记、各资源池已占/余量和全部调度项（含记在原项上的差量）。
- `POST /api/dispatch/entries`：故障入队，请求体为`{"record_id":1,"impact_score":80,"window_start":"...","window_end":"...","spare_need_km":20,"operation_id":"..."}`，按到场顺序追加并试占资源。
- `POST /api/dispatch/reorders`：顺位调整。`mode:"trial"`先试占并返回差量，不写库；`mode:"confirm"`携带`operation_id`和`expected_version`确认落库。`order:[entry_id,...]`指定新顺位，或`auto:true`按影响量/抢修窗口自动排序。确认时未离港项作废旧预占并按新顺位重算，已离港项保持原依据；版本冲突时后到者只留候选变更（409响应带`details.candidate_id`），同一`operation_id`重放直接返回原结果、不二次占用。
- `POST /api/dispatch/entries/{id}/depart`：确认离港，预占转为执行依据（记录执行`mobilize`动作时自动联动）。
- `POST /api/dispatch/recover`：写入失败后从最近一次完整队列快照恢复。
- `GET /api/dispatch/candidates`：候选变更列表。
- `GET /api/dispatch/events`：队列级审计事件。
- `GET /api/dispatch/resources`、`POST /api/dispatch/resources`：资源池查看与登记（船机/班组/备缆）。

故障详情（`GET /api/records/{id}`）内嵌当前调度项，审计时间线记录每次重排的`operation_id`与队列版本，可追到重排来源。记录执行`restore`/`cancel`时自动释放预占。

除`/health`和`/`外，请求需提供`X-User-Id`、`X-Role`，可选`X-Org`。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

测试覆盖完整流程、规则计算、重复引用、权限拒绝、版本冲突，以及调度队列的试占确认、差量记账、离港保护、并发候选、幂等重放和快照恢复。
