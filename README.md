# 第二现场承载协同

面向城市赛事"第二现场"（商圈大屏、社区广场等赛外观赛点）的容量协同服务：各点位维护开放时段、
消防上限、无障碍条件与周边交通；现场人员短时断网时照常记录进出与暂停，联网补传按事件号幂等；
公众导流结合预计到达时刻、数据新鲜度与剩余空间；停电 / 强对流预警 / 转播授权中断即熔断停止推荐，
但建议留痕可回看；商户只见本点位，主管席查看匿名跨区压力；赛后用同一批事件复原溢出流向与处置速度，
全程 k 匿名抑制，无法由统计结果拼出个人行程。

## 运行

```bash
pip install -r requirements.txt
python3 service.py --check
python3 service.py --port 8000
python3 -m unittest discover -s tests -v   # 33 个测试
```

## 接口

| 方法 | 路径 | 角色 | 说明 |
|---|---|---|---|
| GET | `/health` | 任意 | 健康检查 |
| POST | `/admin/venues` | supervisor | 建立点位档案（开放时段、消防上限、无障碍、交通、坐标） |
| GET | `/venues` | 任意 | 点位档案；merchant 自动收窄到本点位 |
| GET | `/venues/{id}` | 任意 | 点位状态；公网只返回 `status` 与剩余档位 `remaining_band`（none/low/medium/high），不暴露精确计数 |
| POST | `/venues/{id}/events` | merchant（本点位） | 单条上报：`enter` / `exit` / `suspend` / `resume` |
| POST | `/venues/{id}/events`（数组或 `{"events":[...]}`） | merchant | 断网恢复后的批量补传 |
| POST | `/alerts`、`POST /alerts/{id}/resolve` | supervisor | 发布 / 解除 `power_outage` / `weather` / `broadcast` 预警（global/district/venue 范围） |
| POST | `/recommendations` | 任意 | 公众导流，见下 |
| GET | `/advice/{id}` | 任意 | 回看某条建议当时的候选、排序理由与排除原因 |
| GET | `/pressure` | supervisor | 匿名跨区压力（区级聚合占用率、点位级状态但无人员标识） |
| GET | `/replay` | supervisor | 赛后复盘：区级溢出边、未满足需求、突发事件处置速度 |

角色通过请求头声明（演示用）：`X-Role: merchant|supervisor`，商户再带 `X-Venue-Id`。

## 关键设计

**断网照常记录、补传绝不二次计数。** 每条事件带客户端生成的稳定 `event_id`，重放返回
`{"duplicate": true}` 并回原结果；事件按 `(occurred_at, event_id)` 重排，迟到事件不会污染
历史时点的占用推导；未来时间超过 30 秒时钟偏移直接拒绝。

**计数校验。** `enter` 超消防上限、暂停期间进场、`exit` 使在场为负、重复 suspend/resume
均被拒绝并记录原因；被拒的进场需求单独入账，作为赛后溢出流向的源。

**导流排序。** 评分 = 100×剩余比例 − 2×路程分钟 − 1×数据龄期分钟，仅对"可达"候选排序。
到达时不在开放时段、计数缺失或超过 15 分钟未更新、已满、暂停、命中预警、不满足无障碍要求的点位
列入 `excluded` 并给出原因。无导航上游时按坐标步行 1.4 m/s 保守估算 ETA。

**熔断与留痕。** 全局预警使推荐列表清空并返回 `halt_reasons`；每条建议（含当时的推荐、排除理由、
生效预警 ID、**仅区级**来源）留存，熔断后 `GET /advice/{id}` 仍可回答"当时为什么这么引导"。

**隐私边界。** 建议记录不落精确坐标 / 手机号等任何标识；压力图只做区级聚合；赛后溢出只输出
"源区→目的区"的按比例归因边，估算人数 < k（k=3）的边抑制进 `suppressed_flow_count`，
处置吞吐在离场样本 < k 时同样抑制为 `null`。任何聚合结果都无法回溯到某个观众。

## 示例

```bash
# 主管建档
curl -s -XPOST localhost:8000/admin/venues -H 'X-Role: supervisor' -H 'Content-Type: application/json' -d '{
  "id":"v1","name":"商圈大屏","district":"静安","fire_capacity":500,
  "opening_hours":[{"open_at":"2026-09-27T11:00:00Z","close_at":"2026-09-27T15:00:00Z"}],
  "accessible":true,"transit":["地铁2号线"],"lat":31.23,"lng":121.47}'

# 商户上报（断网恢复后原样重发即可，event_id 去重）
curl -s -XPOST localhost:8000/venues/v1/events -H 'X-Role: merchant' -H 'X-Venue-Id: v1' \
  -H 'Content-Type: application/json' \
  -d '{"event_id":"dev-a-0001","op":"enter","count":30,"occurred_at":"2026-09-27T11:20:00Z"}'

# 公众求推荐
curl -s -XPOST localhost:8000/recommendations -H 'Content-Type: application/json' -d '{
  "origin":{"district":"静安","lat":31.23,"lng":121.47},"requires_accessible":true}'
```
