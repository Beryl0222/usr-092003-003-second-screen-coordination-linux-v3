# 第二现场承载协同

面向城市赛事“第二现场”的容量协同服务：维护各点位的开放时段、消防上限、
无障碍条件与周边交通；支撑现场断网照记、联网幂等补传；按预计到达时刻、
数据新鲜度与剩余空间导流；停电、强对流预警或转播授权中断时立即停推并
保留当时的建议依据；赛后用同一批事件复原溢出流向与处置速度，且全程不
收集观众身份、统计结果做不到还原个人行程。

## 运行

```bash
python3 service.py --check                       # 配置自检
python3 service.py --port 8000                   # 启动服务
python3 service.py --port 8000 --queue q.jsonl   # 现场模式：入账失败先落本地队列
python3 -m unittest discover -s tests -v         # 全部契约测试（29 项）
```

仅依赖 Python 3.10+ 标准库；`requirements.txt` 为测试用 pytest。

## 角色

角色经请求头 `X-Role` 传入：`admin`（台账/全局）、`supervisor`（主管席）、
`operator`、`merchant`（现场/商户）。现场账号须带 `X-Venue-Id`，只能记录
与查看本点位数据；商户之间看不到彼此点位的实时人数，主管席只看区级聚合。

## 接口

| 方法 | 路径 | 权限 | 说明 |
| --- | --- | --- | --- |
| GET | `/health` | 公开 | 服务巡检 |
| GET | `/venues` | 公开 | 点位静态目录（时段/无障碍/交通，不含实时人数） |
| POST | `/admin/venues` | admin | 维护点位台账，支持跨午夜开放时段 |
| POST | `/events` | 现场（本点位） | 单条 `entry`/`exit`/`headcount`/`pause`/`resume` |
| POST | `/sync` | 现场（本点位） | 断网后批量补传，按 `event_id` 幂等 |
| GET | `/venues/{id}/status` | 本点位/主管 | 在场人数、余量、暂停、开放、数据新鲜度 |
| POST | `/recommend` | 公开 | 导流建议，入参可带 `from`、`travel_seconds`、`party_size`、`filters` |
| GET | `/recommendations/{id}` | 公开 | 回看该次建议的候选与排除原因（为何推荐） |
| POST | `/halts` | 主管 | `power_outage`/`severe_weather`/`broadcast_rights`，点位级或全局 |
| POST | `/halts/{id}/resolve` | 主管 | 解除停推 |
| GET | `/halts` | 主管 | 停推指令清单 |
| GET | `/supervisor/zones` | 主管 | 匿名跨区压力（区级占用率/开放/暂停/停推计数） |
| GET | `/replay?from=&to=&k=` | 主管 | 赛后：处置速度、occupancy 序列、k 匿名溢出流向 |

## 关键设计

**重复补传不双计。** 事件由现场端生成 `event_id`，服务端去重后回放事件流
得到在场人数；`headcount` 为现场盘点绝对值，用于纠偏进出累计误差。离线
队列（`offline_queue.py`）在不确定送达时宁可重发，服务端判为 `duplicate`
后同样视为补传成功；发送失败的后缀条目保留待下次整批重试。

**导流三要素。** 推荐对每个候选计算步行 ETA（或直接给 `travel_seconds`）、
到达时刻是否仍在开放时段、按近 10 分钟净流速外推的到达时余量；人数数据
超过 5 分钟（`STALE_AFTER_SECONDS`）或从无上报的点位不进候选。排序按
「到达时余量 − 行程代价」。

**停推收口但留痕。** 全局停推时 `options` 立即清空；点位级停推、暂停、
满员、到达时闭店只排除对应点位。每次推荐（含被排除的具体原因、约 1km
网格的出发点）都以 `recommendation_id` 留档，可回看“当时为什么给出那条
建议”，停推解除后自动恢复推荐。

**隐私内建而非事后脱敏。** 事件白名单校验，直接拒绝 `phone/device_id/
openid` 等观众身份字段；主管视图只有区级聚合；赛后流向以区域为端点并做
k 匿名（默认 k=5），低于阈值的流向整条抑制且不回零头总量；occupancy 序列
只有时间桶与聚合人数，无法据此拼出任何观众的行程。

**赛后复原。** `/replay` 用同一批不可变事件回放：合并满员/暂停/停推得到
连续受压区间，把受压后 30 分钟内其他点位的净增归为溢出承接；处置速度给
出暂停时长与停推解除时长的 min/中位/p90/max。
