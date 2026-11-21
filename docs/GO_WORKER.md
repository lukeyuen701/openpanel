# Go worker：规划与进度

本文件是 `feat/go-worker` 分支的工作计划，进度记录在文末。

## 0. 结论：合在 Go 接收端里，但分成两个进程

- **同一个 Go 模块、同一个镜像**：沿用 `apps/ingest` 的 `go.mod`，复用已有的 `kafka`（franz-go）、`kv`（rueidis）、`config`、`jsjson`（JS 键序 JSON）、`identity`、`lookups`（Postgres/Redis 缓存读取）、`geo`、metrics。新增 `cmd/worker`（或 `ingest worker` 子命令），和 TS 的 `ROLE=api|worker` 同一个思路。
- **不同进程、不同容器**，理由：
  1. `/track` 在 ClickHouse 或 worker 出问题时必须照常接收（事件先进 Kafka），这是升级文档对用户的承诺。worker 卡在 ClickHouse 写入、内存涨满或崩溃时，不能连累接收端。
  2. 扩容维度不同：接收端按 HTTP 流量扩，worker 按分区扩（默认 24 个分区，副本超过分区数就分不到）。
  3. 资源特性不同：接收端要低延迟，worker 是批量 IO 和大内存缓冲。
  4. 发布节奏不同：worker 迁移期要频繁灰度、回滚，接收端已经稳定。

## 1. 范围（TS 非测试代码行数，取自 wc）

| 层 | 内容 | 规模 | Go 化收益 |
|---|---|---|---|
| A 消费热路径 | Kafka 消费（partition lanes、同 key 串行、重试、死信、durability window）、incoming-event-handler（项目过滤、会话归属、createEvent、profile upsert、AI 事件、通知规则匹配、首个事件标记）、各 buffer 的写入 | ingest 模块约 4.8k（不含 bots 数据）+ buffers 约 3.0k + event 2.1k + profile 1.8k + session 2.2k + group 1.2k（部分属于 API） | 高：每个事件都走 |
| B 刷写与会话生命周期 | 8 个 buffer 的 flush（event、session、profile、profileBackfill、bot、replay、group、aiEvent）、session-end、reaper、vacuum、usage 计数 | 含在上面的 buffers/session 里 | 中到高：按批次跑，但持续占 CPU |
| C 其余 6 个队列 | notification、import、insights、gsc、cohortCompute，以及 cron 里的 salt、organization、onboarding、integration、misc、cohortRefresh 等 | import 3.7k、insight 4.1k、integration 3.1k、organization 2.3k、ai 2.2k、gsc 1.9k、cohort 1.8k、notification 1.0k、misc 0.9k、onboarding 0.6k、salt 0.15k（含 tRPC 也用的 service），另有 23 个 React Email 模板 | 低：量小，主要是业务逻辑和外部 API |

C 层的这些 service 同时服务 tRPC（`ctx.services.cohort/gsc/insight/organization`），任务的生产者也在 TS API 里（import、cohortCompute、gscProjectBackfill）。把 C 层改成 Go 不是"搬走"，而是**同一份业务逻辑永久维护两份**。是否做 C 层由你决定；下面把它放在最后一个阶段，每个队列单独评估。

## 2. 必须保持一致的契约

迁移期 TS 和 Go 会同时读写同一份状态，回滚也靠这一点，所以这些是硬约束：

| 契约 | 现状 | Go 侧要点 |
|---|---|---|
| Kafka 输入记录 | `IncomingEventPayload`（`incoming-event.ts`，注释写明是跨版本兼容的线上格式）。生产方有两个：Go 接收端，以及 Bun API（被转发的请求、identify 里带的 track、AI 事件的 `ai` 草稿）；还有旧版本留下的记录（`id` 可能缺失），以及从死信重放回来的记录 | 接收端的 goldens 只覆盖它自己产生的那部分。消费端的 goldens 还要补上 Bun 产生的记录、缺少 `id` 的旧记录，以及 `headers` 里带 request-id 的情况 |
| 消费组 | 组名 `openpanel-events`，协议名 `RoundRobinAssigner` v0（`kafka.ts:54-61`，沿用 kafkajs 的名字） | franz-go 自带的 RoundRobinBalancer 协议名是 `roundrobin`，会触发 `INCONSISTENT_GROUP_PROTOCOL`。需要自定义 `GroupBalancer`，用同样的名字和标准 ConsumerProtocol 编码。**可行性待实测**：TS 和 Go 成员能否混在一个组里，决定能不能按分区灰度 |
| 处理语义 | 同 key 串行（`consumer.ts:384`）；先确认 Redis 写入成功再提交 offset（durability window）；失败重试、死信写入一个有上限的 Redis 列表；缓冲超过 `maxBuffered` 就暂停拉取 | 一条一条照搬，以 `consumer.test.ts`（1.2k 行）、`partition-lanes.test.ts` 为规格 |
| Redis buffer 格式 | event 列表（JSONEachRow 行）；session 的 `session:<project>:<device>`、`session:profile:*`、`session:wallclock:*` 和 `session:projects`；profile、group、replay、bot、aiEvent、profileBackfill 各自的 key；flush 锁（`base-buffer.ts` 里的 Lua） | **长期契约**。已核实 Bun API 也在写其中几个：`ingest.service.ts` 写 replay（490 行）、group（509 行），identify 走 `upsertProfile` 写 profile，机器人流量走 `createBotEvent` 写 bot。所以 Go 的 flush 必须读 TS 写入的格式，这部分 TS 代码会一直保留。消费端写的是 event、profile（来自事件）、aiEvent 和 session；profileBackfill 由 session-end 写 |
| BullMQ 任务 | 消费路径会往两个 BullMQ 队列写任务：`sessions`（会话结束，jobId 去重、带重试和指数退避）和 `notification`（规则命中后先写 Postgres 的通知行，再入队 `sendNotification`）。任务数据是 `{payload, meta}` 信封 | **建议：由 Go 写一个 Redis Stream 作为中转，仍在运行的 TS worker 再把它转成 BullMQ 任务**，BullMQ 协议只留在 TS 里。据我所知 BullMQ 没有官方 Go 版本，社区库是否可用没有核实。现在的消费端会等入队完成才提交 offset，Go 端要等 Stream 写入成功再提交，语义保持不变。这个 Stream 从 P1 起就作为 TS 和 Go 之间的分界：P3 改为 Go 直接消费它，回滚时重新打开 TS 中转即可 |
| 其他副作用 | `markFirstEvent`（Postgres）；项目缓存（`cachable:*` 这些 key，接收端已经在读）；实时推送 `publishEvent('events','batch',…)` 在 event buffer 的 **flush** 里（`event-buffer.ts:441`），属于 P2 | 照搬，key 名和消息格式不变 |
| 指标 | `buffer_*_count`、`<queue>_waiting_count`、消费 lag 等 | 名字和标签不变，`SCALING.md` 和 e2e 压测脚本都读这些 |

## 3. 阶段

每个阶段都写清：怎么切换、怎么回滚、什么条件下进入下一阶段。

### P0 准备（不上线）
- 在生产 worker 上做一次 CPU profile，确定基线（每秒事件数、每核事件数、lag、buffer 深度）。这是之后判断"Go 化是否达标"的标准，目前没有这个数字。
- 生成 goldens：用 TS 实现处理一批录制的 Kafka 消息，记录它产生的 Redis 写入、BullMQ 任务和 ClickHouse 行。Go 单元测试对着这些结果比对，做法和接收端的 `golden` 一样。
- 搭建影子环境：单独的消费组（如 `openpanel-events-shadow`）、单独的 Redis db 或前缀、单独的 ClickHouse 库。中转 Stream、会话结束任务和实时推送**改写到影子命名空间里，不要关掉**（`env.ts` 已有 `QUEUE_NAMESPACE`）。关掉了，影子环境就不会产生 `session_end` 事件，也就没法和生产比对。只有写真实 Postgres 或会发到外部的副作用才替换成空实现：通知行的写入和发送、`markFirstEvent`、用量计数。
- 写 TS 端的中转：读 Go 写入的 Stream，转成 BullMQ 任务（`sessions`、`notification`），要有去重，同一条记录被投递两次只入队一次，`sessions` 本来就用 jobId 去重。
- 先验证消费组协议能否混用（见上表），再决定 P1 用按分区灰度还是整体切换。
- 定下 BullMQ 方案（建议用中转）。

### P1 Go 消费端（A 层）；刷写仍由 TS 负责
- 实现以下部分：
  - 消费循环、partition lanes、同 key 串行、durability window、重试和死信。
  - incoming-event-handler、createEvent、profile upsert、AI 事件。
  - 通知规则匹配：规则从 Postgres 读并缓存，命中后写 Postgres 通知行，再写中转 Stream。
  - 会话归属：只用 GET/SET 和 pipeline，`session-buffer.test.ts:424` 规定接收路径不发 EVAL。session 的 Lua 清理脚本属于关闭和回收会话的流程，放到 P3。
- **切换**：TS worker 的 `ENABLED_QUEUES` 去掉 `events`，只保留 BullMQ 队列；Go worker 加入 `openpanel-events`。如果协议能混用，先加 1 个 Go 副本，让它分到一部分分区。
- **回滚**：停掉 Go 副本，TS worker 的 `ENABLED_QUEUES` 加回 `events`。前提是 Go 写入的 Redis 状态和 TS 完全一致，这一点在整个迁移期都是不可破的约束。
- **进入下一阶段的条件**：影子环境连续运行一段时间（建议 7 天以上，覆盖一个完整周），比对没有差异（忽略 `inserted_at` 和随机 id）；生产灰度期间 lag 能回到 0，死信率不升高，ClickHouse 里每个项目每天的事件数、会话数和 TS 期间一致。

### P2 Go 刷写（B 层第一部分）
- 8 个 buffer 的 flush，写入 ClickHouse 走 HTTP + JSONEachRow，和 TS 发送同样的行。还要实现两节点故障切换（`clickhouse-failover.test.ts`），以及 `BUFFER_ASYNC_INSERTS` 和插入并发配置。
- profile 的 flush 会先读 ClickHouse 再合并，最容易出偏差，单独比对。
- **TS 需要一个小改动**：`cron` 队列里所有定时任务共用一个队列，没有按任务关闭的开关。要加一个环境变量（如 `DISABLED_CRON_JOBS=flushEvents,...`），否则只能靠 flush 锁互斥，TS 和 Go 会轮流抢锁。
- Go 端的调度用定时器加同一把 Redis 锁就够了，不需要 BullMQ。
- 实时推送 `publishEvent` 随 event buffer 的 flush 一起迁过来。
- **切换**：按 buffer 逐个进行，先迁 bot、aiEvent、replay 这类简单的，event 和 session 放后面，profile 最后。每迁一个，就把它加进 TS 的 `DISABLED_CRON_JOBS`，同时在 Go 端打开。
- **回滚**：Go 停止 flush，从 `DISABLED_CRON_JOBS` 里删掉对应任务。两边用的是同一把锁，不会同时 flush。
- **进入下一阶段的条件**：影子 ClickHouse 库和生产按项目、按天对账，对比各表行数、`uniqExact(id)` 和关键列的校验和，忽略 `inserted_at`；`buffer_*_count` 能回到 0；`buffer_ch_insert_duration_ms` 不比 TS 差；故障切换演练通过（停掉一个 ClickHouse 节点后，buffer 积压但不丢数据，节点恢复后能追平）。

### P3 会话生命周期（B 层第二部分）
- session-end（生成 `session_end` 事件，并做漏斗类通知规则的检查）、reaper（每 5 分钟）、vacuum（每天）、usage 计数。
- session 的 Lua 清理脚本（`session-buffer.ts` 里的 `cleanupScript`）在这一阶段迁过来，脚本内容逐字照搬。
- **切换**：Go 改为直接消费中转 Stream 里的会话结束记录，TS 中转不再处理这一类记录。BullMQ 里剩下的 `sessions` 任务由 TS worker 消费完，在这之前不要关掉 TS 的 `sessions` 队列。reaper 和 vacuum 加进 `DISABLED_CRON_JOBS`。
- **回滚**：Go 停止消费会话结束记录，重新打开 TS 中转。会话结束记录一直在 Stream 里，不会丢。
- **进入下一阶段的条件**：按项目、按天对比 `session_end` 事件数和会话数；对比漏斗类通知的触发次数，在影子环境里比较空实现收到的调用；reaper 运行后，`session:wallclock:*` 里没有超时未关闭的会话。
- 漏斗通知仍然要入队 `notification`，继续经由中转。

### P4 其余队列（C 层，可选，逐个评估）
- 每个队列单独决定。顺序建议按"小、无外部依赖"在前：salt（146 行，接收端本来就读 salt）→ misc → onboarding → cohortRefresh/cohortCompute（SQL 和仪表盘共用，要两边同步维护）→ insight → organization（涉及计费和数据删除）→ gsc（Google OAuth 和 API）→ integration → import（多个外部数据源）→ notification（发送端依赖 React Email 模板，Go 无法直接渲染，需要预渲染或保留一个 TS 渲染服务）。
- API 是 import、cohortCompute、gsc 的生产者：要么 Go 能消费 BullMQ，要么改 API 的入队方式，这会改动 TS API。
- **每个队列的切换**：TS worker 的 `ENABLED_QUEUES` 去掉这个队列（cron 类任务用 `DISABLED_CRON_JOBS`），Go 开始消费。**回滚**：反过来操作。**进入下一个队列的条件**：在影子环境里对同样的输入，对比两边的 Postgres 写入和外部调用（用录制的回放代替真正发出去），运行满一个任务周期（例如 insight 是每周一次）。
- **我的建议**：P3 结束后，TS worker 只剩低频队列，1 个副本就够；收益主要在 A、B 层。这一阶段做不做由你决定。

## 4. 最终形态（P3 之后）

| 进程 | 语言 | 职责 |
|---|---|---|
| op-ingest | Go | `/track` 等热点接收（已上线） |
| op-worker-go | Go（同一镜像，`worker` 子命令） | Kafka 消费、全部 buffer 刷写、会话生命周期 |
| op-worker | TS，1 个副本 | notification、import、insights、gsc、cohortCompute 及其余 cron；BullMQ 中转 |
| op-api / op-dashboard | TS | 不变 |

## 5. 风险

- **fork 维护成本**：upstream 每改一次 buffers、session、event.service、incoming-event-handler，都要同步改 Go。需要一个检查脚本，列出这些文件在 upstream 上的变更，并重新生成 goldens。范围比接收端大得多。
- **数据正确性**：A、B 层出错的后果是数据写错或丢失，不是变慢。所以每个阶段都要有影子比对和按项目对账，不能只看单元测试。
- **过渡期两套实现并存**：P1 到 P3 期间 TS 和 Go 同时在线，所有契约都不能变。这段时间 upstream 的相关改动最好先冻结或批量合并。
- **CPU 兼容性**：镜像用 `CGO_ENABLED=0` 编译，没有设置 `GOAMD64`，默认编译目标不需要 AVX2。你说接收端已经在生产上运行，说明这个镜像能在这台机器上跑；这一点我没有自己验证。

## 6. 开工前需要的信息

- 生产 worker 副本数、每个副本的 CPU 和内存占用、高峰时每秒事件数、Kafka lag 能否回到 0。
- 生产 ClickHouse 是单节点还是集群（`CLICKHOUSE_CLUSTER`），Redis 是否单实例。
- Go 接收端上线后的 `ingest_forwarded_total` 各原因占比（看还有多少流量回到 Bun）。
- BullMQ 方案选哪个（建议用中转）；C 层做不做。

## 7. 进度

| 日期 | 阶段 | 内容 | 结果 |
|---|---|---|---|
| 2026-11-22 | P0 | 消费组协议兼容性 spike：本地 Redpanda，6 分区，一个 TS 成员（@platformatic/kafka，协议 `RoundRobinAssigner` v0）加一个 Go 成员（franz-go，`internal/kafka/balancer.go` 的 `EventsGroupBalancer`） | **通过**。TS 做 leader 时把 [0,2,4] 分给 Go，Go 做 leader 时把 [1,3,5] 分给 TS，双方都能正常消费。结论：P1 可以按分区灰度（混合消费组）。注意：Go worker 必须处理 SIGTERM 并主动离开消费组，否则要等会话超时后才会重新分配 |
| 2026-11-22 | P0 | 差异测试 rig（`apps/ingest/parity/worker-diff.ts`，`bun run worker-parity`）；TS 对 TS 比对，确认哪些字段本身就不确定 | 通过。归一化处理的字段：墙上时钟（inserted_at、profile 时间、BullMQ timestamp、wallclock 分数）、worker 生成的 uuid、不同设备之间的行顺序；Go 不写的 `cachable:*` 不参与比对 |
| 2026-11-22 | P0 | TS 端新增 `DISABLED_CRON_JOBS` | 已提交，有测试 |
| 2026-11-22 | P1 | Go 消费端：consumer、wire、config、jsport、projects、notify、bullmq（写入）、events、`cmd/worker` | **rig 比对 TS 对 Go 无差异**：手写语料 19 条，生成语料 1,970 条（4 个分区、7 批）。另外用 `-race` 编译的二进制跑了一遍 rig，没有数据竞争 |
| 2026-11-22 | P2 | ClickHouse 客户端（请求逐字节对齐 Node 客户端，故障切换，每次重试都重建请求体）、flush 锁（同一段 Lua）、调度器、`GO_WORKER_CRON_JOBS`；bot、ai、replay、group 的 flush | 线上测试：TS 和 Go 写入的行一致；TS 和 Go 的锁互斥 |
| 2026-11-22 | P3 预备 | Go 版 BullMQ consumer（BullMQ 自带的 Lua） | 和 Bun worker 同时消费一个队列，300 个任务每个只执行一次；进程被杀后，任务由卡住任务检查（stalled check）放回队列 |

### 本地无法完成、需要在生产或 staging 上做的门槛
- 生产 worker 的 CPU profile 和基线
- 影子环境连续 7 天的比对
- 生产灰度和切换
- `CLICKHOUSE_CLUSTER=true` 时的表名和行为（本地是单节点）
- 在 Ivy Bridge 上运行 Go worker

## 8. 实现决定（2026-11-22，P1 开工前）

规格：`docs/GO_WORKER_CONTRACTS.md`（P1）、`docs/GO_WORKER_CONTRACTS_FLUSH.md`（P2/P3）。

1. **对 TS 的缺陷保持一致（faithful）。** 规格 P1.17 和 P2.10 列出的缺陷，Go 先原样照搬，以保证 TS 和 Go 逐字节一致，回滚时状态也能互通。会造成数据丢失的缺陷（P1.17 #2，以及 P2 的重试时请求体为空），之后在 TS 和 Go 两边同时修，作为单独的 fix 提交。唯一的例外是，Go 不得引入新的丢数据路径。
2. **BullMQ 改为由 Go 直接写入，不再用 Redis Stream 中转（取代第 2 节的建议）。** 规格 P1.9 已经抓到 BullMQ 6.7.8 `addStandardJob` 的完整调用，包括 Lua 的 SHA 和 msgpack 参数。Go 用 `gen` 从已安装的 bullmq 包里生成 Lua 脚本，`gen:check` 会在 bullmq 升级时报错，做法和 UA 表一样。这样不用新增 TS 中转进程。到 P3，Go 用同一批 BullMQ Lua 脚本消费 `sessions` 队列，TS 和 Go 的 worker 可以同时消费同一个队列（和 Kafka 混合消费组一样），切换和回滚都只是改配置。入队和 TS 一样，等写入完成再继续，Redis 故障时重试等待，直到进程关闭。
3. **`cachable:*` 只读不写。** Go 读共享的项目缓存，没命中就查 Postgres，并放进进程内的 LRU（60 秒），但不写回 Redis，和接收端的做法一致。首个事件写入后照样 `DEL` 对应的 key。差异测试的 rig 比对时忽略 `cachable:*` key 是否存在。
4. **正则用 Go `regexp`。** RE2J（re2js）本身就是从 Go 的 regexp 移植过去的，语义最接近，再用真实规则的样例做兼容性测试。**URL 解析**必须遵循 WHATWG 标准：先评估现成的 Go 实现，用 Bun `URL` 生成的 goldens 验收；如果没有合格的实现，就自己实现规格 P1.16 用到的那部分。
5. **代码位置。** 放在 `apps/ingest` 模块下：`cmd/worker`、`internal/worker/...`。接收端已有的包只复用，不改它们的行为。

## 9. 上线手册

这一节只写操作：每个阶段改哪些变量、怎么一次只动一点、看什么、怎么对账、怎么回滚。规格见 `docs/GO_WORKER_CONTRACTS.md`（P1）和 `docs/GO_WORKER_CONTRACTS_FLUSH.md`（P2、P3），Go worker 的说明见 `apps/ingest/README.md` 的 "The Go worker"。

### 9.0 通用约定

- **一次只做一个阶段里的一步**，每一步做完等 lag 回到 0、对账通过，再做下一步。每一步的回滚都是把这一步改的变量改回去，不需要改数据。
- **Bun worker 的变量**：`ENABLED_QUEUES` 不设置等于全部队列（`events,sessions,cron,notification,import,insights,gsc,cohortCompute`）。要去掉 `events` 时必须把剩下的写全，例如 `ENABLED_QUEUES=sessions,cron,notification,import,insights,gsc,cohortCompute`。`DISABLED_CRON_JOBS` 是逗号分隔的 cron 任务 id，写错名字会让 Bun worker 启动失败；它让这个进程"完成但不执行"这些任务，调度器仍然登记着。
- **Go worker 的变量**：`ROLE=worker`；`GO_WORKER_QUEUES`（目前只接受 `sessions`）；`GO_WORKER_CRON_JOBS`（任务 id 同上；Go 只实现这 10 个：`flushEvents`、`flushSessions`、`flushProfiles`、`flushProfileBackfill`、`flushBots`、`flushReplay`、`flushGroups`、`flushAiEvents`、`sessionReaper`、`sessionVacuum`，写别的名字启动失败）。其余变量和 Bun 的同名，共用同一个 env 文件。
- **Go 容器必须显式写 `ENABLED_QUEUES`**。共用的 env 文件里通常没有它，而"未设置"等于全部队列，Go 容器会因此加入 `openpanel-events` 消费组。不消费 events 的 Go 容器写一个不含 `events` 的值（例如 `ENABLED_QUEUES=cron`；Go 只看其中有没有 `events`，其他名字被接受但不起作用）。compose 的 `environment` 优先于 `env_file`。
- **镜像**：和接收端同一个镜像，但镜像的入口和健康检查是接收端的，所以必须用 `entrypoint` 覆盖（只写 `command: /app/worker` 会变成 `/app/ingest /app/worker`，启动的是接收端），并覆盖健康检查。ops 端口是 `API_PORT`（默认 3000，和 Bun worker 一样）。

```yaml
  op-worker-go:
    image: <和 op-ingest 相同的镜像>
    restart: always
    entrypoint: ["/app/worker"]
    environment:
      - ROLE=worker
      - ENABLED_QUEUES=events          # P1；P2、P3 的写法见下
      # - GO_WORKER_QUEUES=sessions    # P3
      # - GO_WORKER_CRON_JOBS=flushBots,flushAiEvents,flushReplay,flushGroups   # P2
    depends_on:
      op-api:
        condition: service_healthy
      op-rp:
        condition: service_healthy
    env_file:
      - .env
    healthcheck:
      test: ["CMD", "/app/worker", "healthcheck"]
      interval: 10s
      timeout: 5s
      retries: 5
    stop_grace_period: 60s             # SIGTERM 后要刷完 event buffer 并退出消费组；SHUTDOWN_FORCE_EXIT_MS 默认 20 s
    deploy:
      mode: replicated
      replicas: 1
    logging:
      driver: "json-file"
      options:
        max-size: "30m"
        max-file: "3"
```

### 9.1 要盯的指标和检查

所有指标名和 Bun worker 相同，Go worker 在 `API_PORT` 的 `/metrics` 上提供；就绪检查是 `/healthz/ready`（消费循环 60 s 没有动静时返回 503）。

| 看什么 | 来源 | 正常 |
|---|---|---|
| 消费 lag | `rpk group describe openpanel-events`（Redpanda 容器里；外部 Kafka 用它自己的工具）看 `TOTAL-LAG` 和每个分区归属哪个成员 | 切换时短暂上升，几分钟内回到 0 |
| 处理失败和死信 | `kafka_events_handler_failures_total{partition}`、`kafka_events_dead_lettered_total{partition,reason}`、`kafka_events_dead_letter_failed_total`；死信列表 `redis-cli LLEN dead_letter:events`（上限 `INGEST_DEAD_LETTER_MAX_ENTRIES`，默认 1000） | 切换前后增速不变；`dead_letter_failed_total` 保持 0 |
| buffer 深度 | `buffer_event_count`、`buffer_session_count`、`buffer_profile_count`、`buffer_bot_count`、`buffer_replay_count`、`buffer_group_count`、`buffer_profile_backfill_count`、`buffer_event_pending_local_count`（ai 事件 buffer 没有这个指标，用 `LLEN` 看） | 每次 flush 后回到接近 0，不单调上涨 |
| flush | `buffer_flush_total{buffer,result,trigger}`（result：`success`、`error`、`locked`、`paused`）、`buffer_flush_duration_ms`、`buffer_flush_rows_total`、`buffer_ch_insert_duration_ms`、`buffer_ch_fetch_duration_ms`（profile）、`buffer_flush_llen_at_start` | `error` 不增加；`locked` 偶发是正常的（另一个进程持锁）；插入耗时不比 Bun 差 |
| 会话 | `sessions_started_total{kind}`、`session_ends_enqueued_total{source}`、`session_ends_emitted_total`、`session_ends_skipped_total{reason}`、`sessions_reaped_total`、`sessions_vacuumed_total`、`sessions_active_total`、`sessions_projects_active` | 和切换前的速率同量级 |
| BullMQ 队列 | `sessions_waiting_count`、`sessions_active_count`、`sessions_delayed_count`、`sessions_failed_count`、`notification_waiting_count`、`notification_failed_count`、`cron_waiting_count`；`job_duration_ms{name,status}`（只记失败） | `failed` 不增加，`waiting` 回到 0 |
| `/track` 不受影响 | 接收端的 `ingest_http_request_duration_seconds`、`ingest_http_requests_total`、`ingest_forwarded_total{reason}`，或边缘的 `/track` p95 | 切换前后 p95 没有变化。worker 和接收端是不同进程，这一条出现变化说明不是 worker 的问题，先查别处 |
| 进程 | 每个副本的 CPU、内存、重启次数，`/debug/pprof/profile` | 不重启、内存不持续上涨 |

### 9.2 对账查询

用同一份 SQL 比较两个对象：切换前后，或影子库和生产库（`param_db` 换库名）。忽略 `inserted_at`，所以查询里没有它。`events` 的 `checksum` 是每行 `id, name, profile_id, session_id, created_at` 的哈希之和，两边事件相同则相等。`sessions` 是折叠表，用 `sum(sign)` 而不是 `count()`。已在本地开发库 `openpanel`（ClickHouse 26.3）上运行过，下面 8 条都能解析并返回结果；`CLICKHOUSE_CLUSTER=true` 时是对 Distributed 表做聚合，没有子查询，不需要 `GLOBAL`，但没有在集群上运行过。

```bash
# 用法：run_q <文件> <库> <开始日> <结束日>，例如 run_q events.sql openpanel 2026-11-01 2026-11-22
run_q() {
  curl -sS -G "$CLICKHOUSE_HTTP" --data-urlencode "query=$(cat "$1")" \
    --data-urlencode "param_db=$2" --data-urlencode "param_from=$3" --data-urlencode "param_to=$4"
}
```

```sql
-- events.sql：事件。duplicate_rows 就是重试造成的重复行（TS 本来就会有），两边的量级要一致
SELECT project_id, toDate(created_at) AS day, count() AS rows, uniqExact(id) AS ids, count() - uniqExact(id) AS duplicate_rows, uniqExact(session_id) AS sessions, countIf(name = 'session_start') AS session_starts, countIf(name = 'session_end') AS session_ends, sum(revenue) AS revenue, sum(cityHash64(id, name, profile_id, session_id, toString(created_at))) AS checksum
FROM {db:Identifier}.events
WHERE created_at >= toDate({from:String}) AND created_at < toDate({to:String})
GROUP BY project_id, day
ORDER BY project_id, day

-- sessions.sql：会话（折叠表）
SELECT project_id, toDate(created_at) AS day, sum(sign) AS live_rows, uniqExactIf(id, sign = 1) AS ids, sum(sign * event_count) AS events_counted, sum(sign * duration) AS duration_total, sum(sign * revenue) AS revenue
FROM {db:Identifier}.sessions
WHERE created_at >= toDate({from:String}) AND created_at < toDate({to:String})
GROUP BY project_id, day
ORDER BY project_id, day

-- profiles.sql
SELECT project_id, toDate(created_at) AS day, uniqExact(id) AS profiles
FROM {db:Identifier}.profiles
WHERE created_at >= toDate({from:String}) AND created_at < toDate({to:String})
GROUP BY project_id, day
ORDER BY project_id, day

-- bots.sql
SELECT project_id, toDate(created_at) AS day, count() AS rows, uniqExact(id) AS ids
FROM {db:Identifier}.events_bots
WHERE created_at >= toDate({from:String}) AND created_at < toDate({to:String})
GROUP BY project_id, day
ORDER BY project_id, day

-- ai.sql
SELECT project_id, toDate(created_at) AS day, count() AS rows, uniqExact(id) AS ids
FROM {db:Identifier}.ai_events
WHERE created_at >= toDate({from:String}) AND created_at < toDate({to:String})
GROUP BY project_id, day
ORDER BY project_id, day

-- groups.sql
SELECT project_id, toDate(created_at) AS day, uniqExact(id) AS groups
FROM {db:Identifier}.groups
WHERE created_at >= toDate({from:String}) AND created_at < toDate({to:String})
GROUP BY project_id, day
ORDER BY project_id, day

-- replay.sql
SELECT project_id, toDate(started_at) AS day, count() AS chunks, uniqExact(session_id) AS sessions, sum(events_count) AS replay_events
FROM {db:Identifier}.session_replay_chunks
WHERE started_at >= toDate({from:String}) AND started_at < toDate({to:String})
GROUP BY project_id, day
ORDER BY project_id, day

-- open.sql：P3 用。不带 from/to；2 小时前结束的窗口里，开了会话但没有 session_end 的数量
SELECT project_id, countIf(name = 'session_start') AS starts, countIf(name = 'session_end') AS ends, starts - ends AS still_open
FROM {db:Identifier}.events
WHERE created_at >= now() - INTERVAL 3 DAY AND created_at < now() - INTERVAL 2 HOUR AND name IN ('session_start','session_end')
GROUP BY project_id
ORDER BY project_id
```

怎么比：
- **影子对生产**（P1 之后的进入条件）：同一个时间段，把 `param_db` 分别设成影子库和生产库，`events`、`sessions` 的每个项目每天的行数、`ids`、`checksum` 要一致。`checksum` 不一致时，先看 `rows` 和 `ids`，再按 `id` 查差异行。
- **切换前后**（生产没有影子库时）：切换日的每个项目与上周同一个星期几比，`session_starts` 和 `session_ends` 的比值、`sessions` 里 `events_counted` 与 `events` 的 `rows` 的比值不应该变。每个项目的 `duplicate_rows` 占 `rows` 的比例不应该上升。
- 当天的最后几个小时 buffer 里还有没落库的行，对账放在切换后的第二天做，或者把结束日定在今天零点。

### 9.3 P1：Go 消费端

**改什么**

| | 变量 |
|---|---|
| Go worker（新服务 `op-worker-go`） | `ROLE=worker`，`ENABLED_QUEUES=events`，不设 `GO_WORKER_QUEUES` 和 `GO_WORKER_CRON_JOBS`。Kafka 变量、`CLICKHOUSE_URL`、`DATABASE_URL`、`REDIS_URL`、`QUEUE_NAMESPACE` 等和 Bun 完全一致（包括 `KAFKA_CONSUMER_GROUP=openpanel-events`） |
| Bun worker | 开始时不改（继续消费 events，和 Go 混在一个组里）；最后一步去掉 `events`：`ENABLED_QUEUES=sessions,cron,notification,import,insights,gsc,cohortCompute` |

**一次几个分区**

1. 先把 Go 副本数设为 1。它加入 `openpanel-events` 后，分区按成员数轮流分配（默认 24 个分区），所以 Go 拿到的比例大约是 1/(Bun 副本数+1)，要让 Go 先少拿，就保持 Bun 副本数多。用 `rpk group describe openpanel-events` 看哪些分区在 Go 的成员上。
2. 观察一段时间（建议至少覆盖一个业务高峰）：lag 回到 0，死信率不升，`sessions_started_total`、`session_ends_enqueued_total` 速率不变，对账 `events.sql`、`sessions.sql` 的当天数据。
3. 再加 Go 副本，一次加一个，每次等 lag 回到 0（加入和离开都会触发重平衡；重平衡期间同一个分区可能有两个成员短暂处理同一段 offset，见规格 P1.17 #6，所以不要连续快速改动副本数）。
4. 最后把 Bun worker 一个一个重启，把 `events` 从 `ENABLED_QUEUES` 去掉。Bun 的 `cron`、`sessions`、`notification` 等队列仍由它负责，flush 也仍由它做。
5. 停 Go 副本时发 SIGTERM（`stop_grace_period` 足够），它会刷完 event buffer 再离开消费组；不要直接杀进程，否则要等 `KAFKA_SESSION_TIMEOUT_MS`（默认 30 s）后才重新分配。

**回滚**：把 Go 副本数降为 0（一次一个），Bun worker 的 `ENABLED_QUEUES` 加回 `events`（或还原为不设置）并重启。Go 写入的 Redis 状态和 Bun 一致，不需要清理。

**进入 P2 的条件**：第 8 节 P1 的条件（影子环境连续 7 天以上对比无差异；生产灰度期间 lag 回到 0，死信率不升高，`events.sql` 和 `sessions.sql` 每个项目每天和 TS 期间一致）。

### 9.4 P2：Go 刷写

**改什么**：每迁一个 buffer，同时改两处。

| | 变量 |
|---|---|
| Go worker | 新服务或 P1 的 Go 服务，`GO_WORKER_CRON_JOBS=<已迁的 id 加上这一个>`。只做 flush 的服务写 `ENABLED_QUEUES=cron`（不要留空，见 9.0），只用 1 个副本；和 events 消费放同一个服务也可以，但那样扩 events 副本就会同时起多个 flush 调度器 |
| Bun worker | `DISABLED_CRON_JOBS=<同一批 id>`，重启 Bun worker |

两边用同一把 `lock:<buffer>`，所以先让 Go 开始、再让 Bun 禁用，中间没有空档，也不会同时 flush。暂停 BullMQ 的 `cron` 队列（bull-board）对 Go 同样有效（它读 `paused` 标志）。

**一次一个 buffer，顺序**（简单的在前，profile 最后）：

1. `flushBots`、`flushAiEvents`、`flushReplay`、`flushGroups`（可以一个一个，也可以这四个一起，它们互相独立）
2. `flushEvents`
3. `flushSessions`
4. `flushProfileBackfill`，然后 `flushProfiles`（profile 的 flush 会先读 ClickHouse 再合并，最容易有偏差，单独对账）

每一步之后看：对应的 `buffer_<名字>_count` 回到 0，`buffer_flush_total{buffer="…",result="error"}` 不增加，`buffer_ch_insert_duration_ms` 不比 Bun 差，然后对账：bots → `bots.sql`，ai → `ai.sql`，replay → `replay.sql`，groups → `groups.sql`，events → `events.sql`，sessions → `sessions.sql`，profiles → `profiles.sql`（还要抽查几个 profile 的属性是否被合并，而不是被覆盖）。

**故障切换演练**（进入条件之一）：停掉一个 ClickHouse 节点，buffer 积压但不丢数据，节点恢复后追平；对账行数一致。

**回滚**：把这个 id 从 Go 的 `GO_WORKER_CRON_JOBS` 去掉，再从 Bun 的 `DISABLED_CRON_JOBS` 去掉并重启。先让 Bun 开始、再让 Go 停也可以，原因同上。Redis 里没落库的行留在原地，由下一个进程刷。

**进入 P3 的条件**：第 8 节 P2 的条件（按项目、按天对账，`uniqExact(id)` 和校验和一致；`buffer_*_count` 回到 0；`buffer_ch_insert_duration_ms` 不比 TS 差；故障切换演练通过）。

### 9.5 P3：会话生命周期

**顺序限制（必须先做）**：Go 处理 `sessions` 队列里的作业时，用量计数不会自己发邮件，而是往 `notification` 队列写一个 `sendUsageAlert` 作业，由 Bun worker 发送。所以：

1. 所有消费 `notification` 队列的 Bun worker 必须先升级到有 `sendUsageAlert` 作业的版本（提交 `ca015146`，`packages/core/src/modules/notification/notification.jobs.ts`），并且 `notification` 一直留在 Bun 的 `ENABLED_QUEUES` 里；
2. 然后才能设置 `GO_WORKER_QUEUES=sessions`。如果 Bun 没有这个作业，Go 写入的 `sendUsageAlert` 作业会失败或停在队列里，用量提醒邮件就不会发。`SELF_HOSTED` 的安装不发用量提醒，不受这条影响。

**改什么**

| | 变量 |
|---|---|
| Go worker | `GO_WORKER_QUEUES=sessions`；`SESSIONS_CONCURRENCY` 按需（默认 1）；reaper 和 vacuum 迁移时加 `GO_WORKER_CRON_JOBS=…,sessionReaper,sessionVacuum`。`SESSION_REAPER*`、`SESSION_VACUUM*`、`EXPERIMENTAL_PROFILE_BACKFILL*` 必须和 Bun 一致 |
| Bun worker | 先不改（共享 `sessions` 队列）；reaper 和 vacuum 迁移时 `DISABLED_CRON_JOBS` 加 `sessionReaper,sessionVacuum`；最后一步 `ENABLED_QUEUES` 去掉 `sessions` |

**一次几个作业**

1. 共享队列：Go 和 Bun 同时消费 `sessions`，每个作业只被一个进程执行（本地用 300 个作业验证过）。从 1 个 Go 副本、`SESSIONS_CONCURRENCY=1` 开始，Bun 副本多，Go 只拿到一小部分作业；逐步加 Go 副本或并发。
2. 看 `session_ends_emitted_total`、`session_ends_skipped_total{reason}`、`sessions_waiting_count` 和 `sessions_failed_count`；用 `events.sql` 里的 `session_ends`、`sessions.sql` 和 `open.sql` 对账。漏斗通知的触发次数（`notification` 作业数）和 TS 期间比较。
3. 迁 reaper 和 vacuum：一次一个，先 `sessionReaper`，对账 `sessions_reaped_total`、`sessions_active_total`；确认 `session:wallclock:*` 里没有超时未关闭的会话；再 `sessionVacuum`（每天一次，要等一个周期才能看到 `sessions_vacuumed_total`）。vacuum 的 cron 时间依赖容器的时区，Go 和 Bun 容器要一致。
4. 最后把 `sessions` 从 Bun 的 `ENABLED_QUEUES` 去掉。在这之前必须让 `sessions_waiting_count`、`sessions_active_count`、`sessions_delayed_count`（重试中的作业）都回到 0，Bun 才能放手；不要在队列里还有作业时关掉所有消费者。

**回滚**：Go 的 `GO_WORKER_QUEUES` 去掉 `sessions` 并重启（SIGTERM；被杀的进程里没做完的作业由 BullMQ 的卡住作业检查放回队列），Bun 的 `ENABLED_QUEUES` 加回 `sessions`；reaper 和 vacuum 同 P2 的回滚。会话状态在 Redis 里，格式相同，不需要清理。

**进入下一阶段的条件**：第 8 节 P3 的条件（按项目、按天对比 `session_end` 事件数和会话数；漏斗通知触发次数一致；reaper 运行后没有超时未关闭的会话）。

### 9.6 仍然只能在生产或 staging 上完成的门槛

- 生产 worker 的 CPU profile 和基线（每秒事件数、每核事件数、lag、buffer 深度）。
- 影子环境连续 7 天（覆盖完整一周）的对比，以及生产灰度本身。
- `CLICKHOUSE_CLUSTER=true`：`ON CLUSTER` 的 backfill `UPDATE`、Distributed 表的行为、两节点故障切换。本地是单节点。
- Redis Cluster / `QUEUE_CLUSTER=true` 和带命名空间的 BullMQ key 形式（带花括号的 key）：只读过代码，没有运行过；session 的清理脚本有 3 个 key 且没有 hash tag，在 Redis Cluster 上不安全（规格 P3.11 #10）。
- 在生产的 Ivy Bridge CPU（没有 AVX2）上运行 Go worker；镜像本身没有设 `GOAMD64`。
- 带 `SASL`/TLS 的外部 Kafka、生产的消费组成员数和分区数下的重平衡行为。
- 生产容器的时区（vacuum 的 cron 时间）。
- 每个 buffer 在生产流量下的端到端 flush（本地只用小量数据对比）。
- 接收端的镜像目前没有发布：Go worker 用的是同一个镜像，需要先构建并发布，compose 模板和 CLI 的 `doctor` 检查也要补上（新增了变量 `GO_WORKER_QUEUES`、`GO_WORKER_CRON_JOBS` 和 `DISABLED_CRON_JOBS`）。
