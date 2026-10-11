# StarFlow

[English](README.md) | [简体中文](README.zh-CN.md)

面向 Web 的多租户实时推送服务，为弹幕（danmaku）场景设计：单频道极高扇出、
消息寿命极短、单向广播。

Go 服务，JavaScript/TypeScript Web SDK。

**一句话：** 一个无状态 WebSocket 网关，把发布的消息扇出到其频道的每一个
订阅者，配套配额、审核与降级阶梯，以及掌管其背后配置的控制面。

## 目录

- [它不是什么](#它不是什么)
- [两个二进制](#两个二进制)
- [快速开始](#快速开始)
- [配置](#配置)
- [测试](#测试)
- [性能](#性能)
- [容量现状](#容量现状)
- [架构](#架构)
- [仓库结构](#仓库结构)
- [文档](#文档)

## 它不是什么

设计上有意排除了消息持久化、历史、离线投递、重放、逐条 ACK、全局顺序与
频道分片。它是 **QoS0**：不存储、不重传，允许丢失与乱序。正是这个选择让
"单机十万连接"成为内存问题，而不是持久化问题。

## 两个二进制

| 二进制 | 作用 |
|---|---|
| `cmd/gateway` | 数据面与控制面合并在同一个进程中。对该服务的首批部署而言这是正确的默认形态：目标实例数很少，而控制面本来也只通过其存储接口被访问。 |
| `cmd/loadgen` | 压测器。维持一批订阅者，向其发布消息，并写出容量报告。未达标时以非零码退出。 |

## 快速开始

从 vendored 依赖树构建，无需网络访问：

```bash
go build -mod=vendor -o bin/gateway ./cmd/gateway
go build -mod=vendor -o bin/loadgen ./cmd/loadgen
```

然后运行。`GOMEMLIMIT_BYTES` 是唯一没有默认值的配置——进程缺失它就拒绝
启动，因为一个默默默认为无上限的内存限制，最终会以 OOM kill 的形式失败，
而日志里没有任何东西能解释原因：

```bash
export GOMEMLIMIT_BYTES=$((4*1024*1024*1024))
export CONTROL_DSN="starflow.db?_pragma=foreign_keys(1)"
export SECRET_SOURCE=env SECRET_REF=PUSH_TOKEN_SECRET
export PUSH_TOKEN_SECRET="$(openssl rand -hex 32)"

# 播种第一个管理员。没有任何授权记录的实例不存在可达的管理员，
# 这是它变得可管理的方式。
export BOOTSTRAP_TENANT=t1 BOOTSTRAP_SUBJECT=ops

go run ./cmd/gateway
```

三个监听端口，刻意分开：

| 端口 | 承载 |
|---|---|
| `:8080` | `/publish`（HTTP）、`/sub`（WebSocket） |
| `:8081` | `/admin/*` |
| `:8082` | `/readyz`、`/livez`、`/metrics` |

创建租户与应用，然后发布。密钥只在创建时返回一次，之后不再返回：

```bash
curl -s -X POST localhost:8081/admin/tenants \
  -H 'X-Tenant-Id: t1' -H 'X-Admin-Subject: ops' \
  -H 'Content-Type: application/json' -d '{"name":"demo"}'

curl -s -X POST localhost:8081/admin/applications \
  -H 'X-Tenant-Id: t1' -H 'X-Admin-Subject: ops' \
  -H 'Content-Type: application/json' \
  -d '{"name":"web","can_publish":true,"can_subscribe":true}'
# -> {"application":{"app_key":"..."},"secret":"..."}
```

```bash
curl -s -X POST localhost:8080/publish \
  -H 'X-App-Key: <app_key>' -H 'X-App-Secret: <secret>' \
  -H 'Content-Type: application/json' -d '{"channel":"room:1","body":"hello"}'
# -> {"accepted":true,"outcome":"accepted","message_id":...}
```

响应在不等待投递的情况下返回准入结果，因此发布者的延迟从不依赖任何订阅者
的 socket。`outcome` 出现在每一条响应中，包括被拒绝的响应，因此限流与审核
拦截无需解析文本即可区分：

| Outcome | HTTP | 含义 |
|---|---:|---|
| `accepted` | 202 | 已准入，进入投递。 |
| `degraded` | 202 | 按降级策略以削减后的形态准入。 |
| `dropped` | 202 | 按丢弃策略被拒绝。**刻意做成成功形态**——返回错误会招来该策略本意要抑制的重试。 |
| `user_rate_limited` / `tenant_rate_limited` / `channel_rate_limited` | 429 | 超过某个上限。 |
| `blocked` | 422 | 命中带有阻断动作的审核规则。 |
| `too_large` | 413 | 准入内容超出限制。 |
| `duplicate` | 200 | 对近期发布的重试，或发布者自己的重复内容。 |
| `unauthenticated` | 401 | 凭据被拒绝。 |

准入顺序是一份契约，因为上报的 `stage` 取决于它：权限 → 限流 → 禁言/封禁
→ 重复 → 审核 → 大小。大小最后检查，且针对的是*准入后*的内容，因此一条
会缩短消息的替换规则可以把它带回到限制之内。

然后订阅。在 `sdk/` 下：

```ts
import { PushClient } from '@starflow/push';

const client = new PushClient({
  url: 'ws://127.0.0.1:8080/sub',
  appKey: '<app_key>',
  token: '<secret>',     // 或一个签名 token
  channels: ['room:1'],
  callbacks: {
    onMessage: (channel, message) => console.log(channel, message.body),
    onGap: (channel, gap) => console.warn(`dropped ${gap.dropped} messages`),
  },
});

await client.connect();
```

`connect()` 在客户端**完成鉴权且初始订阅就位**后才 resolve，而不是在
socket 打开时。

### 值得了解的 SDK 行为

- **重连带抖动。** 网关重启会在同一瞬间踢掉所有连接；固定间隔的重试会对
  新实例造成惊群。延迟在连续失败时翻倍，并在基准值附近随机化。把
  `jitterRatio` 往 0 调，是拿重连公平性换首次重试的可预测性——在大规模
  重连场景下这几乎总是错误的取舍。
- **被拒绝的频道会被上报，而不是静默丢弃。** `onError` 会以
  `server-refused` 触发，该频道退出 SDK 的集合，于是应用可以显示"房间已满"，
  而不是等待一条永不到来的流。
- **鉴权失败是终止性的。** 错误凭据不会重试。它不可能通过重试变有效，而
  拿着它猛敲网关，正是一台配置错误的客户端演变成一次负载事件的方式。
- **静默断连靠探测发现。** 中间设备可能在没有关闭帧的情况下丢掉连接；
  SDK 按 `heartbeatIntervalMs` 探测，两次该间隔内没有回应即重连。

SDK 需要单独构建与测试——它是独立的包，不属于 Go 模块：

```bash
cd sdk
npm install
npm test      # 先构建，再跑 TypeScript 测试套件
```

## 配置

所有配置都是环境变量，由 `internal/config` 读取。校验会拒绝那种"服务跑起来
之后再也无法诊断"的配置，并指出出错的变量名。

### 必填

| 变量 | 含义 |
|---|---|
| `GOMEMLIMIT_BYTES` | Go 软内存上限。无默认值。 |

### 监听

| 变量 | 默认值 |
|---|---|
| `LISTEN` | `:8080` |
| `ADMIN_LISTEN` | `:8081` |
| `READY_LISTEN` | `:8082` |
| `INSTANCE_ID` | `0` —— 在并发运行的网关之间必须唯一；它是每条消息 id 的一部分 |

### 消息总线

| 变量 | 默认值 |
|---|---|
| `BUS_KIND` | `memory` —— 或 `redis` |
| `BUS_ADDR` | `127.0.0.1:6379` |
| `BUS_PASSWORD` | 空 |
| `BUS_CHANNEL` | `push:admitted` |
| `BUS_CONFIG_CHANNEL` | `push:config` |

### 控制面与密钥

| 变量 | 默认值 |
|---|---|
| `CONTROL_DSN` | 空 —— 一个 SQLite DSN |
| `CONTROL_MAX_OPEN_CONNS` | `16` |
| `CONTROL_POLL_INTERVAL` | `15s` —— 在 pub/sub 传播之上的兜底对账 |
| `SECRET_SOURCE` | `env` —— 或 `file` |
| `SECRET_REF` | `PUSH_TOKEN_SECRET` |
| `BOOTSTRAP_TENANT` / `BOOTSTRAP_SUBJECT` | 空 —— **必须成对设置** |
| `BOOTSTRAP_ROLE` | `admin` |

签名材料只来自密钥源，绝不来自命令行参数：参数会出现在进程表里，主机上的
每个其他进程都能看到。

### 投递

| 变量 | 默认值 | 含义 |
|---|---|---|
| `DELIVERY_FLUSH_INTERVAL` | `10ms` | 合并窗口。延迟旋钮。 |
| `DELIVERY_MERGE_RATIO` | `20` | 合并目标，不是保证。 |
| `DELIVERY_WRITE_BUFFER_BYTES` | `16KB` | 每连接出口缓冲，在握手时一次性分配。 |
| `DELIVERY_READ_BUFFER_BYTES` | `4KB` | 每连接控制帧读缓冲。 |
| `DELIVERY_COMPRESS` | `true` | 对投递批次启用 permessage-deflate。 |
| `DELIVERY_COMPRESS_MIN_BATCH` | `5` | 低于 2 时启动即拒绝。 |
| `DELIVERY_PER_VIEWER_BPS` | `2048` | 整个容量模型都由这个数字推导而来。 |
| `DELIVERY_HEARTBEAT_INTERVAL` | `25s` | |
| `DELIVERY_HEARTBEAT_TIMEOUT` | `10s` | |
| `DELIVERY_DRAIN_GRACE` | `30s` | |
| `DELIVERY_SHARDS` | `1024` | flush 循环遍历的脏列表分片数。 |
| `AUTH_TOKEN_TTL` | `5m` | |
| `AUTH_TOKEN_SKEW` | `2m` | |

### 运行时

| 变量 | 默认值 |
|---|---|
| `GOGC` | `400` |
| `GOMAXPROCS` | `0` —— 运行时默认 |

## 测试

```bash
go test ./...
go vet ./...
gofmt -l ./cmd ./internal

go test -bench=. ./internal/gateway   # 扇出、flush 与存储的基准测试
```

`cmd/loadgen` 生成容量报告。未达标时以非零码退出，因此一条容量声明要么
通过流水线，要么失败，而不是产出一份没人读的文档：

```bash
go run ./cmd/loadgen \
  --publish-url http://127.0.0.1:8080/publish \
  --tenant t1 --app-key <key> --secret <secret> \
  --connections 1000 --publish-qps 10 \
  --duration 30s --ramp 5s --report c1000.json
```

这里的发布速率是 10 msg/s，不是 1,000。按 200 B 计算，每观众是 2 KB/s，
恰好等于每观众预算——于是这次运行把整个带宽额度都花在了每个订阅者身上。
1,000 msg/s 是*平台级*目标，只有摊到多个频道才可达；把它全发给一个频道，
等于要求每个观众吞下 200 KB/s。

### 容器

```bash
docker build -t starflow .
```

模块树是 vendored 的，构建不需要网络访问。镜像里没有 shell，也没有包管理器，
且 `USER` 是数字——`nonroot` 是 distroless 特有的名字，在任何没有该 passwd
条目的基础镜像上都会失败，而 `65532` 无需查找即可解析。签名材料从不烘焙进
镜像：它在启动时从密钥源读取，因此镜像不携带任何凭据。

## 性能

基于 `internal/gateway` 基准测试，单台开发机实测：

| 场景 | 结果 |
|---|---|
| 扇出，1,000 连接，合并 1 | 881 MB/s，6 allocs/op |
| 扇出，1,000 连接，合并 20 | 909 MB/s，6 allocs/op |
| 扇出，10,000 连接，合并 20 | 569 MB/s，14 allocs/op |
| 订阅存储遍历 | 1,000 ns —— 是连续切片的 1.02 倍，比哈希 map 快 188 倍 |
| flush 一轮，1,000 空闲连接 | 16.2 µs |
| flush 一轮，100,000 空闲连接 | 16.5 µs |

这些数字是两件事的证据。分配不随订阅者数量增长——每条消息的开销是一次
编码加上每个订阅者一次 memcpy，没有任何重复编码。以及空闲连接数增长
100 倍，每轮 flush 只多花 1.02 倍，这正是大规模空闲人群近乎免费的原因。
两者都有测试断言，因此退化回 map、或 flush 开销随连接数增长，都会让构建
失败。

### 压缩按批次决定，而不是按配置决定

| 批次大小 | 压缩后 / 原始 |
|---|---|
| 1 | **1.123** —— 反而变大 |
| 5 | 0.308 |
| 20 | **0.12** —— 省下 88% |

一条未合并的 200 字节消息压缩后是原始大小的 112%，因为 deflate 的帧开销
超过了载荷省下的部分。所以 `DELIVERY_COMPRESS` 不能意味着"全都压缩"：
决策在 `protocol.ShouldCompress` 里按每个批次的真实大小做出。一条挂在
`MergeRatio` 上的配置级规则看起来正确，却恰恰会压缩那些会膨胀的批次，
因为 `MergeRatio` 是目标——轻负载频道即使设为 20 也会发出单条消息的批次。

### 合并是延迟旋钮，不是带宽旋钮

20:1 合并把包数降到 1/20，字节数一分不减：

```
合并前:  100 条消息/s x 200 B = 每观众 20 KB/s
合并 20:1 后:  5 包/s    x 4 KB = 每观众 20 KB/s
                                          完全相同
```

它买到的是 CPU 与系统调用余量，从来不是出口带宽。在带宽受限的实例上，
NIC 未打满时投递路径上的 CPU 工作几乎一文不值；调吞吐意味着换更大的网卡，
而不是更紧的循环。在 2 KB/s 下把合并比一味推过大约 20:1，就会开始强制
丢内容，因为每个观众已经收到了预算允许的几乎全部内容。

## 容量现状

文档声明的目标与实际测得的数据：

| 目标 | 状态 |
|---|---|
| 每实例连接数 —— 100,000 | **未验证。** 实测到 10,000；压测机约 16k 的客户端临时端口上限限制的是测试，不是服务。 |
| 平台发布速率 —— 1,000 msg/s | **未验证。** 实测 10 msg/s，即单频道下每观众预算所允许的速率。 |
| 每观众带宽 —— 2 KB/s | **在 1,000 连接下已验证**：2,004 B/s。 |
| 投递延迟 p50/p95/p99 —— 50/100/200 ms | **部分验证。** 1,000 连接下在预算内；5,000 与 10,000 下超标。 |
| 多实例投递，精确一次 | **已验证**，覆盖 1、2、5 实例。 |

迄今为止每一轮端到端运行：零拨号失败、零 gap 通知、每条准入的消息都已
投递。上面的失败全部是延迟——这与扇出路径通常的劣化方向恰好相反。

低于 100% 的接受率是预期之内的，其本身不构成失败。丢弃策略刻意在不告知
发布者的情况下拒绝发布，这会按设计拉低接受率；重要的是拒绝被归因，因此
报告把 `accepted` 与 `degraded` 同每一个拒绝码分开统计。

## 架构

```
        发布者                        订阅者
            |                              |
      POST /publish                 WebSocket /sub
            |                              |
      发布准入                        WebSocket 传输
      权限 → 限流                     hello 握手、鉴权
      → 禁言/封禁 → 重复              订阅 / 退订
      → 审核 → 大小                   读循环、关闭处理
            |                              |
            └──────────► bus ◄─────────────┘
                    memory | redis
                         |
        每个网关接收，按自己的连接存储过滤，
        在自己的 flush 间隔上投递
                         |
              合并 → 大小检查 → 压缩? → 一帧
                         |
              扇出: 一个已编码批次，每个 socket 一次 memcpy
```

两个性质承担了大部分负荷。

**数据面不共享任何订阅状态。** 网关从不分片频道，也从不迁移连接。发布的
消息被广播给每个网关，在本地过滤——代价是不服务该频道的实例多一次冗余
投递，换来的是无需任何发现机制的水平扩展。

**热路径从不触碰控制面。** 每一次准入检查都读内存快照或进程内计数器：
审核规则被编译成驻留在进程内存中的 Aho-Corasick 自动机，租户上限由每个
实例各自求和等于租户总额的计数器执行，封禁检查在快照上解析。这里值得
明说的后果是：一个丢失了内存状态的实例会在自动机重建之前处于无审核发布
状态，因此自动机未编译完成的实例绝不上报就绪、绝不进入服务集合。就绪
检查是失败即关闭的。

**投递帧是换行分隔的 JSON，其中的标识符是十进制字符串。** 消息 id 持有
63 位，超过 2^53 时 JavaScript 数字会静默取整；以数字形式发出会让两条
不同的消息在客户端看起来一模一样。日后更改它是破坏性的线上协议变更，
因此用测试断言，而不是留作隐含约定。

### 配置传播

一次管理面写入先持久化，然后发布一个携带租户、类型与可选 subject 的
`ConfigChange`——从不携带值。每个网关从存储重新加载变更的子集，因此
一条通知不可能让网关持有从未持久化的值。周期性轮询是兜底，保证在总线
有损时最终收敛。

### 降级阶梯

| 级别 | 含义 | 准入 | 合并目标 |
|---|---|---:|---:|
| L0 | 正常 | 100% | 20 |
| L1 | 削减 | 76% | 80 |
| L2 | 抽样 | 50% | 200 |
| L3 | 危急 | 普通内容 0% | 400 |

从 L0 到 L2，对同样的 5,000 条*提供*内容，包数减少 20.8 倍。这里以*提供*
的内容为基准陈述，使各级之间可直接比较。阶梯可以通过管理 API 固定或
释放，且不涉及任何集群协调——每个实例各自持有自己的档位。

## 仓库结构

| 路径 | 内容 |
|---|---|
| `cmd/gateway` | 运行该服务的唯一二进制。 |
| `cmd/loadgen` | 压测器与容量报告。 |
| `internal/publish` | 准入流水线与发布处理器。 |
| `internal/gateway` | 连接存储、分片集合、合并、扇出、flush、传输。 |
| `internal/auth` | 凭据、权限、配额、租户限流。 |
| `internal/moderation` | Aho-Corasick 敏感词匹配与推广规则。 |
| `internal/controlplane` | 存储、迁移、租户快照、配置重载。 |
| `internal/bus` | 内存与 Redis pub/sub。 |
| `internal/admin`、`internal/adminadapters` | 管理 API 及将其绑定到存储的适配器。 |
| `internal/readiness`、`internal/observability` | 探针、指标、告警、追踪。 |
| `internal/protocol` | 帧、id、批次压缩。 |
| `sdk/` | `@starflow/push` TypeScript SDK。 |

## 文档

| 文件 | 内容 |
|---|---|
| `docs/publishing.md` | 发布 API、准入顺序、outcome 码、重复处理。 |
| `docs/control-plane.md` | 数据模型、配置传播、迁移。 |
| `docs/admin-api.md` | 管理面、角色、调用前值得知道的副作用。 |
| `docs/observability.md` | 指标、告警、追踪。 |
| `docs/capacity-report.md` | 推导、实测、conformance 表。 |
| `docs/sdk.md` | Web SDK 选项、回调、行为。 |
