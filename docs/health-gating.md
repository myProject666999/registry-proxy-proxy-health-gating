# 代理健康状态关联的镜像改写（Health Gating）

本文档说明 registry-proxy 如何根据代理实例的健康状态决定是否改写 Pod 镜像，以及各项设计决策的理由。

## 决策规则

准入（Mutating Webhook）在改写某个镜像前，先查询该镜像规则所指向的**代理实例**的当前健康状态：

- **健康（healthy）**：改写为代理地址；
- **不健康（unhealthy）或未知（unknown）**：保留原始镜像引用并放行，工作负载走原来的地址拉取。

这样当代理实例不可用时，新建 Pod 不会被改写成拉不到的代理地址，也就不会反过来拖住集群扩容。一个 Pod 里有多个镜像、分别指向不同代理实例时，**每个镜像独立判断**；一个实例不健康只影响指向它的规则，其余规则照常。

## 后台探测，准入零阻塞

准入请求有严格的时间预算（webhook `timeoutSeconds: 5`），探测**不在**准入路径上同步进行。每个代理实例有一个独立的后台探测协程，按固定周期探测；准入时只读取最近一次探测结果（一次加锁的内存读取），因此即使探测卡住，准入决策也立即返回。探测结果还带有**新鲜度界线**：如果最近一次结果比 `freshnessTTL` 更旧（例如探测被阻塞、迟迟无法完成），该实例被视同**未知**，不再沿用过期的“健康”结论。

## 状态机与迟滞

每个实例维护一个三态状态机：

- **unknown**：启动后、第一次探测完成前；或最近一次结果已过期。
- **healthy**：可以改写。
- **unhealthy**：不可改写。

为防止来回抖动，状态切换带迟滞，且两个方向的阈值不同：

- 连续 `failureThreshold` 次失败：healthy → unhealthy（判不健康更快）；
- 连续 `successThreshold` 次成功：unhealthy → healthy（判恢复更慢）；
- 从 unknown 出发，**一次**成功即转为 healthy（没有理由怀疑实例有问题，这样可以把部署初期没有加速的窗口压到最短）；从 unknown 出发的失败同样按 `failureThreshold` 计数。

默认值：`interval=10s`、`timeout=2s`、`failureThreshold=2`、`successThreshold=3`、`freshnessTTL=30s`。

### 默认值与代理滚动升级的数值估算

代理滚动升级期间会短暂不可用。按默认值估算：

- 判不健康最多需要 `failureThreshold × interval = 20s`（两次连续失败）。一次典型的滚动升级，单实例不可用窗口通常只有几秒到十几秒，**往往连一次连续失败都凑不齐**，健康状态根本不翻转，改写完全不受影响；
- 即使升级较慢（例如不可用 30s）导致翻转为不健康，代理恢复后需要 `successThreshold × interval ≈ 20–30s` 连续成功才恢复改写；
- 整个“不改写”窗口 ≈ 不可用时长 + 约 30s，量级在一分钟上下。此期间 Pod 仍走原始地址正常拉取，**只损失加速效果，不损失可用性**。

因此默认阈值不会让一次正常的滚动升级造成长时间不改写，同时又能在一分钟左右内屏蔽真正宕机的实例。

## 探测请求

探测**不是**检查端口能连通——端口能连上却提供不了服务（进程僵死、上游不可达）的情况更常见。每次探测向代理实例发起一个具体的镜像清单请求：

```
HEAD /v2/<probeImage 仓库>/manifests/<probeImage 标签>
Accept: application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json, ...
User-Agent: registry-proxy-healthcheck
```

默认 `probeImage: library/alpine:latest`，可配置。返回 200 视为健康，其余（错误状态、超时、连接失败）视为失败。

对代理和上游的额外压力：

- 使用 **HEAD** 而非 GET，不传输响应体；
- 清单体积极小，且代理通常会缓存热门镜像的清单，绝大多数探测命中缓存，**很少真正触达上游**，不会对上游限流产生实质影响；
- 每个实例**每个周期只探测一次**，与指向它的规则数量无关（多条规则指向同一实例不会放大探测量）；
- 建议把 `probeImage` 设为代理上常用的公共镜像，进一步降低回源概率。

## 配置校验

`timeout` 必须小于 `interval`，否则探测可能重叠、堆积；`freshnessTTL` 必须大于 `interval`，否则健康实例也会被误判为过期。包含这类组合的配置会被**拒绝**（保留当前生效配置），并在日志中给出明确提示，例如：

```
Rejecting config: health check timeout (5s) must be less than interval (5s), otherwise probes may overlap and pile up
```

## 探测目标与规则热加载

探测目标来自**当前生效的规则集合**（所有 `proxies` 值去重后的实例地址），不是启动时确定一次：

- 规则热加载后，新增的代理地址立即纳入探测，被移除的地址停止探测；
- 同一地址在规则重载后仍保留其健康状态与迟滞计数；地址被移除再加回则从头开始（unknown）；
- 刚纳入的新地址在第一次探测完成前是 unknown，按保守规则不改写。**这意味着新增一条规则后，最初几秒该规则似乎不起作用——这是有意为之**：宁可短暂不加速，也不能把流量推向一个尚未验证的实例。首次探测在目标注册时**立即**触发（不等第一个周期），所以这段窗口通常只有一次探测往返（毫秒级，最坏不超过 `timeout`），无需额外操作即可尽快完成；想进一步缩短，可调小 `timeout`。

## 启动初期的未知窗口

服务启动后、第一次探测完成前，所有实例都是 unknown，不改写。影响评估：窗口 ≈ 一次探测往返（通常毫秒级到 `timeout=2s` 以内），期间新建的 Pod 走原始地址，仅损失极短时间的加速，可接受。首次探测随目标注册立即发起，已是最快。

## 指标与日志

状态变化同时写入日志和 Prometheus 指标（`:8080/metrics`）：

| 指标 | 含义 |
| --- | --- |
| `registry_proxy_health_instance_healthy{instance}` | 实例当前是否健康（1/0，unknown 为 0） |
| `registry_proxy_health_instance_last_state_change_timestamp_seconds{instance}` | 上一次状态变化的时间 |
| `registry_proxy_health_instance_last_probe_timestamp_seconds{instance}` | 上一次探测完成时间（与当前时间比较可发现过期/卡住的探测） |
| `registry_proxy_health_probes_total{instance,result}` | 探测计数（success/failure） |

日志示例：`Proxy instance docker.linkos.org health state changed: healthy -> unhealthy`。

## 已改写 Pod 的风险与补救

代理由健康变为不健康后，**此前已经改写过的 Pod 无法自动撤销改写**，之后可能拉取失败。改写发生时代理是健康的，本需求接受这一风险，且**不提供自动补救**，原因如下：

1. 运行中 Pod 的镜像字段不可变，任何“撤销”都只能是删除重建 Pod——这属于工作负载生命周期管理，远超一个准入 webhook 的职责，会引入控制器、额外 RBAC 和驱逐/重建风险；
2. 准入 webhook 是无状态的请求-响应组件，不追踪它改写过的 Pod，为此引入状态存储得不偿失；
3. 实际影响面有限：镜像已拉取到节点的运行中 Pod 不受影响，只有改写后尚未拉取、且代理恰在此后宕机的 Pod 会失败。

需要人工补救时，删除受影响的 Pod 即可——重建时会按当时的健康状态得到正确的（不改写的）决策。

## 配置参考

```yaml
healthCheck:
  enabled: true            # 关闭后行为与引入健康门控前一致：总是改写
  interval: 10s            # 每个实例的探测周期
  timeout: 2s              # 单次探测超时，必须小于 interval
  failureThreshold: 2      # 连续失败几次判不健康
  successThreshold: 3      # 连续成功几次判恢复
  freshnessTTL: 30s        # 最近一次结果的最大保鲜期，必须大于 interval
  probeImage: library/alpine:latest  # 探测请求的镜像（仓库:标签，不含仓库主机）
```

整段可省略，省略时取上述默认值。修改 ConfigMap `registry-proxy-config` 后热生效：仅规则变化时保留各实例的状态与计数；健康检查参数本身变化时检查器会重建（状态从头开始）。
