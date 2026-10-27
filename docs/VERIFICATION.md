# 验收说明

受测环境：DSH `0.2.1-rc.2`，Node `22.23.3`。核心传输验证于 2026-10-27 完成；`0.1.1` 补齐公开分发与中文显示元数据。

## 自动测试

```bash
npm test
```

16 项：6 项客户端测试、10 项真实 Node HTTP 集成测试。覆盖多流复用、单轮询、下行重放及确认、上行顺序、半关闭、业务错误、取消、断线新代次、BFCache 恢复、认证/来源拒绝、Cookie/Host 隔离、无关 Cookie 轮换、并发轮询、缓冲限制及 Peer disposal。

## 真实链路

测试路径为浏览器 → 公网 HTTPS／Cloudflare → OpenResty HTTPS 反代 → 本机 DSH → 插件 → 原生 Gateway。

使用 Chrome DevTools MCP 在页面执行前把 WebSocket 构造器替换为直接抛错的类，并统计调用次数。界面、目录选择、工作区创建、会话列表和实时订阅正常；6 条逻辑流共用一个页面 HTTP 会话。WebSocket 构造尝试数与网络请求数均为 0。

通过原生 `terminal/create` 创建临时 bash，再用 HTTP 承载的 `terminal/attach` 双向流写入：

```bash
printf 'DSH_HTTP_%s\n' 'DUPLEX_OK'
```

浏览器收到 `snapshot`、`ack`、`output`，实际输出匹配 `DSH_HTTP_DUPLEX_OK`；之后取消并关闭终端。保持页面打开时重启 DSH，页面无需重新登录即可恢复到 6 条流，WebSocket 尝试数仍为 0。

## 公网认证

| 条件 | 响应 |
| --- | --- |
| 未登录访问状态或 open | 401 |
| 伪造转发头但没有 Cookie | 401 |
| 有效 Cookie，非法 Origin | 403 |
| 有效 Cookie和合法公共 Origin | 200 |
| 只有 sessionId，没有 Cookie | 401 |
| 已认证关闭传输会话 | 200 |
| 重用已关闭会话 | 410 |

没有向仓库提交运行凭证、部署日志、用户会话或机器相关配置。模型回复、模型工具审批以及自行建立 WebSocket 的第三方插件不在本次验收范围内。

## 0.1.1 显示与安装

本机 profile 已由旧名称迁移为 `dsh-websocket2http`。原生插件详情实测显示“WebSocket 转 HTTP”、版本 `0.1.1`、新包 ID 与中文组件描述，状态为 `1 total · 1 running`。英文界面也按需求显示上述中文元数据。重载后 HTTP 连接保持正常，WebSocket 尝试数为 0。

## 0.1.2 大历史快照与持续 410 修复

真实使用中发现，原生 Web 界面用 `maxMessages: 500`、`turnWindow: { minMessages: 50, minTurns: 2 }`、`assistantStream: true` 读取历史时，一个历史快照超过原先的 2 MiB 队列限制。0.1.1 会销毁传输会话；重连再次读取同一快照，便进入 `open/send 成功 → poll 410` 循环。客户端自己的 2 MiB 下行缓冲检查也会拒绝这个快照。

对同一真实会话、同一历史读取参数只读复测：

| 版本 | 结果 |
| --- | --- |
| 0.1.1 | `poll` 返回 410，`transport session expired` |
| 0.1.2 | `poll` 返回 200，完整快照响应为 2,264,046 字节（约 2.16 MiB） |

修复改为基于客户端 ACK 的有界背压，常规队列目标为 2 MiB；空队列允许单个最大 32 MiB 的快照。大于单帧上限时仅失败对应逻辑流，返回明确的 `transport/frame-too-large`。客户端在消费者可接收时直接交付，在确实排队时等待容量；取消、离页、失效和 Peer disposal 都会解除等待。

30 项自动测试全部通过：12 项客户端、17 项真实 HTTP Host 集成、1 项真实 Host/Client 联调。联调并发接收一个 3 MiB 快照和 16 个 512 KiB 数据块，总计 11 MiB；消费者每块延迟 5 ms。内容完整有序，仅一个会话，HTTP 全部 200，失败数为 0，两端队列不超过单个大快照；退出后会话与任务均释放。

本机部署修复版并刷新原故障页面后，用户返回的诊断为：`sessions: 1`、`failures: 0`、`connected: true`、`streams: 7`、`bufferedBytes: 0`。没有把用户 Cookie、会话内容或业务会话 ID 写入此记录。
