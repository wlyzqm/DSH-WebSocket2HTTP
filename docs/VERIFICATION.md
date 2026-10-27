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
