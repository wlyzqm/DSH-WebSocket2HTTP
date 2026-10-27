# WebSocket 转 HTTP

**`dsh-websocket2http`** 是 DeepSeek Harness（DSH）的 Web 传输插件。它把原生界面的 WebSocket 实时链路转换为 **HTTP 长轮询**，让禁止 WebSocket 的代理网络也能承载实时更新、双向流和断线恢复。

WebSocket-to-HTTP long polling for the native DeepSeek Harness Web GUI. One polling channel per page, bidirectional streams, cancellation and reconnect, with native DSH authentication.

| 元数据 | 内容 |
| --- | --- |
| 显示名称 | WebSocket 转 HTTP |
| 插件 ID／组件 ID | `dsh-websocket2http` |
| 作者 | [ZotinColin／wlyzqm](https://github.com/wlyzqm) |
| 当前版本 | `0.1.1` |
| 已验证 DSH | `0.2.1-rc.2` |
| Node.js | 22 或更高版本 |
| 许可证 | [MIT](./LICENSE) |

## 安装

在安装了 DSH 的机器上运行：

```bash
dsh plugin --profile web add github:wlyzqm/DSH-WebSocket2HTTP#v0.1.1
```

安装后重启正在运行的 DSH Web 服务，再刷新浏览器。使用其他 profile 时，将 `web` 换成对应名称。直接安装开发分支可使用 `github:wlyzqm/DSH-WebSocket2HTTP`。

源码直接包含运行所需的 JavaScript，没有第三方运行依赖，不需要 `prepare`、编译或发布到 npm。`dsh plugin` 需要其安装方式提供 pnpm；具体要求见[官方打包与安装教程](https://deepseek-harness.github.io/deepseek-harness/en/develop/basic/publish)。

当前通过 `engines.dsh` 精确声明受测版本。其他 DSH 版本需先验证 Connection/Gateway 接口兼容性，再扩大范围。

## 适用场景

- 企业网关或 HTTP 代理明确禁用 WebSocket。
- 网页及普通 API 请求可以访问，但 DSH 一直显示连接失败或反复重连。
- 希望保留原生 DSH Web 界面、会话和终端，而不是另外部署一套聊天前端。

启用后固定使用 HTTP 模式，**不会先尝试 WebSocket**。模型提供方的通信方式保持由对应模型插件管理；如果模型插件自己使用 WebSocket，需要单独配置它。

插件详情的组件区会同时显示组件行 ID 与模块／包名；当前两者均为 `dsh-websocket2http`，因此会看到两条相同文本。这是 DSH 的技术标识展示，`1 total · 1 running` 表示实际只有一个运行组件。

## 工作方式

浏览器在 DSH Connection 启动前安装 `__DSH_TRANSPORT__.openStream`，服务端通过 `ctx.typertGateway.wireStream.open()` 分派原有逻辑流。

| 内容 | 传输方式 |
| --- | --- |
| 普通 RPC、上传和静态资源 | DSH 原有 HTTP 接口 |
| 实时事件及逻辑流下行 | 每个页面共用一个 HTTP 长轮询通道 |
| 上行数据、半关闭及取消 | 有序 HTTP POST |
| 断线及服务重启 | DSH 原有连接代次与基线恢复机制 |

每次轮询最多等待 20 秒，返回完整 JSON。下行保留到客户端确认，重复帧去重；序号缺口或缓冲超限会结束当前连接，由 DSH 重新获取基线。上行操作不会盲目重放。页面离开时尝试关闭连接，空闲会话在 65 秒后释放，从 BFCache 返回时允许重新连接。

## 代理与认证

代理需要允许同源 HTTPS GET/POST，读超时应大于 20 秒。反代继续指向原 DSH Web 端口，不需要开放新端口。部署者如有更短的代理超时，可以调整 `host.js` 的 `pollMs` 默认值并重启。

新增接口先调用 `ctx.connection.admit()`，保留原生 Cookie、Host/Origin 与 Peer 校验。传输会话额外绑定认证 Cookie 和 authority；只有 sessionId 而没有有效认证不能访问。插件不会设置 `ownsHost`，也不会新增 localhost 免密规则。

HTTP 后端经 HTTPS 反代时，建议按[官方公网部署要求](https://github.com/deepseek-ai/deepseek-harness/blob/master/docs/user/guide/public-deployments.md)配置来源与 Cookie，并避免把含启动令牌的 URL 写入访问日志。

## 检查是否生效

登录后在浏览器控制台执行：

```javascript
__DSH_HTTP_TRANSPORT__.snapshot()
```

`connected: true` 表示 HTTP 传输会话已建立。`streams` 是活跃逻辑流数，`sessions` 在重连后增加；快照不包含凭证或会话 ID。也可通过已认证的 `GET /api/http-transport/status` 查看服务端计数。未登录返回 401，非法来源返回 403。

## 本地开发与测试

```bash
git clone https://github.com/wlyzqm/DSH-WebSocket2HTTP.git
cd DSH-WebSocket2HTTP
npm test
dsh plugin --profile web add "link:$PWD"
```

修改后重启 DSH，再刷新页面。测试使用 Node 内置测试框架和虚拟或本机 HTTP 服务，无需模型凭证。

已通过 16 项自动测试，以及在浏览器 WebSocket 构造器被阻断条件下的公网 HTTPS 界面、真实终端双向传输、取消和服务重启恢复验证。详见[验收说明](./docs/VERIFICATION.md)。模型回复没有作为此次验收的一部分。

## 停用或卸载

可以在 DSH 插件页面停用或卸载“WebSocket 转 HTTP”，然后按界面要求重启并刷新页面。CLI 卸载：

```bash
dsh plugin --profile web remove dsh-websocket2http
```

如果停用后界面因网络禁用 WebSocket 无法操作，可在服务所在机器通过 CLI 或 profile 配置移除本 bundle 后重启。

## 公开搜索与发布

此仓库使用 GitHub `dsh-plugin` 主题；[官方 README](https://github.com/deepseek-ai/deepseek-harness#community-and-support)建议通过该主题让社区发现插件。可按 `dsh-websocket2http`、`DSH-WebSocket2HTTP`、`WebSocket 转 HTTP` 或 `HTTP 长轮询` 搜索，关键词也写入了包清单和显示元数据。

GitHub 主题发现与 DSH 的自定义 Discover 市场并不是同一机制：[官方市场指南](https://deepseek-harness.github.io/deepseek-harness/en/guide/plugin-market)说明，Discover 只搜索用户添加的索引，索引维护者决定收录与刷新时间；因此添加主题并不代表所有第三方市场立即收录。即使尚未收录，也可以使用上方 GitHub 安装命令直接安装。

第三方插件若自行创建其他 WebSocket，需要另行适配。本插件接管的是 DSH 原生 Connection/Gateway 流。
