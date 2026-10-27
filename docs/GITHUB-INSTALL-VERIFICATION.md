# 独立服务器 GitHub 在线安装验证

日期：2026-10-27。此次在另一台已有 DSH 的服务器上，使用公开 GitHub 仓库安装 `v0.1.1`，验证 README 中的安装入口。

## 环境与准备

| 项目 | 版本 |
| --- | --- |
| DSH | `0.2.1-rc.2` |
| Node.js | `24.15.0` |
| pnpm | `12.15.0` |

安装前 Web profile 没有第三方依赖，bundle 列表只有 DSH base 和 web-app。先备份 profile 配置，再通过目标机器已有的 Corepack 启用 pnpm：

```bash
corepack enable pnpm
COREPACK_ENABLE_DOWNLOAD_PROMPT=0 corepack pnpm --version
```

## 安装命令与来源

```bash
dsh plugin --profile web add github:wlyzqm/DSH-WebSocket2HTTP#v0.1.1
```

命令执行成功，自动把插件注册进 `dsh.profile.bundles`。profile 的依赖项为：

```json
{
  "dsh-websocket2http": "github:wlyzqm/DSH-WebSocket2HTTP#v0.1.1"
}
```

`pnpm-lock.yaml` 锁定的来源：

```text
https://codeload.github.com/wlyzqm/DSH-WebSocket2HTTP/tar.gz/51a0b5ea2c8cd3eba8438d49a9934a2d9580906e
```

这是 pnpm 对 GitHub 仓库依赖的正常解析方式。没有向目标服务器传输插件离线安装包，也没有使用 `file:` 或 `link:` 依赖。

重启目标机器现有的 DSH 服务后，插件管理接口报告：版本 `0.1.1`，来源 `github:wlyzqm/DSH-WebSocket2HTTP#v0.1.1`，installed/enabled 均为 true，组件 ID 为 `dsh-websocket2http`，中文名称和描述正常。

## 实际链路验证

验收浏览器运行在测试工作站，避免额外占用目标服务器内存。通过目标服务器的公网 HTTPS／Cloudflare 入口登录，在任何页面脚本执行前将 WebSocket 构造器替换为直接抛错的类。

- HTTP 连接正常，5 条逻辑流共用一个传输会话。
- WebSocket 构造尝试数为 0，网络面板没有 WebSocket 请求。
- 浏览器控制台没有错误或警告。
- 在专用测试会话创建原生 bash 终端，经 HTTP 承载的双向 `terminal/attach` 写入命令并收到 `snapshot`、`ack`、`output`。
- 实际输出匹配 `DSH_GITHUB_SZ_DUPLEX_OK`。
- 测试后取消流、关闭终端并归档测试会话。
- 未登录访问插件状态接口返回 401。

验证完成后 DSH 服务保持运行，插件保留启用。本次没有调用模型 API，也没有在目标服务器启动浏览器。
