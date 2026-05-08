# Hermes Agent - 纷享销客 ShareCRM 企信平台插件

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Hermes Agent](https://img.shields.io/badge/Hermes%20Agent-Platform%20Plugin-6366f1)](https://hermes-agent.nousresearch.com)

为 [Hermes Agent](https://hermes-agent.nousresearch.com) 提供纷享销客 (ShareCRM) 企信 IM Gateway 接入能力。通过 SSE 长连接接收消息，HTTP API 发送回复，让 AI Agent 直接与企信用户对话。

## 功能特性

- **SSE 长连接**：通过 `GET /im-gateway/bot/events` 接收企信消息，支持断线自动重连（指数退避）
- **自动鉴权**：Token 管理 + 过期前自动刷新，Token 失效时自动重试
- **群聊上下文**：自动注入 `history_messages` 作为对话上下文
- **回复引用**：解析 `reply_message_id`，注入被引用消息文本
- **Markdown 处理**：自动 strip markdown 格式（企信不支持渲染）
- **发送失败重试**：区分可重试/不可重试错误，网络异常指数退避
- **完整错误处理**：覆盖 ShareCRM 全部错误码 (40001-50001)
- **无核心代码侵入**：基于 Hermes 插件系统，零核心代码修改

## 系统要求

| 依赖 | 说明 |
|------|------|
| **Hermes Agent** | 最新版本 |
| **Python** | 3.10+ |
| **aiohttp** | 3.x (异步 HTTP 客户端) |

## 快速开始

### 1. 安装

```bash
# 克隆插件到 Hermes 插件目录
git clone https://github.com/scutken/hermes-sharecrm.git ~/.hermes/plugins/sharecrm
```

或者手动创建：

```bash
mkdir -p ~/.hermes/plugins/sharecrm
# 将 plugin.yaml, adapter.py, __init__.py 复制到该目录
```

### 2. 安装依赖

```bash
pip install aiohttp
```

### 3. 获取凭证

在纷享销客开放平台注册应用，获取：
- **App ID**（`appId`）
- **App Secret**（`appSecret`）

文档：https://open.fxiaoke.com/im-gateway/docs/bot-api.md

### 4. 配置

```bash
# 方式一：环境变量（推荐）
hermes config set SHARECRM_APP_ID=your_app_id
hermes config set SHARECRM_APP_SECRET=your_app_secret
hermes config set SHARECRM_BASE_URL=https://open.fxiaoke.com

# 方式二：交互式配置向导
hermes gateway setup
# 选择 "纷享销客 ShareCRM"，按提示输入凭证

# 方式三：config.yaml
# 在 ~/.hermes/config.yaml 中添加：
# gateway:
#   platforms:
#     sharecrm:
#       enabled: true
#       extra:
#         app_id: "your_app_id"
#         app_secret: "your_app_secret"
#         base_url: "https://open.fxiaoke.com"
```

### 5. 启动

```bash
# 启动 Gateway（会加载所有已配置平台）
hermes gateway start

# 检查状态
hermes gateway status
```

## 配置参考

### 环境变量

| 变量 | 必填 | 说明 |
|------|------|------|
| `SHARECRM_APP_ID` | ✅ | Gateway 接入应用 ID |
| `SHARECRM_APP_SECRET` | ✅ | Gateway 接入密钥 |
| `SHARECRM_BASE_URL` | 否 | IM Gateway 基础域名，默认 `https://open.fxiaoke.com` |
| `SHARECRM_ALLOWED_USERS` | 否 | 允许交互的用户 ID，逗号分隔 |
| `SHARECRM_ALLOW_ALL_USERS` | 否 | 允许所有用户（开发测试用） |
| `SHARECRM_HOME_CHANNEL` | 否 | Cron/通知投递的默认 chat_id |

### config.yaml 配置

```yaml
gateway:
  platforms:
    sharecrm:
      enabled: true
      extra:
        app_id: "your_app_id"
        app_secret: "your_app_secret"
        base_url: "https://open.fxiaoke.com"     # 可选，默认值
        allowed_users:                            # 可选，允许的用户 ID 列表
          - "7618"
          - "9001"
        max_message_length: 4096                  # 可选，默认 4096
```

## 架构

```
纷享销客企信用户
    │
    ▼
ShareCRM IM Gateway (https://open.fxiaoke.com)
    │                              │
    │ SSE (inbound)                │ HTTP POST (outbound)
    │ /im-gateway/bot/events       │ /im-gateway/qixin/message/send
    ▼                              ▲
┌─────────────────────────────────────┐
│     ShareCRMAdapter (本插件)         │
│  ┌─────────┐  ┌──────────────────┐  │
│  │ SSE 监听 │  │  Token 管理       │  │
│  │ (自动重连)│  │  (自动刷新)       │  │
│  └────┬────┘  └──────────────────┘  │
│       │                              │
│       ▼                              │
│  ┌─────────────────────────────┐    │
│  │   handle_message(event)      │    │
│  │   ├─ 群聊上下文注入           │    │
│  │   ├─ 回复引用解析            │    │
│  │   ├─ Markdown → 纯文本       │    │
│  │   └─ 用户鉴权               │    │
│  └──────────┬──────────────────┘    │
└─────────────┼───────────────────────┘
              │
              ▼
      Hermes Agent Core
      (LLM 推理 + 工具调用)
```

## SSE 事件处理

| 事件 | 处理方式 |
|------|---------|
| `connected` | 更新 bot_full_id，标记连接成功 |
| `message` | 解析消息体 → 注入历史上下文 → 构建 `MessageEvent` → 分发 |
| `reset` | 清空本地游标 (`Last-Event-ID`)，标记需要完全重连 |
| `: keepalive` | SSE comment 心跳，忽略 |

## 错误码处理

| ShareCRM Code | 含义 | 插件行为 |
|---------------|------|---------|
| `0` | 成功 | 正常 |
| `40001`-`40005` | 参数/账号错误 | 不重试，记录日志 |
| `40100`-`40101` | Token 无效/过期 | 自动刷新 Token，重试 1 次 |
| `50000` | 服务内部错误 | 指数退避重试 |
| `50001` | Bot 未在线 | 标记可重试，等待 SSE 恢复 |

## 高级功能

### 用户鉴权

```bash
# 仅允许指定用户
hermes config set SHARECRM_ALLOWED_USERS=7618,9001

# 或允许所有用户（dev 模式）
hermes config set SHARECRM_ALLOW_ALL_USERS=true
```

### Cron 投递

```bash
# 设置通知投递目标
hermes config set SHARECRM_HOME_CHANNEL="0:fs:session123:"

# 创建定时任务
hermes cron create --prompt "每天早上9点发送今日待办" --schedule "0 9 * * *" --deliver sharecrm
```

### 消息长度限制

默认 4096 字符，可在 `config.yaml` 中调整：

```yaml
extra:
  max_message_length: 8000
```

## 文件结构

```
hermes-sharecrm/
├── README.md           # 本文件
├── LICENSE             # MIT License
├── plugin.yaml         # 插件元数据 & 环境变量声明
├── adapter.py          # 核心适配器 (SSE 接收 + HTTP 发送)
└── __init__.py         # 插件入口
```

## 常见问题

**Q: Gateway 启动后看不到 ShareCRM？**

检查环境变量或 config.yaml 是否正确配置。运行 `hermes gateway status` 查看平台列表。

**Q: 消息发送了但企信收不到？**

确认 `chat_id` 来自入站消息的 `data.chat_id` 字段，不要自行构造。ShareCRM 的 `chat_id` 格式为 `{env}:{ea}:{sessionId}:{parentSessionId}`。

**Q: 群聊中 Bot 不响应？**

检查 `SHARECRM_ALLOWED_USERS` 是否正确配置，或尝试 `SHARECRM_ALLOW_ALL_USERS=true`。

**Q: 消息中出现 `**` 乱码？**

Adapter 自动处理了 Markdown 格式剥离。如果仍有问题，确保使用的是最新版本。

**Q: 如何查看日志？**

```bash
hermes logs --follow --level DEBUG | grep -i sharecrm
```

## 相关链接

- [Hermes Agent 文档](https://hermes-agent.nousresearch.com/docs)
- [添加平台适配器指南](https://hermes-agent.nousresearch.com/docs/zh-Hans/developer-guide/adding-platform-adapters)
- [ShareCRM IM Gateway API 文档](https://open.fxiaoke.com/im-gateway/docs/bot-api.md)
- [纷享销客开放平台](https://open.fxiaoke.com)

## License

MIT License - 详见 [LICENSE](LICENSE) 文件。
