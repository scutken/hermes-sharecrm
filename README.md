# Hermes Agent - 纷享销客 ShareCRM 企信插件

让 Hermes Agent 接入纷享销客企信，在企信中和 AI 对话。

对接 [ShareCRM IM Gateway](https://open.fxiaoke.com/im-gateway/docs/bot-api.md)：入站走 SSE 长连接，出站走 HTTP 开放接口。

## 安装

```bash
git clone https://github.com/scutken/hermes-sharecrm.git ~/.hermes/plugins/sharecrm
pip install aiohttp
```

> 新版 Hermes（用户插件默认不自动加载）需要显式启用：
>
> ```bash
> hermes plugins enable sharecrm-platform
> ```
>
> 或者在 `~/.hermes/config.yaml` 里加入：
>
> ```yaml
> plugins:
>   enabled:
>     - sharecrm-platform
> ```


## 配置

编辑 `~/.hermes/.env`，添加以下内容：

```bash
# 必填 — 从纷享销客开放平台获取
SHARECRM_APP_ID=bot-xxxxxxxxxxxxxxxx
SHARECRM_APP_SECRET=your_secret_here

# 可选 — Dashboard 可配；留空则使用 https://open.fxiaoke.com
SHARECRM_BASE_URL=https://open.fxiaoke.com

# 可选 — SSE 协议版本，默认 1.4.0（低于 1.4.0 收不到图片/图文）
SHARECRM_SSE_VERSION=1.4.0

# 可选 — 单条消息最大长度，默认 4096，超长自动分片
SHARECRM_MAX_MESSAGE_LENGTH=4096

# 可选 — 是否把群聊 history_messages 作为上下文注入，默认 true
SHARECRM_INCLUDE_HISTORY=true

# 用户鉴权（至少配一种）
# 方式一：允许指定用户（完整 ID，格式 E.fs.xxxx）
SHARECRM_ALLOWED_USERS=E.fs.8017

# 方式二：允许所有用户（仅限开发测试）
SHARECRM_ALLOW_ALL_USERS=true
```

### 通过 config.yaml 配置

插件实现了 `apply_yaml_config_fn`，也可以在 `~/.hermes/config.yaml` 里配置（`env` 优先于 YAML）：

```yaml
sharecrm:
  app_id: bot-xxxxxxxxxxxxxxxx
  app_secret: your_secret_here
  base_url: https://open.fxiaoke.com
  allowed_users: [E.fs.8017, E.fs.9001]
  allow_all_users: false
  home_channel: "0:fs:session123:"
  max_message_length: 4096
```

## 启动

```bash
hermes gateway restart
```

## 功能说明

### 出站消息支持 Markdown

企信出站接口只接受 `text` 字段，但企信客户端会**渲染文本中的 Markdown**，所以本插件不会剥离 Markdown。Agent 可以正常使用 **粗体**、*斜体*、`代码`、列表、标题、链接、图片链接等语法。

注意：出站**不支持**原生图片/文件/语音附件。需要发图时请给出图片链接（Markdown 图片语法即可）。

### 入站图片

当 SSE 以 `version >= 1.4.0` 建连时，企信会下发图片/图文消息。插件会：

- 把图片下载到 Hermes 图片缓存（走官方 `cache_image_from_bytes`，带 TTL 清理）；
- 通过 `media_urls` 传给 Agent，供视觉能力读取；
- 对图片 URL 做 SSRF 校验（拒绝内网/回环地址）。

### 历史消息上下文

群聊的 `history_messages` 会通过 `MessageEvent.channel_context` 注入，由 Gateway 在触发消息之前拼接，不会污染本轮正文，也不会和会话自身的 transcript 重复。可用 `SHARECRM_INCLUDE_HISTORY=false` 关闭。

### 定时任务 / 进程外投递

插件注册了 `standalone_sender_fn`，因此 `hermes cron` 在**独立进程**中也能投递到 ShareCRM（此前会报 `No live adapter for platform 'sharecrm'`）。

```bash
# 投递到 home channel
hermes cron create --deliver sharecrm --schedule "0 9 * * *" --prompt "..."
```

> 需要 `standalone_sender_fn` 支持，即 **Hermes >= v2026.5.16**。更老的版本只能让 cron 与 gateway 同进程运行。

### 可靠性与重连

- Token 获取按官方建议重试（1s/2s/4s + jitter）；账号/参数类错误不重试。
- SSE 重连优先遵循服务端 `connected` 事件下发的 `retry`，否则指数退避。
- 收到 `reset`（游标失效）会清空 `Last-Event-ID` 并立即重连。
- 发送遇到 `50001`（Bot 未在线）会等待 SSE 恢复后重试一次。
- 相同 `message_id` 会被去重，避免断线重连时的重复处理。

## 用户鉴权

### 查看被拒绝的用户 ID

当未授权用户发消息时，Gateway 会自动回复一个配对码。同时可以在日志中看到被拒绝的用户：

```bash
hermes logs --follow | grep Unauthorized
# 输出示例：Unauthorized user: E.fs.8017 (8017) on sharecrm
```

### 放开指定用户

编辑 `~/.hermes/.env`，修改 `SHARECRM_ALLOWED_USERS`：

```bash
# 单个用户
SHARECRM_ALLOWED_USERS=E.fs.8017

# 多个用户
SHARECRM_ALLOWED_USERS=E.fs.8017,E.fs.9001
```

然后重启：`hermes gateway restart`

### 配对码自助授权

未授权用户给 Bot 发消息时会收到配对码，管理员执行：

```bash
hermes pairing approve sharecrm <配对码>
```

无需重启，即刻生效。

## 环境变量参考

| 变量 | 必填 | 说明 |
|------|------|------|
| `SHARECRM_APP_ID` | 是 | 应用 ID |
| `SHARECRM_APP_SECRET` | 是 | 应用密钥 |
| `SHARECRM_BASE_URL` | 否 | 接口地址，默认 `https://open.fxiaoke.com` |
| `SHARECRM_SSE_VERSION` | 否 | SSE 协议版本，默认 `1.4.0` |
| `SHARECRM_MAX_MESSAGE_LENGTH` | 否 | 单条消息最大长度，默认 `4096` |
| `SHARECRM_INCLUDE_HISTORY` | 否 | 是否注入群聊历史，默认 `true` |
| `SHARECRM_ALLOWED_USERS` | 否 | 允许的用户 ID，逗号分隔 |
| `SHARECRM_ALLOW_ALL_USERS` | 否 | 设为 `true` 允许所有人 |
| `SHARECRM_HOME_CHANNEL` | 否 | 定时通知投递的 chat_id；也可在会话里发 `/sethome` |

## 测试

```bash
# 需要装有 hermes-agent 的 Python 环境
python -m pytest tests/ -q
# 或
python -m unittest discover -s tests -v
```

## 兼容性

插件同时兼容新旧 Hermes 运行时：

- `gateway.platforms._shared`（Hermes >= 2026-09-02）提供 profile-scoped 配置读取；不可用时由 `_compat.py` 退回等价的 `os.environ` 实现。
- `gateway.platforms.event`（模块拆分后）与旧版 `gateway.platforms.base` 的 `MessageEvent` 均支持。

## License

MIT
