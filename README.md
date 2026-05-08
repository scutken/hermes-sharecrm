# Hermes Agent - 纷享销客 ShareCRM 企信插件

让 Hermes Agent 接入纷享销客企信，在企信中和 AI 对话。

## 安装

```bash
git clone https://github.com/scutken/hermes-sharecrm.git ~/.hermes/plugins/sharecrm
pip install aiohttp
```

## 配置

编辑 `~/.hermes/.env`，添加以下内容：

```bash
# 必填 — 从纷享销客开放平台获取
SHARECRM_APP_ID=bot-xxxxxxxxxxxxxxxx
SHARECRM_APP_SECRET=your_secret_here

# 可选 — 默认 https://open.fxiaoke.com
SHARECRM_BASE_URL=https://open.fxiaoke.com

# 用户鉴权（至少配一种）
# 方式一：允许指定用户（完整 ID，格式 E.fs.xxxx）
SHARECRM_ALLOWED_USERS=E.fs.8017

# 方式二：允许所有用户（仅限开发测试）
SHARECRM_ALLOW_ALL_USERS=true
```

## 启动

```bash
hermes gateway restart
```

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
| `SHARECRM_ALLOWED_USERS` | 否 | 允许的用户 ID，逗号分隔 |
| `SHARECRM_ALLOW_ALL_USERS` | 否 | 设为 `true` 允许所有人 |
| `SHARECRM_HOME_CHANNEL` | 否 | 定时通知投递的 chat_id |

## License

MIT
