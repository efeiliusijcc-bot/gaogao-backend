# OpenClaw 云节点配置说明

## 服务器信息

| 项目 | 值 |
|---|---|
| IP | 74.121.148.204 |
| SSH 端口 | 22 |
| SSH 用户 | root |
| OpenClaw 端口 | 1888（映射到容器内部 18789） |
| 部署方式 | Docker |

## OpenClaw 连接信息

| 项目 | 值 |
|---|---|
| API Base URL | `http://74.121.148.204:1888/v1` |
| Health URL | `http://74.121.148.204:1888/health` |
| API Token | `0994bb22cf581322f8400fc135a50418ab4c8097a40bb6e1` |
| 模型 | `openclaw/report-agent` |

## 可用模型列表

- `openclaw`
- `openclaw/default`
- `openclaw/main`
- `openclaw/report-agent`

## 本地项目配置（.env）

已在项目根目录 `.env` 文件中配置以下环境变量：

```
OPENCLAW_BASE_URL=http://74.121.148.204:1888/v1
OPENCLAW_API_KEY=0994bb22cf581322f8400fc135a50418ab4c8097a40bb6e1
OPENCLAW_MODEL=openclaw/report-agent
```

项目通过 `server/index.ts` 中的 `import 'dotenv/config'` 自动加载 `.env` 文件，无需额外操作。

## 连接验证

- Health 检查：`curl http://74.121.148.204:1888/health` 返回 `{"ok":true,"status":"live"}`
- API 认证：使用 Bearer Token 认证，已验证通过
- Chat Completions：`/v1/chat/completions` 接口已验证可用

## 远程 Docker 管理

```bash
# SSH 免密登录
ssh -i ~/.ssh/id_ed25519 root@74.121.148.204

# 查看 OpenClaw 容器状态
docker ps | grep openclaw

# 查看日志
docker logs openclaw

# 重启容器
docker restart openclaw
```
