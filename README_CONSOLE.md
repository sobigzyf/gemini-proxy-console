# Gemini 控制台

控制台入口：`run_console.py`，默认端口 `18423`。

## Docker 部署（推荐）

### 1) 首次启动

```bash
docker compose build
docker compose up -d
docker compose logs -f --tail=200 gemini-console
```

访问：`http://<服务器IP>:18423`

### 2) 保留现有账号和设置再迁移

把你旧环境中的这些文件/目录同步到当前项目目录后，再 `docker compose up -d`：

- `gemini_accounts/`
- `console_config.json`
- `console_state.json`
- `maintenance_status.json`
- `mail_tokens.txt`
- `mailbox_tokens.json`（如果存在）
- `existing_accounts.json`（如果存在）
- `.env`

也可以直接打包/恢复：

```bash
# 在旧环境打包
bash deploy/package_runtime_bundle.sh

# 在新服务器恢复（会解压到项目目录）
bash deploy/restore_runtime_bundle.sh runtime_bundle_*.tar.gz .
```

或者一步完成恢复+部署：

```bash
bash deploy/deploy_with_bundle.sh runtime_bundle_*.tar.gz
```

### 3) 后续修改代码

`docker-compose.yml` 已挂载 `./:/app`，改代码后可直接重启生效：

```bash
docker compose restart gemini-console
```

若变更了依赖（`requirements.txt`）再重建：

```bash
docker compose build --no-cache
docker compose up -d
```

## 本地 Python 方式（可选）

```bash
pip install -r requirements.txt
python run_console.py
```

## 功能

- Web 控制台：启动/停止补号、手动维护、实时日志（SSE）
- EasyProxies 接入：统一代理入口 + 管理 API + 订阅同步
- 自动任务：自动补号、自动维护、优先级调度、节点轮换
- 服务端同步：补号/维护成功后自动同步 `all_account.json`

## 对接 EasyProxiesV2

先部署：`https://github.com/xiamuceer-j/EasyProxiesV2`，确认：

- 监听代理入口（示例：`http://127.0.0.1:2323`）
- 管理 API（示例：`http://127.0.0.1:7840`）

控制台中配置：

- `启用 EasyProxies 主代理`
- `EasyProxies 监听代理`
- `EasyProxies 管理地址`
- `EasyProxies 管理密码`（如配置了密码）
- `启用订阅同步` + `订阅链接`

## 运行参数（常用）

- `CHROME_BIN`：容器默认 `/usr/bin/chromium`
- `UC_STARTUP_RETRIES`：`uc.Chrome` 启动最大重试次数（默认 3）
- `UC_STARTUP_RETRY_WAIT`：启动重试间隔秒数（默认 3）
- `UC_CLEAR_CACHE_ON_RETRY`：默认 `0`，失败重试不清缓存驱动
- `UC_DOWNLOAD_PROXY_ENABLED`：默认 `0`，驱动下载不走代理

## 数量逻辑（避免冲突）

- 顶部 `补号数量`：手动启动一次任务的总量。
- `默认补号数量`：仅用于回填顶部输入框默认值。
- `每次补号数量`：自动补号单次总量。
- `max_replenish_per_round`：兼容字段，后端会自动与 `auto_register_batch_size` 保持一致。
- 启用 EasyProxies 节点轮换后：
  - 补号按 `每节点补号配额` 自动分段并切换节点，直到本次总量完成。
  - 维护按 `每节点维护配额` 自动分段并切换节点，直到待维护账号处理完或不足一段。
