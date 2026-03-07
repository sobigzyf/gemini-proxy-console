# Gemini 控制台

控制台入口：`run_console.py`，默认端口 `18423`。

## Docker 部署（Resin + 控制台）

### 1) 准备环境变量

```bash
cp .env.example .env
# 编辑 .env，至少修改：
# RESIN_ADMIN_TOKEN
# RESIN_PROXY_TOKEN
# WORKER_DOMAIN / EMAIL_DOMAIN / ADMIN_PASSWORD
```

### 2) 首次启动

```bash
docker compose build
docker compose up -d
docker compose logs -f --tail=200 resin gemini-console
```

访问：

- 控制台：`http://<服务器IP>:18423`
- Resin UI：`http://<服务器IP>:2260`

### 3) 保留现有账号和设置再迁移

把旧环境的以下文件/目录同步到项目目录后再 `docker compose up -d`：

- `gemini_accounts/`
- `console_config.json`
- `console_state.json`
- `maintenance_status.json`
- `mail_tokens.txt`
- `mailbox_tokens.json`（如果存在）
- `existing_accounts.json`（如果存在）
- `.env`

也可用现有脚本打包恢复：

```bash
# 旧环境打包
bash deploy/package_runtime_bundle.sh

# 新环境恢复
bash deploy/restore_runtime_bundle.sh runtime_bundle_*.tar.gz .
```

### 4) 后续改代码

`docker-compose.yml` 已挂载 `./:/app`，改代码后：

```bash
docker compose restart gemini-console
```

若改了依赖再重建：

```bash
docker compose build --no-cache
docker compose up -d
```

## 功能说明

- 双代理引擎：`EasyProxies` / `Resin` / `Auto(Resin优先)`
- 补号与维护分段轮换：
  - EasyProxies：按节点配额轮换
  - Resin：按 `platform.account` 身份配额轮换（适合千级节点规模）
- 账号池保障：自动补号、自动维护、优先级调度
- 服务端同步：补号/维护完成后自动同步 `all_account.json`

## 控制台配置建议

### Resin（推荐千级节点）

- 代理引擎：`resin` 或 `auto`
- Resin API 地址（容器内）：`http://resin:2260`
- Resin 代理入口（容器内）：`http://resin:2260`
- `Resin 管理 Token` = `RESIN_ADMIN_TOKEN`
- `Resin 代理 Token` = `RESIN_PROXY_TOKEN`
- 补号/维护 Platform 分开配置（如 `gemini-register` / `gemini-maintain`）

### EasyProxies（保留兼容）

- 仍可单独启用并使用订阅同步
- 可配固定节点与节点配额轮换

## 运行参数（常用）

- `CHROME_BIN`：容器默认 `/usr/bin/chromium`
- `UC_STARTUP_RETRIES`：`uc.Chrome` 启动最大重试次数（默认 3）
- `UC_STARTUP_RETRY_WAIT`：启动重试间隔秒数（默认 3）
- `UC_CLEAR_CACHE_ON_RETRY`：默认 `0`
- `UC_DOWNLOAD_PROXY_ENABLED`：默认 `0`

## 数量逻辑（避免冲突）

- 顶部 `补号数量`：手动启动一次任务总量
- `默认补号数量`：仅用于回填顶部输入框默认值
- `每次补号数量`：自动补号单次总量
- `max_replenish_per_round`：兼容字段，会与 `auto_register_batch_size` 自动对齐
