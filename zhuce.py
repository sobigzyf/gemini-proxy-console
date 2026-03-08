import sys

try:
    import distutils  # type: ignore # noqa: F401
except ModuleNotFoundError:
    try:
        import setuptools._distutils as _distutils

        sys.modules.setdefault("distutils", _distutils)
        sys.modules.setdefault("distutils.version", _distutils.version)
    except Exception:
        pass

import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup
from urllib.parse import parse_qs, unquote, urlparse
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor
from email import policy
from email.header import decode_header, make_header
from email.parser import Parser
import csv
import argparse
import threading
import time, random, json, os, re, subprocess, quopri, requests, shutil, base64, hashlib
try:
    import msvcrt
except ImportError:
    msvcrt = None

# 配置
TOTAL_ACCOUNTS = 1
EMAIL_CODE_TIMEOUT_SECONDS = 180
EMAIL_CODE_POLL_INTERVAL_SECONDS = 5
EMAIL_FETCH_LIMIT = 20
EMAIL_SUBMIT_WAIT_SECONDS = 30
OUTPUT_DIR = "gemini_accounts"
MAIL_TOKENS_TEXT_FILE = "mail_tokens.txt"
MAINTENANCE_STATE_FILE = "maintenance_status.json"
DEFAULT_MAIL_TOKEN_TTL_SECONDS = 7 * 24 * 3600
LOGIN_URL = "https://auth.business.gemini.google/login?continueUrl=https:%2F%2Fbusiness.gemini.google%2F&wiffid=CAoSJDIwNTlhYzBjLTVlMmMtNGUxZS1hY2JkLThmOGY2ZDE0ODM1Mg"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def _load_env_file(path):
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip().lstrip("\ufeff")
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value


_load_env_file(os.path.join(BASE_DIR, ".env"))
DEFAULT_CHROME_BINARY = r"C:\Program Files\Google\Chrome Dev\Application\chrome.exe"
CHROME_BINARY_PATH = os.getenv("CHROME_BIN", DEFAULT_CHROME_BINARY).strip()
EMAIL_SERVICE = None
MAINTENANCE_STATE = None
MAIL_TOKEN_TTL_SECONDS = None
MAIL_TOKENS_FILE_LOCK = threading.Lock()
MAINTENANCE_FILE_LOCK = threading.Lock()
RUNTIME_PROXY = ""
MAIL_PROXY_ROTATE_RETRIES_DEFAULT = 1
MAIL_PROXY_ROTATE_THRESHOLD_DEFAULT = 2

# XPath
XPATH = {
    "email_input": "/html/body/c-wiz/div/div/div[1]/div/div/div/form/div[1]/div[1]/div/span[2]/input",
    "continue_btn": "/html/body/c-wiz/div/div/div[1]/div/div/div/form/div[2]/div/button",
    "verify_btn": "/html/body/c-wiz/div/div/div[1]/div/div/div/form/div[2]/div/div[1]/span/div[1]/button",
}

NAMES = ["James Smith", "John Johnson", "Robert Williams", "Michael Brown", "William Jones",
         "David Garcia", "Mary Miller", "Patricia Davis", "Jennifer Rodriguez", "Linda Martinez"]

# 预创建的邮箱队列
email_queue = []

def log(msg, level="INFO"): print(f"[{level}] {msg}")


def _normalize_proxy_value(proxy_value):
    value = str(proxy_value or "").strip().strip('"').strip("'")
    if not value:
        return ""
    if "://" in value:
        return value
    if ":" in value:
        return f"http://{value}"
    return ""


def _mask_proxy_for_log(proxy_value):
    value = str(proxy_value or "").strip()
    if not value or "@" not in value:
        return value
    return re.sub(r":([^:@/?#]+)@", ":***@", value, count=1)


def _get_runtime_proxy():
    return _normalize_proxy_value(RUNTIME_PROXY)


def _env_int(name, default, minimum=0, maximum=20):
    raw = str(os.getenv(name, "") or "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except Exception:
        return default
    return max(minimum, min(value, maximum))


def _env_flag(name, default=True):
    raw = str(os.getenv(name, "") or "").strip().lower()
    if not raw:
        return default
    return raw not in {"0", "false", "no", "off"}


def _env_float(name, default, minimum=0.1, maximum=10.0):
    raw = str(os.getenv(name, "") or "").strip()
    if not raw:
        return float(default)
    try:
        value = float(raw)
    except Exception:
        return float(default)
    return max(float(minimum), min(float(maximum), value))


def _mail_proxy_rotate_retries():
    return _env_int("MAIL_PROXY_ROTATE_RETRIES", MAIL_PROXY_ROTATE_RETRIES_DEFAULT, 0, 10)


def _mail_proxy_rotate_threshold():
    return _env_int("MAIL_PROXY_ROTATE_THRESHOLD", MAIL_PROXY_ROTATE_THRESHOLD_DEFAULT, 1, 10)


def _mail_timeout_should_rotate():
    return _env_flag("MAIL_TIMEOUT_PROXY_ROTATE", True)


def _status_base_url():
    value = str(os.getenv("SOCKS5_POOL_STATUS_URL", "") or "").strip()
    if not value:
        return ""
    if "://" not in value:
        value = "http://" + value
    return value.rstrip("/")


def _is_retryable_mail_fetch_error(exc):
    text = str(exc or "").lower()
    retryable_keywords = (
        "connection aborted",
        "connectionreseterror",
        "winerror 10054",
        "max retries exceeded",
        "proxyerror",
        "connection reset",
        "read timed out",
        "timeout",
        "temporarily unavailable",
        "remote end closed connection",
        "connection refused",
    )
    return any(k in text for k in retryable_keywords)


def _is_retryable_proxy_runtime_error(exc, driver=None):
    text = str(exc or "").lower()
    retryable_keywords = (
        "err_proxy_connection_failed",
        "proxy connection failed",
        "err_tunnel_connection_failed",
        "err_socks_connection_failed",
        "net::err_proxy",
        "this site can't be reached",
        "无法访问此网站",
        "login page blank/incomplete",
        "nonetype' object has no attribute 'is_displayed'",
        "stacktrace:",
        "connection reset",
        "connection aborted",
        "max retries exceeded",
        "proxyerror",
        "timed out",
        "timeout",
        "winerror 10054",
        "invalid session id",
        "session deleted",
        "not connected to devtools",
        "chrome not reachable",
        "about:blank",
        "blank/incomplete",
        "login page blank",
        "email input verify failed",
        "email input locate/click failed",
        "continue button click failed",
        "proxy auth prompt detected",
    )
    if any(k in text for k in retryable_keywords):
        return True
    if driver is not None:
        try:
            current_url = str(getattr(driver, "current_url", "") or "").lower()
            if current_url.startswith("chrome-error://") or "chromewebdata" in current_url:
                return True
        except Exception:
            pass
    return False


class MailCodeRetryableError(RuntimeError):
    pass


class ProxyRotateRetryRequired(RuntimeError):
    def __init__(self, reason, email="", elapsed=0.0, account=None):
        super().__init__(reason)
        self.reason = str(reason or "")
        self.email = str(email or "")
        try:
            self.elapsed = max(0.0, float(elapsed))
        except Exception:
            self.elapsed = 0.0
        self.account = account


def _should_retry_login_locally(reason):
    text = str(reason or "").lower()
    keywords = (
        "open login/input email failed",
        "login page",
        "err_connection_reset",
        "connection reset",
        "continue button click failed",
    )
    return any(k in text for k in keywords)


def _switch_proxy_once():
    status_base = _status_base_url()
    if not status_base:
        return False, "SOCKS5_POOL_STATUS_URL not configured"

    def _read_status():
        active_proxy = ""
        active_region = ""
        try:
            status_resp = requests.get(f"{status_base}/api/status", timeout=8)
            if status_resp.status_code >= 400:
                return "", "", f"status request failed: HTTP {status_resp.status_code} {status_resp.text[:120]}"
            payload = status_resp.json() if status_resp.content else {}
            if isinstance(payload, dict):
                active_proxy = str(payload.get("active_proxy") or "").strip()
                active_region = str(payload.get("active_region") or "").strip()
            return active_proxy, active_region, ""
        except Exception as exc:
            return "", "", f"status request failed: {exc}"

    before_proxy, before_region, before_err = _read_status()
    switch_attempts = _env_int("MAIL_PROXY_SWITCH_ATTEMPTS", 3, 1, 10)
    validate_enabled = _env_flag("MAIL_PROXY_SWITCH_VALIDATE", True)
    validate_timeout = _env_int("MAIL_PROXY_SWITCH_VALIDATE_TIMEOUT", 6, 3, 20)
    runtime_proxy = _get_runtime_proxy()
    if not runtime_proxy:
        validate_enabled = False
    switch_url = f"{status_base}/api/switch"
    after_proxy = ""
    after_region = ""
    last_err = ""
    validate_err = ""
    validate_loc = ""
    validate_ip = ""

    for attempt in range(1, switch_attempts + 1):
        try:
            switch_resp = requests.get(switch_url, timeout=8)
        except Exception as exc:
            return False, f"switch request failed: {exc}"

        if switch_resp.status_code >= 400:
            return False, f"switch request failed: HTTP {switch_resp.status_code} {switch_resp.text[:120]}"

        after_proxy, after_region, last_err = _read_status()
        changed = (not before_proxy or not after_proxy or after_proxy != before_proxy)

        validate_ok = True
        if validate_enabled:
            validate_ok, validate_loc, validate_ip, validate_err = _probe_runtime_proxy(
                runtime_proxy,
                timeout_seconds=validate_timeout,
            )

        if changed and validate_ok:
            detail = (
                f"proxy switched: active={after_proxy or '?'} region={after_region or '?'}"
                f"{f' (before={before_proxy})' if before_proxy else ''}"
            )
            if validate_enabled:
                detail += f"; validated loc={validate_loc or '?'} ip={validate_ip or '?'}"
            if before_err:
                detail += f"; pre-status-error={before_err}"
            return True, detail

        if attempt < switch_attempts:
            time.sleep(0.4)

    if last_err and not after_proxy and not after_region:
        detail = f"proxy switched, but status unavailable: {last_err}"
        if validate_enabled and validate_err:
            detail += f"; validate-error={validate_err}"
    else:
        detail = (
            f"proxy switched: active={after_proxy or '?'} region={after_region or '?'}"
            f"{f' (before={before_proxy})' if before_proxy else ''}"
        )
        if before_proxy and after_proxy and before_proxy == after_proxy:
            detail += " (unchanged)"
        if validate_enabled:
            if validate_err:
                detail += f"; validate-error={validate_err}"
            else:
                detail += "; not-validated"
        if before_err:
            detail += f"; pre-status-error={before_err}"
    return False, detail


def _rotate_proxy_for_mail_retry(reason, retry_index, retry_limit):
    ok, detail = _switch_proxy_once()
    if ok:
        log(
            f"触发代理轮换重试 ({retry_index}/{retry_limit}): {detail}; reason={reason}",
            "WARN",
        )
    else:
        log(
            f"触发代理轮换重试（轮换失败但继续） ({retry_index}/{retry_limit}): {detail}; reason={reason}",
            "WARN",
        )


def _probe_runtime_proxy(runtime_proxy, timeout_seconds=8):
    proxy = _normalize_proxy_value(runtime_proxy)
    if not proxy:
        return True, "", "", ""
    timeout = max(3, int(timeout_seconds))
    targets = (
        ("https://cloudflare.com/cdn-cgi/trace", "trace"),
        ("https://www.google.com/generate_204", "status"),
        ("https://accounts.google.com", "status"),
        ("https://auth.business.gemini.google/login", "status"),
    )
    errors = []
    for url, mode in targets:
        try:
            resp = requests.get(
                url,
                headers={"User-Agent": "GeminiRegister/1.0"},
                proxies={"http": proxy, "https": proxy},
                timeout=timeout,
                allow_redirects=False,
            )
            status = int(resp.status_code or 0)
            if status >= 400:
                errors.append(f"{url} -> HTTP {resp.status_code}")
                continue
            if mode == "trace":
                text = resp.text or ""
                loc_m = re.search(r"(?m)^loc=(.+)$", text)
                ip_m = re.search(r"(?m)^ip=(.+)$", text)
                loc = str(loc_m.group(1)).strip() if loc_m else ""
                ip = str(ip_m.group(1)).strip() if ip_m else ""
                return True, loc, ip, ""
            if mode == "status" and status in {200, 204, 301, 302, 303, 307, 308}:
                return True, "", "", ""
            errors.append(f"{url} -> unexpected HTTP {status}")
        except Exception as exc:
            errors.append(f"{url} -> {exc}")
    return False, "", "", "; ".join(errors)[:500]


def _ensure_runtime_proxy_ready_for_browser():
    runtime_proxy = _get_runtime_proxy()
    if not runtime_proxy:
        return
    runtime_strategy = str(os.getenv("PROXY_STRATEGY", "") or "").strip().lower()
    if runtime_strategy not in {"socks5_pool", "easyproxies"}:
        return
    allow_active_switch = runtime_strategy == "socks5_pool"

    precheck_retries = _env_int("BROWSER_PROXY_PRECHECK_RETRIES", 2, 1, 6)
    precheck_timeout = _env_int("BROWSER_PROXY_PRECHECK_TIMEOUT", 8, 3, 20)
    last_err = "unknown"

    for attempt in range(1, precheck_retries + 1):
        ok, loc, ip, err = _probe_runtime_proxy(runtime_proxy, timeout_seconds=precheck_timeout)
        if ok:
            log(
                f"Browser proxy precheck passed: attempt={attempt}/{precheck_retries}, "
                f"loc={loc or '?'} ip={ip or '?'}"
            )
            return

        last_err = err or "unknown"
        if allow_active_switch:
            switched, switch_detail = _switch_proxy_once()
            if switched:
                log(
                    "Browser proxy precheck failed, switched socks5 upstream and retrying: "
                    f"attempt={attempt}/{precheck_retries}, {switch_detail}; err={last_err}",
                    "WARN",
                )
            else:
                log(
                    "Browser proxy precheck failed: "
                    f"attempt={attempt}/{precheck_retries}, err={last_err}, switch={switch_detail}",
                    "WARN",
                )
        else:
            log(
                "Browser proxy precheck failed (easyproxies, no local switch): "
                f"attempt={attempt}/{precheck_retries}, err={last_err}",
                "WARN",
            )

        if attempt < precheck_retries:
            time.sleep(0.5)

    raise RuntimeError(f"Browser proxy precheck failed after {precheck_retries} attempts: {last_err}")


def _utc_now_iso(timespec="milliseconds"):
    return datetime.now(timezone.utc).isoformat(timespec=timespec).replace("+00:00", "Z")


def _parse_iso_datetime(value):
    if not value:
        return None
    try:
        text = str(value).strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
        return dt.replace(tzinfo=None)
    except Exception:
        return None


def _infer_mail_token_ttl_seconds(path):
    if not os.path.exists(path):
        return DEFAULT_MAIL_TOKEN_TTL_SECONDS
    deltas = []
    try:
        with open(path, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f, delimiter="\t")
            for row in reader:
                if not isinstance(row, dict):
                    continue
                created_dt = _parse_iso_datetime(row.get("created_at"))
                expires_dt = _parse_iso_datetime(row.get("expires_at"))
                if not created_dt or not expires_dt:
                    continue
                delta = int((expires_dt - created_dt).total_seconds())
                if delta > 0:
                    deltas.append(delta)
    except Exception as e:
        log(f"推断 mail token 有效期失败，使用默认 7 天: {e}", "WARN")
        return DEFAULT_MAIL_TOKEN_TTL_SECONDS

    if not deltas:
        return DEFAULT_MAIL_TOKEN_TTL_SECONDS
    deltas.sort()
    return deltas[len(deltas) // 2]


def _get_mail_token_ttl_seconds():
    global MAIL_TOKEN_TTL_SECONDS
    if MAIL_TOKEN_TTL_SECONDS is not None:
        return MAIL_TOKEN_TTL_SECONDS
    path = os.path.join(BASE_DIR, MAIL_TOKENS_TEXT_FILE)
    MAIL_TOKEN_TTL_SECONDS = _infer_mail_token_ttl_seconds(path)
    days = MAIL_TOKEN_TTL_SECONDS / 86400
    log(f"mail token 过期策略: {days:.2f} 天")
    return MAIL_TOKEN_TTL_SECONDS


def _estimate_expires_at(created_at):
    created_dt = _parse_iso_datetime(created_at)
    if not created_dt:
        created_dt = datetime.now(timezone.utc).replace(tzinfo=None)
        created_at = created_dt.isoformat(timespec="milliseconds") + "Z"
    ttl_seconds = _get_mail_token_ttl_seconds()
    expires_dt = created_dt + timedelta(seconds=ttl_seconds)
    return created_at, expires_dt.isoformat(timespec="milliseconds") + "Z"


def upsert_mail_token_record(token, address, created_at=None, expires_at=None):
    token = str(token or "").strip()
    address = str(address or "").strip().lower()
    if not token or not address:
        return

    created_at = str(created_at or "").strip() or _utc_now_iso()
    expires_at = str(expires_at or "").strip()
    if not expires_at:
        created_at, expires_at = _estimate_expires_at(created_at)

    path = os.path.join(BASE_DIR, MAIL_TOKENS_TEXT_FILE)
    with MAIL_TOKENS_FILE_LOCK:
        rows = []
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8-sig", newline="") as f:
                    reader = csv.DictReader(f, delimiter="\t")
                    for row in reader:
                        if not isinstance(row, dict):
                            continue
                        old_address = str(row.get("address") or "").strip().lower()
                        if old_address:
                            rows.append(
                                {
                                    "token": str(row.get("token") or "").strip(),
                                    "address": old_address,
                                    "created_at": str(row.get("created_at") or "").strip(),
                                    "expires_at": str(row.get("expires_at") or "").strip(),
                                }
                            )
            except Exception as e:
                log(f"读取 {MAIL_TOKENS_TEXT_FILE} 失败，将重建文件: {e}", "WARN")
                rows = []

        replaced = False
        for row in rows:
            if row.get("address") == address:
                row["token"] = token
                row["created_at"] = created_at
                row["expires_at"] = expires_at
                replaced = True
                break
        if not replaced:
            rows.append(
                {
                    "token": token,
                    "address": address,
                    "created_at": created_at,
                    "expires_at": expires_at,
                }
            )

        with open(path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=["token", "address", "created_at", "expires_at"],
                delimiter="\t",
                lineterminator="\n",
            )
            writer.writeheader()
            writer.writerows(rows)
    log(f"已更新 {MAIL_TOKENS_TEXT_FILE}: {address} -> expires_at={expires_at}")


def _load_maintenance_state():
    global MAINTENANCE_STATE
    with MAINTENANCE_FILE_LOCK:
        if isinstance(MAINTENANCE_STATE, dict):
            return MAINTENANCE_STATE

        path = os.path.join(BASE_DIR, MAINTENANCE_STATE_FILE)
        if not os.path.exists(path):
            MAINTENANCE_STATE = {}
            return MAINTENANCE_STATE

        try:
            with open(path, "r", encoding="utf-8") as f:
                raw = json.load(f)
            MAINTENANCE_STATE = raw if isinstance(raw, dict) else {}
        except Exception as e:
            log(f"读取维护状态失败，将重建: {e}", "WARN")
            MAINTENANCE_STATE = {}
    return MAINTENANCE_STATE


def _save_maintenance_state():
    with MAINTENANCE_FILE_LOCK:
        path = os.path.join(BASE_DIR, MAINTENANCE_STATE_FILE)
        dir_path = os.path.dirname(path)
        if dir_path:
            os.makedirs(dir_path, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(MAINTENANCE_STATE or {}, f, indent=2, ensure_ascii=False)


def mark_account_updated(email, config=None):
    email = str(email or "").strip().lower()
    if not email:
        return
    state = _load_maintenance_state()
    row = state.get(email) if isinstance(state.get(email), dict) else {}
    row["updated"] = True
    row["updated_at"] = _utc_now_iso(timespec="seconds")
    if isinstance(config, dict):
        row["config_id"] = config.get("config_id")
        row["config_expires_at"] = config.get("expires_at")
    state[email] = row
    _save_maintenance_state()
    log(f"已更新维护状态: {email}")


def init_email_service():
    global EMAIL_SERVICE
    if EMAIL_SERVICE is not None:
        return

    try:
        log("正在初始化邮箱服务...")
        from email_service import EmailService as LocalEmailService
        EMAIL_SERVICE = LocalEmailService()
        log(f"邮箱服务初始化成功 (worker={EMAIL_SERVICE.worker_domain}, domain={EMAIL_SERVICE.email_domain})")
    except Exception as e:
        raise RuntimeError(f"初始化本地邮箱服务失败: {e}") from None


def validate_mail_config():
    log("验证邮箱配置...")
    init_email_service()
    log("邮箱配置验证通过")


def _extract_major_version(text):
    match = re.search(r"(\d+)(?:\.\d+){1,3}", text or "")
    return int(match.group(1)) if match else None


def _get_chrome_major_version(binary_path):
    env_major = os.getenv("CHROME_MAJOR_VERSION", "").strip()
    if env_major.isdigit():
        log(f"使用环境变量指定的 Chrome 版本: {env_major}")
        return int(env_major)

    log(f"正在通过 PowerShell 检测 Chrome 版本: {binary_path}")
    escaped_path = binary_path.replace("'", "''")
    ps_cmd = f"(Get-Item '{escaped_path}').VersionInfo.ProductVersion"
    result = subprocess.run(
        ["powershell", "-NoProfile", "-Command", ps_cmd],
        capture_output=True,
        text=True,
        timeout=10,
    )
    if result.returncode != 0:
        log(f"PowerShell 检测版本失败 (returncode={result.returncode}): {result.stderr.strip()}", "WARN")
        return None
    version_str = result.stdout.strip()
    major = _extract_major_version(version_str)
    log(f"Chrome 完整版本: {version_str}, 主版本: {major}")
    return major


def _get_uc_cache_driver_path():
    return os.path.join(
        os.environ.get("APPDATA", ""),
        "undetected_chromedriver",
        "undetected_chromedriver.exe",
    )


def _get_uc_cache_dir():
    appdata = os.environ.get("APPDATA", "")
    if not appdata:
        return ""
    return os.path.join(appdata, "undetected_chromedriver")


def _clear_uc_cache_dir():
    cache_dir = _get_uc_cache_dir()
    if not cache_dir or not os.path.exists(cache_dir):
        return
    try:
        shutil.rmtree(cache_dir)
        log(f"已清理 UC 驱动缓存目录: {cache_dir}")
    except Exception as e:
        log(f"清理 UC 驱动缓存失败: {e}", "WARN")


def _get_uc_driver_backup_path():
    backup_dir = os.path.join(BASE_DIR, ".uc_driver_cache")
    return os.path.join(backup_dir, "undetected_chromedriver.exe")


def _backup_uc_cache_driver():
    cache_driver = _get_uc_cache_driver_path()
    if not cache_driver or not os.path.exists(cache_driver):
        return
    backup_path = _get_uc_driver_backup_path()
    try:
        os.makedirs(os.path.dirname(backup_path), exist_ok=True)
        shutil.copy2(cache_driver, backup_path)
    except Exception as e:
        log(f"备份 UC 驱动缓存失败: {e}", "WARN")


def _restore_uc_cache_driver_if_missing(chrome_major):
    target = _get_uc_cache_driver_path()
    if target and os.path.exists(target):
        return target

    candidates = []
    backup_path = _get_uc_driver_backup_path()
    candidates.append(backup_path)

    local_appdata = os.environ.get("LOCALAPPDATA", "")
    if local_appdata:
        candidates.append(os.path.join(local_appdata, "undetected_chromedriver", "undetected_chromedriver.exe"))

    path_driver = shutil.which("chromedriver")
    if path_driver:
        candidates.append(path_driver)

    for cand in candidates:
        cand_path = str(cand or "").strip()
        if not cand_path or not os.path.exists(cand_path):
            continue
        cand_major = _get_driver_major_version(cand_path)
        if chrome_major and cand_major and cand_major != chrome_major:
            continue
        try:
            os.makedirs(os.path.dirname(target), exist_ok=True)
            shutil.copy2(cand_path, target)
            log(f"已恢复 UC 缓存驱动: {cand_path} -> {target}")
            return target
        except Exception as e:
            log(f"恢复 UC 缓存驱动失败: {e}", "WARN")
            continue
    return target if target and os.path.exists(target) else ""


def _should_retry_uc_startup(err_text):
    text = str(err_text or "").lower()
    retry_keywords = (
        "retrieval incomplete",
        "bad gateway",
        "502",
        "winerror 10054",
        "connection reset",
        "remote end closed connection",
        "timed out",
        "timeout",
        "urlopen error",
        "proxyerror",
        "connection aborted",
        "connection refused",
        "max retries exceeded",
        "ssl",
        "eof occurred",
        "incomplete read",
    )
    return any(k in text for k in retry_keywords)


def _apply_driver_download_proxy(runtime_proxy):
    enabled = str(os.getenv("UC_DOWNLOAD_PROXY_ENABLED", "1") or "1").strip().lower()
    if enabled in ("0", "false", "no", "off"):
        log("已禁用驱动下载代理 (UC_DOWNLOAD_PROXY_ENABLED=0)")
        return
    proxy = _normalize_proxy_value(runtime_proxy)
    if not proxy or not proxy.startswith(("http://", "https://")):
        return
    changed = False
    for key in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"):
        if not str(os.environ.get(key) or "").strip():
            os.environ[key] = proxy
            changed = True
    no_proxy_value = str(os.environ.get("NO_PROXY") or os.environ.get("no_proxy") or "").strip()
    local_excludes = ["127.0.0.1", "localhost", "::1"]
    if no_proxy_value:
        exists = {item.strip().lower() for item in no_proxy_value.split(",") if item.strip()}
        missing = [item for item in local_excludes if item.lower() not in exists]
        if missing:
            no_proxy_value = no_proxy_value + "," + ",".join(missing)
            os.environ["NO_PROXY"] = no_proxy_value
            os.environ["no_proxy"] = no_proxy_value
    else:
        no_proxy_value = ",".join(local_excludes)
        os.environ["NO_PROXY"] = no_proxy_value
        os.environ["no_proxy"] = no_proxy_value
    if changed:
        log(f"已设置驱动下载代理: {proxy}")


def _prepare_chrome_proxy_runtime(proxy_value):
    proxy = _normalize_proxy_value(proxy_value)
    if not proxy:
        return "", ""
    try:
        parsed = urlparse(proxy)
        scheme = str(parsed.scheme or "http").strip().lower()
        host = str(parsed.hostname or "").strip()
        port = int(parsed.port or 0)
        raw_username = parsed.username or ""
        raw_password = parsed.password or ""
        username = unquote(raw_username) if raw_username else ""
        password = unquote(raw_password) if raw_password else ""
    except Exception:
        return proxy, ""

    if not host or port <= 0:
        return proxy, ""
    if not username:
        return proxy, ""

    if raw_username != username or raw_password != password:
        log("检测到代理鉴权信息含 URL 编码，已自动解码后注入浏览器扩展")

    if scheme not in {"http", "https", "socks4", "socks5"}:
        log(f"代理协议不支持自动鉴权扩展，将按原代理参数尝试: {scheme}", "WARN")
        return proxy, ""

    proxy_no_auth = f"{scheme}://{host}:{port}"
    try:
        ext_root = os.path.join(BASE_DIR, ".runtime_proxy_ext")
        os.makedirs(ext_root, exist_ok=True)
        key = f"{scheme}|{host}|{port}|{username}|{password}"
        ext_id = hashlib.sha1(key.encode("utf-8")).hexdigest()[:16]
        ext_dir = os.path.join(ext_root, ext_id)
        os.makedirs(ext_dir, exist_ok=True)

        manifest = {
            "name": "Runtime Proxy Auth",
            "version": "1.0.0",
            "manifest_version": 3,
            "permissions": ["proxy", "storage", "webRequest", "webRequestAuthProvider"],
            "host_permissions": ["<all_urls>"],
            "background": {"service_worker": "background.js"},
        }
        with open(os.path.join(ext_dir, "manifest.json"), "w", encoding="utf-8") as f:
            json.dump(manifest, f, ensure_ascii=False, indent=2)

        bg_js = f"""
const config = {{
  mode: "fixed_servers",
  rules: {{
    singleProxy: {{
      scheme: {json.dumps(scheme)},
      host: {json.dumps(host)},
      port: {port}
    }},
    bypassList: ["localhost", "127.0.0.1", "::1"]
  }}
}};

chrome.proxy.settings.set({{ value: config, scope: "regular" }}, function() {{}});

chrome.webRequest.onAuthRequired.addListener(
  function(details, callback) {{
    callback({{
      authCredentials: {{
        username: {json.dumps(username)},
        password: {json.dumps(password)}
      }}
    }});
  }},
  {{ urls: ["<all_urls>"] }},
  ["asyncBlocking"]
);
""".strip()
        with open(os.path.join(ext_dir, "background.js"), "w", encoding="utf-8") as f:
            f.write(bg_js + "\n")

        return proxy_no_auth, ext_dir
    except Exception as e:
        log(f"生成代理鉴权扩展失败，将按原代理参数尝试: {e}", "WARN")
        return proxy, ""


def _get_driver_major_version(driver_path):
    if not os.path.exists(driver_path):
        log(f"缓存驱动不存在: {driver_path}")
        return None
    log(f"正在检测缓存驱动版本: {driver_path}")
    result = subprocess.run(
        [driver_path, "--version"],
        capture_output=True,
        text=True,
        timeout=10,
    )
    if result.returncode != 0:
        log(f"检测缓存驱动版本失败 (returncode={result.returncode})", "WARN")
        return None
    major = _extract_major_version(result.stdout.strip())
    log(f"缓存驱动版本: {result.stdout.strip()}, 主版本: {major}")
    return major


def _cleanup_cached_driver_if_mismatch(chrome_major):
    driver_path = _get_uc_cache_driver_path()
    log(f"检查缓存驱动: {driver_path}")
    driver_major = _get_driver_major_version(driver_path)
    if not chrome_major or not driver_major:
        log("跳过驱动版本比对（版本信息不完整）")
        return
    if chrome_major == driver_major:
        log(f"缓存驱动版本 {driver_major} 与浏览器 {chrome_major} 一致，无需清理")
        return
    if os.path.exists(driver_path):
        try:
            os.remove(driver_path)
            log(f"检测到驱动版本 {driver_major} 与浏览器 {chrome_major} 不一致，已清理旧驱动缓存")
        except Exception as e:
            log(f"清理旧驱动缓存失败: {e}", "WARN")


def create_browser_driver():
    if not CHROME_BINARY_PATH:
        raise RuntimeError("CHROME_BIN is empty.")
    if not os.path.exists(CHROME_BINARY_PATH):
        raise FileNotFoundError(f"Chrome executable not found: {CHROME_BINARY_PATH}")

    log(f"Chrome path: {CHROME_BINARY_PATH}")
    chrome_major = _get_chrome_major_version(CHROME_BINARY_PATH)
    if chrome_major:
        log(f"Chrome major version: {chrome_major}")
    else:
        log("Cannot detect Chrome major version, UC default strategy will be used", "WARN")

    _cleanup_cached_driver_if_mismatch(chrome_major)
    restored_cache_driver = _restore_uc_cache_driver_if_missing(chrome_major)

    log("Configuring ChromeOptions...")
    options = uc.ChromeOptions()
    options.binary_location = CHROME_BINARY_PATH
    browser_stability_flags = (
        "--disable-backgrounding-occluded-windows",
        "--disable-background-timer-throttling",
        "--disable-renderer-backgrounding",
        "--disable-features=CalculateNativeWinOcclusion,BackForwardCache,IntensiveWakeUpThrottling",
    )
    for flag in browser_stability_flags:
        try:
            options.add_argument(flag)
        except Exception:
            pass
    runtime_proxy = _get_runtime_proxy()
    proxy_for_browser = runtime_proxy
    proxy_ext_dir = ""
    if runtime_proxy:
        proxy_for_browser, proxy_ext_dir = _prepare_chrome_proxy_runtime(runtime_proxy)
        options.add_argument(f"--proxy-server={proxy_for_browser}")
        options.add_argument("--proxy-bypass-list=<-loopback>")
        if proxy_ext_dir:
            options.add_argument(f"--load-extension={proxy_ext_dir}")
            log(f"已启用代理鉴权扩展: {proxy_ext_dir}")
        log(f"Browser proxy: {_mask_proxy_for_log(runtime_proxy)}")
    runtime_strategy = str(os.getenv("PROXY_STRATEGY", "") or "").strip()
    runtime_upstream = _normalize_proxy_value(os.getenv("PROXY_UPSTREAM", ""))
    runtime_region = str(os.getenv("PROXY_REGION", "") or "").strip()
    if runtime_strategy or runtime_upstream or runtime_region:
        log(
            "Proxy runtime detail: "
            f"strategy={runtime_strategy or '?'} "
            f"upstream={_mask_proxy_for_log(runtime_upstream) or '?'} "
            f"region={runtime_region or '?'}"
        )
    _apply_driver_download_proxy(runtime_proxy)

    kwargs = {
        "options": options,
        "browser_executable_path": CHROME_BINARY_PATH,
        "use_subprocess": True,
        "patcher_force_close": True,
    }
    if chrome_major:
        kwargs["version_main"] = chrome_major
    cache_driver_path = restored_cache_driver or _get_uc_cache_driver_path()
    if cache_driver_path and os.path.exists(cache_driver_path):
        kwargs["driver_executable_path"] = cache_driver_path

    startup_retries = max(1, min(int(os.getenv("UC_STARTUP_RETRIES", "3") or "3"), 5))
    startup_wait = max(1.0, float(os.getenv("UC_STARTUP_RETRY_WAIT", "3") or "3"))
    clear_cache_on_retry = _env_flag("UC_CLEAR_CACHE_ON_RETRY", False)

    for attempt in range(1, startup_retries + 1):
        if runtime_proxy:
            try:
                _ensure_runtime_proxy_ready_for_browser()
            except Exception as e:
                short = str(e) or repr(e)
                log(f"Browser proxy precheck failed before startup: {short}", "ERR")
                if attempt >= startup_retries:
                    raise RuntimeError(f"Browser startup failed: {short}") from None
                time.sleep(startup_wait)
                continue
        log(
            f"Starting uc.Chrome (version_main={chrome_major}) [attempt={attempt}/{startup_retries}], this step may download/patch driver..."
        )
        t0 = time.time()
        try:
            driver = uc.Chrome(**kwargs)
            log(f"Browser started successfully (elapsed {time.time()-t0:.1f}s)")
            _backup_uc_cache_driver()
            return driver
        except Exception as e:
            message = str(e)
            short = message.splitlines()[0] if message else repr(e)
            log(f"Browser startup failed (elapsed {time.time()-t0:.1f}s): {short}", "ERR")

            if attempt >= startup_retries or not _should_retry_uc_startup(short):
                raise RuntimeError(f"Browser startup failed: {short}") from None

            if clear_cache_on_retry:
                log("Retryable startup error detected, clearing UC cache and retrying...", "WARN")
                _clear_uc_cache_dir()
            else:
                log("Retryable startup error detected, keep UC cache and retry (UC_CLEAR_CACHE_ON_RETRY=0)", "WARN")
            time.sleep(startup_wait)

    raise RuntimeError("Browser startup failed: exceeded retry limit")

def create_email():
    """创建临时邮箱"""
    try:
        if EMAIL_SERVICE is None:
            init_email_service()
        log(f"正在请求创建邮箱 ({EMAIL_SERVICE.worker_domain})...")
        t0 = time.time()
        jwt, email = EMAIL_SERVICE.create_email()
        elapsed = time.time() - t0
        if jwt and email:
            created_at = _utc_now_iso()
            _, expires_at = _estimate_expires_at(created_at)
            upsert_mail_token_record(jwt, email, created_at=created_at, expires_at=expires_at)
            log(f"邮箱创建成功: {email} (耗时 {elapsed:.1f}s)")
            return {"jwt": jwt, "email": email, "created_at": created_at, "expires_at": expires_at}
        log(f"本地邮箱服务返回空邮箱 (耗时 {elapsed:.1f}s)", "ERR")
    except Exception as e:
        log(f"创建邮箱失败: {e}", "ERR")
    return None

def prefetch_email():
    """预创建邮箱到队列"""
    log("后台预创建邮箱...")
    account = create_email()
    if account:
        email_queue.append(account)
        log(f"预创建邮箱已入队: {account.get('email')} (队列长度={len(email_queue)})")
    else:
        log("预创建邮箱失败", "WARN")

def get_email():
    """获取邮箱（优先从队列取）"""
    if email_queue:
        account = email_queue.pop(0)
        log(f"使用预创建邮箱: {account.get('email')}")
        return account
    account = create_email()
    if account:
        log(f"邮箱创建: {account.get('email')}")
    return account


MAIL_TEXT_FIELD_HINTS = (
    "raw",
    "text",
    "html",
    "subject",
    "body",
    "content",
    "message",
    "source",
    "snippet",
    "preview",
)


def _normalize_email(value):
    return str(value or "").strip().lower()


def _parse_mail_timestamp(value):
    if not value:
        return 0.0
    try:
        text = str(value).strip()
        if not text:
            return 0.0
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        return datetime.fromisoformat(text).timestamp()
    except Exception:
        return 0.0


def _append_unique_text(candidates, value, max_len=300_000):
    if not isinstance(value, str):
        return
    text = value.strip()
    if not text:
        return
    if len(text) > max_len:
        text = text[:max_len]
    if text not in candidates:
        candidates.append(text)


def _looks_like_base64_text(text):
    compact = re.sub(r"\s+", "", str(text or ""))
    if len(compact) < 40 or (len(compact) % 4 != 0):
        return False
    return bool(re.fullmatch(r"[A-Za-z0-9+/=]+", compact))


def _decode_text_variants(text):
    variants = []
    if not isinstance(text, str):
        return variants

    _append_unique_text(variants, text)

    try:
        mime_words_decoded = str(make_header(decode_header(text)))
        _append_unique_text(variants, mime_words_decoded)
    except Exception:
        pass

    try:
        qp_decoded = quopri.decodestring(text).decode("utf-8", errors="ignore")
        _append_unique_text(variants, qp_decoded)
    except Exception:
        pass

    compact = re.sub(r"\s+", "", text)
    if _looks_like_base64_text(compact):
        try:
            decoded_bytes = base64.b64decode(compact, validate=True)
            for encoding in ("utf-8", "latin-1"):
                try:
                    decoded = decoded_bytes.decode(encoding, errors="ignore")
                    _append_unique_text(variants, decoded)
                except Exception:
                    continue
        except Exception:
            pass

    return variants


def _collect_mail_text_candidates(value, out, depth=0, max_depth=5, visited=None):
    if visited is None:
        visited = set()
    if value is None or depth > max_depth:
        return

    if isinstance(value, (dict, list, tuple, set)):
        obj_id = id(value)
        if obj_id in visited:
            return
        visited.add(obj_id)

    if isinstance(value, str):
        for variant in _decode_text_variants(value):
            _append_unique_text(out, variant)
            trimmed = variant.strip()
            if len(trimmed) >= 2 and trimmed[0] in "{[" and trimmed[-1] in "}]":
                try:
                    parsed = json.loads(trimmed)
                except Exception:
                    continue
                _collect_mail_text_candidates(parsed, out, depth + 1, max_depth, visited)
        return

    if isinstance(value, (bytes, bytearray)):
        try:
            decoded = bytes(value).decode("utf-8", errors="ignore")
            _collect_mail_text_candidates(decoded, out, depth + 1, max_depth, visited)
        except Exception:
            pass
        return

    if isinstance(value, dict):
        prioritized = []
        for key in MAIL_TEXT_FIELD_HINTS:
            if key in value:
                prioritized.append(value.get(key))
        for key, item in value.items():
            if key not in MAIL_TEXT_FIELD_HINTS:
                prioritized.append(item)
        for item in prioritized:
            _collect_mail_text_candidates(item, out, depth + 1, max_depth, visited)
        return

    if isinstance(value, (list, tuple, set)):
        for item in value:
            _collect_mail_text_candidates(item, out, depth + 1, max_depth, visited)


def _build_mail_signature(mail_item):
    if not isinstance(mail_item, dict):
        return ""
    mail_id = str(mail_item.get("id") or "").strip()
    ts = str(mail_item.get("timestamp") or "").strip()
    addr = str(mail_item.get("address") or "").strip().lower()
    subject = str(mail_item.get("subject") or "").strip()
    preview = str(mail_item.get("preview") or "").strip()
    if mail_id:
        return f"id:{mail_id}|{ts}|{len(subject)}|{len(preview)}"
    return f"fallback:{ts}|{addr}|{len(subject)}|{preview[:80]}"


_MAIL_CODE_KEYWORD_RE = re.compile(
    r"(?:验证码|驗證碼|verification(?:\s*code)?|one[-\s]*time(?:\s*code)?|auth(?:entication)?\s*code|security\s*code|confirm(?:ation)?\s*code)",
    re.IGNORECASE,
)
_MAIL_CODE_TOKEN_STOPWORDS = {
    "GOOGLE",
    "GEMINI",
    "VERIFY",
    "VERIFI",
    "SIGNIN",
    "PLEASE",
    "SECURE",
    "ACCOUNT",
}


def _extract_code_candidates_from_text(text, max_candidates=6):
    if not text:
        return []
    normalized = re.sub(r"\s+", " ", text.replace("\u200b", "")).strip()
    if not normalized:
        return []
    upper_text = normalized.upper()

    score_map = {}
    pos_map = {}

    def _bump(token, score, pos):
        token = str(token or "").strip().upper()
        if not re.fullmatch(r"[A-Z0-9]{6}", token):
            return
        if token in _MAIL_CODE_TOKEN_STOPWORDS:
            return
        prev = score_map.get(token, 0)
        score_map[token] = max(prev, int(score))
        prev_pos = pos_map.get(token)
        if prev_pos is None or pos < prev_pos:
            pos_map[token] = max(0, int(pos))

    keyword_patterns = [
        r"(?:验证码|驗證碼|verification(?:\s*code)?|one[-\s]*time(?:\s*code)?|auth(?:entication)?\s*code)\s*(?:is|为|是|:|：|-)?\s*([A-Z0-9]{6})",
        r"\b([A-Z0-9]{6})\b\s*(?:is\s+)?(?:your\s+)?(?:验证码|驗證碼|verification(?:\s*code)?|one[-\s]*time(?:\s*code)?)",
    ]
    for pattern in keyword_patterns:
        for match in re.finditer(pattern, upper_text, re.IGNORECASE):
            token = match.group(1).upper()
            pos = match.start(1)
            score = 160 + (24 if pos < 600 else 0)
            _bump(token, score, pos)

    for match in re.finditer(r"\b([A-Z0-9]{6})\b", upper_text):
        token = match.group(1).upper()
        if token in _MAIL_CODE_TOKEN_STOPWORDS:
            continue
        pos = match.start(1)
        has_digit = any(ch.isdigit() for ch in token)
        has_alpha = any(ch.isalpha() for ch in token)
        score = 0
        if has_digit and has_alpha:
            score += 100
        elif has_digit:
            score += 82
        elif has_alpha:
            score += 70
        window_start = max(0, pos - 56)
        window_end = min(len(upper_text), pos + 56)
        if _MAIL_CODE_KEYWORD_RE.search(upper_text[window_start:window_end]):
            score += 55
        if pos < 600:
            score += 18
        if len(set(token)) <= 2:
            score -= 30
        _bump(token, score, pos)

    if not score_map:
        return []

    ranked = sorted(score_map.items(), key=lambda kv: (-kv[1], pos_map.get(kv[0], 10**9), kv[0]))
    selected = [token for token, score in ranked if score >= 70]
    if not selected:
        selected = [token for token, _ in ranked[:1]]
    return selected[: max(1, int(max_candidates or 1))]


def _extract_code_from_text(text):
    candidates = _extract_code_candidates_from_text(text, max_candidates=1)
    return candidates[0] if candidates else None


def _extract_code_candidates_from_mail(mail_payload, max_candidates=6):
    candidates = []
    _collect_mail_text_candidates(mail_payload, candidates)
    if not candidates:
        return []

    # Parse MIME message parts from all possible textual payloads.
    for candidate in list(candidates):
        if not isinstance(candidate, str) or not candidate.strip():
            continue
        try:
            message = Parser(policy=policy.default).parsestr(candidate)
            if message.is_multipart():
                for part in message.walk():
                    ctype = (part.get_content_type() or "").lower()
                    if ctype not in {"text/plain", "text/html"}:
                        continue
                    payload = part.get_payload(decode=True)
                    charset = part.get_content_charset() or "utf-8"
                    if isinstance(payload, (bytes, bytearray)):
                        text = payload.decode(charset, errors="ignore")
                        _append_unique_text(candidates, text)
            else:
                payload = message.get_payload(decode=True)
                charset = message.get_content_charset() or "utf-8"
                if isinstance(payload, (bytes, bytearray)):
                    text = payload.decode(charset, errors="ignore")
                    _append_unique_text(candidates, text)
        except Exception:
            pass

    score_map = {}
    first_seen_idx = {}

    def _add_token(token, score, idx):
        token = str(token or "").strip().upper()
        if not re.fullmatch(r"[A-Z0-9]{6}", token):
            return
        if token in _MAIL_CODE_TOKEN_STOPWORDS:
            return
        score_map[token] = max(score_map.get(token, 0), int(score))
        if token not in first_seen_idx:
            first_seen_idx[token] = int(idx)

    # Parse HTML bodies and pull visible text.
    for idx, candidate in enumerate(list(candidates)):
        if not isinstance(candidate, str) or not candidate.strip():
            continue
        text_bonus = max(0, 70 - (idx * 5))
        try:
            soup = BeautifulSoup(candidate, "html.parser")
            span = soup.find("span", class_="verification-code")
            if span:
                span_text = span.get_text(" ", strip=True).upper()
                match = re.search(r"\b([A-Z0-9]{6})\b", span_text)
                if match:
                    _add_token(match.group(1), 420 - idx, idx)
            visible = soup.get_text(" ", strip=True)
            _append_unique_text(candidates, visible)
        except Exception:
            pass

        for rank, token in enumerate(_extract_code_candidates_from_text(candidate, max_candidates=max_candidates)):
            rank_bonus = max(0, 24 - (rank * 5))
            _add_token(token, 120 + text_bonus + rank_bonus, idx)

    if not score_map:
        return []

    ranked = sorted(
        score_map.items(),
        key=lambda kv: (-kv[1], first_seen_idx.get(kv[0], 10**9), kv[0]),
    )
    return [token for token, _ in ranked[: max(1, int(max_candidates or 1))]]


def _extract_code_from_mail(mail_payload):
    candidates = _extract_code_candidates_from_mail(mail_payload, max_candidates=1)
    return candidates[0] if candidates else None


def _fetch_recent_mail_contents(
    jwt,
    expected_email=None,
    timeout=15,
    exclude_signatures=None,
    min_timestamp=0.0,
    allow_missing_timestamp=True,
):
    if EMAIL_SERVICE is None:
        init_email_service()

    worker_domain = (getattr(EMAIL_SERVICE, "worker_domain", "") or "").strip()
    if not worker_domain:
        return []
    base = worker_domain if worker_domain.startswith(("http://", "https://")) else f"https://{worker_domain}"

    r = requests.get(
        f"{base}/api/mails",
        params={"limit": EMAIL_FETCH_LIMIT, "offset": 0},
        headers={
            "Authorization": f"Bearer {jwt}",
            "Content-Type": "application/json",
        },
        timeout=timeout,
    )
    if r.status_code != 200:
        return []

    payload = r.json() if r.content else {}
    rows = payload.get("results") if isinstance(payload, dict) else None
    if not isinstance(rows, list):
        return []

    expected = _normalize_email(expected_email)
    excluded = exclude_signatures or set()
    candidates = []
    for idx, row in enumerate(rows):
        if not isinstance(row, dict):
            continue
        row_address = _normalize_email(
            row.get("address")
            or row.get("to")
            or row.get("email")
            or row.get("recipient")
        )
        if expected and row_address and row_address != expected:
            continue
        row_timestamp = row.get("timestamp") or row.get("createdAt") or row.get("created_at")
        ts_float = _parse_mail_timestamp(row_timestamp)
        if min_timestamp > 0:
            if ts_float > 0 and ts_float < float(min_timestamp):
                continue
            if ts_float <= 0 and not allow_missing_timestamp:
                continue
        candidate = {
            "id": row.get("id"),
            "timestamp": row_timestamp,
            "_ts": ts_float,
            "address": row_address,
            "subject": row.get("subject"),
            "preview": row.get("snippet") or row.get("preview"),
            "row": row,
            "_idx": idx,
        }
        signature = _build_mail_signature(candidate)
        if signature and signature in excluded:
            continue
        candidate["signature"] = signature
        candidates.append(candidate)

    # Prefer newest first. If timestamp is missing, preserve API order (typically DESC).
    candidates.sort(
        key=lambda x: (float(x.get("_ts") or 0.0), -x.get("_idx", 0)),
        reverse=True,
    )
    return candidates


def _manual_triggered():
    if msvcrt is None:
        return False
    triggered = False
    try:
        while msvcrt.kbhit():
            ch = msvcrt.getwch()
            if ch and ch.lower() == "x":
                triggered = True
    except Exception:
        return False
    return triggered


def _clear_manual_input_buffer():
    if msvcrt is None:
        return
    try:
        while msvcrt.kbhit():
            msvcrt.getwch()
    except Exception:
        return


def _prompt_manual_code():
    while True:
        manual = input("\n[INPUT] 请输入手动验证码（6位字母或数字）: ").strip().replace(" ", "").replace("-", "")
        manual = manual.upper()
        if re.fullmatch(r"[A-Z0-9]{6}", manual):
            _clear_manual_input_buffer()
            return manual
        print("[WARN] 验证码格式无效，请输入6位字母或数字（例如 BZGLHF 或 123456）")


def _wait_interval_or_manual(start_time, timeout):
    remaining = timeout - (time.time() - start_time)
    if remaining <= 0:
        return None
    wait_seconds = min(EMAIL_CODE_POLL_INTERVAL_SECONDS, max(1, int(remaining)))
    for _ in range(wait_seconds):
        if _manual_triggered():
            code = _prompt_manual_code()
            log(f"手动验证码: {code}")
            return code
        print(f"  等待验证码... ({int(time.time()-start_time)}s)", end='\r')
        time.sleep(1)
    return None


def _build_code_payload(primary_code, alternatives=None, source="mail"):
    primary = _normalize_code_value(primary_code)
    if not _is_valid_code_token(primary):
        return None
    alt_list = []
    seen = {primary}
    for item in list(alternatives or []):
        candidate = _normalize_code_value(item)
        if not _is_valid_code_token(candidate):
            continue
        if candidate in seen:
            continue
        seen.add(candidate)
        alt_list.append(candidate)
    return {
        "code": primary,
        "alternatives": alt_list,
        "source": str(source or "mail"),
    }


def get_code(email, jwt, timeout=EMAIL_CODE_TIMEOUT_SECONDS, submitted_at_ts=0.0):
    """Fetch verification code from mailbox."""
    init_email_service()
    worker_domain = (getattr(EMAIL_SERVICE, "worker_domain", "") or "").strip()
    if worker_domain:
        base = worker_domain if worker_domain.startswith(("http://", "https://")) else f"https://{worker_domain}"
        log(f"验证码拉取地址: {base}/api/mails")
    log("等待验证码...（按 x 可手动输入验证码并继续）")
    start = time.time()
    discarded_signatures = set()
    last_diag_log_at = 0.0
    last_warn_signature = ""
    consecutive_fetch_failures = 0
    rotate_threshold = _mail_proxy_rotate_threshold()
    strict_window_seconds = _env_int("MAIL_CODE_STRICT_WINDOW_SECONDS", 120, 0, 600)
    timestamp_skew_seconds = _env_int("MAIL_CODE_TIMESTAMP_SKEW_SECONDS", 8, 0, 120)
    while time.time() - start < timeout:
        if _manual_triggered():
            code = _prompt_manual_code()
            log(f"手动验证码: {code}")
            return _build_code_payload(code, alternatives=[], source="manual")
        try:
            now_ts = time.time()
            strict_filter = bool(submitted_at_ts) and (
                strict_window_seconds <= 0 or now_ts <= (float(submitted_at_ts) + strict_window_seconds)
            )
            min_timestamp = max(0.0, float(submitted_at_ts) - float(timestamp_skew_seconds)) if strict_filter else 0.0
            mails = _fetch_recent_mail_contents(
                jwt,
                expected_email=email,
                timeout=15,
                exclude_signatures=discarded_signatures,
                min_timestamp=min_timestamp,
                allow_missing_timestamp=not strict_filter,
            )
            consecutive_fetch_failures = 0
            if mails:
                for item in mails:
                    codes = _extract_code_candidates_from_mail(item.get("row"), max_candidates=4)
                    if codes:
                        primary = codes[0]
                        alternatives = codes[1:]
                        if alternatives:
                            log(f"验证码候选: primary={primary}, alternatives={','.join(alternatives)}")
                        else:
                            log(f"验证码: {primary}")
                        payload = _build_code_payload(primary, alternatives=alternatives, source="mail")
                        if payload:
                            return payload
                latest = mails[0]
                signature = latest.get("signature") or ""
                now = time.time()
                if signature != last_warn_signature or (now - last_diag_log_at) >= 30:
                    ts = latest.get("timestamp")
                    log(
                        f"已收到{len(mails)}封邮件，但未识别到验证码格式"
                        f"{' (timestamp=' + str(ts) + ')' if ts else ''}",
                        "WARN",
                    )
                    last_warn_signature = signature
                    last_diag_log_at = now
            else:
                now = time.time()
                if now - last_diag_log_at >= 20:
                    log(f"验证码轮询中... 暂未收到邮件 (elapsed={int(now - start)}s)")
                    last_diag_log_at = now
        except Exception as e:
            log(f"拉取邮件失败: {e}", "WARN")
            if _is_retryable_mail_fetch_error(e):
                consecutive_fetch_failures += 1
                if consecutive_fetch_failures >= rotate_threshold:
                    raise MailCodeRetryableError(
                        f"mail fetch failed {consecutive_fetch_failures} times: {e}"
                    ) from None
            else:
                consecutive_fetch_failures = 0
        manual_code = _wait_interval_or_manual(start, timeout)
        if manual_code:
            return _build_code_payload(manual_code, alternatives=[], source="manual")
    log("验证码超时", "ERR")
    if _mail_timeout_should_rotate():
        raise MailCodeRetryableError(f"verification code timeout ({int(timeout)}s)")
    return None

def save_config(email, driver, timeout=10):
    """保存配置，带轮询等待所有字段"""
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # 轮询等待所有关键字段出现
    log(f"等待配置数据 (最多{timeout}s)...")
    start = time.time()
    data = None

    while time.time() - start < timeout:
        cookies = driver.get_cookies()
        url = driver.current_url
        parsed = urlparse(url)

        # 解析 config_id
        path_parts = url.split('/')
        config_id = None
        for i, p in enumerate(path_parts):
            if p == 'cid' and i+1 < len(path_parts):
                config_id = path_parts[i+1].split('?')[0]
                break

        # 获取 cookies
        cookie_dict = {c['name']: c for c in cookies}
        ses_cookie = cookie_dict.get('__Secure-C_SES', {})
        host_cookie = cookie_dict.get('__Host-C_OSES', {})

        # 获取 csesidx
        csesidx = parse_qs(parsed.query).get('csesidx', [None])[0]

        # 检查所有关键字段是否都有值
        if (ses_cookie.get('value') and
            host_cookie.get('value') and
            csesidx and
            config_id):

            data = {
                "id": email,
                "csesidx": csesidx,
                "config_id": config_id,
                "secure_c_ses": ses_cookie.get('value'),
                "host_c_oses": host_cookie.get('value'),
                "expires_at": datetime.fromtimestamp(ses_cookie.get('expiry', 0) - 43200).strftime('%Y-%m-%d %H:%M:%S') if ses_cookie.get('expiry') else None
            }
            log(f"配置数据已就绪 ({time.time() - start:.1f}s)")
            break

        time.sleep(1)

    if not data:
        # 最后一次尝试，记录缺失字段
        cookies = driver.get_cookies()
        url = driver.current_url
        parsed = urlparse(url)
        cookie_dict = {c['name']: c for c in cookies}

        missing = []
        if not cookie_dict.get('__Secure-C_SES', {}).get('value'): missing.append('secure_c_ses')
        if not cookie_dict.get('__Host-C_OSES', {}).get('value'): missing.append('host_c_oses')
        if not parse_qs(parsed.query).get('csesidx', [None])[0]: missing.append('csesidx')

        path_parts = url.split('/')
        has_config_id = False
        for i, p in enumerate(path_parts):
            if p == 'cid' and i+1 < len(path_parts):
                has_config_id = True
                break
        if not has_config_id: missing.append('config_id')

        log(f"配置不完整，缺失字段: {', '.join(missing)}，跳过: {email}", "WARN")
        return None

    with open(f"{OUTPUT_DIR}/{email}.json", 'w') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    log(f"已保存: {email}.json")
    return data

def fast_type(element, text, delay=0.02):
    """快速输入文本"""
    type_scale = _env_float("LOGIN_TYPE_DELAY_SCALE", 1.0, 0.5, 4.0)
    actual_delay = max(0.005, float(delay) * type_scale)
    for c in text:
        element.send_keys(c)
        time.sleep(actual_delay)


def _normalize_code_value(value):
    return re.sub(r"[\s-]+", "", str(value or "")).strip().upper()


def _is_valid_code_token(value):
    return bool(re.fullmatch(r"[A-Z0-9]{6}", _normalize_code_value(value)))


_CODE_AMBIGUOUS_MAP = {
    "0": ("O",),
    "O": ("0",),
    "1": ("I", "L"),
    "I": ("1", "L"),
    "L": ("1", "I", "O"),
    "5": ("S",),
    "S": ("5",),
    "2": ("Z",),
    "Z": ("2",),
    "8": ("B",),
    "B": ("8",),
}


def _build_code_input_candidates(primary_code, extra_codes=None):
    limit = _env_int("MAIL_CODE_INPUT_VARIANT_LIMIT", 12, 1, 40)
    base_candidates = []
    for item in [primary_code] + list(extra_codes or []):
        code = _normalize_code_value(item)
        if _is_valid_code_token(code):
            base_candidates.append(code)

    if not base_candidates:
        return []

    merged = []
    seen = set()

    def _add_candidate(item):
        code = _normalize_code_value(item)
        if not _is_valid_code_token(code):
            return
        if code in seen:
            return
        seen.add(code)
        merged.append(code)

    for code in base_candidates:
        _add_candidate(code)
        for idx, ch in enumerate(code):
            replacements = _CODE_AMBIGUOUS_MAP.get(ch, ())
            for repl in replacements:
                _add_candidate(code[:idx] + repl + code[idx + 1 :])
                if len(merged) >= limit:
                    return merged[:limit]
        if len(merged) >= limit:
            return merged[:limit]
    return merged[:limit]


def _input_value_matches(element, expected):
    try:
        actual = element.get_attribute("value")
    except Exception:
        return False
    return _normalize_code_value(actual) == _normalize_code_value(expected)


def _set_input_value_js(driver, element, value):
    try:
        return bool(
            driver.execute_script(
                "var el = arguments[0];"
                "var nextVal = String(arguments[1] == null ? '' : arguments[1]);"
                "if (!el) { return false; }"
                "try { el.focus(); } catch (e) {}"
                "var proto = Object.getPrototypeOf(el);"
                "var descriptor = (proto && Object.getOwnPropertyDescriptor(proto, 'value'))"
                " || Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')"
                " || Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype, 'value');"
                "if (descriptor && descriptor.set) { descriptor.set.call(el, nextVal); }"
                "else { el.value = nextVal; }"
                "try { if (typeof el.setSelectionRange === 'function') {"
                "el.setSelectionRange(nextVal.length, nextVal.length); } } catch (e) {}"
                "el.dispatchEvent(new Event('input', {bubbles:true}));"
                "el.dispatchEvent(new Event('change', {bubbles:true}));"
                "return String(el.value == null ? '' : el.value) === nextVal;",
                element,
                value,
            )
        )
    except Exception:
        return False


def _input_code_with_global_js(driver, code):
    try:
        result = driver.execute_script(
            "var code = String(arguments[0] == null ? '' : arguments[0]).trim();"
            "if (!code) { return {ok:false, mode:'empty'}; }"
            "var norm = function(v){ return String(v == null ? '' : v).replace(/[\\s-]+/g,'').toUpperCase(); };"
            "var setVal = function(el, val){"
            "if (!el) { return false; }"
            "try { el.focus(); } catch (e) {}"
            "var proto = Object.getPrototypeOf(el);"
            "var descriptor = (proto && Object.getOwnPropertyDescriptor(proto, 'value'))"
            " || Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')"
            " || Object.getOwnPropertyDescriptor(HTMLTextAreaElement.prototype, 'value');"
            "if (descriptor && descriptor.set) { descriptor.set.call(el, val); }"
            "else { el.value = val; }"
            "try { if (typeof el.setSelectionRange === 'function') {"
            "el.setSelectionRange(val.length, val.length); } } catch (e) {}"
            "el.dispatchEvent(new Event('input', {bubbles:true}));"
            "el.dispatchEvent(new Event('change', {bubbles:true}));"
            "return norm(el.value) === norm(val);"
            "};"
            "var selectors = ["
            "'input[name=\"pinInput\"]',"
            "'input[autocomplete=\"one-time-code\"]',"
            "'input[maxlength=\"6\"]',"
            "'input[type=\"tel\"]',"
            "'input[inputmode=\"numeric\"]',"
            "'input[aria-label*=\"verification\" i]',"
            "'input[aria-label*=\"code\" i]'"
            "];"
            "var candidates = [];"
            "var seen = new Set();"
            "for (var i = 0; i < selectors.length; i++) {"
            "var els = [];"
            "try { els = document.querySelectorAll(selectors[i]); } catch (e) { els = []; }"
            "for (var j = 0; j < els.length; j++) {"
            "var el = els[j];"
            "if (!seen.has(el)) { seen.add(el); candidates.push(el); }"
            "}"
            "}"
            "for (var k = 0; k < candidates.length; k++) {"
            "var target = candidates[k];"
            "if (!target || target.disabled) { continue; }"
            "var maxlength = Number(target.getAttribute('maxlength') || 0);"
            "if (maxlength === 1) { continue; }"
            "if (setVal(target, code)) { return {ok:true, mode:'single'}; }"
            "}"
            "var cells = [];"
            "try {"
            "cells = Array.prototype.slice.call(document.querySelectorAll('input[maxlength=\"1\"]'))"
            ".filter(function(el){ return !!el && !el.disabled; });"
            "} catch (e) { cells = []; }"
            "if (cells.length >= 4) {"
            "var n = Math.min(code.length, cells.length);"
            "var composed = '';"
            "for (var idx = 0; idx < n; idx++) {"
            "setVal(cells[idx], code.charAt(idx));"
            "composed += String(cells[idx].value || '');"
            "}"
            "if (norm(composed) === norm(code.slice(0, n))) { return {ok:true, mode:'cells'}; }"
            "}"
            "return {ok:false, mode:'none'};",
            code,
        )
    except Exception:
        return False
    if isinstance(result, dict):
        return bool(result.get("ok"))
    return bool(result)


def _safe_quit_driver(driver):
    if not driver:
        return
    try:
        driver.quit()
    except Exception:
        pass
    # Prevent uc.__del__ from calling quit() again and printing WinError 6.
    try:
        driver.quit = lambda: None
    except Exception:
        pass


def _wait_clickable(driver, by, locator, timeout=30):
    end_at = time.time() + max(1, int(timeout))
    last_err = ""
    while time.time() < end_at:
        try:
            elements = driver.find_elements(by, locator)
        except Exception as exc:
            last_err = str(exc)
            time.sleep(0.2)
            continue
        for el in elements:
            if el is None:
                continue
            try:
                if el.is_displayed() and el.is_enabled():
                    return el
            except Exception as exc:
                last_err = str(exc)
                continue
        time.sleep(0.2)
    raise RuntimeError(f"element not clickable: {locator}; err={last_err or 'timeout'}")


def _wait_clickable_any(driver, locators, timeout=30):
    end_at = time.time() + max(1, int(timeout))
    last_err = ""
    while time.time() < end_at:
        for by, locator in locators:
            try:
                elements = driver.find_elements(by, locator)
            except Exception as exc:
                last_err = f"{locator}: {exc}"
                continue
            for el in elements:
                if el is None:
                    continue
                try:
                    if el.is_displayed() and el.is_enabled():
                        return el
                except Exception as exc:
                    last_err = f"{locator}: {exc}"
        time.sleep(0.2)
    raise RuntimeError(f"no clickable element matched; err={last_err or 'timeout'}")


def _login_page_diag(driver):
    try:
        url = str(getattr(driver, "current_url", "") or "")
    except Exception:
        url = ""
    try:
        title = str(getattr(driver, "title", "") or "")
    except Exception:
        title = ""
    try:
        html = str(driver.page_source or "")
    except Exception:
        html = ""
    err_mark = ""
    m = re.search(r"ERR_[A-Z_]+", html)
    if m:
        err_mark = m.group(0)
    return f"url={url or '-'}, title={title or '-'}, html={len(html)}, err={err_mark or '-'}"


def _collect_login_url_candidates():
    candidates = [
        LOGIN_URL,
        "https://auth.business.gemini.google/login",
        "https://accounts.google.com/ServiceLogin?continue=https%3A%2F%2Fbusiness.gemini.google%2F",
        "https://accounts.google.com/",
    ]
    fallback_raw = str(os.getenv("LOGIN_URL_FALLBACKS", "") or "")
    if fallback_raw:
        for token in re.split(r"[\r\n,;]+", fallback_raw):
            value = str(token or "").strip()
            if value:
                candidates.append(value)
    ordered = []
    seen = set()
    for url in candidates:
        value = str(url or "").strip()
        if not value or value in seen:
            continue
        ordered.append(value)
        seen.add(value)
    return ordered


def _open_login_page_with_retries(driver, step_sleep_scale, page_ready_timeout):
    nav_retries = _env_int("LOGIN_NAV_RETRIES", 3, 1, 8)
    nav_retry_sleep = _env_float("LOGIN_NAV_RETRY_SLEEP", 1.0, 0.1, 10.0)
    candidates = _collect_login_url_candidates()
    last_err = ""

    for attempt in range(1, nav_retries + 1):
        target_url = candidates[(attempt - 1) % len(candidates)]
        try:
            try:
                driver.set_page_load_timeout(max(15, int(page_ready_timeout) + 5))
            except Exception:
                pass
            driver.get(target_url)
            time.sleep(1.0 * step_sleep_scale)
            end_at = time.time() + page_ready_timeout
            while time.time() < end_at:
                try:
                    ready_state = str(driver.execute_script("return document.readyState") or "").lower()
                    if ready_state in {"interactive", "complete"}:
                        break
                except Exception:
                    pass
                time.sleep(0.2)

            current_url = str(getattr(driver, "current_url", "") or "")
            try:
                page_source = str(driver.page_source or "")
            except Exception:
                page_source = ""
            page_hint = page_source.lower()
            if (
                "proxy" in page_hint and "authentication" in page_hint
            ) or ("要求提供用户名和密码" in page_source):
                raise RuntimeError(
                    f"proxy auth prompt detected ({_login_page_diag(driver)})"
                )
            if "err_connection_reset" in page_hint or "err_connection_closed" in page_hint:
                raise RuntimeError(f"login page network error ({_login_page_diag(driver)})")
            if "about:blank" in current_url.lower() or "chrome-error://" in current_url.lower() or len(page_source) < 500:
                raise RuntimeError(
                    f"login page blank/incomplete ({_login_page_diag(driver)})"
                )
            if attempt > 1:
                log(f"登录页重试成功: attempt={attempt}/{nav_retries}, url={target_url}")
            return
        except Exception as exc:
            last_err = str(exc)
            retryable = _is_retryable_proxy_runtime_error(exc, driver)
            if attempt < nav_retries and retryable:
                log(f"登录页打开失败，准备重试 {attempt}/{nav_retries}: {last_err}", "WARN")
                time.sleep(nav_retry_sleep * attempt)
                continue
            raise

    raise RuntimeError(f"open login page failed after retries: {last_err or 'unknown'}")


def _try_dismiss_login_overlays(driver):
    # Best-effort: GDPR/cookie dialogs sometimes block email input click.
    button_locators = (
        (By.XPATH, "//button[.//span[contains(.,'Accept all')] or contains(.,'Accept all')]"),
        (By.XPATH, "//button[.//span[contains(.,'I agree')] or contains(.,'I agree')]"),
        (By.XPATH, "//button[contains(.,'全部接受') or contains(.,'接受全部') or contains(.,'同意')]"),
    )
    for by, locator in button_locators:
        try:
            elements = driver.find_elements(by, locator)
        except Exception:
            continue
        for el in elements:
            try:
                if el.is_displayed() and el.is_enabled():
                    driver.execute_script("arguments[0].click();", el)
                    time.sleep(0.2)
                    log("检测到并点击了登录页遮罩按钮", "WARN")
                    return
            except Exception:
                continue


def _open_login_and_submit_email(driver, wait, email):
    # Slow network guardrails (all values can be tuned via env).
    page_ready_timeout = _env_int("LOGIN_PAGE_READY_TIMEOUT", 20, 5, 120)
    email_input_timeout = _env_int("LOGIN_EMAIL_INPUT_TIMEOUT", 60, 15, 180)
    continue_btn_timeout = _env_int("LOGIN_CONTINUE_BTN_TIMEOUT", 45, 10, 180)
    step_sleep_scale = _env_float("LOGIN_STEP_SLEEP_SCALE", 1.0, 0.5, 4.0)

    _open_login_page_with_retries(
        driver,
        step_sleep_scale=step_sleep_scale,
        page_ready_timeout=page_ready_timeout,
    )

    _try_dismiss_login_overlays(driver)

    log("输入邮箱...")
    email_locators = (
        (By.ID, "identifierId"),
        (By.NAME, "identifier"),
        (By.CSS_SELECTOR, "input[type='email']"),
        (By.XPATH, "//input[contains(@aria-label,'Email') or contains(@aria-label,'邮箱') or contains(@aria-label,'phone')]"),
        (By.XPATH, XPATH["email_input"]),
    )
    try:
        inp = _wait_clickable_any(driver, email_locators, timeout=email_input_timeout)
        inp.click()
        inp.clear()
        fast_type(inp, email)
    except Exception as exc:
        raise RuntimeError(f"email input locate/click failed: {exc}; diag={_login_page_diag(driver)}") from None

    typed_value = str(inp.get_attribute("value") or "").strip()
    if typed_value != email:
        log("邮箱输入校验失败，改用 JS 回填", "WARN")
        try:
            driver.execute_script("arguments[0].value = '';", inp)
            time.sleep(0.1)
            driver.execute_script(
                "arguments[0].value = arguments[1];"
                "arguments[0].dispatchEvent(new Event('input', {bubbles:true}));"
                "arguments[0].dispatchEvent(new Event('change', {bubbles:true}));",
                inp,
                email,
            )
            time.sleep(0.3)
        except Exception:
            pass
        typed_value = str(inp.get_attribute("value") or "").strip()
        if typed_value != email:
            raise RuntimeError(
                f"email input verify failed: expected={email}, actual={typed_value or '<empty>'}"
            )

    log(f"邮箱: {email}")
    time.sleep(0.5 * step_sleep_scale)
    continue_locators = (
        (By.ID, "identifierNext"),
        (By.XPATH, "//div[@id='identifierNext']//button"),
        (By.XPATH, "//button[.//span[contains(.,'Next') or contains(.,'下一步') or contains(.,'继续')]]"),
        (By.XPATH, XPATH["continue_btn"]),
    )
    try:
        btn = _wait_clickable_any(driver, continue_locators, timeout=continue_btn_timeout)
        driver.execute_script("arguments[0].click();", btn)
    except Exception as exc:
        raise RuntimeError(f"continue button click failed: {exc}; diag={_login_page_diag(driver)}") from None
    time.sleep(0.6 * step_sleep_scale)
    log(f"点击继续 (email_timeout={email_input_timeout}s, continue_timeout={continue_btn_timeout}s)")


def _input_verification_code_once(driver, wait, code):
    code = _normalize_code_value(code)
    if not _is_valid_code_token(code):
        return False

    selectors = (
        "input[name='pinInput']",
        "input[autocomplete='one-time-code']",
        "input[maxlength='6']",
        "input[type='tel']",
        "input[inputmode='numeric']",
    )
    input_timeout = _env_int("LOGIN_CODE_INPUT_TIMEOUT", 12, 4, 90)
    end_at = time.time() + input_timeout

    while time.time() < end_at:
        located_any = False
        for sel in selectors:
            try:
                fields = driver.find_elements(By.CSS_SELECTOR, sel)
            except Exception:
                fields = []
            for field in fields:
                if not field:
                    continue
                try:
                    if not field.is_displayed() or not field.is_enabled():
                        continue
                except Exception:
                    continue
                located_any = True
                try:
                    field.click()
                    time.sleep(0.05)
                except Exception:
                    pass
                try:
                    field.clear()
                except Exception:
                    pass
                try:
                    fast_type(field, code, 0.05)
                except Exception:
                    pass
                if _input_value_matches(field, code):
                    return True
                if _set_input_value_js(driver, field, code) and _input_value_matches(field, code):
                    log(f"验证码输入改用 JS 回填: {sel}", "WARN")
                    return True

        try:
            span = driver.find_element(By.CSS_SELECTOR, "span[data-index='0']")
            span.click()
            time.sleep(0.1)
            active = driver.switch_to.active_element
            active.send_keys(code)
            if _input_value_matches(active, code):
                return True
            if _set_input_value_js(driver, active, code) and _input_value_matches(active, code):
                log("验证码输入改用 JS 回填: active_element", "WARN")
                return True
            located_any = True
        except Exception:
            pass

        try:
            otp_inputs = [
                el
                for el in driver.find_elements(By.CSS_SELECTOR, "input[maxlength='1']")
                if el.is_displayed() and el.is_enabled()
            ]
            if len(otp_inputs) >= 6:
                located_any = True
                for idx, ch in enumerate(code[:6]):
                    try:
                        otp_inputs[idx].click()
                        otp_inputs[idx].send_keys(ch)
                    except Exception:
                        pass
                typed = "".join(str(el.get_attribute("value") or "").strip() for el in otp_inputs[:6])
                if _normalize_code_value(typed) == _normalize_code_value(code[:6]):
                    return True
                js_ok = True
                for idx, ch in enumerate(code[:6]):
                    if not _set_input_value_js(driver, otp_inputs[idx], ch):
                        js_ok = False
                typed = "".join(str(el.get_attribute("value") or "").strip() for el in otp_inputs[:6])
                if js_ok and _normalize_code_value(typed) == _normalize_code_value(code[:6]):
                    log("验证码分格输入改用 JS 回填", "WARN")
                    return True
        except Exception:
            pass

        try:
            active = driver.switch_to.active_element
            if active:
                located_any = True
                try:
                    active.send_keys(code)
                except Exception:
                    pass
                if _input_value_matches(active, code):
                    return True
                if _set_input_value_js(driver, active, code) and _input_value_matches(active, code):
                    log("验证码输入改用 JS 回填: 最后兜底", "WARN")
                    return True
        except Exception:
            pass

        try:
            if _input_code_with_global_js(driver, code):
                log("验证码输入改用全局 JS 回填", "WARN")
                return True
        except Exception:
            pass

        time.sleep(0.2 if located_any else 0.35)
    return False


def _input_verification_code(driver, wait, code, extra_codes=None):
    code_candidates = _build_code_input_candidates(code, extra_codes=extra_codes)
    if not code_candidates:
        return False
    primary = code_candidates[0]
    if len(code_candidates) > 1:
        log(f"验证码候选尝试: primary={primary}, variants={','.join(code_candidates[1:])}")
    for idx, candidate in enumerate(code_candidates, start=1):
        if _input_verification_code_once(driver, wait, candidate):
            if candidate != primary:
                log(f"验证码输入采用候选码: {candidate} (primary={primary})", "WARN")
            return True
        if idx < len(code_candidates):
            log(
                f"验证码候选尝试失败，继续下一个 ({idx}/{len(code_candidates)}): {candidate}",
                "WARN",
            )
    log(
        f"验证码写入失败: primary={primary}, tried={','.join(code_candidates)}",
        "ERR",
    )
    return False


def _is_workspace_url(url):
    text = str(url or "").strip().lower()
    return "business.gemini.google" in text and "/cid/" in text


def _find_visible_name_input(driver):
    selectors = [
        "input[formcontrolname='fullName']",
        "input[name='fullName']",
        "input[autocomplete='name']",
        "input[placeholder='Full name']",
        "input[placeholder='全名']",
        "input[aria-label='Full name']",
        "input[aria-label='全名']",
    ]
    for sel in selectors:
        try:
            elements = driver.find_elements(By.CSS_SELECTOR, sel)
        except Exception:
            continue
        for element in elements:
            if element is None:
                continue
            try:
                if not element.is_displayed() or not element.is_enabled():
                    continue
                readonly = str(element.get_attribute("readonly") or "").strip().lower()
                disabled = str(element.get_attribute("disabled") or "").strip().lower()
                if readonly in {"readonly", "true"}:
                    continue
                if disabled in {"disabled", "true"}:
                    continue
                return element, sel
            except Exception:
                continue
    return None, ""


def _wait_name_input_or_workspace(driver, timeout_seconds):
    end_at = time.time() + max(5, int(timeout_seconds))
    while time.time() < end_at:
        try:
            current_url = str(driver.current_url or "")
        except Exception:
            current_url = ""

        if "signin-error" in current_url.lower():
            return None, "", "signin_error"
        if _is_workspace_url(current_url):
            return None, "", "workspace"

        name_input, selector = _find_visible_name_input(driver)
        if name_input is not None:
            return name_input, selector, "name"

        time.sleep(0.5)
    return None, "", "timeout"


def _click_verify_button(driver):
    click_timeout = _env_int("LOGIN_VERIFY_CLICK_TIMEOUT", 18, 5, 120)
    end_at = time.time() + click_timeout
    locators = (
        (By.XPATH, XPATH["verify_btn"]),
        (By.ID, "totpNext"),
        (By.XPATH, "//button[@type='submit']"),
        (By.XPATH, "//button[.//span[contains(.,'Verify') or contains(.,'验证') or contains(.,'继续') or contains(.,'Next')]]"),
    )
    while time.time() < end_at:
        for by, locator in locators:
            try:
                elements = driver.find_elements(by, locator)
            except Exception:
                continue
            for btn in elements:
                if btn is None:
                    continue
                try:
                    if not btn.is_displayed() or not btn.is_enabled():
                        continue
                    driver.execute_script("arguments[0].click();", btn)
                    return True
                except Exception:
                    continue
        try:
            active = driver.switch_to.active_element
            if active:
                active.send_keys(Keys.ENTER)
                return True
        except Exception:
            pass
        time.sleep(0.2)
    return False


def _wait_post_verify_result(driver, timeout_seconds):
    end_at = time.time() + max(10, int(timeout_seconds))
    last_url = ""
    while time.time() < end_at:
        try:
            current_url = str(driver.current_url or "")
        except Exception:
            current_url = ""
        if current_url:
            last_url = current_url
        lower = current_url.lower()
        if "business.gemini.google" in lower and "/cid/" in lower:
            return "workspace", current_url
        if "signin-error" in lower:
            return "signin_error", current_url
        time.sleep(0.5)
    return "timeout", last_url


def register(driver, executor):
    """注册单个账号"""
    start_time = time.time()
    mail_account = get_email()
    if not mail_account:
        return None, False, None, 0
    email = mail_account["email"]
    jwt = mail_account["jwt"]

    wait = WebDriverWait(driver, 30)

    # 1~3. 打开登录页并输入邮箱
    try:
        _open_login_and_submit_email(driver, wait, email)
    except Exception as e:
        if _is_retryable_proxy_runtime_error(e, driver):
            raise ProxyRotateRetryRequired(
                reason=f"open login/input email failed: {e}",
                email=email,
                elapsed=time.time() - start_time,
                account=mail_account,
            ) from None
        raise

    # 异步预创建下一个邮箱
    executor.submit(prefetch_email)

    # 4. 获取验证码
    log(f"邮箱已提交，等待 {EMAIL_SUBMIT_WAIT_SECONDS}s 后开始拉取验证码")
    submitted_at_ts = time.time()
    time.sleep(EMAIL_SUBMIT_WAIT_SECONDS)
    try:
        code_payload = get_code(email, jwt, timeout=EMAIL_CODE_TIMEOUT_SECONDS, submitted_at_ts=submitted_at_ts)
    except MailCodeRetryableError as exc:
        raise ProxyRotateRetryRequired(
            reason=str(exc),
            email=email,
            elapsed=time.time() - start_time,
            account=mail_account,
        ) from None
    code = ""
    code_alternatives = []
    if isinstance(code_payload, dict):
        code = _normalize_code_value(code_payload.get("code"))
        code_alternatives = [
            _normalize_code_value(item)
            for item in list(code_payload.get("alternatives") or [])
            if _is_valid_code_token(item)
        ]
    else:
        code = _normalize_code_value(code_payload)
    if not _is_valid_code_token(code):
        return email, False, None, time.time() - start_time

    # 5. 输入验证码
    time.sleep(1)
    log(f"输入验证码: {code}")
    if code_alternatives:
        log(f"验证码备选: {','.join(code_alternatives)}")
    try:
        if not _input_verification_code(driver, wait, code, extra_codes=code_alternatives):
            raise RuntimeError("no supported verification input element found")
    except Exception as e:
        if _is_retryable_proxy_runtime_error(e, driver):
            raise ProxyRotateRetryRequired(
                reason=f"input verification code failed: {e}",
                email=email,
                elapsed=time.time() - start_time,
                account=mail_account,
            ) from None
        log(f"验证码输入失败: {e}", "ERR")
        return email, False, None, time.time() - start_time

    # 6. 点击验证
    time.sleep(0.5)
    if not _click_verify_button(driver):
        log("点击验证失败：未找到可点击的验证按钮", "WARN")
        return email, False, None, time.time() - start_time
    log("点击验证")

    # 在慢网场景下，点击后先等一次页面状态，避免长时间停留在验证码页
    post_verify_timeout = _env_int("LOGIN_POST_VERIFY_TIMEOUT", 40, 10, 240)
    stage, post_verify_url = _wait_post_verify_result(driver, max(12, post_verify_timeout // 2))
    if stage == "signin_error":
        log(f"验证码后进入 signin-error，跳过: {post_verify_url}", "WARN")
        return email, False, None, time.time() - start_time

    # 7. 输入姓名 - 等待真正的姓名页面出现（慢网场景下避免提前输入到错误输入框）
    log("等待姓名输入页面...")
    try:
        name_page_timeout = _env_int("LOGIN_NAME_INPUT_TIMEOUT", 45, 10, 240)
        post_verify_wait = _env_float("LOGIN_POST_VERIFY_WAIT_SECONDS", 0.8, 0.2, 8.0)
        time.sleep(post_verify_wait)
        name_inp, found_sel, stage = _wait_name_input_or_workspace(driver, timeout_seconds=name_page_timeout)

        if stage == "workspace":
            log("未出现姓名输入框，但已进入工作台，跳过姓名填写", "WARN")
        elif stage == "signin_error":
            log(f"验证码后进入 signin-error，跳过: {post_verify_url or '-'}", "WARN")
            return email, False, None, time.time() - start_time
        elif stage == "name" and name_inp is not None:
            log(f"找到姓名输入框: {found_sel}")
            name = random.choice(NAMES)
            name_inp.click()
            time.sleep(0.2)
            name_inp.clear()
            fast_type(name_inp, name)
            log(f"姓名: {name}")
            time.sleep(0.3)
            name_inp.send_keys(Keys.ENTER)
            time.sleep(1)
        else:
            current_url = ""
            try:
                current_url = str(driver.current_url or "")
            except Exception:
                current_url = ""
            if stage == "timeout" and _click_verify_button(driver):
                log("验证码页仍未跳转，已再次点击验证并重试姓名页面等待", "WARN")
                name_inp, found_sel, stage = _wait_name_input_or_workspace(driver, timeout_seconds=max(15, name_page_timeout // 2))
                if stage == "name" and name_inp is not None:
                    log(f"找到姓名输入框: {found_sel}")
                    name = random.choice(NAMES)
                    name_inp.click()
                    time.sleep(0.2)
                    name_inp.clear()
                    fast_type(name_inp, name)
                    log(f"姓名: {name}")
                    time.sleep(0.3)
                    name_inp.send_keys(Keys.ENTER)
                    time.sleep(1)
                elif stage == "workspace":
                    log("未出现姓名输入框，但已进入工作台，跳过姓名填写", "WARN")
                elif stage == "signin_error":
                    log(f"验证码后进入 signin-error，跳过: {current_url or '-'}", "WARN")
                    return email, False, None, time.time() - start_time
                else:
                    log(f"未找到姓名输入框 (timeout={name_page_timeout}s, stage={stage}, url={current_url or '-'})", "ERR")
                    return email, False, None, time.time() - start_time
            else:
                log(f"未找到姓名输入框 (timeout={name_page_timeout}s, stage={stage}, url={current_url or '-'})", "ERR")
                return email, False, None, time.time() - start_time
    except Exception as e:
        log(f"姓名输入异常: {e}", "ERR")
        return email, False, None, time.time() - start_time

    # 8. 等待进入工作台
    log("等待工作台...")
    for _ in range(30):  # 最多等30秒
        time.sleep(1)
        url = driver.current_url
        if 'business.gemini.google' in url and '/cid/' in url:
            log(f"已进入工作台: {url}")
            break
    else:
        log(f"未跳转到带 cid 的页面，当前: {driver.current_url}", "WARN")

    # 9. 保存配置
    elapsed = time.time() - start_time
    config = save_config(email, driver)
    if config:
        mark_account_updated(email, config)
        log(f"注册成功: {email} (耗时: {elapsed:.1f}s)")
        return email, True, config, elapsed
    return email, False, None, elapsed

def main(total_accounts=None, proxy=None):
    global RUNTIME_PROXY
    if proxy is not None:
        normalized_proxy = _normalize_proxy_value(proxy)
        if str(proxy).strip() and not normalized_proxy:
            log(f"代理格式无效，将忽略: {proxy}", "WARN")
        RUNTIME_PROXY = normalized_proxy

    target_accounts = TOTAL_ACCOUNTS
    try:
        if total_accounts is not None:
            parsed_total = int(total_accounts)
            if parsed_total > 0:
                target_accounts = parsed_total
    except Exception:
        pass

    try:
        validate_mail_config()
    except RuntimeError as e:
        log(str(e), "ERR")
        return 1

    print(f"\n{'='*50}\nGemini Business 批量注册 - 共 {target_accounts} 个\n{'='*50}\n")

    driver = None
    executor = ThreadPoolExecutor(max_workers=2)
    fatal_browser_error = False
    success, fail, accounts = 0, 0, []
    total_time = 0
    times = []
    mail_rotate_retries = _mail_proxy_rotate_retries()
    local_login_retries = _env_int("LOGIN_LOCAL_RETRIES", 2, 0, 10)
    local_login_retry_sleep = _env_float("LOGIN_LOCAL_RETRY_SLEEP", 1.2, 0.1, 10.0)

    # 预创建第一个邮箱
    executor.submit(prefetch_email)

    for i in range(target_accounts):
        print(f"\n{'#'*40}\n注册 {i+1}/{target_accounts}\n{'#'*40}\n")
        rotate_retry_count = 0
        local_retry_count = 0

        while True:
            # 确保 driver 有效
            if driver is None:
                log("创建新浏览器...")
                t_browser = time.time()
                try:
                    driver = create_browser_driver()
                except Exception as e:
                    log(str(e), "ERR")
                    log("停止执行，请检查代理、Chrome版本或驱动缓存后重试", "ERR")
                    fatal_browser_error = True
                    break
                log(f"浏览器就绪，等待 2s 让浏览器完全启动... (总耗时 {time.time()-t_browser:.1f}s)")
                time.sleep(2)
            else:
                try:
                    _ = driver.current_url
                except:
                    log("浏览器已关闭，重新创建...")
                    _safe_quit_driver(driver)
                    t_browser = time.time()
                    try:
                        driver = create_browser_driver()
                    except Exception as e:
                        log(str(e), "ERR")
                        log("停止执行，请检查代理、Chrome版本或驱动缓存后重试", "ERR")
                        driver = None
                        fatal_browser_error = True
                        break
                    log(f"浏览器就绪，等待 2s 让浏览器完全启动... (总耗时 {time.time()-t_browser:.1f}s)")
                    time.sleep(2)

            try:
                email, ok, cfg, elapsed = register(driver, executor)
                total_time += elapsed
                if ok and cfg:
                    success += 1
                    accounts.append((email, cfg))
                    times.append(elapsed)
                else:
                    fail += 1
                break
            except ProxyRotateRetryRequired as rotate_exc:
                total_time += rotate_exc.elapsed
                if _should_retry_login_locally(rotate_exc.reason) and local_retry_count < local_login_retries:
                    local_retry_count += 1
                    if rotate_exc.account:
                        email_queue.insert(0, rotate_exc.account)
                    log(
                        f"登录链路本地重试（不切换代理） {local_retry_count}/{local_login_retries}: {rotate_exc.reason}",
                        "WARN",
                    )
                    _safe_quit_driver(driver)
                    driver = None
                    time.sleep(local_login_retry_sleep * local_retry_count)
                    continue
                if rotate_retry_count >= mail_rotate_retries:
                    log(
                        f"可重试代理异常已达最大轮换重试次数: {mail_rotate_retries}, reason={rotate_exc.reason}",
                        "ERR",
                    )
                    fail += 1
                    break
                rotate_retry_count += 1
                if rotate_exc.account:
                    email_queue.insert(0, rotate_exc.account)
                _rotate_proxy_for_mail_retry(rotate_exc.reason, rotate_retry_count, mail_rotate_retries)
                _safe_quit_driver(driver)
                driver = None
                continue
            except Exception as e:
                log(f"异常: {e}", "ERR")
                fail += 1
                _safe_quit_driver(driver)
                driver = None  # 标记为需要重新创建
                break

        if fatal_browser_error:
            break

        avg_time = total_time / (i + 1) if total_time > 0 else 0
        print(f"\n进度: {i+1}/{target_accounts} | 成功: {success} | 失败: {fail} | 平均耗时: {avg_time:.1f}s")

        if i < target_accounts - 1 and driver:
            try: driver.delete_all_cookies()
            except: pass
            time.sleep(random.randint(2, 3))

    executor.shutdown(wait=False)
    if driver:
        _safe_quit_driver(driver)

    # 统计信息
    avg = sum(times) / len(times) if times else 0
    min_t = min(times) if times else 0
    max_t = max(times) if times else 0
    print(f"\n{'='*50}")
    print(f"完成! 成功: {success}, 失败: {fail}")
    print(f"总耗时: {total_time:.1f}s | 平均: {avg:.1f}s | 最快: {min_t:.1f}s | 最慢: {max_t:.1f}s")
    print(f"配置保存在: {OUTPUT_DIR}/")
    print(f"{'='*50}")
    return 1 if fatal_browser_error else 0

def parse_cli_args():
    parser = argparse.ArgumentParser(description="Gemini Business 批量注册工具")
    parser.add_argument(
        "--total",
        type=int,
        default=TOTAL_ACCOUNTS,
        help=f"注册总数，默认 {TOTAL_ACCOUNTS}",
    )
    parser.add_argument(
        "--proxy",
        default="",
        help="浏览器代理地址，如 http://127.0.0.1:7897 或 socks5://127.0.0.1:7890",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_cli_args()
    total = args.total if isinstance(args.total, int) and args.total > 0 else TOTAL_ACCOUNTS
    sys.exit(main(total_accounts=total, proxy=args.proxy))

