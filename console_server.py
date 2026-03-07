from __future__ import annotations

import asyncio
import base64
import json
import locale
import os
import queue
import re
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, AsyncGenerator, Dict, List, Optional, Set
from urllib.parse import parse_qsl, quote, urlencode, urlparse, urlunparse

from fastapi import FastAPI, HTTPException
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
import requests
try:
    import yaml
except Exception:
    yaml = None

from proxy_pool import (
    is_location_supported,
    normalize_proxy_value,
    parse_trace,
    trace_via_proxy,
)

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "gemini_accounts"
ALL_ACCOUNT_FILE = OUTPUT_DIR / "all_account.json"
CONFIG_FILE = BASE_DIR / "console_config.json"
STATE_FILE = BASE_DIR / "console_state.json"
REGISTER_SCRIPT = BASE_DIR / "zhuce.py"
MAINTAIN_SCRIPT = BASE_DIR / "weihu.py"
STATIC_DIR = BASE_DIR / "console_static"

OUTPUT_DIR.mkdir(exist_ok=True)
STATIC_DIR.mkdir(exist_ok=True)

DEFAULT_CONFIG: Dict[str, Any] = {
    "proxy": "",
    "proxy_engine": str(os.getenv("PROXY_ENGINE", "easyproxies") or "easyproxies").strip().lower() or "easyproxies",
    "easyproxies_enabled": True,
    "easyproxies_listen_proxy": "http://127.0.0.1:2323",
    "easyproxies_api_url": "http://127.0.0.1:7840",
    "easyproxies_password": "",
    "easyproxies_subscription_enabled": False,
    "easyproxies_subscription_url": "",
    "easyproxies_subscription_refresh_minutes": 60,
    "easyproxies_refresh_before_task": True,
    "easyproxies_retry_forever": True,
    "easyproxies_retry_times": 3,
    "easyproxies_retry_interval_seconds": 8,
    "easyproxies_rotate_interval_seconds": 120,
    "easyproxies_node_rotation_enabled": True,
    "easyproxies_node_register_quota": 5,
    "easyproxies_node_maintain_quota": 20,
    "easyproxies_fixed_node": "",
    "resin_enabled": str(os.getenv("RESIN_ENABLED", "0") or "0").strip().lower() in {"1", "true", "yes", "on"},
    "resin_api_url": str(os.getenv("RESIN_API_URL", "http://127.0.0.1:2260") or "http://127.0.0.1:2260").strip(),
    "resin_proxy_url": str(os.getenv("RESIN_PROXY_URL", "http://127.0.0.1:2260") or "http://127.0.0.1:2260").strip(),
    "resin_admin_token": str(os.getenv("RESIN_ADMIN_TOKEN", "") or "").strip(),
    "resin_proxy_token": str(os.getenv("RESIN_PROXY_TOKEN", "") or "").strip(),
    "resin_platform_register": str(os.getenv("RESIN_PLATFORM_REGISTER", "gemini-register") or "gemini-register").strip(),
    "resin_platform_maintain": str(os.getenv("RESIN_PLATFORM_MAINTAIN", "gemini-maintain") or "gemini-maintain").strip(),
    "resin_retry_forever": True,
    "resin_retry_times": 3,
    "resin_retry_interval_seconds": 8,
    "resin_node_rotation_enabled": True,
    "resin_node_register_quota": 5,
    "resin_node_maintain_quota": 20,
    "resin_rotation_pool_size": 2048,
    "auto_maintain": False,
    "maintain_interval_minutes": 30,
    "maintain_interval_hours": 4.0,
    "auto_register": False,
    "auto_task_priority": "maintain",
    "auto_register_interval_hours": 4.0,
    "auto_register_batch_size": 20,
    "guarantee_enabled": True,
    "guarantee_target_accounts": 200,
    "guarantee_window_hours": 4.0,
    "min_accounts": 20,
    "max_replenish_per_round": 20,
    "register_default_count": 1,
    "account_sync_enabled": False,
    "account_sync_url": "",
    "account_sync_auth_mode": "session",
    "account_sync_login_url": "",
    "account_sync_auth_header_name": "X-API-Key",
    "account_sync_auth_query_name": "api_key",
    "account_sync_api_key": "",
    "account_sync_timeout_seconds": 20,
    "account_sync_platform": "gemini",
    "account_sync_after_register": True,
    "account_sync_after_maintain": True,
    "task_watchdog_enabled": True,
    "task_stall_timeout_seconds": 300,
    "task_stall_restart_enabled": True,
    "task_stall_restart_max": 5,
    "proxy_fail_guard_enabled": True,
    "proxy_fail_guard_threshold": 3,
    "proxy_fail_guard_pause_seconds": 60,
}

UPLOAD_PLATFORMS = ("gemini",)
SYNC_AUTH_MODES = ("none", "bearer", "header", "query", "session")

_cfg_lock = threading.RLock()


def _now_str() -> str:
    return datetime.now().strftime("%H:%M:%S")


def _load_json(path: Path, fallback: Dict[str, Any]) -> Dict[str, Any]:
    if not path.exists():
        return dict(fallback)
    try:
        # Be tolerant of UTF-8 BOM files saved by some Windows editors.
        raw = json.loads(path.read_text(encoding="utf-8-sig"))
        if isinstance(raw, dict):
            merged = dict(fallback)
            merged.update(raw)
            return merged
    except Exception:
        pass
    return dict(fallback)


def _save_json(path: Path, data: Dict[str, Any]) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _mask_secret(value: str, keep: int = 8) -> str:
    text = str(value or "")
    if not text:
        return ""
    if len(text) <= keep:
        return "*" * len(text)
    return text[:keep] + "..."


def _append_query_param(url: str, key: str, value: str) -> str:
    parsed = urlparse(url)
    query = parse_qsl(parsed.query, keep_blank_values=True)
    replaced = False
    out_query: List[tuple[str, str]] = []
    for k, v in query:
        if k == key:
            out_query.append((k, value))
            replaced = True
        else:
            out_query.append((k, v))
    if not replaced:
        out_query.append((key, value))
    return urlunparse(parsed._replace(query=urlencode(out_query)))


ALLOWED_PROXY_SCHEMES = {"http", "https", "socks4", "socks5"}
SUPPORTED_PROXY_ENGINES = {"easyproxies", "resin", "auto"}


def _normalize_proxy_engine(value: Any) -> str:
    engine = str(value or "").strip().lower()
    if engine in {"socks5_pool", "socks5-pool"}:
        engine = "easyproxies"
    if engine in {"proxy_pool", "zenproxy"}:
        engine = "auto"
    if engine not in SUPPORTED_PROXY_ENGINES:
        engine = "easyproxies"
    return engine


def _normalize_subscription_url(raw: str) -> str:
    value = str(raw or "").strip()
    if not value:
        return ""
    if "://" not in value:
        return "https://" + value
    return value


def _normalize_http_url(raw: str) -> str:
    value = str(raw or "").strip()
    if not value:
        return ""
    if "://" not in value:
        return "http://" + value
    return value


def _normalize_proxy_endpoint(raw: str, default: str = "") -> str:
    value = str(raw or "").strip()
    if not value:
        value = str(default or "").strip()
    if not value:
        return ""
    if "://" in value:
        parsed = urlparse(value)
        scheme = str(parsed.scheme or "").lower()
        if scheme in {"socks5", "socks4", "http", "https"}:
            return normalize_proxy_value(value)
        return ""
    return normalize_proxy_value(value)


def _mask_proxy_for_log(proxy: str) -> str:
    value = str(proxy or "").strip()
    if not value or "@" not in value:
        return value
    return re.sub(r":([^:@/?#]+)@", ":***@", value, count=1)


def _normalize_proxy_scheme(scheme: Any) -> str:
    value = str(scheme or "").strip().lower()
    if value in {"http", "https"}:
        return value
    if value in {"socks", "socks5", "socks5h"}:
        return "socks5"
    if value in {"socks4", "socks4a"}:
        return "socks4"
    return ""


def _build_proxy_url(host: Any, port: Any, scheme: Any = "http", username: Any = "", password: Any = "") -> str:
    host_value = str(host or "").strip()
    port_value = str(port or "").strip()
    if not host_value or not port_value or not port_value.isdigit():
        return ""

    scheme_value = _normalize_proxy_scheme(scheme) or "http"
    user_value = str(username or "").strip()
    pass_value = str(password or "").strip()
    auth = ""
    if user_value:
        auth = quote(user_value, safe="")
        if pass_value:
            auth += ":" + quote(pass_value, safe="")
        auth += "@"
    return normalize_proxy_value(f"{scheme_value}://{auth}{host_value}:{port_value}")


def _parse_proxy_line(raw_line: str) -> str:
    line = str(raw_line or "").strip().strip('"').strip("'")
    if not line:
        return ""
    if line.startswith(("#", ";", "//")):
        return ""
    if line.startswith("- "):
        line = line[2:].strip()
        if not line:
            return ""

    if "://" in line:
        scheme = str(urlparse(line).scheme or "").lower()
        if scheme not in ALLOWED_PROXY_SCHEMES:
            return ""
        return normalize_proxy_value(line)

    m = re.match(r"^([^:@\s]+):([^@\s]+)@([^:\s]+):(\d{2,5})$", line)
    if m:
        return _build_proxy_url(
            host=m.group(3),
            port=m.group(4),
            scheme="http",
            username=m.group(1),
            password=m.group(2),
        )

    parts = line.split(":")
    if len(parts) == 4 and parts[1].isdigit():
        return _build_proxy_url(
            host=parts[0],
            port=parts[1],
            scheme="http",
            username=parts[2],
            password=parts[3],
        )

    if re.match(r"^[^:\s]+:\d{2,5}$", line):
        return normalize_proxy_value(line)

    if "," in line:
        csv_parts = [p.strip() for p in line.split(",")]
        if len(csv_parts) == 2 and csv_parts[1].isdigit():
            return normalize_proxy_value(f"{csv_parts[0]}:{csv_parts[1]}")
    return ""


def _collect_proxies_from_obj(obj: Any, out: List[str]) -> None:
    if obj is None:
        return
    if isinstance(obj, str):
        parsed = _parse_proxy_line(obj)
        if parsed:
            out.append(parsed)
        return
    if isinstance(obj, (list, tuple, set)):
        for item in obj:
            _collect_proxies_from_obj(item, out)
        return
    if isinstance(obj, dict):
        host = obj.get("server") or obj.get("host") or obj.get("ip") or obj.get("address")
        port = obj.get("port")
        if host and port:
            parsed = _build_proxy_url(
                host=host,
                port=port,
                scheme=obj.get("type") or obj.get("protocol") or obj.get("scheme") or "http",
                username=obj.get("username") or obj.get("user") or "",
                password=obj.get("password") or obj.get("pass") or obj.get("passwd") or "",
            )
            if parsed:
                out.append(parsed)

        for key in ("proxy", "proxy_url", "url", "value"):
            if key in obj:
                _collect_proxies_from_obj(obj.get(key), out)
        for key in ("proxies", "proxy_list", "list", "data", "result", "items", "nodes"):
            if key in obj:
                _collect_proxies_from_obj(obj.get(key), out)
        return


def _maybe_decode_base64_subscription(text: str) -> str:
    compact = re.sub(r"\s+", "", str(text or ""))
    if len(compact) < 16:
        return ""
    if re.search(r"[^A-Za-z0-9+/=_-]", compact):
        return ""
    candidate = compact.replace("-", "+").replace("_", "/")
    candidate += "=" * ((4 - len(candidate) % 4) % 4)
    try:
        decoded = base64.b64decode(candidate, validate=False)
        decoded_text = decoded.decode("utf-8", errors="ignore").strip()
    except Exception:
        return ""
    if not decoded_text:
        return ""
    if "\n" not in decoded_text and "://" not in decoded_text and ":" not in decoded_text:
        return ""
    return decoded_text


def _collect_subscription_uri_schemes(raw_text: str) -> Dict[str, int]:
    text = str(raw_text or "").strip()
    if not text:
        return {}
    blocks = [text]
    decoded = _maybe_decode_base64_subscription(text)
    if decoded and decoded != text:
        blocks.append(decoded)

    counts: Dict[str, int] = {}
    for block in blocks:
        for raw_line in block.splitlines():
            line = str(raw_line or "").strip()
            if not line:
                continue
            m = re.match(r"^([A-Za-z][A-Za-z0-9+.\-]{1,20})://", line)
            if not m:
                continue
            scheme = m.group(1).lower()
            counts[scheme] = counts.get(scheme, 0) + 1
    return counts


def _parse_proxy_subscription(raw_text: str) -> List[str]:
    text = str(raw_text or "").strip()
    if not text:
        return []

    proxies: List[str] = []
    try:
        payload = json.loads(text)
    except Exception:
        payload = None
    if payload is not None:
        _collect_proxies_from_obj(payload, proxies)
    elif yaml is not None and (
        re.search(r"(?im)^\s*proxies\s*:", text) is not None
        or re.search(r"(?im)^\s*-\s*name\s*:", text) is not None
    ):
        try:
            yaml_payload = yaml.safe_load(text)
        except Exception:
            yaml_payload = None
        if yaml_payload is not None:
            _collect_proxies_from_obj(yaml_payload, proxies)

    blocks = [text]
    decoded = _maybe_decode_base64_subscription(text)
    if decoded and decoded != text:
        blocks.append(decoded)

    for block in blocks:
        for raw_line in block.splitlines():
            parsed = _parse_proxy_line(raw_line)
            if parsed:
                proxies.append(parsed)

    unique: List[str] = []
    seen: set[str] = set()
    for item in proxies:
        normalized = normalize_proxy_value(item)
        if not normalized:
            continue
        scheme = str(urlparse(normalized).scheme or "").lower()
        if scheme not in ALLOWED_PROXY_SCHEMES:
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        unique.append(normalized)
    return unique


def _load_all_account_rows() -> List[Dict[str, Any]]:
    if not ALL_ACCOUNT_FILE.exists():
        return []
    try:
        raw = json.loads(ALL_ACCOUNT_FILE.read_text(encoding="utf-8"))
    except Exception as exc:
        raise RuntimeError(f"Failed to read all_account.json: {exc}") from None
    if not isinstance(raw, list):
        raise RuntimeError("all_account.json format error: expected a list")
    rows: List[Dict[str, Any]] = []
    for item in raw:
        if isinstance(item, dict):
            rows.append(item)
    return rows


def _derive_login_url_from_sync_url(sync_url: str) -> str:
    parsed = urlparse(sync_url)
    if not parsed.scheme or not parsed.netloc:
        return ""
    return urlunparse((parsed.scheme, parsed.netloc, "/login", "", "", ""))


def _normalize_sync_target_url(sync_url: str) -> str:
    parsed = urlparse(sync_url)
    if not parsed.scheme or not parsed.netloc:
        return sync_url
    path = parsed.path or ""
    if path in {"", "/"}:
        parsed = parsed._replace(path="/admin/accounts-config")
    return urlunparse(parsed)


def _build_accounts_config_payload(rows: List[Dict[str, Any]]) -> tuple[List[Dict[str, Any]], int]:
    payload: List[Dict[str, Any]] = []
    skipped = 0
    for idx, row in enumerate(rows, 1):
        secure_c_ses = str(row.get("secure_c_ses") or "").strip()
        csesidx = str(row.get("csesidx") or "").strip()
        config_id = str(row.get("config_id") or "").strip()
        if not secure_c_ses or not csesidx or not config_id:
            skipped += 1
            continue

        account_id = str(row.get("id") or row.get("email") or f"account_{idx}").strip()
        out: Dict[str, Any] = {
            "id": account_id,
            "secure_c_ses": secure_c_ses,
            "csesidx": csesidx,
            "config_id": config_id,
        }
        host_c_oses = str(row.get("host_c_oses") or "").strip()
        if host_c_oses:
            out["host_c_oses"] = host_c_oses
        expires_at = str(row.get("expires_at") or "").strip()
        if expires_at:
            out["expires_at"] = expires_at
        if isinstance(row.get("disabled"), bool):
            out["disabled"] = row.get("disabled")
        payload.append(out)
    return payload, skipped


def _normalize_config(cfg: Dict[str, Any]) -> Dict[str, Any]:
    incoming = dict(cfg or {})
    # Legacy socks5-pool compatibility mapping.
    if "easyproxies_enabled" not in incoming and "socks5_pool_enabled" in incoming:
        incoming["easyproxies_enabled"] = bool(incoming.get("socks5_pool_enabled"))
    if "easyproxies_api_url" not in incoming and incoming.get("socks5_pool_status_url"):
        incoming["easyproxies_api_url"] = incoming.get("socks5_pool_status_url")
    if "easyproxies_listen_proxy" not in incoming and incoming.get("socks5_pool_listen_addr"):
        incoming["easyproxies_listen_proxy"] = incoming.get("socks5_pool_listen_addr")
    if "easyproxies_retry_forever" not in incoming and "socks5_pool_retry_forever" in incoming:
        incoming["easyproxies_retry_forever"] = bool(incoming.get("socks5_pool_retry_forever"))
    if "easyproxies_retry_interval_seconds" not in incoming and "socks5_pool_retry_interval_seconds" in incoming:
        incoming["easyproxies_retry_interval_seconds"] = incoming.get("socks5_pool_retry_interval_seconds")
    if "easyproxies_rotate_interval_seconds" not in incoming and "socks5_pool_rotate_interval_seconds" in incoming:
        incoming["easyproxies_rotate_interval_seconds"] = incoming.get("socks5_pool_rotate_interval_seconds")
    if "easyproxies_subscription_url" not in incoming and incoming.get("proxy_subscription_url"):
        incoming["easyproxies_subscription_url"] = incoming.get("proxy_subscription_url")
    if "easyproxies_subscription_enabled" not in incoming and "proxy_subscription_enabled" in incoming:
        incoming["easyproxies_subscription_enabled"] = bool(incoming.get("proxy_subscription_enabled"))
    if "easyproxies_subscription_refresh_minutes" not in incoming and "proxy_subscription_refresh_minutes" in incoming:
        incoming["easyproxies_subscription_refresh_minutes"] = incoming.get("proxy_subscription_refresh_minutes")

    out = dict(DEFAULT_CONFIG)
    out.update(incoming)
    out["proxy"] = str(out.get("proxy") or "").strip()
    out["proxy_engine"] = _normalize_proxy_engine(out.get("proxy_engine"))

    out["easyproxies_enabled"] = bool(out.get("easyproxies_enabled", True))
    out["easyproxies_listen_proxy"] = _normalize_proxy_endpoint(
        str(out.get("easyproxies_listen_proxy") or ""),
        default="http://127.0.0.1:2323",
    ) or "http://127.0.0.1:2323"
    out["easyproxies_api_url"] = _normalize_http_url(str(out.get("easyproxies_api_url") or "http://127.0.0.1:7840"))
    out["easyproxies_password"] = str(out.get("easyproxies_password") or "").strip()
    out["easyproxies_subscription_enabled"] = bool(out.get("easyproxies_subscription_enabled", False))
    out["easyproxies_subscription_url"] = _normalize_subscription_url(str(out.get("easyproxies_subscription_url") or ""))
    out["easyproxies_refresh_before_task"] = bool(out.get("easyproxies_refresh_before_task", True))
    out["easyproxies_retry_forever"] = bool(out.get("easyproxies_retry_forever", True))
    try:
        out["easyproxies_retry_times"] = max(1, min(int(out.get("easyproxies_retry_times") or 3), 60))
    except Exception:
        out["easyproxies_retry_times"] = 3
    try:
        out["easyproxies_retry_interval_seconds"] = max(1, min(int(out.get("easyproxies_retry_interval_seconds") or 8), 300))
    except Exception:
        out["easyproxies_retry_interval_seconds"] = 8
    try:
        out["easyproxies_rotate_interval_seconds"] = max(0, min(int(out.get("easyproxies_rotate_interval_seconds") or 120), 3600))
    except Exception:
        out["easyproxies_rotate_interval_seconds"] = 120
    out["easyproxies_node_rotation_enabled"] = bool(out.get("easyproxies_node_rotation_enabled", True))
    try:
        out["easyproxies_node_register_quota"] = max(1, min(int(out.get("easyproxies_node_register_quota") or 5), 500))
    except Exception:
        out["easyproxies_node_register_quota"] = 5
    try:
        out["easyproxies_node_maintain_quota"] = max(1, min(int(out.get("easyproxies_node_maintain_quota") or 20), 5000))
    except Exception:
        out["easyproxies_node_maintain_quota"] = 20
    out["easyproxies_fixed_node"] = str(out.get("easyproxies_fixed_node") or "").strip()
    if len(out["easyproxies_fixed_node"]) > 200:
        out["easyproxies_fixed_node"] = out["easyproxies_fixed_node"][:200]
    try:
        out["easyproxies_subscription_refresh_minutes"] = max(
            1,
            min(int(out.get("easyproxies_subscription_refresh_minutes") or 60), 24 * 60),
        )
    except Exception:
        out["easyproxies_subscription_refresh_minutes"] = 60

    out["resin_enabled"] = bool(out.get("resin_enabled", False))
    out["resin_api_url"] = _normalize_http_url(str(out.get("resin_api_url") or "http://127.0.0.1:2260"))
    out["resin_proxy_url"] = _normalize_proxy_endpoint(
        str(out.get("resin_proxy_url") or ""),
        default="http://127.0.0.1:2260",
    ) or "http://127.0.0.1:2260"
    out["resin_admin_token"] = str(out.get("resin_admin_token") or "").strip()
    out["resin_proxy_token"] = str(out.get("resin_proxy_token") or "").strip()
    out["resin_platform_register"] = str(out.get("resin_platform_register") or "gemini-register").strip() or "gemini-register"
    out["resin_platform_maintain"] = str(out.get("resin_platform_maintain") or "gemini-maintain").strip() or "gemini-maintain"
    if len(out["resin_platform_register"]) > 80:
        out["resin_platform_register"] = out["resin_platform_register"][:80]
    if len(out["resin_platform_maintain"]) > 80:
        out["resin_platform_maintain"] = out["resin_platform_maintain"][:80]
    out["resin_retry_forever"] = bool(out.get("resin_retry_forever", True))
    try:
        out["resin_retry_times"] = max(1, min(int(out.get("resin_retry_times") or 3), 60))
    except Exception:
        out["resin_retry_times"] = 3
    try:
        out["resin_retry_interval_seconds"] = max(1, min(int(out.get("resin_retry_interval_seconds") or 8), 300))
    except Exception:
        out["resin_retry_interval_seconds"] = 8
    out["resin_node_rotation_enabled"] = bool(out.get("resin_node_rotation_enabled", True))
    try:
        out["resin_node_register_quota"] = max(1, min(int(out.get("resin_node_register_quota") or 5), 5000))
    except Exception:
        out["resin_node_register_quota"] = 5
    try:
        out["resin_node_maintain_quota"] = max(1, min(int(out.get("resin_node_maintain_quota") or 20), 50000))
    except Exception:
        out["resin_node_maintain_quota"] = 20
    try:
        out["resin_rotation_pool_size"] = max(1, min(int(out.get("resin_rotation_pool_size") or 2048), 50000))
    except Exception:
        out["resin_rotation_pool_size"] = 2048

    out["auto_maintain"] = bool(out.get("auto_maintain", False))
    try:
        out["maintain_interval_minutes"] = max(5, int(out.get("maintain_interval_minutes") or 30))
    except Exception:
        out["maintain_interval_minutes"] = 30
    try:
        out["maintain_interval_hours"] = max(0.1, min(float(out.get("maintain_interval_hours") or 4.0), 72.0))
    except Exception:
        out["maintain_interval_hours"] = 4.0
    out["auto_register"] = bool(out.get("auto_register", False))
    auto_task_priority = str(out.get("auto_task_priority") or "maintain").strip().lower()
    if auto_task_priority not in {"register", "maintain"}:
        auto_task_priority = "maintain"
    out["auto_task_priority"] = auto_task_priority
    try:
        out["auto_register_interval_hours"] = max(0.1, min(float(out.get("auto_register_interval_hours") or 4.0), 72.0))
    except Exception:
        out["auto_register_interval_hours"] = 4.0
    try:
        auto_batch_raw = int(out.get("auto_register_batch_size") or 0)
    except Exception:
        auto_batch_raw = 0
    try:
        legacy_batch_raw = int(out.get("max_replenish_per_round") or 0)
    except Exception:
        legacy_batch_raw = 0
    batch_size = auto_batch_raw if auto_batch_raw > 0 else legacy_batch_raw
    if batch_size <= 0:
        batch_size = 20
    out["auto_register_batch_size"] = max(1, min(int(batch_size), 500))
    out["guarantee_enabled"] = bool(out.get("guarantee_enabled", True))
    try:
        out["guarantee_target_accounts"] = max(1, min(int(out.get("guarantee_target_accounts") or 200), 10000))
    except Exception:
        out["guarantee_target_accounts"] = 200
    try:
        out["guarantee_window_hours"] = max(0.5, min(float(out.get("guarantee_window_hours") or 4.0), 24.0))
    except Exception:
        out["guarantee_window_hours"] = 4.0
    try:
        out["min_accounts"] = max(1, int(out.get("min_accounts") or 20))
    except Exception:
        out["min_accounts"] = 20
    # Keep legacy key in sync to avoid multi-field conflicts in old clients.
    out["max_replenish_per_round"] = out["auto_register_batch_size"]
    try:
        out["register_default_count"] = max(1, int(out.get("register_default_count") or 1))
    except Exception:
        out["register_default_count"] = 1

    out["account_sync_enabled"] = bool(out.get("account_sync_enabled", False))
    out["account_sync_url"] = str(out.get("account_sync_url") or "").strip()
    sync_auth_mode = str(out.get("account_sync_auth_mode") or "bearer").strip().lower()
    if sync_auth_mode not in SYNC_AUTH_MODES:
        sync_auth_mode = "session"
    out["account_sync_auth_mode"] = sync_auth_mode
    out["account_sync_login_url"] = str(out.get("account_sync_login_url") or "").strip()
    out["account_sync_auth_header_name"] = str(out.get("account_sync_auth_header_name") or "X-API-Key").strip() or "X-API-Key"
    out["account_sync_auth_query_name"] = str(out.get("account_sync_auth_query_name") or "api_key").strip() or "api_key"
    out["account_sync_api_key"] = str(out.get("account_sync_api_key") or "").strip()
    try:
        out["account_sync_timeout_seconds"] = max(5, min(int(out.get("account_sync_timeout_seconds") or 20), 120))
    except Exception:
        out["account_sync_timeout_seconds"] = 20
    platform = str(out.get("account_sync_platform") or "gemini").strip().lower() or "gemini"
    if platform not in UPLOAD_PLATFORMS:
        platform = "gemini"
    out["account_sync_platform"] = platform
    out["account_sync_after_register"] = True
    out["account_sync_after_maintain"] = True
    out["task_watchdog_enabled"] = bool(out.get("task_watchdog_enabled", True))
    try:
        out["task_stall_timeout_seconds"] = max(30, min(int(out.get("task_stall_timeout_seconds") or 300), 7200))
    except Exception:
        out["task_stall_timeout_seconds"] = 300
    out["task_stall_restart_enabled"] = bool(out.get("task_stall_restart_enabled", True))
    try:
        out["task_stall_restart_max"] = max(0, min(int(out.get("task_stall_restart_max") or 5), 50))
    except Exception:
        out["task_stall_restart_max"] = 5
    out["proxy_fail_guard_enabled"] = bool(out.get("proxy_fail_guard_enabled", True))
    try:
        out["proxy_fail_guard_threshold"] = max(2, min(int(out.get("proxy_fail_guard_threshold") or 3), 20))
    except Exception:
        out["proxy_fail_guard_threshold"] = 3
    try:
        out["proxy_fail_guard_pause_seconds"] = max(10, min(int(out.get("proxy_fail_guard_pause_seconds") or 60), 900))
    except Exception:
        out["proxy_fail_guard_pause_seconds"] = 60
    normalized: Dict[str, Any] = {}
    for key in DEFAULT_CONFIG.keys():
        normalized[key] = out.get(key)
    return normalized


_config = _normalize_config(_load_json(CONFIG_FILE, DEFAULT_CONFIG))
_state = _load_json(STATE_FILE, {"success": 0, "fail": 0})


def get_config() -> Dict[str, Any]:
    with _cfg_lock:
        return dict(_config)


def set_config(data: Dict[str, Any]) -> Dict[str, Any]:
    global _config
    with _cfg_lock:
        merged = dict(_config)
        for key, value in (data or {}).items():
            if key in {"account_sync_api_key", "easyproxies_password", "resin_admin_token", "resin_proxy_token"}:
                if str(value or "").strip():
                    merged[key] = str(value).strip()
                continue
            merged[key] = value
        _config = _normalize_config(merged)
        _save_json(CONFIG_FILE, _config)
        return dict(_config)


def _save_state_counts(success: int, fail: int) -> None:
    _save_json(STATE_FILE, {"success": int(success), "fail": int(fail)})


def _list_account_files() -> List[Path]:
    files: List[Path] = []
    if not OUTPUT_DIR.exists():
        return files
    for fp in OUTPUT_DIR.glob("*.json"):
        if fp.name == ALL_ACCOUNT_FILE.name:
            continue
        files.append(fp)
    files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return files


def _load_json_obj(path: Path) -> Optional[Dict[str, Any]]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(raw, dict):
            return raw
    except Exception:
        return None
    return None


def merge_all_accounts() -> int:
    rows: List[Dict[str, Any]] = []
    for fp in sorted(_list_account_files(), key=lambda p: p.name):
        row = _load_json_obj(fp)
        if row:
            rows.append(row)
    ALL_ACCOUNT_FILE.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")
    return len(rows)


def _parse_expire_dt(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            dt = datetime.strptime(text, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except Exception:
            pass
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def build_pool_status(min_accounts: Optional[int] = None) -> Dict[str, Any]:
    cfg = get_config()
    threshold = int(min_accounts or cfg.get("min_accounts", 20))
    files = _list_account_files()
    total_files = len(files)
    now_utc = datetime.now(timezone.utc)
    valid_count = 0
    expired_count = 0
    unknown_count = 0
    sample_expired: List[str] = []

    for fp in files:
        row = _load_json_obj(fp) or {}
        exp_dt = _parse_expire_dt(row.get("expires_at"))
        if exp_dt is None:
            unknown_count += 1
            continue
        if exp_dt > now_utc:
            valid_count += 1
        else:
            expired_count += 1
            if len(sample_expired) < 5:
                sample_expired.append(str(row.get("id") or fp.stem))

    candidate_count = valid_count + unknown_count
    gap = max(0, threshold - candidate_count)
    return {
        "total_files": total_files,
        "valid_count": valid_count,
        "unknown_count": unknown_count,
        "expired_count": expired_count,
        "threshold": threshold,
        "gap": gap,
        "healthy": gap == 0,
        "expired_samples": sample_expired,
        "all_account_exists": ALL_ACCOUNT_FILE.exists(),
    }


def _effective_pool_threshold(cfg: Dict[str, Any]) -> int:
    base = max(1, int(cfg.get("min_accounts") or 20))
    if bool(cfg.get("guarantee_enabled", True)):
        try:
            base = max(base, int(cfg.get("guarantee_target_accounts") or 200))
        except Exception:
            base = max(base, 200)
    return base


class RuntimeManager:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.register_status = "idle"   # idle|running|stopping
        self.maintain_status = "idle"   # idle|running|stopping
        self.current_proxy = ""
        self.current_proxy_strategy = ""
        self.current_proxy_upstream = ""
        self.current_proxy_region = ""
        self.current_proxy_updated_at = 0.0
        self.success_count = int(_state.get("success", 0))
        self.fail_count = int(_state.get("fail", 0))
        self.last_register_started_at = 0.0
        self.last_maintain_started_at = 0.0
        self.last_auto_register_at = 0.0
        self.next_auto_register_at = 0.0
        self.next_auto_maintain_at = 0.0
        self._register_proc: Optional[subprocess.Popen[str]] = None
        self._maintain_proc: Optional[subprocess.Popen[str]] = None
        self._register_target = 0
        self._register_watchdog_restarts = 0
        self._maintain_watchdog_restarts = 0
        self.sync_status = "idle"  # idle|running
        self.last_sync_at = 0.0
        self.last_sync_ok: Optional[bool] = None
        self.last_sync_reason = ""
        self.last_sync_count = 0
        self.last_sync_skipped = 0
        self.last_sync_error = ""
        self._subscription_cache: List[str] = []
        self._subscription_cache_url = ""
        self._subscription_cache_at = 0.0
        self._subscription_cursor = 0
        self._easyproxies_last_sub_sync_at = 0.0
        self._easyproxies_last_refresh_at = 0.0
        self._easyproxies_last_sync_url = ""
        self._easyproxies_rotation_cursor = 0
        self._easyproxies_rotation_node = ""
        self._easyproxies_rotation_register_used = 0
        self._easyproxies_rotation_maintain_used = 0
        self._easyproxies_last_switched_node = ""
        self._easyproxies_last_switch_at = 0.0
        self._resin_rotation_register_cursor = 0
        self._resin_rotation_register_account = ""
        self._resin_rotation_register_used = 0
        self._resin_rotation_maintain_cursor = 0
        self._resin_rotation_maintain_account = ""
        self._resin_rotation_maintain_used = 0
        self._resin_last_switched_account = ""
        self._resin_last_switch_at = 0.0
        self._resin_platform_cache: Dict[str, Dict[str, Any]] = {}
        self._resin_platform_cache_at = 0.0
        self._item_sync_queue: queue.Queue = queue.Queue(maxsize=2000)
        self._item_sync_thread = threading.Thread(target=self._item_sync_worker, daemon=True)
        self._item_sync_thread.start()
        self._sse_queues: List[asyncio.Queue] = []
        self._sse_lock = threading.Lock()
        self._auto_thread: Optional[threading.Thread] = None
        self._auto_stop = threading.Event()

    def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=300)
        with self._sse_lock:
            self._sse_queues.append(q)
        return q

    def unsubscribe(self, q: asyncio.Queue) -> None:
        with self._sse_lock:
            try:
                self._sse_queues.remove(q)
            except ValueError:
                pass

    @staticmethod
    def _repair_possible_mojibake(text: str) -> str:
        line = str(text or "")
        if not line:
            return ""
        hints = ("闁", "閻", "缂", "锟", "?", "鈹")
        if not any(h in line for h in hints):
            return line
        for enc in ("gbk", "cp936", "gb18030"):
            try:
                repaired = line.encode(enc).decode("utf-8")
                if repaired and repaired != line:
                    return repaired
            except Exception:
                continue
        return line

    def _broadcast(self, level: str, message: str, step: str = "") -> None:
        text = self._repair_possible_mojibake(message)
        event = {
            "ts": _now_str(),
            "level": level,
            "message": text,
            "step": step,
        }
        with self._sse_lock:
            for q in list(self._sse_queues):
                try:
                    q.put_nowait(event)
                except asyncio.QueueFull:
                    pass
        print(f"[{event['ts']}] [{level.upper()}] {text}")

    def info(self, message: str, step: str = "") -> None:
        self._broadcast("info", message, step=step)

    def warn(self, message: str, step: str = "") -> None:
        self._broadcast("warn", message, step=step)

    def error(self, message: str, step: str = "") -> None:
        self._broadcast("error", message, step=step)

    def success(self, message: str, step: str = "") -> None:
        self._broadcast("success", message, step=step)

    def _item_sync_worker(self) -> None:
        while True:
            item = self._item_sync_queue.get()
            if item is None:
                continue
            if not isinstance(item, dict):
                continue
            reason = str(item.get("reason") or "item-sync").strip() or "item-sync"
            email = str(item.get("email") or "").strip()
            kind = str(item.get("kind") or "").strip() or "task"
            try:
                merged = merge_all_accounts()
                self.info(
                    f"Incremental sync triggered: kind={kind}, email={email or '-'}, merged={merged}",
                    step="sync",
                )
                self._sync_accounts_to_server(reason=reason, force=False)
            except Exception as exc:
                self.warn(
                    f"Incremental sync failed: kind={kind}, email={email or '-'}, err={self._repair_possible_mojibake(str(exc))}",
                    step="sync",
                )

    def _enqueue_item_sync(self, kind: str, email: str, auto_triggered: bool) -> None:
        cfg = get_config()
        if not bool(cfg.get("account_sync_enabled", False)):
            return
        task_kind = str(kind or "").strip() or "task"
        account = str(email or "").strip()
        reason = f"item-{'auto' if auto_triggered else 'manual'}-{task_kind}-{int(time.time())}"
        payload = {"reason": reason, "email": account, "kind": task_kind}
        try:
            self._item_sync_queue.put_nowait(payload)
        except queue.Full:
            self.warn("Incremental sync queue is full, dropping one event", step="sync")

    def _sync_accounts_to_server(self, reason: str, force: bool = False) -> Dict[str, Any]:
        cfg = get_config()
        enabled = bool(cfg.get("account_sync_enabled", False))
        if not enabled and not force:
            return {"ok": False, "skipped": True, "reason": "disabled", "count": 0, "skipped_count": 0}

        sync_url = str(cfg.get("account_sync_url") or "").strip()
        if not sync_url:
            raise RuntimeError("Sync target URL is not configured (account_sync_url)")
        sync_url = _normalize_sync_target_url(sync_url)

        rows = _load_all_account_rows()
        payload, skipped_count = _build_accounts_config_payload(rows)
        account_count = len(payload)
        if account_count <= 0:
            raise RuntimeError("No valid accounts to sync (missing secure_c_ses/csesidx/config_id)")

        timeout = max(5, min(int(cfg.get("account_sync_timeout_seconds") or 20), 120))
        auth_mode = str(cfg.get("account_sync_auth_mode") or "session").strip().lower()
        api_key = str(cfg.get("account_sync_api_key") or "").strip()
        headers: Dict[str, str] = {"Content-Type": "application/json"}
        request_url = sync_url

        with self._lock:
            self.sync_status = "running"
        self.info(
            f"Start syncing accounts to server: reason={reason}, count={account_count}, skipped={skipped_count}, mode={auth_mode}",
            step="sync",
        )

        response_status: Optional[int] = None
        try:
            if auth_mode == "session":
                if not api_key:
                    raise RuntimeError("session auth mode requires account_sync_api_key (ADMIN_KEY)")
                login_url = str(cfg.get("account_sync_login_url") or "").strip() or _derive_login_url_from_sync_url(sync_url)
                if not login_url:
                    raise RuntimeError("Cannot derive login URL from sync URL, please set account_sync_login_url")

                with requests.Session() as sess:
                    login_resp = sess.post(login_url, data={"admin_key": api_key}, timeout=timeout)
                    if login_resp.status_code >= 400:
                        raise RuntimeError(f"Login to server failed: HTTP {login_resp.status_code} {login_resp.text[:200]}")
                    resp = sess.put(request_url, json=payload, timeout=timeout)
                    response_status = resp.status_code
                    if response_status >= 400:
                        raise RuntimeError(f"Upload failed: HTTP {response_status} {resp.text[:300]}")
                    resp_text = resp.text[:200]
            else:
                if auth_mode == "bearer":
                    if not api_key:
                        raise RuntimeError("bearer auth mode requires account_sync_api_key")
                    headers["Authorization"] = f"Bearer {api_key}"
                elif auth_mode == "header":
                    if not api_key:
                        raise RuntimeError("header auth mode requires account_sync_api_key")
                    header_name = str(cfg.get("account_sync_auth_header_name") or "X-API-Key").strip() or "X-API-Key"
                    headers[header_name] = api_key
                elif auth_mode == "query":
                    if not api_key:
                        raise RuntimeError("query auth mode requires account_sync_api_key")
                    query_name = str(cfg.get("account_sync_auth_query_name") or "api_key").strip() or "api_key"
                    request_url = _append_query_param(request_url, query_name, api_key)

                resp = requests.put(request_url, headers=headers, json=payload, timeout=timeout)
                response_status = resp.status_code
                if response_status >= 400:
                    raise RuntimeError(f"Upload failed: HTTP {response_status} {resp.text[:300]}")
                resp_text = resp.text[:200]

            with self._lock:
                self.sync_status = "idle"
                self.last_sync_at = time.time()
                self.last_sync_ok = True
                self.last_sync_reason = reason
                self.last_sync_count = account_count
                self.last_sync_skipped = skipped_count
                self.last_sync_error = ""
            self.success(
                f"Account sync success: HTTP {response_status}, count={account_count}, skipped={skipped_count}",
                step="sync",
            )
            return {
                "ok": True,
                "status_code": response_status,
                "count": account_count,
                "skipped_count": skipped_count,
                "reason": reason,
                "response_preview": resp_text,
            }
        except Exception as exc:
            err = self._repair_possible_mojibake(str(exc))
            with self._lock:
                self.sync_status = "idle"
                self.last_sync_at = time.time()
                self.last_sync_ok = False
                self.last_sync_reason = reason
                self.last_sync_count = account_count
                self.last_sync_skipped = skipped_count
                self.last_sync_error = err
            self.error(f"Account sync failed: {err}", step="sync")
            raise

    def sync_accounts_now(self, reason: str, force: bool = False, ensure_merged: bool = True) -> Dict[str, Any]:
        if ensure_merged:
            merged = merge_all_accounts()
            self.info(f"Synced pre-merge all_account.json: {merged} rows", step="sync")
        return self._sync_accounts_to_server(reason=reason, force=force)

    def _maybe_auto_sync(self, kind: str, return_code: int, auto_triggered: bool = False) -> None:
        if return_code != 0:
            return
        cfg = get_config()
        if not bool(cfg.get("account_sync_enabled", False)):
            return

        reason = f"{'auto' if auto_triggered else 'manual'}-{kind}"
        try:
            self._sync_accounts_to_server(reason=reason, force=False)
        except Exception:
            pass

    def sync_easyproxies_subscription_now(self, force: bool = True) -> Dict[str, Any]:
        cfg = get_config()
        return self._sync_easyproxies_subscription(cfg, force=force)

    @staticmethod
    def _easyproxies_nodes_summary(nodes_payload: Any) -> tuple[List[Dict[str, Any]], int, int]:
        nodes: List[Dict[str, Any]] = []
        if isinstance(nodes_payload, dict):
            raw_nodes = nodes_payload.get("nodes")
            if isinstance(raw_nodes, list):
                for node in raw_nodes:
                    if isinstance(node, dict):
                        nodes.append(node)
            total = int(nodes_payload.get("total_nodes") or len(nodes))
        else:
            total = 0

        healthy = 0
        for node in nodes:
            if bool(node.get("initial_check_done")) and bool(node.get("available")) and not bool(node.get("blacklisted")):
                healthy += 1
        return nodes, total, healthy

    @staticmethod
    def _easyproxies_is_hk_cn_node(node: Dict[str, Any]) -> bool:
        region = str(node.get("region") or "").strip().upper()
        country_raw = str(node.get("country") or "").strip()
        country = country_raw.upper()
        name = str(node.get("name") or node.get("tag") or "").strip()
        name_upper = name.upper()

        if region in {"HK", "CN", "HKG", "CHN"}:
            return True
        if "HONG KONG" in country or "HONGKONG" in country or "CHINA" in country:
            return True
        if any(token in country_raw for token in ("香港", "中国", "中國")):
            return True
        if re.search(r"(^|[^A-Z0-9])(HK|CN)([^A-Z0-9]|$)", name_upper):
            return True
        if "HONG KONG" in name_upper or "HONGKONG" in name_upper or "CHINA" in name_upper:
            return True
        if any(token in name for token in ("香港", "中国", "中國")):
            return True
        return False

    def _easyproxies_collect_hk_cn_names(self, nodes: List[Dict[str, Any]]) -> List[str]:
        names: List[str] = []
        seen: Set[str] = set()
        for node in nodes:
            if not isinstance(node, dict):
                continue
            if not self._easyproxies_is_hk_cn_node(node):
                continue
            name = str(node.get("name") or node.get("tag") or "").strip()
            if not name or name in seen:
                continue
            names.append(name)
            seen.add(name)
        return names

    def _easyproxies_auto_exclude_hk_cn_nodes(
        self,
        cfg: Dict[str, Any],
        nodes: Optional[List[Dict[str, Any]]] = None,
        reason: str = "",
    ) -> Dict[str, Any]:
        node_list: List[Dict[str, Any]] = [n for n in (nodes or []) if isinstance(n, dict)]
        if not node_list:
            try:
                payload = self._easyproxies_request(cfg, "GET", "/api/nodes", timeout=12)
                node_list, _, _ = self._easyproxies_nodes_summary(payload)
            except Exception as exc:
                err = self._repair_possible_mojibake(str(exc))
                self.warn(f"EasyProxies auto-exclude HK/CN failed ({reason or 'unknown'}): {err}", step="proxy")
                return {"ok": False, "changed": 0, "matched": 0, "skipped": 0, "error": err, "reason": reason}

        matched = self._easyproxies_collect_hk_cn_names(node_list)
        if not matched:
            return {"ok": True, "changed": 0, "matched": 0, "skipped": 0, "reason": reason, "names": []}

        pending = list(matched)

        preview = ", ".join(pending[:4])
        if len(pending) > 4:
            preview += ", ..."
        self.warn(
            f"EasyProxies soft-skipped HK/CN nodes (no disable): count={len(pending)} "
            f"reason={reason or 'unknown'} names={preview}",
            step="proxy",
        )
        return {
            "ok": True,
            "changed": 0,
            "matched": len(matched),
            "skipped": len(matched),
            "reason": reason,
            "names": pending,
        }

    def _easyproxies_list_config_nodes(self, cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
        payload = self._easyproxies_request(cfg, "GET", "/api/nodes/config", timeout=12)
        nodes: List[Dict[str, Any]] = []
        if isinstance(payload, dict):
            raw_nodes = payload.get("nodes")
            if isinstance(raw_nodes, list):
                for node in raw_nodes:
                    if isinstance(node, dict):
                        nodes.append(node)
        return nodes

    def _easyproxies_collect_rotation_candidates(self, config_nodes: List[Dict[str, Any]]) -> List[str]:
        names: List[str] = []
        seen: Set[str] = set()
        for node in config_nodes:
            name = str(node.get("name") or node.get("tag") or "").strip()
            if not name or name in seen:
                continue
            if self._easyproxies_is_hk_cn_node(node):
                continue
            names.append(name)
            seen.add(name)
        return names

    def _easyproxies_collect_healthy_runtime_names(self, cfg: Dict[str, Any]) -> Set[str]:
        names: Set[str] = set()
        try:
            payload = self._easyproxies_request(cfg, "GET", "/api/nodes", timeout=12)
        except Exception:
            return names
        nodes, _, _ = self._easyproxies_nodes_summary(payload)
        for node in nodes:
            if not isinstance(node, dict):
                continue
            if self._easyproxies_is_hk_cn_node(node):
                continue
            if not bool(node.get("initial_check_done")):
                continue
            if not bool(node.get("available")):
                continue
            if bool(node.get("blacklisted")):
                continue
            name = str(node.get("name") or node.get("tag") or "").strip()
            if name:
                names.add(name)
        return names

    @staticmethod
    def _easyproxies_compose_proxy_url(
        scheme: str,
        host: str,
        port: int,
        username: str = "",
        password: str = "",
    ) -> str:
        use_scheme = str(scheme or "http").strip().lower() or "http"
        use_host = str(host or "").strip()
        use_port = max(1, int(port or 0))
        user = str(username or "").strip()
        pwd = str(password or "")
        auth = ""
        if user:
            auth = quote(user, safe="")
            if pwd:
                auth += f":{quote(pwd, safe='')}"
            auth += "@"
        return f"{use_scheme}://{auth}{use_host}:{use_port}"

    @staticmethod
    def _easyproxies_find_node(nodes: List[Dict[str, Any]], node_name: str) -> Optional[Dict[str, Any]]:
        target = str(node_name or "").strip()
        if not target:
            return None
        target_l = target.lower()
        for node in nodes:
            if not isinstance(node, dict):
                continue
            name = str(node.get("name") or node.get("tag") or "").strip()
            if not name:
                continue
            if name == target or name.lower() == target_l:
                return node
        for node in nodes:
            if not isinstance(node, dict):
                continue
            tag = str(node.get("tag") or "").strip()
            if tag and (tag == target or tag.lower() == target_l):
                return node
        return None

    def _resolve_easyproxies_proxy_forced_node(
        self,
        cfg: Dict[str, Any],
        node_name: str,
        kind: str,
    ) -> tuple[Dict[str, Any], str]:
        use_node = str(node_name or "").strip()
        if not use_node:
            raise RuntimeError("EasyProxies fixed node is empty")

        if bool(cfg.get("easyproxies_refresh_before_task", True)):
            self._sync_easyproxies_subscription(cfg, force=False)

        listen_proxy = _normalize_proxy_endpoint(
            cfg.get("easyproxies_listen_proxy") or "",
            default="http://127.0.0.1:2323",
        )
        if not listen_proxy:
            raise RuntimeError("EasyProxies listen proxy is invalid")
        listen_parsed = urlparse(listen_proxy)
        listen_scheme = str(listen_parsed.scheme or "http").strip().lower() or "http"
        listen_host = str(listen_parsed.hostname or "").strip()
        listen_user = str(listen_parsed.username or "").strip()
        listen_pass = str(listen_parsed.password or "").strip()

        nodes_payload = self._easyproxies_request(cfg, "GET", "/api/nodes", timeout=12)
        nodes, total, healthy = self._easyproxies_nodes_summary(nodes_payload)
        if total <= 0:
            raise RuntimeError("EasyProxies has no available nodes")
        node = self._easyproxies_find_node(nodes, use_node)
        if not node:
            preview = ", ".join(str((n.get("name") or n.get("tag") or "")).strip() for n in nodes[:6] if isinstance(n, dict))
            if len(nodes) > 6:
                preview += ", ..."
            raise RuntimeError(f"EasyProxies fixed node not found: {use_node} (candidates={preview or '-'})")

        selected_name = str(node.get("name") or node.get("tag") or use_node).strip() or use_node
        selected_tag = str(node.get("tag") or "").strip()
        selected_port = int(node.get("port") or 0)
        selected_listen_addr = str(node.get("listen_address") or "").strip()
        selected_region = str(node.get("region") or "").strip().upper()
        selected_country = str(node.get("country") or "").strip()

        if selected_port <= 0:
            raise RuntimeError(
                f"EasyProxies fixed node has no dedicated port: {selected_name} "
                "(need EasyProxies mode=multi-port/hybrid)"
            )

        settings_mode = ""
        mp_addr = ""
        mp_user = ""
        mp_pass = ""
        listener_user = ""
        listener_pass = ""
        try:
            settings = self._easyproxies_request(cfg, "GET", "/api/settings", timeout=12)
            if isinstance(settings, dict):
                settings_mode = str(settings.get("mode") or "").strip().lower()
                mp_addr = str(settings.get("multi_port_address") or "").strip()
                mp_user = str(settings.get("multi_port_username") or "").strip()
                mp_pass = str(settings.get("multi_port_password") or "").strip()
                listener_user = str(settings.get("listener_username") or "").strip()
                listener_pass = str(settings.get("listener_password") or "").strip()
        except Exception as exc:
            self.warn(f"EasyProxies fixed node settings fetch failed: {self._repair_possible_mojibake(str(exc))}", step="proxy")

        cfg_user = ""
        cfg_pass = ""
        try:
            cfg_nodes = self._easyproxies_list_config_nodes(cfg)
            cfg_node = self._easyproxies_find_node(cfg_nodes, selected_name) or self._easyproxies_find_node(cfg_nodes, selected_tag)
            if isinstance(cfg_node, dict):
                cfg_user = str(cfg_node.get("username") or "").strip()
                cfg_pass = str(cfg_node.get("password") or "").strip()
        except Exception:
            pass

        node_host = selected_listen_addr or mp_addr or listen_host
        if node_host in {"0.0.0.0", "::", "[::]"}:
            node_host = listen_host
        if not node_host:
            api_host = str(urlparse(_normalize_http_url(cfg.get("easyproxies_api_url") or "")).hostname or "").strip()
            node_host = api_host or "127.0.0.1"

        if settings_mode and settings_mode not in {"multi-port", "hybrid"}:
            self.warn(
                f"EasyProxies fixed node is configured while mode={settings_mode}; "
                "node dedicated port may not be reachable",
                step="proxy",
            )

        node_user = cfg_user or mp_user or listener_user or listen_user
        node_pass = cfg_pass or mp_pass or listener_pass or listen_pass
        fixed_proxy = self._easyproxies_compose_proxy_url(
            scheme=listen_scheme,
            host=node_host,
            port=selected_port,
            username=node_user,
            password=node_pass,
        )

        precheck_retries = max(1, min(int(cfg.get("easyproxies_retry_times") or 3), 8))
        precheck_loc = ""
        precheck_ip = ""
        last_precheck_error = ""
        for attempt in range(1, precheck_retries + 1):
            try:
                trace_text = trace_via_proxy(fixed_proxy, timeout=8)
                supported, precheck_loc, precheck_ip = is_location_supported(trace_text)
                if supported:
                    self.info(
                        f"EasyProxies fixed node precheck passed: node={selected_name}, attempt={attempt}/{precheck_retries}, "
                        f"loc={precheck_loc or '?'} ip={precheck_ip or '?'}",
                        step="proxy",
                    )
                    break
                last_precheck_error = f"location restricted: loc={precheck_loc or '?'}"
            except Exception as exc:
                last_precheck_error = self._repair_possible_mojibake(str(exc))
            if attempt < precheck_retries:
                time.sleep(0.4)
        else:
            raise RuntimeError(
                f"EasyProxies fixed node precheck failed: node={selected_name}, "
                f"err={last_precheck_error or 'unknown error'}"
            )

        self.info(
            "Proxy strategy selected: easyproxies | "
            f"browser={_mask_proxy_for_log(fixed_proxy)} | "
            f"upstream=easyproxies-node:{selected_name} | "
            f"region={precheck_loc or selected_region or '?'}",
            step="proxy",
        )
        self.info(
            f"EasyProxies fixed node selected: node={selected_name}, tag={selected_tag or '-'}, "
            f"port={selected_port}, healthy={healthy}/{total}, country={selected_country or '-'}",
            step="proxy",
        )
        return (
            {
                "strategy": "easyproxies",
                "proxy": fixed_proxy,
                "upstream_proxy": f"easyproxies-node:{selected_name}",
                "region": precheck_loc or selected_region or "",
                "node_name": selected_name,
                "fixed_node": selected_name,
            },
            selected_name,
        )

    def _easyproxies_activate_single_node(
        self,
        cfg: Dict[str, Any],
        node_name: str,
        config_nodes: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        use_nodes = config_nodes if isinstance(config_nodes, list) else self._easyproxies_list_config_nodes(cfg)
        all_names: List[str] = []
        for node in use_nodes:
            if not isinstance(node, dict):
                continue
            name = str(node.get("name") or node.get("tag") or "").strip()
            if name:
                all_names.append(name)
        if node_name not in all_names:
            raise RuntimeError(f"EasyProxies node not found in config: {node_name}")

        # Soft select only: keep EasyProxies config immutable during rotation.
        _ = cfg

        with self._lock:
            self._easyproxies_last_switched_node = node_name
            self._easyproxies_last_switch_at = time.time()

    def _resolve_easyproxies_proxy_with_rotation(
        self,
        cfg: Dict[str, Any],
        kind: str,
        requested_units: int,
    ) -> tuple[Dict[str, Any], int, str]:
        if not bool(cfg.get("easyproxies_node_rotation_enabled", True)):
            proxy_info = self._resolve_easyproxies_proxy(cfg)
            return proxy_info, max(1, int(requested_units or 1)), ""

        config_nodes = self._easyproxies_list_config_nodes(cfg)
        if not config_nodes:
            raise RuntimeError("EasyProxies config nodes list is empty")

        hk_cn_names = self._easyproxies_collect_hk_cn_names(config_nodes)
        if hk_cn_names:
            self.warn(f"EasyProxies rotation soft-skipping HK/CN config nodes: {len(hk_cn_names)}", step="proxy")

        candidates = self._easyproxies_collect_rotation_candidates(config_nodes)
        if not candidates:
            raise RuntimeError("EasyProxies has no non-HK/CN nodes for rotation")
        healthy_runtime_names = self._easyproxies_collect_healthy_runtime_names(cfg)
        if healthy_runtime_names:
            prioritized = [name for name in candidates if name in healthy_runtime_names]
            if prioritized:
                candidates = prioritized
            else:
                self.warn("EasyProxies rotation: no healthy runtime node matched config candidates, fallback all", step="proxy")

        task_kind = "maintain" if str(kind or "").strip().lower() == "maintain" else "register"
        try:
            req_units = max(1, int(requested_units or 1))
        except Exception:
            req_units = 1
        register_quota = max(1, int(cfg.get("easyproxies_node_register_quota") or 5))
        maintain_quota = max(1, int(cfg.get("easyproxies_node_maintain_quota") or 20))

        with self._lock:
            start_idx = self._easyproxies_rotation_cursor % max(1, len(candidates))
            current_node = self._easyproxies_rotation_node
            current_reg_used = self._easyproxies_rotation_register_used
            current_maint_used = self._easyproxies_rotation_maintain_used

        last_error = ""
        for offset in range(len(candidates)):
            idx = (start_idx + offset) % len(candidates)
            node_name = candidates[idx]
            if node_name == current_node:
                reg_used = max(0, int(current_reg_used))
                maintain_used = max(0, int(current_maint_used))
            else:
                reg_used = 0
                maintain_used = 0

            if task_kind == "register":
                remaining = max(0, register_quota - reg_used)
            else:
                remaining = max(0, maintain_quota - maintain_used)
            if remaining <= 0:
                continue

            units = min(req_units, remaining)
            try:
                self._easyproxies_activate_single_node(cfg, node_name, config_nodes=config_nodes)
                cfg_no_refresh = dict(cfg)
                cfg_no_refresh["easyproxies_refresh_before_task"] = False
                proxy_info = self._resolve_easyproxies_proxy(cfg_no_refresh)
            except Exception as exc:
                last_error = self._repair_possible_mojibake(str(exc))
                self.warn(f"EasyProxies rotate candidate failed: node={node_name}, err={last_error}", step="proxy")
                continue

            with self._lock:
                if self._easyproxies_rotation_node != node_name:
                    self._easyproxies_rotation_node = node_name
                    self._easyproxies_rotation_register_used = 0
                    self._easyproxies_rotation_maintain_used = 0
                if task_kind == "register":
                    self._easyproxies_rotation_register_used += units
                    consumed = self._easyproxies_rotation_register_used >= register_quota
                else:
                    self._easyproxies_rotation_maintain_used += units
                    consumed = self._easyproxies_rotation_maintain_used >= maintain_quota
                self._easyproxies_rotation_cursor = (idx + 1) % len(candidates) if consumed else idx

            self.info(
                f"EasyProxies node selected: node={node_name}, kind={task_kind}, units={units}, "
                f"quota={register_quota if task_kind == 'register' else maintain_quota}",
                step="proxy",
            )
            proxy_info["node_name"] = node_name
            proxy_info["node_task_kind"] = task_kind
            proxy_info["node_units"] = units
            return proxy_info, units, node_name

        raise RuntimeError(f"EasyProxies rotate failed: no healthy node available ({last_error or 'unknown error'})")

    def _easyproxies_force_switch_next_node(self, reason: str = "") -> None:
        with self._lock:
            self._easyproxies_rotation_cursor += 1
            old_node = self._easyproxies_rotation_node
            self._easyproxies_rotation_node = ""
            self._easyproxies_rotation_register_used = 0
            self._easyproxies_rotation_maintain_used = 0
        self.warn(
            f"EasyProxies force switch to next node ({reason or 'runtime-error'}), prev={old_node or '-'}",
            step="proxy",
        )

    def _easyproxies_reset_rotation_state(self, reason: str = "") -> None:
        with self._lock:
            self._easyproxies_rotation_cursor = 0
            self._easyproxies_rotation_node = ""
            self._easyproxies_rotation_register_used = 0
            self._easyproxies_rotation_maintain_used = 0
        self.info(f"EasyProxies rotation state reset ({reason or 'manual'})", step="proxy")

    def _resin_api_request(
        self,
        cfg: Dict[str, Any],
        method: str,
        path: str,
        timeout: int = 12,
        json_body: Optional[Dict[str, Any]] = None,
        params: Optional[Dict[str, Any]] = None,
    ) -> Any:
        api_base = _normalize_http_url(cfg.get("resin_api_url") or "").rstrip("/")
        if not api_base:
            raise RuntimeError("Resin API URL is empty")
        admin_token = str(cfg.get("resin_admin_token") or "").strip()
        if not admin_token:
            raise RuntimeError("Resin admin token is empty")
        url_path = "/" + str(path or "").lstrip("/")
        headers = {
            "Authorization": f"Bearer {admin_token}",
            "Accept": "application/json",
        }
        try:
            resp = requests.request(
                method=str(method or "GET").upper(),
                url=f"{api_base}{url_path}",
                headers=headers,
                params=params,
                json=json_body,
                timeout=timeout,
            )
        except Exception as exc:
            raise RuntimeError(f"Resin request failed ({method} {url_path}): {exc}") from None

        if resp.status_code >= 400:
            err_msg = resp.text[:180]
            try:
                payload = resp.json() if resp.content else {}
                if isinstance(payload, dict):
                    err_obj = payload.get("error")
                    if isinstance(err_obj, dict):
                        err_msg = str(err_obj.get("message") or err_msg).strip() or err_msg
            except Exception:
                pass
            raise RuntimeError(f"HTTP {resp.status_code}: {err_msg}")
        if not resp.content:
            return {}
        try:
            return resp.json()
        except Exception:
            return {"raw": resp.text}

    def _resin_find_platform_by_name(self, cfg: Dict[str, Any], platform_name: str) -> Optional[Dict[str, Any]]:
        expected = str(platform_name or "").strip().lower()
        if not expected:
            return None
        cache_ttl = 30.0
        with self._lock:
            cached = dict(self._resin_platform_cache.get(expected) or {})
            cached_age = time.time() - float(self._resin_platform_cache_at or 0.0)
        if cached and cached_age <= cache_ttl:
            return cached

        offset = 0
        limit = 200
        matched: Optional[Dict[str, Any]] = None
        while offset < 10000:
            payload = self._resin_api_request(
                cfg,
                "GET",
                "/api/v1/platforms",
                timeout=12,
                params={"sort_by": "name", "sort_order": "asc", "limit": limit, "offset": offset},
            )
            items = payload.get("items") if isinstance(payload, dict) and isinstance(payload.get("items"), list) else []
            for item in items:
                if not isinstance(item, dict):
                    continue
                if str(item.get("name") or "").strip().lower() == expected:
                    matched = item
                    break
            if matched is not None:
                break
            total = int(payload.get("total") or len(items)) if isinstance(payload, dict) else len(items)
            offset += limit
            if not items or offset >= max(total, len(items)):
                break

        if matched:
            with self._lock:
                self._resin_platform_cache[expected] = dict(matched)
                self._resin_platform_cache_at = time.time()
        return matched

    def _resin_ensure_platform(self, cfg: Dict[str, Any], platform_name: str) -> Dict[str, Any]:
        target_name = str(platform_name or "").strip()
        if not target_name:
            raise RuntimeError("Resin platform name is empty")
        found = self._resin_find_platform_by_name(cfg, target_name)
        if isinstance(found, dict):
            return found
        try:
            created = self._resin_api_request(
                cfg,
                "POST",
                "/api/v1/platforms",
                timeout=15,
                json_body={"name": target_name},
            )
            if isinstance(created, dict):
                with self._lock:
                    self._resin_platform_cache[target_name.lower()] = dict(created)
                    self._resin_platform_cache_at = time.time()
                return created
        except Exception as exc:
            # Handle concurrent create from another worker.
            if "409" not in str(exc):
                raise

        found = self._resin_find_platform_by_name(cfg, target_name)
        if isinstance(found, dict):
            return found
        raise RuntimeError(f"Resin platform not found and create failed: {target_name}")

    @staticmethod
    def _resin_normalize_account_name(raw: str, fallback: str) -> str:
        account = re.sub(r"[^0-9A-Za-z._-]+", "-", str(raw or "").strip())
        account = account.strip(".-")
        if not account:
            account = str(fallback or "").strip()
        if not account:
            return ""
        if len(account) > 80:
            account = account[:80]
        return account

    @staticmethod
    def _resin_rotation_account_name(kind: str, idx: int) -> str:
        prefix = "reg" if str(kind or "").strip().lower() == "register" else "maint"
        return f"{prefix}-{int(idx):05d}"

    def _resin_compose_proxy_url(self, cfg: Dict[str, Any], platform_name: str, account_name: str) -> str:
        proxy_base = _normalize_proxy_endpoint(
            cfg.get("resin_proxy_url") or "",
            default="http://127.0.0.1:2260",
        )
        if not proxy_base:
            raise RuntimeError("Resin proxy URL is empty")
        parsed = urlparse(proxy_base)
        scheme = str(parsed.scheme or "http").strip().lower() or "http"
        host = str(parsed.hostname or "").strip()
        if not host:
            raise RuntimeError("Resin proxy URL is invalid")
        if ":" in host and not host.startswith("["):
            host = f"[{host}]"
        hostport = host
        if parsed.port:
            hostport = f"{host}:{parsed.port}"

        platform_text = str(platform_name or "").strip()
        account_text = self._resin_normalize_account_name(account_name, "default")
        identity = f"{platform_text}.{account_text}" if platform_text else account_text
        username = quote(identity, safe="._-")
        token = str(cfg.get("resin_proxy_token") or "").strip()
        if token:
            auth = f"{username}:{quote(token, safe='')}@"
        else:
            auth = f"{username}@"
        return normalize_proxy_value(f"{scheme}://{auth}{hostport}")

    def _resin_select_rotation_account(
        self,
        cfg: Dict[str, Any],
        kind: str,
        requested_units: int,
    ) -> tuple[str, int]:
        task_kind = "maintain" if str(kind or "").strip().lower() == "maintain" else "register"
        try:
            req_units = max(1, int(requested_units or 1))
        except Exception:
            req_units = 1

        if task_kind == "register":
            quota = max(1, int(cfg.get("resin_node_register_quota") or 5))
        else:
            quota = max(1, int(cfg.get("resin_node_maintain_quota") or 20))
        pool_size = max(1, int(cfg.get("resin_rotation_pool_size") or 2048))

        with self._lock:
            if task_kind == "register":
                start_idx = self._resin_rotation_register_cursor % pool_size
                current_account = self._resin_rotation_register_account
                current_used = max(0, int(self._resin_rotation_register_used))
            else:
                start_idx = self._resin_rotation_maintain_cursor % pool_size
                current_account = self._resin_rotation_maintain_account
                current_used = max(0, int(self._resin_rotation_maintain_used))

        for offset in range(pool_size):
            idx = (start_idx + offset) % pool_size
            account = self._resin_rotation_account_name(task_kind, idx)
            used = current_used if account == current_account else 0
            remaining = max(0, quota - used)
            if remaining <= 0:
                continue
            units = min(req_units, remaining)
            with self._lock:
                switched = False
                if task_kind == "register":
                    if self._resin_rotation_register_account != account:
                        switched = True
                    self._resin_rotation_register_account = account
                    self._resin_rotation_register_used = used + units
                    consumed = self._resin_rotation_register_used >= quota
                    self._resin_rotation_register_cursor = (idx + 1) % pool_size if consumed else idx
                else:
                    if self._resin_rotation_maintain_account != account:
                        switched = True
                    self._resin_rotation_maintain_account = account
                    self._resin_rotation_maintain_used = used + units
                    consumed = self._resin_rotation_maintain_used >= quota
                    self._resin_rotation_maintain_cursor = (idx + 1) % pool_size if consumed else idx
                if switched:
                    self._resin_last_switched_account = account
                    self._resin_last_switch_at = time.time()
            return account, units

        raise RuntimeError("Resin rotation failed: no account slot available")

    def _resin_force_switch_next_account(self, kind: str, reason: str = "") -> None:
        task_kind = "maintain" if str(kind or "").strip().lower() == "maintain" else "register"
        with self._lock:
            if task_kind == "register":
                old_account = self._resin_rotation_register_account
                self._resin_rotation_register_cursor += 1
                self._resin_rotation_register_account = ""
                self._resin_rotation_register_used = 0
            else:
                old_account = self._resin_rotation_maintain_account
                self._resin_rotation_maintain_cursor += 1
                self._resin_rotation_maintain_account = ""
                self._resin_rotation_maintain_used = 0
        self.warn(
            f"Resin force switch next identity ({reason or 'runtime-error'}), prev={old_account or '-'}",
            step="proxy",
        )

    def _resin_reset_rotation_state(self, reason: str = "") -> None:
        with self._lock:
            self._resin_rotation_register_cursor = 0
            self._resin_rotation_register_account = ""
            self._resin_rotation_register_used = 0
            self._resin_rotation_maintain_cursor = 0
            self._resin_rotation_maintain_account = ""
            self._resin_rotation_maintain_used = 0
        self.info(f"Resin rotation state reset ({reason or 'manual'})", step="proxy")

    def _resolve_resin_proxy_for_task(
        self,
        cfg: Dict[str, Any],
        kind: str,
        requested_units: int,
        fixed_account: str = "",
    ) -> tuple[Dict[str, Any], int, str]:
        if not bool(cfg.get("resin_enabled", False)):
            raise RuntimeError("Resin is disabled")
        task_kind = "maintain" if str(kind or "").strip().lower() == "maintain" else "register"
        if task_kind == "register":
            platform_name = str(cfg.get("resin_platform_register") or "").strip()
        else:
            platform_name = str(cfg.get("resin_platform_maintain") or "").strip()
        if not platform_name:
            raise RuntimeError("Resin platform name is empty")

        platform = self._resin_ensure_platform(cfg, platform_name)
        platform_name = str(platform.get("name") or platform_name).strip() or platform_name

        try:
            req_units = max(1, int(requested_units or 1))
        except Exception:
            req_units = 1

        fixed_account_name = self._resin_normalize_account_name(fixed_account, "")
        if fixed_account_name:
            account_name = fixed_account_name
            units = req_units
        elif bool(cfg.get("resin_node_rotation_enabled", True)):
            account_name, units = self._resin_select_rotation_account(cfg, task_kind, req_units)
        else:
            account_name = self._resin_rotation_account_name(task_kind, 0)
            units = req_units

        browser_proxy = self._resin_compose_proxy_url(cfg, platform_name, account_name)
        upstream_identity = f"resin:{platform_name}.{account_name}"
        self.info(
            f"Resin identity selected: platform={platform_name}, account={account_name}, kind={task_kind}, units={units}",
            step="proxy",
        )
        self.info(
            "Proxy strategy selected: resin | "
            f"browser={_mask_proxy_for_log(browser_proxy)} | upstream={upstream_identity}",
            step="proxy",
        )
        return {
            "strategy": "resin",
            "proxy": browser_proxy,
            "upstream_proxy": upstream_identity,
            "region": "",
            "resin_platform": platform_name,
            "resin_account": account_name,
            "node_name": account_name,
            "node_units": units,
        }, units, account_name

    def _resolve_resin_proxy_with_retry(
        self,
        cfg: Dict[str, Any],
        kind: str,
        requested_units: int,
        fixed_account: str = "",
    ) -> tuple[Dict[str, Any], int, str]:
        retry_forever = bool(cfg.get("resin_retry_forever", True))
        retry_times = max(1, min(int(cfg.get("resin_retry_times") or 3), 60))
        retry_interval = max(1, min(int(cfg.get("resin_retry_interval_seconds") or 8), 300))
        attempt = 0
        while True:
            attempt += 1
            try:
                return self._resolve_resin_proxy_for_task(cfg, kind, requested_units, fixed_account=fixed_account)
            except Exception as exc:
                self.warn(
                    f"Resin resolve failed: attempt={attempt}"
                    f"{'' if retry_forever else '/' + str(retry_times)} err={exc}",
                    step="proxy",
                )
                if (not retry_forever) and attempt >= retry_times:
                    raise
                time.sleep(retry_interval)

    def test_resin(self) -> Dict[str, Any]:
        cfg = get_config()
        if not bool(cfg.get("resin_enabled", False)):
            return {"ok": False, "error": "Resin is disabled"}
        try:
            health = requests.get(f"{_normalize_http_url(cfg.get('resin_api_url') or '').rstrip('/')}/healthz", timeout=6)
            if health.status_code >= 400:
                raise RuntimeError(f"/healthz HTTP {health.status_code}: {health.text[:120]}")
        except Exception as exc:
            return {"ok": False, "stage": "healthz", "error": self._repair_possible_mojibake(str(exc))}

        try:
            platform_name = str(cfg.get("resin_platform_register") or "").strip() or "gemini-register"
            platform = self._resin_ensure_platform(cfg, platform_name)
            account = self._resin_rotation_account_name("register", int(time.time()) % 99999)
            proxy_url = self._resin_compose_proxy_url(cfg, str(platform.get("name") or platform_name), account)
            trace_text = trace_via_proxy(proxy_url, timeout=10)
            trace_map = parse_trace(trace_text)
            supported, loc, ip = is_location_supported(trace_text)
            leases = self._resin_api_request(
                cfg,
                "GET",
                f"/api/v1/platforms/{platform.get('id')}/leases",
                timeout=12,
                params={"limit": 1, "offset": 0},
            )
            lease_total = int(leases.get("total") or 0) if isinstance(leases, dict) else 0
            return {
                "ok": True,
                "api_url": _normalize_http_url(cfg.get("resin_api_url") or ""),
                "proxy_url": _normalize_proxy_endpoint(cfg.get("resin_proxy_url") or "", default="http://127.0.0.1:2260"),
                "platform": str(platform.get("name") or platform_name),
                "platform_id": str(platform.get("id") or ""),
                "test_account": account,
                "loc": loc or "",
                "ip": ip or "",
                "supported": bool(supported),
                "active_leases": lease_total,
                "trace": trace_map,
            }
        except Exception as exc:
            return {"ok": False, "stage": "proxy", "error": self._repair_possible_mojibake(str(exc))}

    def test_easyproxies(self) -> Dict[str, Any]:
        cfg = get_config()
        listen_proxy = _normalize_proxy_endpoint(
            cfg.get("easyproxies_listen_proxy") or "",
            default="http://127.0.0.1:2323",
        )
        if not listen_proxy:
            return {"ok": False, "error": "easyproxies listen proxy is empty"}

        try:
            nodes_payload = self._easyproxies_request(cfg, "GET", "/api/nodes", timeout=12)
            nodes, total, healthy = self._easyproxies_nodes_summary(nodes_payload)
            auto_result = self._easyproxies_auto_exclude_hk_cn_nodes(cfg, nodes, reason="test-initial")
            auto_skipped = int(auto_result.get("skipped") or auto_result.get("matched") or 0)
        except Exception as exc:
            return {
                "ok": False,
                "stage": "api_nodes",
                "error": self._repair_possible_mojibake(str(exc)),
            }

        try:
            trace_text = trace_via_proxy(listen_proxy, timeout=10)
            trace_map = parse_trace(trace_text)
            supported, loc, ip = is_location_supported(trace_text)
            if (not supported) and loc in {"CN", "HK"}:
                retry_auto = self._easyproxies_auto_exclude_hk_cn_nodes(cfg, reason="test-trace")
                retry_skipped = int(retry_auto.get("skipped") or retry_auto.get("matched") or 0)
                auto_skipped += retry_skipped
            return {
                "ok": bool(supported),
                "stage": "proxy_trace",
                "supported": bool(supported),
                "loc": loc or "",
                "ip": ip or "",
                "trace": trace_map,
                "total_nodes": total,
                "healthy_nodes": healthy,
                "auto_disabled_nodes": auto_skipped,
                "auto_skipped_nodes": auto_skipped,
                "listen_proxy": listen_proxy,
                "error": None if supported else "location restricted (CN/HK)",
            }
        except Exception as exc:
            parsed = urlparse(listen_proxy)
            likely_auth_required = not bool(parsed.username) and not bool(parsed.password)
            hint = ""
            if likely_auth_required:
                hint = "listener may require auth; use http://user:pass@host:port if configured in EasyProxies listener"
            return {
                "ok": False,
                "stage": "proxy_trace",
                "error": self._repair_possible_mojibake(str(exc)),
                "total_nodes": total,
                "healthy_nodes": healthy,
                "listen_proxy": listen_proxy,
                "hint": hint,
            }

    def list_easyproxies_nodes(self) -> Dict[str, Any]:
        cfg = get_config()
        enabled = bool(cfg.get("easyproxies_enabled", True))

        settings_mode = ""
        try:
            settings = self._easyproxies_request(cfg, "GET", "/api/settings", timeout=10)
            if isinstance(settings, dict):
                settings_mode = str(settings.get("mode") or "").strip().lower()
        except Exception:
            settings_mode = ""

        payload = self._easyproxies_request(cfg, "GET", "/api/nodes", timeout=12)
        nodes, total, healthy = self._easyproxies_nodes_summary(payload)
        out_nodes: List[Dict[str, Any]] = []
        for node in nodes:
            if not isinstance(node, dict):
                continue
            name = str(node.get("name") or node.get("tag") or "").strip()
            if not name:
                continue
            out_nodes.append(
                {
                    "name": name,
                    "tag": str(node.get("tag") or "").strip(),
                    "region": str(node.get("region") or "").strip(),
                    "country": str(node.get("country") or "").strip(),
                    "port": int(node.get("port") or 0),
                    "listen_address": str(node.get("listen_address") or "").strip(),
                    "available": bool(node.get("available")),
                    "initial_check_done": bool(node.get("initial_check_done")),
                    "blacklisted": bool(node.get("blacklisted")),
                }
            )
        out_nodes.sort(key=lambda x: str(x.get("name") or "").lower())
        return {
            "ok": True,
            "enabled": enabled,
            "mode": settings_mode,
            "total_nodes": total,
            "healthy_nodes": healthy,
            "nodes": out_nodes,
        }

    @staticmethod
    def _mask_upstream_proxy_for_log(proxy: str) -> str:
        return _mask_proxy_for_log(str(proxy or "").strip())

    def _save_proxy_state(self, proxy_info: Dict[str, Any]) -> None:
        strategy = str(proxy_info.get("strategy") or "").strip()
        browser_proxy = normalize_proxy_value(proxy_info.get("proxy") or "")
        upstream_proxy = normalize_proxy_value(proxy_info.get("upstream_proxy") or "")
        region = str(proxy_info.get("region") or "").strip()
        with self._lock:
            self.current_proxy = browser_proxy
            self.current_proxy_strategy = strategy
            self.current_proxy_upstream = upstream_proxy
            self.current_proxy_region = region
            self.current_proxy_updated_at = time.time()

    def _build_proxy_env(self, proxy_info: Dict[str, Any]) -> Dict[str, str]:
        strategy = str(proxy_info.get("strategy") or "").strip()
        upstream_proxy = normalize_proxy_value(proxy_info.get("upstream_proxy") or "")
        region = str(proxy_info.get("region") or "").strip()
        resin_platform = str(proxy_info.get("resin_platform") or "").strip()
        resin_account = str(proxy_info.get("resin_account") or "").strip()
        env: Dict[str, str] = {}
        if strategy:
            env["PROXY_STRATEGY"] = strategy
        if upstream_proxy:
            env["PROXY_UPSTREAM"] = upstream_proxy
        if region:
            env["PROXY_REGION"] = region
        if resin_platform:
            env["PROXY_PLATFORM"] = resin_platform
        if resin_account:
            env["PROXY_ACCOUNT"] = resin_account
        return env

    def _easyproxies_auth_token(self, cfg: Dict[str, Any]) -> tuple[str, str]:
        api_base = _normalize_http_url(cfg.get("easyproxies_api_url") or "").rstrip("/")
        if not api_base:
            raise RuntimeError("EasyProxies API URL is empty")

        token = ""
        no_password = False
        try:
            auth_check = requests.get(f"{api_base}/api/auth", timeout=8)
            if auth_check.status_code >= 400:
                raise RuntimeError(f"HTTP {auth_check.status_code}: {auth_check.text[:120]}")
            payload = auth_check.json() if auth_check.content else {}
            if isinstance(payload, dict):
                no_password = bool(payload.get("no_password"))
        except Exception as exc:
            raise RuntimeError(f"EasyProxies auth check failed: {exc}") from None

        if no_password:
            return api_base, token

        password = str(cfg.get("easyproxies_password") or "").strip()
        if not password:
            raise RuntimeError("EasyProxies password is empty")
        try:
            auth_resp = requests.post(
                f"{api_base}/api/auth",
                json={"password": password},
                timeout=10,
            )
            if auth_resp.status_code >= 400:
                raise RuntimeError(f"HTTP {auth_resp.status_code}: {auth_resp.text[:120]}")
            data = auth_resp.json() if auth_resp.content else {}
            token = str(data.get("token") or "").strip() if isinstance(data, dict) else ""
            if not token:
                raise RuntimeError("No token in /api/auth response")
        except Exception as exc:
            raise RuntimeError(f"EasyProxies login failed: {exc}") from None
        return api_base, token

    def _easyproxies_request(
        self,
        cfg: Dict[str, Any],
        method: str,
        path: str,
        timeout: int = 12,
        json_body: Optional[Dict[str, Any]] = None,
    ) -> Any:
        api_base, token = self._easyproxies_auth_token(cfg)
        url_path = "/" + str(path or "").lstrip("/")
        headers: Dict[str, str] = {}
        if token:
            headers["Authorization"] = f"Bearer {token}"

        try:
            resp = requests.request(
                method=str(method or "GET").upper(),
                url=f"{api_base}{url_path}",
                headers=headers,
                json=json_body,
                timeout=timeout,
            )
            if resp.status_code >= 400:
                raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:180]}")
            if not resp.content:
                return {}
            try:
                return resp.json()
            except Exception:
                return {"raw": resp.text}
        except Exception as exc:
            raise RuntimeError(f"EasyProxies request failed ({method} {url_path}): {exc}") from None

    def _sync_easyproxies_subscription(self, cfg: Dict[str, Any], force: bool = False) -> Dict[str, Any]:
        if not bool(cfg.get("easyproxies_subscription_enabled", False)):
            return {"ok": True, "skipped": True, "reason": "disabled"}
        sub_url = _normalize_subscription_url(cfg.get("easyproxies_subscription_url") or "")
        if not sub_url:
            return {"ok": False, "skipped": True, "reason": "empty subscription url"}

        refresh_minutes = max(1, min(int(cfg.get("easyproxies_subscription_refresh_minutes") or 60), 24 * 60))
        now_ts = time.time()
        with self._lock:
            same_url = self._easyproxies_last_sync_url == sub_url
            elapsed = now_ts - float(self._easyproxies_last_sub_sync_at or 0.0)
        if (not force) and same_url and elapsed < refresh_minutes * 60:
            return {"ok": True, "skipped": True, "reason": "cooldown"}

        settings = self._easyproxies_request(cfg, "GET", "/api/settings", timeout=12)
        if not isinstance(settings, dict):
            raise RuntimeError("EasyProxies /api/settings returned invalid payload")

        payload = dict(settings)
        payload["subscriptions"] = [sub_url]
        payload["sub_refresh_enabled"] = True
        payload["sub_refresh_interval"] = f"{refresh_minutes}m"
        payload["sub_refresh_timeout"] = str(payload.get("sub_refresh_timeout") or "30s")
        payload["sub_refresh_health_check_timeout"] = str(payload.get("sub_refresh_health_check_timeout") or "60s")
        payload["sub_refresh_drain_timeout"] = str(payload.get("sub_refresh_drain_timeout") or "30s")
        payload["sub_refresh_min_available_nodes"] = int(payload.get("sub_refresh_min_available_nodes") or 1)

        self._easyproxies_request(cfg, "PUT", "/api/settings", timeout=15, json_body=payload)
        self._easyproxies_request(cfg, "POST", "/api/subscription/refresh", timeout=25)
        self._easyproxies_request(cfg, "POST", "/api/reload", timeout=20)

        with self._lock:
            self._easyproxies_last_sub_sync_at = now_ts
            self._easyproxies_last_refresh_at = now_ts
            self._easyproxies_last_sync_url = sub_url
        auto_result = self._easyproxies_auto_exclude_hk_cn_nodes(cfg, reason="subscription-sync")
        auto_skipped = int(auto_result.get("skipped") or auto_result.get("matched") or 0)
        self.info(
            f"EasyProxies subscription synced and refreshed: url={sub_url}, interval={refresh_minutes}m",
            step="proxy",
        )
        if auto_skipped > 0:
            self.info(f"EasyProxies post-sync soft-skipped HK/CN nodes: {auto_skipped}", step="proxy")
        return {"ok": True, "skipped": False, "subscription_url": sub_url, "refresh_minutes": refresh_minutes}

    def _refresh_easyproxies_runtime(self, cfg: Dict[str, Any], reason: str) -> None:
        if not bool(cfg.get("easyproxies_enabled", False)):
            return
        try:
            self._sync_easyproxies_subscription(cfg, force=True)
        except Exception as exc:
            self.warn(f"EasyProxies runtime refresh failed ({reason}): {exc}", step="proxy")
            return
        self.info(f"EasyProxies runtime refresh done ({reason})", step="proxy")

    def _resolve_easyproxies_proxy(self, cfg: Dict[str, Any]) -> Dict[str, Any]:
        listen_proxy = _normalize_proxy_endpoint(
            cfg.get("easyproxies_listen_proxy") or "",
            default="http://127.0.0.1:2323",
        )
        if not listen_proxy:
            raise RuntimeError("EasyProxies listen proxy is invalid")

        if bool(cfg.get("easyproxies_refresh_before_task", True)):
            self._sync_easyproxies_subscription(cfg, force=False)

        nodes_payload = self._easyproxies_request(cfg, "GET", "/api/nodes", timeout=12)
        nodes, total, healthy = self._easyproxies_nodes_summary(nodes_payload)
        auto_result = self._easyproxies_auto_exclude_hk_cn_nodes(cfg, nodes, reason="resolve-initial")
        auto_skipped = int(auto_result.get("skipped") or auto_result.get("matched") or 0)
        if auto_skipped > 0:
            self.info(f"EasyProxies resolve soft-skipped HK/CN nodes: {auto_skipped}", step="proxy")
        if total <= 0:
            raise RuntimeError("EasyProxies has no available nodes")

        self.info(f"EasyProxies ready: total={total}, healthy={healthy}, proxy={_mask_proxy_for_log(listen_proxy)}", step="proxy")

        precheck_retries = max(1, min(int(cfg.get("easyproxies_retry_times") or 3), 8))
        precheck_loc = ""
        precheck_ip = ""
        last_precheck_error = ""
        for attempt in range(1, precheck_retries + 1):
            try:
                trace_text = trace_via_proxy(listen_proxy, timeout=8)
                supported, precheck_loc, precheck_ip = is_location_supported(trace_text)
                if supported:
                    self.info(
                        f"EasyProxies browser proxy precheck passed: attempt={attempt}/{precheck_retries}, "
                        f"loc={precheck_loc or '?'} ip={precheck_ip or '?'}",
                        step="proxy",
                    )
                    break
                last_precheck_error = f"location restricted: loc={precheck_loc or '?'}"
                if precheck_loc in {"CN", "HK"}:
                    retry_auto = self._easyproxies_auto_exclude_hk_cn_nodes(cfg, reason=f"resolve-precheck-{attempt}")
                    retry_skipped = int(retry_auto.get("skipped") or retry_auto.get("matched") or 0)
                    if retry_skipped > 0:
                        self.warn(
                            f"EasyProxies precheck loc={precheck_loc} -> soft-skip HK/CN candidates={retry_skipped} (no disable)",
                            step="proxy",
                        )
            except Exception as exc:
                last_precheck_error = str(exc)
            if attempt < precheck_retries:
                time.sleep(0.4)
        else:
            raise RuntimeError(f"EasyProxies precheck failed: {last_precheck_error or 'unknown error'}")

        self.info(
            "Proxy strategy selected: easyproxies | "
            f"browser={_mask_proxy_for_log(listen_proxy)} | "
            f"upstream=easyproxies-pool | "
            f"region={precheck_loc or '?'}",
            step="proxy",
        )
        return {
            "strategy": "easyproxies",
            "proxy": listen_proxy,
            "upstream_proxy": "",
            "region": precheck_loc or "",
        }

    def _next_subscription_proxy(self, cfg: Dict[str, Any]) -> Dict[str, Any]:
        subscription_url = _normalize_subscription_url(cfg.get("proxy_subscription_url") or "")
        if not subscription_url:
            raise RuntimeError("Subscription URL is empty")

        refresh_minutes = max(1, min(int(cfg.get("proxy_subscription_refresh_minutes") or 10), 24 * 60))
        now_ts = time.time()

        with self._lock:
            if self._subscription_cache_url != subscription_url:
                self._subscription_cache_url = subscription_url
                self._subscription_cache = []
                self._subscription_cache_at = 0.0
                self._subscription_cursor = 0
            cache = list(self._subscription_cache)
            cache_age = now_ts - float(self._subscription_cache_at or 0.0)

        should_refresh = not cache or cache_age >= refresh_minutes * 60
        if should_refresh:
            try:
                resp = requests.get(
                    subscription_url,
                    headers={"User-Agent": "GeminiConsole/1.0", "Accept": "*/*"},
                    timeout=15,
                )
                if resp.status_code >= 400:
                    raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:120]}")
                parsed_proxies = _parse_proxy_subscription(resp.text)
                if not parsed_proxies:
                    scheme_counts = _collect_subscription_uri_schemes(resp.text)
                    unsupported = {
                        k: v for k, v in scheme_counts.items()
                        if k not in ALLOWED_PROXY_SCHEMES
                    }
                    if unsupported:
                        details = ", ".join(f"{k}:{v}" for k, v in sorted(unsupported.items()))
                        raise RuntimeError(
                            f"Subscription contains unsupported node schemes ({details}); "
                            "need http/https/socks4/socks5 proxy list"
                        )
                    raise RuntimeError("Subscription content did not contain any usable proxies")

                with self._lock:
                    self._subscription_cache = parsed_proxies
                    self._subscription_cache_at = now_ts
                    if self._subscription_cursor >= len(parsed_proxies):
                        self._subscription_cursor = 0
                    cache = list(parsed_proxies)
                self.info(f"Subscription proxy list refreshed: {len(cache)} entries", step="proxy")
            except Exception as exc:
                with self._lock:
                    cache = list(self._subscription_cache)
                if cache:
                    self.warn(f"Subscription refresh failed, using cached list ({len(cache)}): {exc}", step="proxy")
                else:
                    raise RuntimeError(f"Subscription refresh failed: {exc}") from None

        if not cache:
            raise RuntimeError("Subscription proxy list is empty")

        with self._lock:
            idx = self._subscription_cursor % len(cache)
            proxy = cache[idx]
            self._subscription_cursor = (idx + 1) % len(cache)
            current_idx = idx + 1

        self.info(
            f"Using subscription proxy rotation: {current_idx}/{len(cache)} -> {_mask_proxy_for_log(proxy)}",
            step="proxy",
        )
        self.info(
            "Proxy strategy selected: subscription | "
            f"browser={_mask_proxy_for_log(proxy)} | upstream={self._mask_upstream_proxy_for_log(proxy)}",
            step="proxy",
        )
        return {
            "strategy": "subscription",
            "proxy": proxy,
            "upstream_proxy": proxy,
            "region": "",
        }

    def _resolve_runtime_proxy(
        self,
        cfg: Dict[str, Any],
        kind: str = "register",
        requested_units: int = 1,
    ) -> Dict[str, Any]:
        static_proxy = normalize_proxy_value(cfg.get("proxy") or "")
        engine = _normalize_proxy_engine(cfg.get("proxy_engine"))
        easy_enabled = bool(cfg.get("easyproxies_enabled", True))
        resin_enabled = bool(cfg.get("resin_enabled", False))

        providers: List[str] = []
        if engine == "easyproxies":
            if easy_enabled:
                providers.append("easyproxies")
        elif engine == "resin":
            if resin_enabled:
                providers.append("resin")
        else:
            if resin_enabled:
                providers.append("resin")
            if easy_enabled:
                providers.append("easyproxies")

        for provider in providers:
            if provider == "easyproxies":
                easy_retry_forever = bool(cfg.get("easyproxies_retry_forever", True))
                easy_retry_times = max(1, min(int(cfg.get("easyproxies_retry_times") or 3), 60))
                easy_retry_interval = max(1, min(int(cfg.get("easyproxies_retry_interval_seconds") or 8), 300))
                attempt = 0
                while True:
                    attempt += 1
                    try:
                        return self._resolve_easyproxies_proxy(cfg)
                    except Exception as exc:
                        self.warn(
                            f"EasyProxies resolve failed: attempt={attempt}"
                            f"{'' if easy_retry_forever else '/' + str(easy_retry_times)} err={exc}",
                            step="proxy",
                        )
                        if (not easy_retry_forever) and attempt >= easy_retry_times:
                            break
                        time.sleep(easy_retry_interval)
                continue

            if provider == "resin":
                try:
                    proxy_info, _, _ = self._resolve_resin_proxy_with_retry(cfg, kind, requested_units)
                    return proxy_info
                except Exception as exc:
                    self.warn(f"Resin unavailable, fallback to next strategy: {exc}", step="proxy")
                continue

        if static_proxy:
            self.info(
                "Proxy strategy selected: static | "
                f"browser={_mask_proxy_for_log(static_proxy)} | upstream={self._mask_upstream_proxy_for_log(static_proxy)}",
                step="proxy",
            )
            return {
                "strategy": "static",
                "proxy": static_proxy,
                "upstream_proxy": static_proxy,
                "region": "",
            }
        self.warn("Proxy strategy selected: direct (no proxy)", step="proxy")
        return {
            "strategy": "direct",
            "proxy": "",
            "upstream_proxy": "",
            "region": "",
        }

    @staticmethod
    def _decode_subprocess_line(raw: bytes) -> str:
        if not raw:
            return ""
        encodings = ["utf-8", "utf-8-sig", "gb18030", "cp936"]
        pref = str(locale.getpreferredencoding(False) or "").strip()
        if pref:
            encodings.append(pref)
        tried = set()
        for enc in encodings:
            if not enc or enc in tried:
                continue
            tried.add(enc)
            try:
                return RuntimeManager._repair_possible_mojibake(raw.decode(enc))
            except Exception:
                continue
        return RuntimeManager._repair_possible_mojibake(raw.decode("utf-8", errors="replace"))

    def _run_subprocess(
        self,
        kind: str,
        cmd: List[str],
        auto_triggered: bool,
        target_count: int = 0,
        extra_env: Optional[Dict[str, str]] = None,
        remaining_after_success: int = 0,
        fixed_node_name: str = "",
    ) -> None:
        proc: Optional[subprocess.Popen] = None
        cfg = get_config()
        watchdog_enabled = bool(cfg.get("task_watchdog_enabled", True))
        stall_timeout_seconds = max(30, min(int(cfg.get("task_stall_timeout_seconds") or 300), 7200))
        stall_restart_enabled = bool(cfg.get("task_stall_restart_enabled", True))
        stall_restart_max = max(0, min(int(cfg.get("task_stall_restart_max") or 5), 50))
        rotate_interval_seconds = max(0, min(int(cfg.get("easyproxies_rotate_interval_seconds") or 120), 3600))
        proxy_fail_guard_enabled = bool(cfg.get("proxy_fail_guard_enabled", True))
        proxy_fail_guard_threshold = max(2, min(int(cfg.get("proxy_fail_guard_threshold") or 3), 20))
        proxy_fail_guard_pause_seconds = max(10, min(int(cfg.get("proxy_fail_guard_pause_seconds") or 60), 900))

        with self._lock:
            run_strategy = str(self.current_proxy_strategy or "").strip()
            run_upstream = str(self.current_proxy_upstream or "").strip()
        fixed_node_name = str(fixed_node_name or "").strip()
        easyproxies_rotation_active = (
            run_strategy == "easyproxies"
            and bool(cfg.get("easyproxies_node_rotation_enabled", True))
            and not fixed_node_name
        )
        resin_rotation_active = (
            run_strategy == "resin"
            and bool(cfg.get("resin_node_rotation_enabled", True))
            and not fixed_node_name
        )
        segmented_rotation_active = easyproxies_rotation_active or resin_rotation_active
        if run_strategy in {"easyproxies", "resin"} and (segmented_rotation_active or fixed_node_name):
            # Active per-node rotation mode should not reload runtime during task, it may break browser session.
            rotate_interval_seconds = 0

        self.info(
            "Task runtime monitor: "
            f"watchdog={'on' if watchdog_enabled else 'off'}, "
            f"stall_timeout={stall_timeout_seconds}s, "
            f"auto_restart={'on' if stall_restart_enabled else 'off'}, "
            f"restart_max={stall_restart_max if stall_restart_max > 0 else 'inf'}, "
            f"strategy={run_strategy or 'direct'}, "
            f"upstream={self._mask_upstream_proxy_for_log(run_upstream) or '-'}, "
            f"rotate_interval={rotate_interval_seconds}s, "
            f"proxy_fail_guard={'on' if proxy_fail_guard_enabled else 'off'}"
            f"(threshold={proxy_fail_guard_threshold}, pause={proxy_fail_guard_pause_seconds}s)",
            step=kind,
        )

        stalled = False
        stall_seconds = 0
        proxy_fail_guard_triggered = False
        proxy_fail_streak = 0
        proxy_related_error_seen = False
        proxy_fail_guard_cooldown_seconds = 6.0
        last_proxy_fail_mark_at = 0.0
        interval_switch_suppressed_until = 0.0
        restart_requested = False
        try:
            remaining_after_success = max(0, int(remaining_after_success or 0))
        except Exception:
            remaining_after_success = 0
        if kind == "register":
            restart_target = max(1, int(target_count or 1))
        else:
            restart_target = max(0, int(target_count or 0))
        restart_delay_seconds = 0

        progress_done = 0
        target_units = max(0, int(target_count or 0))
        progress_total = max(1, target_units) if target_units > 0 else (1 if kind == "register" else 0)
        progress_success = 0
        progress_fail = 0

        summary_success = 0
        summary_fail = 0
        has_summary = False
        return_code = -1
        task_success = 0
        task_fail = 0

        eof_sentinel = object()

        try:
            run_env = dict(os.environ)
            run_env["PYTHONIOENCODING"] = "utf-8"
            run_env["PYTHONUTF8"] = "1"
            run_env["PYTHONUNBUFFERED"] = "1"
            easyproxies_api_url = _normalize_http_url(cfg.get("easyproxies_api_url") or "")
            if easyproxies_api_url:
                run_env["EASYPROXIES_API_URL"] = easyproxies_api_url
            resin_api_url = _normalize_http_url(cfg.get("resin_api_url") or "")
            if resin_api_url:
                run_env["RESIN_API_URL"] = resin_api_url
            # Keep scripts from trying old socks5-pool switching paths.
            if run_strategy in {"easyproxies", "resin"}:
                # Manager-side engine/rotation handles switching; disable legacy in-script switch.
                run_env["MAIL_PROXY_ROTATE_RETRIES"] = "0"
                run_env["MAIL_PROXY_SWITCH_ATTEMPTS"] = "0"
                run_env["MAIL_PROXY_SWITCH_VALIDATE"] = "0"
                run_env["MAIL_TIMEOUT_PROXY_ROTATE"] = "0"
            else:
                run_env.setdefault("MAIL_PROXY_ROTATE_RETRIES", "2")
                run_env.setdefault("MAIL_PROXY_SWITCH_ATTEMPTS", "3")
                run_env.setdefault("MAIL_PROXY_SWITCH_VALIDATE", "1")
                run_env.setdefault("MAIL_TIMEOUT_PROXY_ROTATE", "1")
            run_env.setdefault("MAIL_PROXY_ROTATE_THRESHOLD", "2")
            run_env.setdefault("MAIL_PROXY_SWITCH_VALIDATE_TIMEOUT", "6")
            run_env.setdefault("BROWSER_PROXY_PRECHECK_RETRIES", "3")
            run_env.setdefault("BROWSER_PROXY_PRECHECK_TIMEOUT", "6")
            run_env.setdefault("UC_CLEAR_CACHE_ON_RETRY", "0")
            run_env.setdefault("UC_DOWNLOAD_PROXY_ENABLED", "0")
            if extra_env:
                for k, v in extra_env.items():
                    key = str(k or "").strip()
                    if not key:
                        continue
                    run_env[key] = str(v or "")

            proc = subprocess.Popen(
                cmd,
                cwd=str(BASE_DIR),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=False,
                bufsize=0,
                env=run_env,
            )
            with self._lock:
                if kind == "register":
                    self._register_proc = proc
                else:
                    self._maintain_proc = proc

            summary_patterns = [
                re.compile(r"完成!\s*成功:\s*(\d+)\s*,\s*失败:\s*(\d+)"),
                re.compile(r"success[:=]\s*(\d+).*?fail(?:ure)?[:=]\s*(\d+)", re.IGNORECASE),
            ]
            progress_patterns = [
                re.compile(r"进度:\s*(\d+)\s*/\s*(\d+)\s*\|\s*成功:\s*(\d+)\s*\|\s*失败:\s*(\d+)"),
                re.compile(r"progress:\s*(\d+)\s*/\s*(\d+).*?success:\s*(\d+).*?fail(?:ure)?:\s*(\d+)", re.IGNORECASE),
            ]

            line_queue: queue.Queue = queue.Queue(maxsize=2000)

            def _reader() -> None:
                if proc is None or proc.stdout is None:
                    line_queue.put(eof_sentinel)
                    return
                try:
                    while True:
                        raw = proc.stdout.readline()
                        if raw in (None, b""):
                            break
                        line_queue.put(raw)
                except Exception as exc:
                    try:
                        line_queue.put(f"[reader-error] {exc}".encode("utf-8", errors="ignore"))
                    except Exception:
                        pass
                finally:
                    try:
                        line_queue.put(eof_sentinel)
                    except Exception:
                        pass

            threading.Thread(target=_reader, daemon=True).start()

            last_log_at = time.time()
            next_rotate_at = last_log_at + rotate_interval_seconds if rotate_interval_seconds > 0 else 0.0
            log_error_rotate_cooldown_seconds = 20
            next_log_error_rotate_at = 0.0
            proxy_fail_guard_keywords = (
                "err_proxy_connection_failed",
                "net::err_proxy_connection_failed",
                "err_tunnel_connection_failed",
                "err_socks_connection_failed",
                "proxy connection failed",
                "this site can't be reached",
                "无法访问此网站",
                "open login/input email failed",
                "login page blank/incomplete",
                "nonetype' object has no attribute 'is_displayed'",
                "stacktrace:",
                "browser startup failed",
                "浏览器启动失败",
                "browser proxy precheck failed before startup",
                "可重试代理异常已达最大轮换重试次数",
            )
            proxy_fail_guard_reset_keywords = (
                "browser started successfully",
                "输入邮箱",
                "邮箱:",
                "验证码:",
                "登录成功",
                "注册成功",
                "已进入工作台",
            )

            while True:
                try:
                    item = line_queue.get(timeout=1)
                except queue.Empty:
                    now_ts = time.time()
                    if proc.poll() is not None and line_queue.empty():
                        break
                    if run_strategy == "easyproxies" and rotate_interval_seconds > 0 and now_ts >= next_rotate_at:
                        if now_ts < interval_switch_suppressed_until:
                            next_rotate_at = max(interval_switch_suppressed_until, now_ts) + rotate_interval_seconds
                        else:
                            self._refresh_easyproxies_runtime(cfg, reason=f"interval-{kind}")
                            next_rotate_at = now_ts + rotate_interval_seconds
                    if watchdog_enabled and (now_ts - last_log_at) >= stall_timeout_seconds:
                        stalled = True
                        stall_seconds = int(now_ts - last_log_at)
                        self.warn(
                            f"No task logs for {stall_seconds}s, watchdog will terminate and restart {kind}",
                            step=kind,
                        )
                        try:
                            proc.terminate()
                        except Exception:
                            pass
                        break
                    continue

                if item is eof_sentinel:
                    break

                raw = item
                if raw in (None, b""):
                    continue
                line = self._decode_subprocess_line(raw).rstrip("\r\n")
                if not line:
                    continue

                last_log_at = time.time()
                self.info(line, step=kind)

                if kind == "register":
                    m_item = re.search(r"注册成功[:：]\s*([^\s]+@[^\s]+)", line)
                    if m_item:
                        self._enqueue_item_sync("register", m_item.group(1), auto_triggered)
                elif kind == "maintain":
                    m_item = re.search(r"登录成功[:：]\s*([^\s]+@[^\s]+)", line)
                    if m_item:
                        self._enqueue_item_sync("maintain", m_item.group(1), auto_triggered)

                if run_strategy == "easyproxies":
                    lower_line = line.lower()
                    if any(k in lower_line for k in proxy_fail_guard_keywords):
                        proxy_related_error_seen = True
                    if proxy_fail_guard_enabled:
                        is_proxy_fail_mark = any(k in lower_line for k in proxy_fail_guard_keywords)
                        if is_proxy_fail_mark:
                            now_ts = time.time()
                            if (now_ts - last_proxy_fail_mark_at) >= proxy_fail_guard_cooldown_seconds:
                                proxy_fail_streak += 1
                                last_proxy_fail_mark_at = now_ts
                                self.warn(
                                    f"Proxy fail streak: {proxy_fail_streak}/{proxy_fail_guard_threshold}",
                                    step=kind,
                                )
                            if proxy_fail_streak >= proxy_fail_guard_threshold:
                                proxy_fail_guard_triggered = True
                                self.warn(
                                    "Proxy fail guard triggered, terminating current task run and "
                                    f"pausing {proxy_fail_guard_pause_seconds}s before restart",
                                    step=kind,
                                )
                                try:
                                    self._refresh_easyproxies_runtime(cfg, reason=f"proxy-guard-{kind}")
                                except Exception:
                                    pass
                                try:
                                    proc.terminate()
                                except Exception:
                                    pass
                                break
                        elif any(k in line for k in proxy_fail_guard_reset_keywords):
                            if proxy_fail_streak > 0:
                                self.info("Proxy fail streak reset", step=kind)
                            proxy_fail_streak = 0

                    if ("browser proxy precheck failed" in lower_line) or ("触发代理轮换重试" in line):
                        interval_switch_suppressed_until = max(interval_switch_suppressed_until, time.time() + 90)

                    # Avoid fighting with zhuce.py/weihu.py internal proxy rotation.
                    script_is_rotating = ("触发代理轮换重试" in line) or ("proxy switched:" in lower_line)
                    should_switch_now = False
                    if not script_is_rotating:
                        if "可重试代理异常已达最大轮换重试次数" in line:
                            should_switch_now = True
                        elif "proxy rotate retries reached" in lower_line:
                            should_switch_now = True
                    if should_switch_now:
                        now_ts = time.time()
                        if now_ts >= next_log_error_rotate_at:
                            self.warn(
                                "Detected terminal proxy/runtime error log, refreshing EasyProxies now",
                                step=kind,
                            )
                            self._refresh_easyproxies_runtime(cfg, reason=f"log-error-{kind}")
                            next_log_error_rotate_at = now_ts + log_error_rotate_cooldown_seconds
                            if rotate_interval_seconds > 0:
                                next_rotate_at = now_ts + rotate_interval_seconds

                for pattern in progress_patterns:
                    m = pattern.search(line)
                    if m:
                        progress_done = int(m.group(1))
                        progress_total = max(1, int(m.group(2)))
                        progress_success = int(m.group(3))
                        progress_fail = int(m.group(4))
                        break

                if kind == "register":
                    for pattern in summary_patterns:
                        m = pattern.search(line)
                        if m:
                            summary_success = int(m.group(1))
                            summary_fail = int(m.group(2))
                            has_summary = True
                            break

            try:
                return_code = proc.wait(timeout=12)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass
                try:
                    return_code = proc.wait(timeout=5)
                except Exception:
                    return_code = -9

            if kind == "register":
                final_success = summary_success if has_summary else progress_success
                final_fail = summary_fail if has_summary else progress_fail
                task_success = final_success
                task_fail = final_fail

                if (stalled or proxy_fail_guard_triggered) and (progress_success > 0 or progress_fail > 0):
                    with self._lock:
                        self.success_count += progress_success
                        self.fail_count += progress_fail
                        _save_state_counts(self.success_count, self.fail_count)
                    self.info(
                        "Register partial progress saved before restart: "
                        f"success={progress_success}, fail={progress_fail}",
                        step="register",
                    )

                if return_code == 0:
                    with self._lock:
                        self.success_count += final_success
                        self.fail_count += final_fail
                        self._register_watchdog_restarts = 0
                        _save_state_counts(self.success_count, self.fail_count)
                    self.success(
                        f"{'Auto' if auto_triggered else 'Manual'} register task finished: target={target_count}, success={final_success}, fail={final_fail}",
                        step="register",
                    )
                    if (
                        remaining_after_success > 0
                        and segmented_rotation_active
                        and final_success > 0
                    ):
                        restart_requested = True
                        restart_target = max(1, int(remaining_after_success))
                        self.info(
                            "Register segmented continuation: "
                            f"segment_done={target_count}, remaining={restart_target}",
                            step="register",
                        )
                    elif remaining_after_success > 0 and final_success <= 0:
                        self.warn(
                            "Register segmented continuation paused: "
                            "current segment has zero success, avoiding useless retries",
                            step="register",
                        )
                    if auto_triggered and final_success <= 0 and final_fail > 0:
                        cooldown_seconds = max(
                            600,
                            int(float(cfg.get("auto_register_interval_hours") or 4.0) * 3600),
                        )
                        with self._lock:
                            self.next_auto_register_at = time.time() + cooldown_seconds
                        self.warn(
                            "Auto register cooldown enabled: "
                            f"success=0 fail={final_fail}, next retry in {cooldown_seconds}s",
                            step="register",
                        )
                else:
                    self.error(f"Register task exited abnormally, code={return_code}", step="register")
            else:
                maintain_processed = max(progress_done, progress_success + progress_fail)
                task_success = max(0, int(progress_success or 0))
                task_fail = max(0, int(progress_fail or 0))
                if return_code == 0:
                    with self._lock:
                        self._maintain_watchdog_restarts = 0
                    self.success(f"{'Auto' if auto_triggered else 'Manual'} maintain task completed", step="maintain")
                    if (
                        (not restart_requested)
                        and segmented_rotation_active
                        and target_units > 0
                        and maintain_processed >= target_units
                        and progress_success > 0
                    ):
                        restart_requested = True
                        restart_target = target_units
                        self.info(
                            "Maintain segmented continuation: "
                            f"segment_done={maintain_processed}, limit={target_units}, switching next node",
                            step="maintain",
                        )
                    elif target_units > 0 and maintain_processed >= target_units and progress_success <= 0:
                        self.warn(
                            "Maintain segmented continuation paused: "
                            "no successful account in this segment, avoiding endless retries",
                            step="maintain",
                        )
                else:
                    self.error(f"Maintain task exited abnormally, code={return_code}", step="maintain")

            if proxy_fail_guard_triggered:
                if easyproxies_rotation_active:
                    self._easyproxies_force_switch_next_node(reason=f"proxy-fail-guard-{kind}")
                elif resin_rotation_active:
                    self._resin_force_switch_next_account(kind, reason=f"proxy-fail-guard-{kind}")
                if kind == "register":
                    processed = max(progress_done, progress_success + progress_fail)
                    base_total = max(progress_total, int(target_count or 1))
                    restart_target = max(1, base_total - processed)
                    with self._lock:
                        self._register_watchdog_restarts += 1
                        restart_idx = self._register_watchdog_restarts
                    if stall_restart_max > 0 and restart_idx > stall_restart_max:
                        self.error(
                            f"Proxy fail guard restart limit reached for register: {restart_idx-1}/{stall_restart_max}",
                            step="register",
                        )
                    else:
                        restart_requested = True
                        restart_delay_seconds = proxy_fail_guard_pause_seconds
                        self.warn(
                            "Proxy fail guard restarting register: "
                            f"restart={restart_idx}/{stall_restart_max if stall_restart_max > 0 else 'inf'}, "
                            f"remaining={restart_target}, pause={restart_delay_seconds}s",
                            step="register",
                        )
                else:
                    with self._lock:
                        self._maintain_watchdog_restarts += 1
                        restart_idx = self._maintain_watchdog_restarts
                    if stall_restart_max > 0 and restart_idx > stall_restart_max:
                        self.error(
                            f"Proxy fail guard restart limit reached for maintain: {restart_idx-1}/{stall_restart_max}",
                            step="maintain",
                        )
                    else:
                        restart_requested = True
                        restart_delay_seconds = proxy_fail_guard_pause_seconds
                        self.warn(
                            "Proxy fail guard restarting maintain: "
                            f"restart={restart_idx}/{stall_restart_max if stall_restart_max > 0 else 'inf'}, "
                            f"pause={restart_delay_seconds}s",
                            step="maintain",
                        )

            if (not restart_requested) and stalled and stall_restart_enabled:
                if easyproxies_rotation_active:
                    self._easyproxies_force_switch_next_node(reason=f"watchdog-stall-{kind}")
                elif resin_rotation_active:
                    self._resin_force_switch_next_account(kind, reason=f"watchdog-stall-{kind}")
                if kind == "register":
                    processed = max(progress_done, progress_success + progress_fail)
                    base_total = max(progress_total, int(target_count or 1))
                    restart_target = max(1, base_total - processed)
                    with self._lock:
                        self._register_watchdog_restarts += 1
                        restart_idx = self._register_watchdog_restarts
                    if stall_restart_max > 0 and restart_idx > stall_restart_max:
                        self.error(
                            f"Watchdog restart limit reached for register: {restart_idx-1}/{stall_restart_max}",
                            step="register",
                        )
                    else:
                        restart_requested = True
                        self.warn(
                            f"Watchdog restarting register: restart={restart_idx}/{stall_restart_max if stall_restart_max > 0 else 'inf'}, remaining={restart_target}",
                            step="register",
                        )
                else:
                    with self._lock:
                        self._maintain_watchdog_restarts += 1
                        restart_idx = self._maintain_watchdog_restarts
                    if stall_restart_max > 0 and restart_idx > stall_restart_max:
                        self.error(
                            f"Watchdog restart limit reached for maintain: {restart_idx-1}/{stall_restart_max}",
                            step="maintain",
                        )
                    else:
                        restart_requested = True
                        self.warn(
                            f"Watchdog restarting maintain: restart={restart_idx}/{stall_restart_max if stall_restart_max > 0 else 'inf'}",
                            step="maintain",
                        )

            if (
                (not restart_requested)
                and (return_code != 0)
                and run_strategy in {"easyproxies", "resin"}
                and proxy_fail_guard_enabled
                and stall_restart_enabled
                and proxy_related_error_seen
            ):
                if easyproxies_rotation_active:
                    self._easyproxies_force_switch_next_node(reason=f"abnormal-exit-{kind}")
                elif resin_rotation_active:
                    self._resin_force_switch_next_account(kind, reason=f"abnormal-exit-{kind}")
                if run_strategy == "easyproxies":
                    try:
                        self._refresh_easyproxies_runtime(cfg, reason=f"abnormal-exit-{kind}")
                    except Exception:
                        pass
                if kind == "register":
                    processed = max(progress_done, progress_success + progress_fail)
                    base_total = max(progress_total, int(target_count or 1))
                    restart_target = max(1, base_total - processed)
                    with self._lock:
                        self._register_watchdog_restarts += 1
                        restart_idx = self._register_watchdog_restarts
                    if stall_restart_max > 0 and restart_idx > stall_restart_max:
                        self.error(
                            f"Proxy runtime restart limit reached for register: {restart_idx-1}/{stall_restart_max}",
                            step="register",
                        )
                    else:
                        restart_requested = True
                        restart_delay_seconds = max(restart_delay_seconds, proxy_fail_guard_pause_seconds)
                        self.warn(
                            "Proxy runtime abnormal exit detected, restarting register: "
                            f"restart={restart_idx}/{stall_restart_max if stall_restart_max > 0 else 'inf'}, "
                            f"remaining={restart_target}, pause={restart_delay_seconds}s",
                            step="register",
                        )
                else:
                    with self._lock:
                        self._maintain_watchdog_restarts += 1
                        restart_idx = self._maintain_watchdog_restarts
                    if stall_restart_max > 0 and restart_idx > stall_restart_max:
                        self.error(
                            f"Proxy runtime restart limit reached for maintain: {restart_idx-1}/{stall_restart_max}",
                            step="maintain",
                        )
                    else:
                        restart_requested = True
                        restart_delay_seconds = max(restart_delay_seconds, proxy_fail_guard_pause_seconds)
                        self.warn(
                            "Proxy runtime abnormal exit detected, restarting maintain: "
                            f"restart={restart_idx}/{stall_restart_max if stall_restart_max > 0 else 'inf'}, "
                            f"pause={restart_delay_seconds}s",
                            step="maintain",
                        )

            if restart_requested and auto_triggered and proxy_related_error_seen and task_success <= 0:
                if kind == "register":
                    cooldown_seconds = max(
                        900,
                        int(float(cfg.get("auto_register_interval_hours") or 4.0) * 3600),
                    )
                    with self._lock:
                        self.next_auto_register_at = time.time() + cooldown_seconds
                else:
                    maintain_interval_hours = float(cfg.get("maintain_interval_hours") or 0.0)
                    if maintain_interval_hours > 0:
                        cooldown_seconds = max(900, int(maintain_interval_hours * 3600))
                    else:
                        cooldown_seconds = max(900, int(cfg.get("maintain_interval_minutes") or 30) * 60)
                    with self._lock:
                        self.next_auto_maintain_at = time.time() + cooldown_seconds
                restart_requested = False
                restart_delay_seconds = 0
                self.warn(
                    f"Auto restart suppressed for {kind}: success=0 fail={task_fail}, "
                    f"cooldown={cooldown_seconds}s",
                    step=kind,
                )

            if not restart_requested:
                # Refresh all_account.json after each register/maintain run
                try:
                    merged = merge_all_accounts()
                    self.info(f"Account summary refreshed: all_account.json ({merged} rows)", step="merge")
                except Exception as exc:
                    self.warn(f"Refresh all_account.json failed: {exc}", step="merge")
                self._maybe_auto_sync(kind=kind, return_code=return_code, auto_triggered=auto_triggered)
        except Exception as exc:
            self.error(f"Failed to start {kind} subprocess: {exc}", step=kind)
        finally:
            with self._lock:
                if kind == "register":
                    self.register_status = "idle"
                    self._register_proc = None
                    self._register_target = 0
                else:
                    self.maintain_status = "idle"
                    self._maintain_proc = None
            if proc is not None and proc.stdout is not None:
                try:
                    proc.stdout.close()
                except Exception:
                    pass

        if restart_requested:
            try:
                if restart_delay_seconds > 0:
                    self.warn(
                        f"Auto restart cooldown: waiting {restart_delay_seconds}s before restarting {kind}",
                        step=kind,
                    )
                    time.sleep(restart_delay_seconds)
                if kind == "register":
                    self.start_register(
                        restart_target,
                        auto_triggered=auto_triggered,
                        internal_restart=True,
                        fixed_node_name=fixed_node_name,
                    )
                else:
                    self.start_maintain(
                        auto_triggered=auto_triggered,
                        internal_restart=True,
                        limit=max(0, int(restart_target or 0)),
                        fixed_node_name=fixed_node_name,
                    )
            except Exception as exc:
                self.error(f"Watchdog restart {kind} failed: {exc}", step=kind)

    def start_register(
        self,
        count: int,
        auto_triggered: bool = False,
        internal_restart: bool = False,
        fixed_node_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        if not REGISTER_SCRIPT.exists():
            raise RuntimeError(f"Register script not found: {REGISTER_SCRIPT}")
        target_requested = max(1, int(count))
        target = target_requested
        with self._lock:
            if self.register_status in ("running", "stopping"):
                raise RuntimeError("Register task is already running")
            self.register_status = "running"
            self.last_register_started_at = time.time()
            self._register_target = target_requested
            if not internal_restart:
                self._register_watchdog_restarts = 0

        try:
            cfg = get_config()
            proxy_engine = _normalize_proxy_engine(cfg.get("proxy_engine"))
            requested_fixed_node = str(fixed_node_name or "").strip() if fixed_node_name is not None else ""
            # Manual/API start should respect request value (including empty to clear),
            # auto scheduler keeps backward-compatibility by reading persisted config.
            if fixed_node_name is None:
                use_fixed_node = str(cfg.get("easyproxies_fixed_node") or "").strip() if proxy_engine == "easyproxies" else ""
            else:
                use_fixed_node = requested_fixed_node

            if proxy_engine == "easyproxies":
                if use_fixed_node:
                    if not bool(cfg.get("easyproxies_enabled", True)):
                        raise RuntimeError("Fixed node requires easyproxies_enabled=true")
                    proxy_info, rotated_node = self._resolve_easyproxies_proxy_forced_node(cfg, use_fixed_node, "register")
                elif bool(cfg.get("easyproxies_enabled", True)) and bool(cfg.get("easyproxies_node_rotation_enabled", True)):
                    if (
                        not auto_triggered
                        and fixed_node_name is not None
                        and not requested_fixed_node
                        and not internal_restart
                    ):
                        with self._lock:
                            has_prev_rotation_node = bool(self._easyproxies_rotation_node)
                        if has_prev_rotation_node:
                            self._easyproxies_force_switch_next_node(reason="manual-next-register")
                    try:
                        proxy_info, rotated_units, rotated_node = self._resolve_easyproxies_proxy_with_rotation(
                            cfg,
                            "register",
                            target_requested,
                        )
                        target = max(1, int(rotated_units or target_requested))
                    except Exception as exc:
                        self.warn(f"EasyProxies rotation failed, fallback to runtime strategy: {exc}", step="proxy")
                        proxy_info = self._resolve_runtime_proxy(cfg, kind="register", requested_units=target_requested)
                        rotated_node = ""
                else:
                    proxy_info = self._resolve_runtime_proxy(cfg, kind="register", requested_units=target_requested)
                    rotated_node = ""
            elif proxy_engine == "resin":
                if (
                    not auto_triggered
                    and fixed_node_name is not None
                    and not requested_fixed_node
                    and not internal_restart
                    and bool(cfg.get("resin_node_rotation_enabled", True))
                ):
                    with self._lock:
                        has_prev_rotation_identity = bool(self._resin_rotation_register_account)
                    if has_prev_rotation_identity:
                        self._resin_force_switch_next_account("register", reason="manual-next-register")
                try:
                    proxy_info, rotated_units, rotated_node = self._resolve_resin_proxy_with_retry(
                        cfg,
                        "register",
                        target_requested,
                        fixed_account=use_fixed_node,
                    )
                    target = max(1, int(rotated_units or target_requested))
                except Exception as exc:
                    self.warn(f"Resin rotation failed, fallback to runtime strategy: {exc}", step="proxy")
                    fallback_cfg = dict(cfg)
                    fallback_cfg["resin_enabled"] = False
                    fallback_cfg["proxy_engine"] = "auto"
                    proxy_info = self._resolve_runtime_proxy(fallback_cfg, kind="register", requested_units=target_requested)
                    rotated_node = ""
            else:
                proxy_info = self._resolve_runtime_proxy(cfg, kind="register", requested_units=target_requested)
                rotated_node = ""
        except Exception:
            with self._lock:
                self.register_status = "idle"
                self._register_target = 0
            raise
        proxy = normalize_proxy_value(proxy_info.get("proxy") or "")
        strategy = str(proxy_info.get("strategy") or "").strip()
        upstream_proxy = normalize_proxy_value(proxy_info.get("upstream_proxy") or "")
        region = str(proxy_info.get("region") or "").strip()
        remaining_after_success = max(0, int(target_requested - target))
        self._save_proxy_state(proxy_info)
        if auto_triggered:
            with self._lock:
                self.last_auto_register_at = time.time()

        cmd = [
            sys.executable,
            "-u",
            "-W",
            "ignore::DeprecationWarning",
            str(REGISTER_SCRIPT),
            "--total",
            str(target),
        ]
        if proxy:
            cmd.extend(["--proxy", proxy])

        self.info(
            f"{'Auto' if auto_triggered else 'Manual'} start register task: "
            f"requested={target_requested}, segment={target}, strategy={strategy or 'direct'}, browser_proxy={proxy or 'direct'}, "
            f"upstream={self._mask_upstream_proxy_for_log(upstream_proxy) or '-'}, region={region or '-'}",
            step="register",
        )
        if strategy == "resin":
            if rotated_node and use_fixed_node:
                self.info(
                    f"Register fixed Resin identity: account={rotated_node}, requested={target_requested}, actual={target}",
                    step="register",
                )
            elif rotated_node:
                self.info(
                    f"Register Resin rotation: account={rotated_node}, requested={target_requested}, actual={target}",
                    step="register",
                )
        else:
            if rotated_node and use_fixed_node:
                self.info(f"Register fixed node: node={rotated_node}, requested={target_requested}, actual={target}", step="register")
            elif rotated_node:
                self.info(f"Register node rotation: node={rotated_node}, requested={target_requested}, actual={target}", step="register")
        if internal_restart:
            with self._lock:
                restart_idx = self._register_watchdog_restarts
            if restart_idx > 0:
                self.warn(f"Watchdog restart register task: restart={restart_idx}, target={target}", step="register")
        extra_env = self._build_proxy_env(proxy_info)
        t = threading.Thread(
            target=self._run_subprocess,
            args=("register", cmd, auto_triggered, target, extra_env, remaining_after_success, use_fixed_node),
            daemon=True,
        )
        t.start()
        return {
            "status": "started",
            "target_count": target_requested,
            "segment_count": target,
            "remaining_count": remaining_after_success,
            "proxy": proxy,
            "proxy_strategy": strategy,
            "proxy_upstream": upstream_proxy,
            "proxy_region": region,
            "proxy_node": rotated_node or "",
        }

    def stop_register(self) -> None:
        with self._lock:
            proc = self._register_proc
            if self.register_status not in ("running", "stopping") or proc is None:
                raise RuntimeError("No running register task")
            self.register_status = "stopping"
        self.warn("Received stop register request, terminating subprocess...", step="register")
        try:
            proc.terminate()
            proc.wait(timeout=8)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass

    def start_maintain(
        self,
        auto_triggered: bool = False,
        internal_restart: bool = False,
        limit: int = 0,
        fixed_node_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        if not MAINTAIN_SCRIPT.exists():
            raise RuntimeError(f"Maintain script not found: {MAINTAIN_SCRIPT}")
        maintain_limit = max(0, int(limit or 0))
        with self._lock:
            if self.maintain_status in ("running", "stopping"):
                raise RuntimeError("Maintain task is already running")
            if self.register_status in ("running", "stopping"):
                raise RuntimeError("Register task is running, maintain cannot start")
            self.maintain_status = "running"
            self.last_maintain_started_at = time.time()
            if not internal_restart:
                self._maintain_watchdog_restarts = 0

        try:
            cfg = get_config()
            proxy_engine = _normalize_proxy_engine(cfg.get("proxy_engine"))
            rotated_node = ""
            requested_fixed_node = str(fixed_node_name or "").strip() if fixed_node_name is not None else ""
            # Manual/API start should respect request value (including empty to clear),
            # auto scheduler keeps backward-compatibility by reading persisted config.
            if fixed_node_name is None:
                use_fixed_node = str(cfg.get("easyproxies_fixed_node") or "").strip() if proxy_engine == "easyproxies" else ""
            else:
                use_fixed_node = requested_fixed_node

            if proxy_engine == "easyproxies":
                if use_fixed_node:
                    if not bool(cfg.get("easyproxies_enabled", True)):
                        raise RuntimeError("Fixed node requires easyproxies_enabled=true")
                    proxy_info, rotated_node = self._resolve_easyproxies_proxy_forced_node(cfg, use_fixed_node, "maintain")
                elif bool(cfg.get("easyproxies_enabled", True)) and bool(cfg.get("easyproxies_node_rotation_enabled", True)):
                    if (
                        not auto_triggered
                        and fixed_node_name is not None
                        and not requested_fixed_node
                        and not internal_restart
                    ):
                        with self._lock:
                            has_prev_rotation_node = bool(self._easyproxies_rotation_node)
                        if has_prev_rotation_node:
                            self._easyproxies_force_switch_next_node(reason="manual-next-maintain")
                    requested_units = maintain_limit or int(cfg.get("easyproxies_node_maintain_quota") or 20)
                    try:
                        proxy_info, rotated_units, rotated_node = self._resolve_easyproxies_proxy_with_rotation(
                            cfg,
                            "maintain",
                            requested_units,
                        )
                        maintain_limit = max(1, int(rotated_units or requested_units))
                    except Exception as exc:
                        self.warn(f"EasyProxies maintain rotation failed, fallback to runtime strategy: {exc}", step="proxy")
                        proxy_info = self._resolve_runtime_proxy(cfg, kind="maintain", requested_units=max(1, requested_units))
                else:
                    proxy_info = self._resolve_runtime_proxy(
                        cfg,
                        kind="maintain",
                        requested_units=max(1, int(maintain_limit or 1)),
                    )
            elif proxy_engine == "resin":
                requested_units = maintain_limit or int(cfg.get("resin_node_maintain_quota") or 20)
                if (
                    not auto_triggered
                    and fixed_node_name is not None
                    and not requested_fixed_node
                    and not internal_restart
                    and bool(cfg.get("resin_node_rotation_enabled", True))
                ):
                    with self._lock:
                        has_prev_rotation_identity = bool(self._resin_rotation_maintain_account)
                    if has_prev_rotation_identity:
                        self._resin_force_switch_next_account("maintain", reason="manual-next-maintain")
                try:
                    proxy_info, rotated_units, rotated_node = self._resolve_resin_proxy_with_retry(
                        cfg,
                        "maintain",
                        max(1, requested_units),
                        fixed_account=use_fixed_node,
                    )
                    maintain_limit = max(1, int(rotated_units or requested_units))
                except Exception as exc:
                    self.warn(f"Resin maintain rotation failed, fallback to runtime strategy: {exc}", step="proxy")
                    fallback_cfg = dict(cfg)
                    fallback_cfg["resin_enabled"] = False
                    fallback_cfg["proxy_engine"] = "auto"
                    proxy_info = self._resolve_runtime_proxy(
                        fallback_cfg,
                        kind="maintain",
                        requested_units=max(1, requested_units),
                    )
                    rotated_node = ""
            else:
                proxy_info = self._resolve_runtime_proxy(
                    cfg,
                    kind="maintain",
                    requested_units=max(1, int(maintain_limit or 1)),
                )
        except Exception:
            with self._lock:
                self.maintain_status = "idle"
            raise
        proxy = normalize_proxy_value(proxy_info.get("proxy") or "")
        strategy = str(proxy_info.get("strategy") or "").strip()
        upstream_proxy = normalize_proxy_value(proxy_info.get("upstream_proxy") or "")
        region = str(proxy_info.get("region") or "").strip()
        self._save_proxy_state(proxy_info)
        cmd = [
            sys.executable,
            "-u",
            "-W",
            "ignore::DeprecationWarning",
            str(MAINTAIN_SCRIPT),
        ]
        if proxy:
            cmd.extend(["--proxy", proxy])
        if maintain_limit > 0:
            cmd.extend(["--limit", str(maintain_limit)])
        self.info(
            f"{'Auto' if auto_triggered else 'Manual'} start maintain task: "
            f"strategy={strategy or 'direct'}, browser_proxy={proxy or 'direct'}, "
            f"upstream={self._mask_upstream_proxy_for_log(upstream_proxy) or '-'}, region={region or '-'}, "
            f"limit={maintain_limit if maintain_limit > 0 else 'all'}",
            step="maintain",
        )
        if strategy == "resin":
            if rotated_node and use_fixed_node:
                self.info(
                    f"Maintain fixed Resin identity: account={rotated_node}, limit={maintain_limit if maintain_limit > 0 else 'all'}",
                    step="maintain",
                )
            elif rotated_node:
                self.info(f"Maintain Resin rotation: account={rotated_node}, limit={maintain_limit}", step="maintain")
        else:
            if rotated_node and use_fixed_node:
                self.info(f"Maintain fixed node: node={rotated_node}, limit={maintain_limit if maintain_limit > 0 else 'all'}", step="maintain")
            elif rotated_node:
                self.info(f"Maintain node rotation: node={rotated_node}, limit={maintain_limit}", step="maintain")
        if internal_restart:
            with self._lock:
                restart_idx = self._maintain_watchdog_restarts
            if restart_idx > 0:
                self.warn(f"Watchdog restart maintain task: restart={restart_idx}", step="maintain")
        extra_env = self._build_proxy_env(proxy_info)
        t = threading.Thread(
            target=self._run_subprocess,
            args=("maintain", cmd, auto_triggered, maintain_limit, extra_env, 0, use_fixed_node),
            daemon=True,
        )
        t.start()
        return {
            "status": "started",
            "proxy": proxy,
            "proxy_strategy": strategy,
            "proxy_upstream": upstream_proxy,
            "proxy_region": region,
            "proxy_node": rotated_node or "",
            "limit": maintain_limit,
        }

    def stop_maintain(self) -> None:
        with self._lock:
            proc = self._maintain_proc
            if self.maintain_status not in ("running", "stopping") or proc is None:
                raise RuntimeError("No running maintain task")
            self.maintain_status = "stopping"
        self.warn("Received stop maintain request, terminating subprocess...", step="maintain")
        try:
            proc.terminate()
            proc.wait(timeout=8)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass

    def stop_all(self) -> Dict[str, Any]:
        result = {"register": "idle", "maintain": "idle"}
        try:
            self.stop_register()
            result["register"] = "stopping"
        except Exception:
            pass
        try:
            self.stop_maintain()
            result["maintain"] = "stopping"
        except Exception:
            pass
        return result

    def proxy_monitor(self) -> Dict[str, Any]:
        cfg = get_config()
        with self._lock:
            strategy = self.current_proxy_strategy
            browser_proxy = self.current_proxy
            upstream_proxy = self.current_proxy_upstream
            region = self.current_proxy_region
            updated_at = self.current_proxy_updated_at
            sub_synced_at = self._easyproxies_last_sub_sync_at
            sub_refreshed_at = self._easyproxies_last_refresh_at

        monitor: Dict[str, Any] = {
            "current": {
                "strategy": strategy or "",
                "browser_proxy": browser_proxy or "",
                "upstream_proxy": upstream_proxy or "",
                "region": region or "",
                "updated_at": updated_at,
            },
            "engine": _normalize_proxy_engine(cfg.get("proxy_engine")),
            "easyproxies": {
                "enabled": bool(cfg.get("easyproxies_enabled", True)),
                "api_url": _normalize_http_url(cfg.get("easyproxies_api_url") or ""),
                "listen_proxy": _normalize_proxy_endpoint(
                    cfg.get("easyproxies_listen_proxy") or "",
                    default="http://127.0.0.1:2323",
                ),
                "subscription_enabled": bool(cfg.get("easyproxies_subscription_enabled", False)),
                "subscription_url": _normalize_subscription_url(cfg.get("easyproxies_subscription_url") or ""),
                "last_sub_sync_at": sub_synced_at,
                "last_sub_refresh_at": sub_refreshed_at,
                "ok": None,
            },
            "resin": {
                "enabled": bool(cfg.get("resin_enabled", False)),
                "api_url": _normalize_http_url(cfg.get("resin_api_url") or ""),
                "proxy_url": _normalize_proxy_endpoint(
                    cfg.get("resin_proxy_url") or "",
                    default="http://127.0.0.1:2260",
                ),
                "platform_register": str(cfg.get("resin_platform_register") or "").strip(),
                "platform_maintain": str(cfg.get("resin_platform_maintain") or "").strip(),
                "ok": None,
            },
            "trace": {"ok": None},
        }

        api_base = _normalize_http_url(cfg.get("easyproxies_api_url") or "").rstrip("/")
        if bool(cfg.get("easyproxies_enabled", True)) and api_base:
            try:
                data = self._easyproxies_request(cfg, "GET", "/api/nodes", timeout=10)
                if not isinstance(data, dict):
                    data = {}
                nodes = data.get("nodes") if isinstance(data.get("nodes"), list) else []
                total = int(data.get("total_nodes") or len(nodes))
                healthy = 0
                for node in nodes:
                    if not isinstance(node, dict):
                        continue
                    if bool(node.get("initial_check_done")) and bool(node.get("available")) and not bool(node.get("blacklisted")):
                        healthy += 1
                monitor["easyproxies"] = {
                    "enabled": bool(cfg.get("easyproxies_enabled", True)),
                    "api_url": api_base,
                    "listen_proxy": _normalize_proxy_endpoint(
                        cfg.get("easyproxies_listen_proxy") or "",
                        default="http://127.0.0.1:2323",
                    ),
                    "ok": True,
                    "total_nodes": total,
                    "healthy_nodes": healthy,
                    "subscription_enabled": bool(cfg.get("easyproxies_subscription_enabled", False)),
                    "subscription_url": _normalize_subscription_url(cfg.get("easyproxies_subscription_url") or ""),
                    "last_sub_sync_at": sub_synced_at,
                    "last_sub_refresh_at": sub_refreshed_at,
                }
            except Exception as exc:
                monitor["easyproxies"] = {
                    "enabled": bool(cfg.get("easyproxies_enabled", True)),
                    "api_url": api_base,
                    "listen_proxy": _normalize_proxy_endpoint(
                        cfg.get("easyproxies_listen_proxy") or "",
                        default="http://127.0.0.1:2323",
                    ),
                    "ok": False,
                    "error": self._repair_possible_mojibake(str(exc)),
                }

        resin_api_base = _normalize_http_url(cfg.get("resin_api_url") or "").rstrip("/")
        if bool(cfg.get("resin_enabled", False)) and resin_api_base:
            try:
                health_resp = requests.get(f"{resin_api_base}/healthz", timeout=6)
                if health_resp.status_code >= 400:
                    raise RuntimeError(f"/healthz HTTP {health_resp.status_code}: {health_resp.text[:120]}")
                platform_payload = self._resin_api_request(
                    cfg,
                    "GET",
                    "/api/v1/platforms",
                    timeout=10,
                    params={"limit": 200, "offset": 0, "sort_by": "name", "sort_order": "asc"},
                )
                items = (
                    platform_payload.get("items")
                    if isinstance(platform_payload, dict) and isinstance(platform_payload.get("items"), list)
                    else []
                )
                plat_names: Set[str] = set()
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    name = str(item.get("name") or "").strip()
                    if name:
                        plat_names.add(name.lower())
                reg_name = str(cfg.get("resin_platform_register") or "").strip().lower()
                maint_name = str(cfg.get("resin_platform_maintain") or "").strip().lower()
                monitor["resin"] = {
                    "enabled": bool(cfg.get("resin_enabled", False)),
                    "api_url": resin_api_base,
                    "proxy_url": _normalize_proxy_endpoint(
                        cfg.get("resin_proxy_url") or "",
                        default="http://127.0.0.1:2260",
                    ),
                    "ok": True,
                    "platform_register": str(cfg.get("resin_platform_register") or "").strip(),
                    "platform_maintain": str(cfg.get("resin_platform_maintain") or "").strip(),
                    "platform_total": int(platform_payload.get("total") or len(items))
                    if isinstance(platform_payload, dict)
                    else len(items),
                    "platform_register_exists": bool(reg_name and reg_name in plat_names),
                    "platform_maintain_exists": bool(maint_name and maint_name in plat_names),
                }
            except Exception as exc:
                monitor["resin"] = {
                    "enabled": bool(cfg.get("resin_enabled", False)),
                    "api_url": resin_api_base,
                    "proxy_url": _normalize_proxy_endpoint(
                        cfg.get("resin_proxy_url") or "",
                        default="http://127.0.0.1:2260",
                    ),
                    "ok": False,
                    "error": self._repair_possible_mojibake(str(exc)),
                    "platform_register": str(cfg.get("resin_platform_register") or "").strip(),
                    "platform_maintain": str(cfg.get("resin_platform_maintain") or "").strip(),
                }

        if browser_proxy:
            try:
                trace_text = trace_via_proxy(browser_proxy, timeout=8)
                trace_map = parse_trace(trace_text)
                supported, loc, ip = is_location_supported(trace_text)
                monitor["trace"] = {
                    "ok": True,
                    "supported": bool(supported),
                    "loc": loc or "",
                    "ip": ip or "",
                    "raw": trace_map,
                }
            except Exception as exc:
                monitor["trace"] = {
                    "ok": False,
                    "error": self._repair_possible_mojibake(str(exc)),
                }
        else:
            monitor["trace"] = {"ok": False, "error": "direct mode (no proxy)"}

        return monitor

    def status(self) -> Dict[str, Any]:
        cfg = get_config()
        with self._lock:
            return {
                "register_status": self.register_status,
                "maintain_status": self.maintain_status,
                "proxy_engine": _normalize_proxy_engine(cfg.get("proxy_engine")),
                "current_proxy": self.current_proxy,
                "current_proxy_strategy": self.current_proxy_strategy,
                "current_proxy_upstream": self.current_proxy_upstream,
                "current_proxy_region": self.current_proxy_region,
                "current_proxy_updated_at": self.current_proxy_updated_at,
                "watchdog_register_restarts": self._register_watchdog_restarts,
                "watchdog_maintain_restarts": self._maintain_watchdog_restarts,
                "success": self.success_count,
                "fail": self.fail_count,
                "last_register_started_at": self.last_register_started_at,
                "last_maintain_started_at": self.last_maintain_started_at,
                "register_target": self._register_target,
                "last_auto_register_at": self.last_auto_register_at,
                "next_auto_register_at": self.next_auto_register_at,
                "next_auto_maintain_at": self.next_auto_maintain_at,
                "easyproxies_rotation_node": self._easyproxies_rotation_node,
                "easyproxies_rotation_register_used": self._easyproxies_rotation_register_used,
                "easyproxies_rotation_maintain_used": self._easyproxies_rotation_maintain_used,
                "easyproxies_last_switched_node": self._easyproxies_last_switched_node,
                "easyproxies_last_switch_at": self._easyproxies_last_switch_at,
                "resin_rotation_register_account": self._resin_rotation_register_account,
                "resin_rotation_register_used": self._resin_rotation_register_used,
                "resin_rotation_maintain_account": self._resin_rotation_maintain_account,
                "resin_rotation_maintain_used": self._resin_rotation_maintain_used,
                "resin_last_switched_account": self._resin_last_switched_account,
                "resin_last_switch_at": self._resin_last_switch_at,
                "sync_status": self.sync_status,
                "last_sync_at": self.last_sync_at,
                "last_sync_ok": self.last_sync_ok,
                "last_sync_reason": self.last_sync_reason,
                "last_sync_count": self.last_sync_count,
                "last_sync_skipped": self.last_sync_skipped,
                "last_sync_error": self.last_sync_error,
            }

    def ensure_auto_loop(self) -> None:
        if self._auto_thread and self._auto_thread.is_alive():
            return
        self._auto_stop.clear()
        self._auto_thread = threading.Thread(target=self._auto_loop, daemon=True)
        self._auto_thread.start()

    def stop_auto_loop(self) -> None:
        self._auto_stop.set()

    def _auto_loop(self) -> None:
        self.info("Auto scheduler thread started", step="auto")
        while not self._auto_stop.is_set():
            cfg = get_config()
            now_ts = time.time()
            try:
                threshold = _effective_pool_threshold(cfg)
                maintain_interval_hours = float(cfg.get("maintain_interval_hours") or 0.0)
                if maintain_interval_hours > 0:
                    maintain_interval_seconds = max(300, int(maintain_interval_hours * 3600))
                else:
                    maintain_interval_seconds = max(300, int(cfg.get("maintain_interval_minutes") or 30) * 60)
                register_interval_seconds = max(300, int(float(cfg.get("auto_register_interval_hours") or 4.0) * 3600))
                guarantee_window_seconds = max(1800, int(float(cfg.get("guarantee_window_hours") or 4.0) * 3600))
                auto_register_batch = max(1, int(cfg.get("auto_register_batch_size") or 20))
                priority = str(cfg.get("auto_task_priority") or "maintain").strip().lower()
                if priority not in {"register", "maintain"}:
                    priority = "maintain"

                auto_maintain_enabled = bool(cfg.get("auto_maintain"))
                auto_register_enabled = bool(cfg.get("auto_register"))

                with self._lock:
                    if auto_maintain_enabled:
                        if self.next_auto_maintain_at <= 0:
                            self.next_auto_maintain_at = now_ts
                    else:
                        self.next_auto_maintain_at = 0

                    if auto_register_enabled:
                        if self.next_auto_register_at <= 0:
                            self.next_auto_register_at = now_ts
                    else:
                        self.next_auto_register_at = 0

                    runtime_idle = (
                        self.register_status == "idle"
                        and self.maintain_status == "idle"
                    )
                    next_maintain_at = float(self.next_auto_maintain_at or 0.0)
                    next_register_at = float(self.next_auto_register_at or 0.0)
                    last_auto_register_at = float(self.last_auto_register_at or 0.0)

                maintain_due = auto_maintain_enabled and runtime_idle and now_ts >= next_maintain_at

                register_due = False
                register_batch = 0
                if auto_register_enabled and runtime_idle and (now_ts - last_auto_register_at) >= 20:
                    pool = build_pool_status(min_accounts=threshold)
                    gap = int(pool.get("gap", 0))
                    if gap > 0:
                        candidate_count = int(pool.get("valid_count", 0)) + int(pool.get("unknown_count", 0))
                        urgent = candidate_count < threshold
                        by_window = (now_ts - last_auto_register_at) >= guarantee_window_seconds
                        by_interval = now_ts >= next_register_at
                        if urgent or by_window or by_interval:
                            register_due = True
                            register_batch = min(gap, auto_register_batch)
                            self.info(
                                f"Auto register pending: gap={gap}, batch={register_batch}, threshold={threshold}, "
                                f"urgent={'yes' if urgent else 'no'}",
                                step="auto",
                            )

                if maintain_due and register_due:
                    self.info(
                        f"Auto priority dispatch: priority={priority}, maintain=due, register=due",
                        step="auto",
                    )

                order = [priority, "register" if priority == "maintain" else "maintain"]
                started = False
                for task_name in order:
                    if task_name == "register" and register_due:
                        try:
                            self.start_register(register_batch, auto_triggered=True)
                        except Exception as exc:
                            self.warn(f"Auto register start failed: {exc}", step="auto")
                        finally:
                            with self._lock:
                                self.next_auto_register_at = now_ts + register_interval_seconds
                        started = True
                        break

                    if task_name == "maintain" and maintain_due:
                        try:
                            self.start_maintain(auto_triggered=True)
                        except Exception as exc:
                            self.warn(f"Auto maintain start failed: {exc}", step="auto")
                        finally:
                            with self._lock:
                                self.next_auto_maintain_at = now_ts + maintain_interval_seconds
                        started = True
                        break

                if not started and auto_register_enabled is False:
                    with self._lock:
                        self.next_auto_register_at = 0

                if not started and auto_maintain_enabled is False:
                    with self._lock:
                        self.next_auto_maintain_at = 0

                self._auto_stop.wait(5)
            except Exception as exc:
                self.error(f"Auto scheduler error: {exc}", step="auto")
                self._auto_stop.wait(5)
        self.info("Auto scheduler thread stopped", step="auto")


manager = RuntimeManager()

app = FastAPI(title="Gemini Console", version="1.0.0")


class StartRequest(BaseModel):
    count: int = 1
    fixed_node: Optional[str] = None


class MaintainRequest(BaseModel):
    fixed_node: Optional[str] = None


class ProxyCheckRequest(BaseModel):
    proxy: str


class ConfigRequest(BaseModel):
    proxy: str = ""
    proxy_engine: str = "easyproxies"
    easyproxies_enabled: bool = True
    easyproxies_listen_proxy: str = "http://127.0.0.1:2323"
    easyproxies_api_url: str = "http://127.0.0.1:7840"
    easyproxies_password: str = ""
    easyproxies_subscription_enabled: bool = False
    easyproxies_subscription_url: str = ""
    easyproxies_subscription_refresh_minutes: int = 60
    easyproxies_refresh_before_task: bool = True
    easyproxies_retry_forever: bool = True
    easyproxies_retry_times: int = 3
    easyproxies_retry_interval_seconds: int = 8
    easyproxies_rotate_interval_seconds: int = 120
    easyproxies_node_rotation_enabled: bool = True
    easyproxies_node_register_quota: int = 5
    easyproxies_node_maintain_quota: int = 20
    easyproxies_fixed_node: str = ""
    resin_enabled: bool = False
    resin_api_url: str = "http://127.0.0.1:2260"
    resin_proxy_url: str = "http://127.0.0.1:2260"
    resin_admin_token: str = ""
    resin_proxy_token: str = ""
    resin_platform_register: str = "gemini-register"
    resin_platform_maintain: str = "gemini-maintain"
    resin_retry_forever: bool = True
    resin_retry_times: int = 3
    resin_retry_interval_seconds: int = 8
    resin_node_rotation_enabled: bool = True
    resin_node_register_quota: int = 5
    resin_node_maintain_quota: int = 20
    resin_rotation_pool_size: int = 2048
    auto_maintain: bool = False
    maintain_interval_minutes: int = 30
    maintain_interval_hours: float = 4.0
    auto_register: bool = False
    auto_task_priority: str = "maintain"
    auto_register_interval_hours: float = 4.0
    auto_register_batch_size: int = 20
    guarantee_enabled: bool = True
    guarantee_target_accounts: int = 200
    guarantee_window_hours: float = 4.0
    min_accounts: int = 20
    max_replenish_per_round: int = 20
    register_default_count: int = 1
    account_sync_enabled: bool = False
    account_sync_url: str = ""
    account_sync_auth_mode: str = "session"
    account_sync_login_url: str = ""
    account_sync_auth_header_name: str = "X-API-Key"
    account_sync_auth_query_name: str = "api_key"
    account_sync_api_key: str = ""
    account_sync_timeout_seconds: int = 20
    account_sync_after_register: bool = True
    account_sync_after_maintain: bool = True
    task_watchdog_enabled: bool = True
    task_stall_timeout_seconds: int = 300
    task_stall_restart_enabled: bool = True
    task_stall_restart_max: int = 5
    proxy_fail_guard_enabled: bool = True
    proxy_fail_guard_threshold: int = 3
    proxy_fail_guard_pause_seconds: int = 60


class SyncAccountsRequest(BaseModel):
    force: bool = True
    merge_before_sync: bool = True
    reason: str = "manual"


@app.on_event("startup")
def _on_startup() -> None:
    manager.ensure_auto_loop()


@app.on_event("shutdown")
def _on_shutdown() -> None:
    manager.stop_auto_loop()


@app.get("/", response_class=HTMLResponse)
async def index() -> HTMLResponse:
    html_path = STATIC_DIR / "index.html"
    if not html_path.exists():
        return HTMLResponse("<h1>console_static/index.html not found</h1>", status_code=404)
    return HTMLResponse(html_path.read_text(encoding="utf-8"))


@app.get("/api/status")
async def api_status() -> Dict[str, Any]:
    cfg = get_config()
    pool = await run_in_threadpool(build_pool_status, _effective_pool_threshold(cfg))
    data = manager.status()
    data["pool"] = pool
    return data


@app.get("/api/config")
async def api_get_config() -> Dict[str, Any]:
    cfg = get_config()
    masked = dict(cfg)
    easy_pwd = str(masked.get("easyproxies_password") or "")
    resin_admin = str(masked.get("resin_admin_token") or "")
    resin_proxy = str(masked.get("resin_proxy_token") or "")
    sync_key = str(masked.get("account_sync_api_key") or "")
    masked["easyproxies_password"] = ""
    masked["easyproxies_password_preview"] = _mask_secret(easy_pwd, keep=6)
    masked["resin_admin_token"] = ""
    masked["resin_admin_token_preview"] = _mask_secret(resin_admin, keep=8)
    masked["resin_proxy_token"] = ""
    masked["resin_proxy_token_preview"] = _mask_secret(resin_proxy, keep=8)
    masked["account_sync_api_key"] = ""
    masked["account_sync_api_key_preview"] = _mask_secret(sync_key, keep=8)
    return masked


@app.post("/api/config")
async def api_set_config(req: ConfigRequest) -> Dict[str, Any]:
    prev_cfg = get_config()
    cfg = set_config(req.model_dump())
    try:
        rotation_sensitive_keys = (
            "proxy_engine",
            "easyproxies_fixed_node",
            "easyproxies_node_rotation_enabled",
            "easyproxies_node_register_quota",
            "easyproxies_node_maintain_quota",
            "easyproxies_enabled",
            "resin_node_rotation_enabled",
            "resin_node_register_quota",
            "resin_node_maintain_quota",
            "resin_rotation_pool_size",
            "resin_platform_register",
            "resin_platform_maintain",
            "resin_enabled",
        )
        need_reset = any(prev_cfg.get(k) != cfg.get(k) for k in rotation_sensitive_keys)
        if need_reset:
            manager._easyproxies_reset_rotation_state(reason="config-updated")
            manager._resin_reset_rotation_state(reason="config-updated")
    except Exception:
        pass
    easy_pwd = str(cfg.get("easyproxies_password") or "")
    resin_admin = str(cfg.get("resin_admin_token") or "")
    resin_proxy = str(cfg.get("resin_proxy_token") or "")
    sync_key = str(cfg.get("account_sync_api_key") or "")
    safe = dict(cfg)
    safe["easyproxies_password"] = ""
    safe["easyproxies_password_preview"] = _mask_secret(easy_pwd, keep=6)
    safe["resin_admin_token"] = ""
    safe["resin_admin_token_preview"] = _mask_secret(resin_admin, keep=8)
    safe["resin_proxy_token"] = ""
    safe["resin_proxy_token_preview"] = _mask_secret(resin_proxy, keep=8)
    safe["account_sync_api_key"] = ""
    safe["account_sync_api_key_preview"] = _mask_secret(sync_key, keep=8)
    manager.success("Configuration saved", step="config")
    return {"status": "saved", "config": safe}


@app.post("/api/start")
async def api_start(req: StartRequest) -> Dict[str, Any]:
    try:
        return manager.start_register(
            max(1, req.count),
            auto_triggered=False,
            fixed_node_name=(str(req.fixed_node).strip() if req.fixed_node is not None else ""),
        )
    except Exception as exc:
        raise HTTPException(status_code=409, detail=manager._repair_possible_mojibake(str(exc)))


@app.post("/api/stop")
async def api_stop() -> Dict[str, str]:
    try:
        manager.stop_register()
        return {"status": "stopping"}
    except Exception as exc:
        raise HTTPException(status_code=409, detail=manager._repair_possible_mojibake(str(exc)))


@app.post("/api/maintain")
async def api_maintain(req: Optional[MaintainRequest] = None) -> Dict[str, Any]:
    try:
        fixed_node_name = (str(req.fixed_node).strip() if req and req.fixed_node is not None else "")
        return manager.start_maintain(auto_triggered=False, fixed_node_name=fixed_node_name)
    except Exception as exc:
        raise HTTPException(status_code=409, detail=manager._repair_possible_mojibake(str(exc)))


@app.post("/api/maintain/stop")
async def api_maintain_stop() -> Dict[str, str]:
    try:
        manager.stop_maintain()
        return {"status": "stopping"}
    except Exception as exc:
        raise HTTPException(status_code=409, detail=manager._repair_possible_mojibake(str(exc)))


@app.post("/api/stop-all")
async def api_stop_all() -> Dict[str, Any]:
    result = manager.stop_all()
    return {"status": "stopping", "result": result}


@app.get("/api/pool/status")
async def api_pool_status() -> Dict[str, Any]:
    cfg = get_config()
    return await run_in_threadpool(build_pool_status, _effective_pool_threshold(cfg))


@app.post("/api/pool/merge")
async def api_pool_merge() -> Dict[str, Any]:
    count = await run_in_threadpool(merge_all_accounts)
    manager.success(f"Refreshed all_account.json: {count} rows", step="merge")
    return {"status": "merged", "count": count}


@app.post("/api/sync/accounts")
async def api_sync_accounts(req: SyncAccountsRequest) -> Dict[str, Any]:
    try:
        result = await run_in_threadpool(
            manager.sync_accounts_now,
            str(req.reason or "manual").strip() or "manual",
            bool(req.force),
            bool(req.merge_before_sync),
        )
        return {"status": "ok", "result": result}
    except Exception as exc:
        raise HTTPException(status_code=409, detail=manager._repair_possible_mojibake(str(exc)))


@app.post("/api/easyproxies/test")
async def api_easyproxies_test() -> Dict[str, Any]:
    return await run_in_threadpool(manager.test_easyproxies)


@app.post("/api/resin/test")
async def api_resin_test() -> Dict[str, Any]:
    return await run_in_threadpool(manager.test_resin)


@app.get("/api/easyproxies/nodes")
async def api_easyproxies_nodes() -> Dict[str, Any]:
    try:
        return await run_in_threadpool(manager.list_easyproxies_nodes)
    except Exception as exc:
        raise HTTPException(status_code=409, detail=manager._repair_possible_mojibake(str(exc)))


@app.post("/api/easyproxies/sync-subscription")
async def api_easyproxies_sync_subscription() -> Dict[str, Any]:
    try:
        result = await run_in_threadpool(manager.sync_easyproxies_subscription_now, True)
        return {"status": "ok", "result": result}
    except Exception as exc:
        raise HTTPException(status_code=409, detail=manager._repair_possible_mojibake(str(exc)))


@app.post("/api/check-proxy")
async def api_check_proxy(req: ProxyCheckRequest) -> Dict[str, Any]:
    proxy = normalize_proxy_value(req.proxy)
    if not proxy:
        return {"ok": False, "error": "Proxy is empty", "loc": None, "ip": None}
    try:
        trace_text = await run_in_threadpool(trace_via_proxy, proxy, 10)
        supported, loc, ip = is_location_supported(trace_text)
        return {"ok": supported, "loc": loc or None, "ip": ip or None, "error": None if supported else "location restricted (CN/HK)"}
    except Exception as exc:
        return {"ok": False, "error": manager._repair_possible_mojibake(str(exc)), "loc": None, "ip": None}


@app.get("/api/proxy/monitor")
async def api_proxy_monitor() -> Dict[str, Any]:
    return await run_in_threadpool(manager.proxy_monitor)


@app.get("/api/logs")
async def api_logs() -> StreamingResponse:
    async def event_generator() -> AsyncGenerator[str, None]:
        q = manager.subscribe()
        try:
            yield f"data: {json.dumps({'ts': '', 'level': 'connected', 'message': 'Log stream connected', 'step': ''}, ensure_ascii=False)}\n\n"
            while True:
                try:
                    event = await asyncio.wait_for(q.get(), timeout=20.0)
                    yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
                except asyncio.TimeoutError:
                    yield ": heartbeat\n\n"
                except Exception:
                    break
        finally:
            manager.unsubscribe(q)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


def main() -> None:
    import uvicorn

    print("=" * 56)
    print(" Gemini Console - 注册/维护控制台")
    print(" http://localhost:18423")
    print("=" * 56)
    uvicorn.run(app, host="0.0.0.0", port=18423, log_level="warning")


if __name__ == "__main__":
    main()

