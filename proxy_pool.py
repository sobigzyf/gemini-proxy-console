"""
Proxy pool helpers for GEMINI registration/maintenance tasks.

Features:
- Normalize/fetch proxy from proxy pool API.
- Parse heterogeneous proxy API responses.
- Relay-based Cloudflare trace check.
- Fallback-friendly runtime proxy resolution helpers.
"""

from __future__ import annotations

import socket
import time
import re
from typing import Any, Dict, Optional, Tuple
from urllib.parse import urlparse

import requests

DEFAULT_TRACE_URL = "https://cloudflare.com/cdn-cgi/trace"
DEFAULT_POOL_API_URL = "https://zenproxy.top/api/fetch"
DEFAULT_POOL_AUTH_MODE = "query"  # "query" | "header"
DEFAULT_POOL_COUNTRY = "US"
DEFAULT_POOL_COUNT = 1


def _mask_sensitive(text: Any) -> str:
    raw = str(text or "")
    if not raw:
        return ""
    out = re.sub(r"(api_key=)[^&\s)\"']+", r"\1***", raw, flags=re.IGNORECASE)
    out = re.sub(r"(Authorization:\s*Bearer\s+)[^\s\"']+", r"\1***", out, flags=re.IGNORECASE)
    out = re.sub(r"(Bearer\s+)[^\s\"']+", r"\1***", out, flags=re.IGNORECASE)
    return out


def normalize_proxy_value(proxy_value: Any) -> str:
    value = str(proxy_value or "").strip().strip('"').strip("'")
    if not value:
        return ""
    if value.startswith("{") or value.startswith("[") or value.startswith("<"):
        return ""
    if "://" in value:
        return value
    if ":" not in value:
        return ""
    return f"http://{value}"


def proxies_dict(proxy_value: Any) -> Optional[Dict[str, str]]:
    normalized = normalize_proxy_value(proxy_value)
    if not normalized:
        return None
    return {"http": normalized, "https": normalized}


def pool_relay_url_from_fetch_url(api_url: str) -> str:
    raw = str(api_url or "").strip()
    if not raw:
        return ""
    if "://" not in raw:
        raw = "https://" + raw
    try:
        parsed = urlparse(raw)
        scheme = parsed.scheme or "https"
        netloc = parsed.netloc
        if not netloc:
            return ""
        return f"{scheme}://{netloc}/api/relay"
    except Exception:
        return ""


def _pool_host_from_api_url(api_url: str) -> str:
    raw = str(api_url or "").strip()
    if not raw:
        return ""
    if "://" not in raw:
        raw = "https://" + raw
    try:
        parsed = urlparse(raw)
        return str(parsed.hostname or "").strip()
    except Exception:
        return ""


def _build_proxy_from_host_port(host: Any, port: Any, proxy_type: Any = "") -> str:
    host_value = str(host or "").strip()
    port_value = str(port or "").strip()
    if not host_value or not port_value:
        return ""
    proxy_type_value = str(proxy_type or "").strip().lower()
    if proxy_type_value in ("socks5", "socks", "shadowsocks"):
        return normalize_proxy_value(f"socks5://{host_value}:{port_value}")
    return normalize_proxy_value(f"http://{host_value}:{port_value}")


def extract_proxy_from_obj(obj: Any, relay_host: str = "") -> str:
    if isinstance(obj, str):
        return normalize_proxy_value(obj)

    if isinstance(obj, (list, tuple)):
        for item in obj:
            proxy = extract_proxy_from_obj(item, relay_host)
            if proxy:
                return proxy
        return ""

    if isinstance(obj, dict):
        local_port = obj.get("local_port")
        if local_port in (None, ""):
            local_port = obj.get("localPort")
        if local_port not in (None, ""):
            if relay_host:
                proxy = normalize_proxy_value(f"http://{relay_host}:{local_port}")
                if proxy:
                    return proxy
            proxy = normalize_proxy_value(f"http://127.0.0.1:{local_port}")
            if proxy:
                return proxy

        host = str(obj.get("ip") or obj.get("host") or obj.get("server") or "").strip()
        port = str(obj.get("port") or "").strip()
        proxy_type = obj.get("type") or obj.get("protocol") or obj.get("scheme") or ""
        if host and port:
            proxy = _build_proxy_from_host_port(host, port, proxy_type)
            if proxy:
                return proxy

        for key in ("proxy", "proxy_url", "url", "value", "result", "data", "proxy_list", "list", "proxies"):
            if key in obj:
                proxy = extract_proxy_from_obj(obj.get(key), relay_host)
                if proxy:
                    return proxy

        for value in obj.values():
            proxy = extract_proxy_from_obj(value, relay_host)
            if proxy:
                return proxy
    return ""


def proxy_tcp_reachable(proxy_url: str, timeout_seconds: float = 1.2) -> bool:
    value = str(proxy_url or "").strip()
    if not value:
        return False
    if "://" not in value:
        value = "http://" + value
    try:
        parsed = urlparse(value)
        host = parsed.hostname
        port = parsed.port
        if not host:
            return False
        if port is None:
            port = 1080 if parsed.scheme and parsed.scheme.startswith("socks") else 80
        with socket.create_connection((host, int(port)), timeout=timeout_seconds):
            return True
    except Exception:
        return False


def fetch_proxy_from_pool(
    pool_cfg: Dict[str, Any],
    timeout: int = 10,
    retries: int = 3,
    require_reachable: bool = False,
) -> str:
    api_url = str(pool_cfg.get("api_url") or DEFAULT_POOL_API_URL).strip() or DEFAULT_POOL_API_URL
    auth_mode = str(pool_cfg.get("auth_mode") or DEFAULT_POOL_AUTH_MODE).strip().lower() or DEFAULT_POOL_AUTH_MODE
    if auth_mode not in ("query", "header"):
        auth_mode = DEFAULT_POOL_AUTH_MODE
    api_key = str(pool_cfg.get("api_key") or "").strip()
    if not api_key:
        raise RuntimeError("代理池 API Key 为空")

    try:
        count = max(1, min(int(pool_cfg.get("count") or DEFAULT_POOL_COUNT), 20))
    except Exception:
        count = DEFAULT_POOL_COUNT
    country = str(pool_cfg.get("country") or DEFAULT_POOL_COUNTRY).strip().upper() or DEFAULT_POOL_COUNTRY
    relay_host = _pool_host_from_api_url(api_url)

    headers: Dict[str, str] = {"Accept": "application/json"}
    params: Dict[str, Any] = {"count": count, "country": country}
    if auth_mode == "query":
        params["api_key"] = api_key
    else:
        headers["Authorization"] = f"Bearer {api_key}"

    last_error = ""
    for i in range(max(1, retries)):
        try:
            resp = requests.get(api_url, params=params, headers=headers, timeout=timeout)
            if resp.status_code >= 400:
                last_error = _mask_sensitive(f"HTTP {resp.status_code}: {resp.text[:180]}")
                if i < retries - 1:
                    time.sleep(min(0.3 * (i + 1), 1.0))
                continue

            payload: Any
            try:
                payload = resp.json()
            except Exception:
                payload = resp.text

            proxy = extract_proxy_from_obj(payload, relay_host)
            if not proxy:
                last_error = _mask_sensitive(f"响应未解析到代理: {str(payload)[:180]}")
                if i < retries - 1:
                    time.sleep(min(0.3 * (i + 1), 1.0))
                continue

            if require_reachable and not proxy_tcp_reachable(proxy):
                last_error = f"代理不可达: {proxy}"
                if i < retries - 1:
                    time.sleep(min(0.3 * (i + 1), 1.0))
                    continue
            return proxy
        except Exception as exc:
            last_error = _mask_sensitive(str(exc))
            if i < retries - 1:
                time.sleep(min(0.3 * (i + 1), 1.0))

    raise RuntimeError(f"代理池取号失败: {_mask_sensitive(last_error or 'unknown error')}")


def parse_trace(text: str) -> Dict[str, str]:
    info: Dict[str, str] = {}
    for raw_line in str(text or "").splitlines():
        line = raw_line.strip()
        if not line or "=" not in line:
            continue
        k, v = line.split("=", 1)
        info[k.strip()] = v.strip()
    return info


def trace_via_proxy(proxy: str, timeout: int = 10) -> str:
    px = proxies_dict(proxy)
    if not px:
        raise RuntimeError("代理为空")
    resp = requests.get(DEFAULT_TRACE_URL, proxies=px, timeout=timeout)
    if resp.status_code != 200:
        raise RuntimeError(f"trace 请求失败: HTTP {resp.status_code}")
    return str(resp.text or "")


def trace_via_pool_relay(pool_cfg: Dict[str, Any], timeout: int = 10, retries: int = 2) -> str:
    api_url = str(pool_cfg.get("api_url") or DEFAULT_POOL_API_URL).strip() or DEFAULT_POOL_API_URL
    api_key = str(pool_cfg.get("api_key") or "").strip()
    if not api_key:
        raise RuntimeError("代理池 API Key 为空")
    country = str(pool_cfg.get("country") or DEFAULT_POOL_COUNTRY).strip().upper() or DEFAULT_POOL_COUNTRY

    relay_url = pool_relay_url_from_fetch_url(api_url)
    if not relay_url:
        raise RuntimeError("relay 地址解析失败")

    params = {
        "api_key": api_key,
        "url": DEFAULT_TRACE_URL,
        "country": country,
    }
    last_error = ""
    for i in range(max(1, retries)):
        try:
            resp = requests.get(relay_url, params=params, timeout=timeout)
            if resp.status_code == 200:
                return str(resp.text or "")
            last_error = _mask_sensitive(f"HTTP {resp.status_code}: {resp.text[:180]}")
        except Exception as exc:
            last_error = _mask_sensitive(str(exc))
        if i < retries - 1:
            time.sleep(min(0.3 * (i + 1), 1.0))
    raise RuntimeError(f"relay 请求失败: {_mask_sensitive(last_error or 'unknown error')}")


def is_location_supported(trace_text: str) -> Tuple[bool, str, str]:
    trace = parse_trace(trace_text)
    loc = str(trace.get("loc") or "").upper()
    ip = str(trace.get("ip") or "")
    if not loc:
        return False, "", ip
    return loc not in {"CN", "HK"}, loc, ip
