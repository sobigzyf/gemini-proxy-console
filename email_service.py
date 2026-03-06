"""
Local email service for self-contained GEMINI deployment.
"""

from pathlib import Path
import os
import random
import re
import string
import requests


def _load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip().lstrip("\ufeff")
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


class EmailService:
    """Temporary mail service backed by Cloudflare Worker."""

    def __init__(self) -> None:
        base_dir = Path(__file__).resolve().parent
        _load_env_file(base_dir / ".env")

        self.worker_domain = (os.getenv("WORKER_DOMAIN") or "").strip()
        self.email_domain = (os.getenv("EMAIL_DOMAIN") or "").strip()
        self.admin_password = (os.getenv("ADMIN_PASSWORD") or "").strip()

        missing = []
        if not self.worker_domain:
            missing.append("WORKER_DOMAIN")
        if not self.email_domain:
            missing.append("EMAIL_DOMAIN")
        if not self.admin_password:
            missing.append("ADMIN_PASSWORD")
        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

    def _base_url(self) -> str:
        if self.worker_domain.startswith(("http://", "https://")):
            return self.worker_domain.rstrip("/")
        return f"https://{self.worker_domain}"

    def _generate_random_name(self) -> str:
        letters1 = "".join(random.choices(string.ascii_lowercase, k=random.randint(4, 6)))
        numbers = "".join(random.choices(string.digits, k=random.randint(1, 3)))
        letters2 = "".join(random.choices(string.ascii_lowercase, k=random.randint(0, 5)))
        return letters1 + numbers + letters2

    def _sanitize_name(self, name: str) -> str:
        cleaned = re.sub(r"[^a-z0-9._-]", "", str(name or "").strip().lower())
        return cleaned[:64]

    def _create_email_by_name(self, name: str, domain: str | None = None):
        url = f"{self._base_url()}/admin/new_address"
        normalized_name = self._sanitize_name(name)
        normalized_domain = str(domain or self.email_domain).strip().lower()
        if not normalized_name or not normalized_domain:
            return None, None
        try:
            res = requests.post(
                url,
                json={
                    "enablePrefix": True,
                    "name": normalized_name,
                    "domain": normalized_domain,
                },
                headers={
                    "x-admin-auth": self.admin_password,
                    "Content-Type": "application/json",
                },
                timeout=10,
            )
            if res.status_code == 200:
                data = res.json()
                return data.get("jwt"), data.get("address")
            print(f"[-] 创建邮箱接口返回错误: {res.status_code} - {res.text}")
            return None, None
        except Exception as e:
            print(f"[-] 创建邮箱网络异常 ({url}): {e}")
            return None, None

    def create_email(self):
        """Create a temporary mailbox and return (jwt, address)."""
        return self._create_email_by_name(self._generate_random_name(), self.email_domain)

    def create_email_with_name(self, name: str, domain: str | None = None):
        """Create a mailbox by a specified local-part name."""
        return self._create_email_by_name(name, domain or self.email_domain)

    def fetch_first_email(self, jwt, timeout: int = 10):
        """Fetch the latest mail raw content for this mailbox token."""
        try:
            res = requests.get(
                f"{self._base_url()}/api/mails",
                params={"limit": 10, "offset": 0},
                headers={
                    "Authorization": f"Bearer {jwt}",
                    "Content-Type": "application/json",
                },
                timeout=timeout,
            )

            if res.status_code != 200:
                return None

            data = res.json()
            results = data.get("results") if isinstance(data, dict) else None
            if isinstance(results, list) and results:
                first = results[0] if isinstance(results[0], dict) else {}
                raw_email_content = first.get("raw")
                if isinstance(raw_email_content, str):
                    return raw_email_content
            return None
        except Exception as e:
            print(f"获取邮件失败: {e}")
            return None

