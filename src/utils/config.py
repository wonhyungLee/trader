import os
import re
import yaml
from typing import Any, Dict


_env_pattern = re.compile(r"\${([^}]+)}")


def _load_dotenv(path: str = ".env") -> None:
    """Minimal .env loader (no extra dependency).

    Existing environment variables are preserved; .env only fills missing keys.
    Lines starting with `#` are ignored and values may be quoted.
    """
    if not os.path.exists(path):
        return

    try:
        with open(path, "r", encoding="utf-8") as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                # keep already-exported values
                if key and key not in os.environ:
                    os.environ[key] = value
    except Exception:
        # .env loading failures should not break the app
        return


def _load_personal_env(path: str = "개인정보") -> None:
    """Load keys from the local 개인정보 file if present.

    The file is cloud-config style but contains KEY=\"...\" pairs; this loader
    fills missing env vars only and never prints secrets.
    """
    if not os.path.exists(path):
        return

    kv: Dict[str, str] = {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                if key:
                    kv[key] = value
                    if key not in os.environ:
                        os.environ[key] = value
    except Exception:
        return

    if "KIS_APP_KEY" not in os.environ:
        for i in range(1, 51):
            key = kv.get(f"KIS{i}_KEY")
            if key:
                os.environ["KIS_APP_KEY"] = key
                break

    if "KIS_APP_SECRET" not in os.environ:
        for i in range(1, 51):
            sec = kv.get(f"KIS{i}_SECRET")
            if sec:
                os.environ["KIS_APP_SECRET"] = sec
                break

    if "KIS_ACCOUNT_NO" not in os.environ:
        for i in range(1, 51):
            num = kv.get(f"KIS{i}_ACCOUNT_NUMBER")
            code = kv.get(f"KIS{i}_ACCOUNT_CODE")
            if num and code:
                os.environ["KIS_ACCOUNT_NO"] = num
                if "KIS_ACNT_PRDT_CD" not in os.environ:
                    os.environ["KIS_ACNT_PRDT_CD"] = code
                break


def _sub_env(value: str) -> str:
    """Replace ${VAR} with environment variable if present."""
    def repl(match: re.Match[str]) -> str:
        key = match.group(1)
        return os.environ.get(key, match.group(0))

    return _env_pattern.sub(repl, value)


def load_yaml(path: str) -> Dict[str, Any]:
    # Populate os.environ from .env and 개인정보 before substitution
    _load_dotenv()
    _load_personal_env()
    with open(path, "r", encoding="utf-8") as f:
        raw = f.read()
    # env substitution for string values
    substituted = _env_pattern.sub(lambda m: os.environ.get(m.group(1), m.group(0)), raw)
    data = yaml.safe_load(substituted) or {}
    return data


def load_settings(path: str = "config/settings.yaml") -> Dict[str, Any]:
    return load_yaml(path)
