import os
from dataclasses import asdict, dataclass
from typing import Any, Dict, Optional
from urllib.parse import urlsplit, urlunsplit


def _env_str(name: str, default: str) -> str:
    return os.getenv(name, default).strip()


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return int(raw.strip())


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return float(raw.strip())


def _is_local_mode() -> bool:
    mode = (
        os.getenv("APP_ENV")
        or os.getenv("ENVIRONMENT")
        or os.getenv("APOLLO_ENV")
        or "local"
    )
    return mode.lower() in {"local", "dev", "development"}


def _redact_url(url: str) -> str:
    if "://" not in url:
        return "***"
    parts = urlsplit(url)
    if parts.scheme == "sqlite":
        return url
    host = parts.hostname or ""
    port = f":{parts.port}" if parts.port else ""
    user = parts.username or ""
    auth = f"{user}:***@" if user else ""
    netloc = f"{auth}{host}{port}" if host else "***"
    return urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))


@dataclass(frozen=True)
class AppConfig:
    app_env: str
    database_url: str
    config_override_enabled: bool
    config_lock_enabled: bool
    eis_min_entry_score: int
    portfolio_heat_hard_cap: float
    drawdown_halt_pct: float
    rotation_advantage_ratio_min: float
    cpas_target_usd: float
    data_provider_primary: str
    data_provider_fallback: str
    data_freshness_sla_seconds: int
    data_completeness_min_ratio: float
    calendar_session_start: str
    calendar_session_end: str

    def to_public_dict(self) -> Dict[str, Any]:
        payload = asdict(self)
        payload["database_url"] = _redact_url(self.database_url)
        return payload


LOCKED_PARAMETER_DEFAULTS: Dict[str, Any] = {
    "EIS_MIN_ENTRY_SCORE": 67,
    "PORTFOLIO_HEAT_HARD_CAP": 0.22,
    "DRAWDOWN_HALT_PCT": 0.12,
    "ROTATION_ADVANTAGE_RATIO_MIN": 1.20,
    "CPAS_TARGET_USD": 6.0,
}

_RUNTIME_CONFIG: Optional[AppConfig] = None


def _validate_clock_hhmm(label: str, value: str) -> Optional[str]:
    parts = value.split(":")
    if len(parts) != 2:
        return f"{label} must be HH:MM format."
    try:
        hh = int(parts[0])
        mm = int(parts[1])
    except ValueError:
        return f"{label} must contain numeric HH:MM values."
    if not (0 <= hh <= 23 and 0 <= mm <= 59):
        return f"{label} has invalid time values."
    return None


def _validate_config(cfg: AppConfig) -> None:
    errors: list[str] = []

    if not (0 <= cfg.eis_min_entry_score <= 100):
        errors.append("EIS_MIN_ENTRY_SCORE must be between 0 and 100.")
    if not (0 < cfg.portfolio_heat_hard_cap <= 1):
        errors.append("PORTFOLIO_HEAT_HARD_CAP must be in (0, 1].")
    if not (0 < cfg.drawdown_halt_pct <= 1):
        errors.append("DRAWDOWN_HALT_PCT must be in (0, 1].")
    if cfg.rotation_advantage_ratio_min < 1.0:
        errors.append("ROTATION_ADVANTAGE_RATIO_MIN must be >= 1.0.")
    if cfg.cpas_target_usd <= 0:
        errors.append("CPAS_TARGET_USD must be > 0.")
    if cfg.data_freshness_sla_seconds <= 0:
        errors.append("DATA_FRESHNESS_SLA_SECONDS must be > 0.")
    if not (0 < cfg.data_completeness_min_ratio <= 1):
        errors.append("DATA_COMPLETENESS_MIN_RATIO must be in (0, 1].")
    if not cfg.data_provider_primary:
        errors.append("DATA_PROVIDER_PRIMARY cannot be empty.")
    if not cfg.data_provider_fallback:
        errors.append("DATA_PROVIDER_FALLBACK cannot be empty.")

    start_error = _validate_clock_hhmm("CALENDAR_SESSION_START", cfg.calendar_session_start)
    if start_error:
        errors.append(start_error)
    end_error = _validate_clock_hhmm("CALENDAR_SESSION_END", cfg.calendar_session_end)
    if end_error:
        errors.append(end_error)

    if cfg.config_lock_enabled and not cfg.config_override_enabled:
        changed = []
        for env_name, default_value in LOCKED_PARAMETER_DEFAULTS.items():
            raw = os.getenv(env_name)
            if raw is None:
                continue
            if str(raw).strip() != str(default_value):
                changed.append(env_name)
        if changed:
            errors.append(
                "Locked parameters changed without override. "
                "Set CONFIG_OVERRIDE_ENABLED=true to allow this change: "
                + ", ".join(sorted(changed))
            )

    if errors:
        raise ValueError("Invalid configuration: " + " ".join(errors))


def load_config() -> AppConfig:
    app_env = (
        os.getenv("APP_ENV")
        or os.getenv("ENVIRONMENT")
        or os.getenv("APOLLO_ENV")
        or "local"
    )
    if _is_local_mode():
        database_url = _env_str("DATABASE_URL", "sqlite:///./apollo67.db")
    else:
        database_url = _env_str("DATABASE_URL", "")
        if not database_url:
            raise ValueError(
                "DATABASE_URL is required outside local mode "
                "(expected postgresql://... or postgres://...)."
            )

    cfg = AppConfig(
        app_env=app_env,
        database_url=database_url,
        config_override_enabled=_env_bool("CONFIG_OVERRIDE_ENABLED", False),
        config_lock_enabled=_env_bool("CONFIG_LOCK_ENABLED", True),
        eis_min_entry_score=_env_int("EIS_MIN_ENTRY_SCORE", 67),
        portfolio_heat_hard_cap=_env_float("PORTFOLIO_HEAT_HARD_CAP", 0.22),
        drawdown_halt_pct=_env_float("DRAWDOWN_HALT_PCT", 0.12),
        rotation_advantage_ratio_min=_env_float("ROTATION_ADVANTAGE_RATIO_MIN", 1.20),
        cpas_target_usd=_env_float("CPAS_TARGET_USD", 6.0),
        data_provider_primary=_env_str("DATA_PROVIDER_PRIMARY", "stub_primary"),
        data_provider_fallback=_env_str("DATA_PROVIDER_FALLBACK", "stub_fallback"),
        data_freshness_sla_seconds=_env_int("DATA_FRESHNESS_SLA_SECONDS", 300),
        data_completeness_min_ratio=_env_float("DATA_COMPLETENESS_MIN_RATIO", 0.98),
        calendar_session_start=_env_str("CALENDAR_SESSION_START", "09:30"),
        calendar_session_end=_env_str("CALENDAR_SESSION_END", "16:00"),
    )
    _validate_config(cfg)
    return cfg


def initialise_config() -> AppConfig:
    global _RUNTIME_CONFIG
    _RUNTIME_CONFIG = load_config()
    return _RUNTIME_CONFIG


def get_config() -> AppConfig:
    if _RUNTIME_CONFIG is None:
        return initialise_config()
    return _RUNTIME_CONFIG
