"""Small J-Quants API client with refresh-token auth and pagination."""
from __future__ import annotations

import logging
import os
import time
from datetime import date, datetime
from typing import Any

import requests
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

BASE_URL = os.getenv("JQUANTS_API_BASE", "https://api.jquants.com/v1").rstrip("/")
AUTH_REFRESH_PATH = "/token/auth_refresh"
AUTH_USER_PATH = "/token/auth_user"

_ID_TOKEN: str | None = None
_V2_CLIENT: Any | None = None


def _opt(name: str) -> str:
    return os.getenv(name, "").strip()


_USE_V2 = _opt("JQUANTS_API_VERSION").lower() == "v2"


def normalize_code(code: Any) -> str:
    text = str(code or "").strip()
    if text.endswith(".T"):
        text = text[:-2]
    compact = "".join(ch for ch in text.upper() if ch.isalnum())
    if len(compact) >= 5 and compact[-1] == "0" and compact[:4].isalnum():
        return compact[:4]
    if len(compact) >= 4 and compact[:4].isalnum():
        return compact[:4]
    return compact or text


def _refresh_token_from_password() -> str | None:
    email = _opt("JQUANTS_EMAIL")
    password = _opt("JQUANTS_PASSWORD")
    if not email or not password:
        return None
    r = requests.post(f"{BASE_URL}{AUTH_USER_PATH}", json={"mailaddress": email, "password": password}, timeout=20)
    if r.status_code >= 400:
        raise RuntimeError(f"J-Quants refresh token auth failed status={r.status_code} body={r.text[:200]}")
    token = r.json().get("refreshToken")
    if token:
        logger.info("J-Quants refresh token obtained from email/password")
    return token


def get_id_token(force_refresh: bool = False) -> str:
    global _ID_TOKEN
    global _USE_V2
    if _USE_V2:
        _get_v2_client()
        return "v2-api-key"
    if _ID_TOKEN and not force_refresh:
        return _ID_TOKEN

    refresh_token = _opt("JQUANTS_REFRESH_TOKEN") or _opt("JQUANTS_API_KEY") or _refresh_token_from_password()
    if not refresh_token:
        raise RuntimeError("JQUANTS_REFRESH_TOKEN is not set")

    r = requests.post(f"{BASE_URL}{AUTH_REFRESH_PATH}", params={"refreshtoken": refresh_token}, timeout=20)
    if r.status_code >= 400:
        try:
            _get_v2_client()
            _USE_V2 = True
            logger.info("J-Quants V1 id token failed, but V2 API key client is available")
            return "v2-api-key"
        except Exception:
            pass
        raise RuntimeError(f"J-Quants id token failed status={r.status_code} body={r.text[:200]}")
    token = r.json().get("idToken")
    if not token:
        raise RuntimeError("J-Quants idToken missing in response")
    _ID_TOKEN = token
    logger.info("J-Quants id token ok")
    return token


def _api_key() -> str:
    return _opt("JQUANTS_API_KEY") or _opt("JQUANTS_REFRESH_TOKEN")


def _get_v2_client():
    global _V2_CLIENT
    if _V2_CLIENT is not None:
        return _V2_CLIENT
    api_key = _api_key()
    if not api_key:
        raise RuntimeError("JQUANTS_API_KEY is not set")
    import jquantsapi

    _V2_CLIENT = jquantsapi.ClientV2(api_key=api_key)
    return _V2_CLIENT


def get(path: str, params: dict[str, Any] | None = None, *, paginate: bool = True) -> dict[str, Any] | list[dict]:
    params = {k: v for k, v in (params or {}).items() if v is not None}
    rows: list[dict] = []
    pagination_key = None
    retried = False

    while True:
        req_params = dict(params)
        if pagination_key:
            req_params["pagination_key"] = pagination_key
        token = get_id_token(force_refresh=False)
        r = requests.get(
            f"{BASE_URL}{path}",
            headers={"Authorization": f"Bearer {token}"},
            params=req_params,
            timeout=40,
        )
        if r.status_code in {401, 403} and not retried:
            retried = True
            token = get_id_token(force_refresh=True)
            r = requests.get(
                f"{BASE_URL}{path}",
                headers={"Authorization": f"Bearer {token}"},
                params=req_params,
                timeout=40,
            )
        if r.status_code >= 400:
            raise RuntimeError(f"J-Quants GET {path} failed status={r.status_code} body={r.text[:300]}")
        data = r.json()
        if not paginate:
            return data
        key = _first_payload_key(data)
        if key:
            rows.extend(data.get(key) or [])
        pagination_key = data.get("pagination_key")
        if not pagination_key:
            break
        time.sleep(float(_opt("JQUANTS_SLEEP_SEC") or 0.2))
    return rows


def _first_payload_key(data: dict[str, Any]) -> str | None:
    for key in ("info", "daily_quotes", "statements"):
        if isinstance(data.get(key), list):
            return key
    for key, value in data.items():
        if isinstance(value, list):
            return key
    return None


def _fmt_date(value: str | date | None) -> str | None:
    if value is None:
        return None
    return value.isoformat() if hasattr(value, "isoformat") else str(value)


def get_listed_info(date: str | date | None = None) -> list[dict]:
    params = {"date": _fmt_date(date)}
    if _USE_V2:
        cli = _get_v2_client()
        return cli.get_eq_master(date=_fmt_date(date) or "").to_dict("records")
    try:
        return get("/listed/info", params=params, paginate=True)  # type: ignore[return-value]
    except Exception as e:
        logger.warning("J-Quants V1 listed/info failed; trying V2: %s", e)
        cli = _get_v2_client()
        df = cli.get_eq_master(date=_fmt_date(date) or "")
        return df.to_dict("records")


def get_daily_quotes(
    code: str | None = None,
    date: str | date | None = None,
    from_date: str | date | None = None,
    to_date: str | date | None = None,
) -> list[dict]:
    params = {
        "code": f"{normalize_code(code)}0" if code else None,
        "date": _fmt_date(date),
        "from": _fmt_date(from_date),
        "to": _fmt_date(to_date),
    }
    if _USE_V2:
        cli = _get_v2_client()
        return cli.get_eq_bars_daily(
            code=params["code"] or "",
            date_yyyymmdd=_fmt_date(date) or "",
            from_yyyymmdd=_fmt_date(from_date) or "",
            to_yyyymmdd=_fmt_date(to_date) or "",
        ).to_dict("records")
    try:
        return get("/prices/daily_quotes", params=params, paginate=True)  # type: ignore[return-value]
    except Exception as e:
        logger.warning("J-Quants V1 daily_quotes failed; trying V2: %s", e)
        cli = _get_v2_client()
        df = cli.get_eq_bars_daily(
            code=params["code"] or "",
            date_yyyymmdd=_fmt_date(date) or "",
            from_yyyymmdd=_fmt_date(from_date) or "",
            to_yyyymmdd=_fmt_date(to_date) or "",
        )
        return df.to_dict("records")


def get_statements(
    code: str | None = None,
    date: str | date | None = None,
    from_date: str | date | None = None,
    to_date: str | date | None = None,
) -> list[dict]:
    params = {
        "code": f"{normalize_code(code)}0" if code else None,
        "date": _fmt_date(date),
        "from": _fmt_date(from_date),
        "to": _fmt_date(to_date),
    }
    if _USE_V2:
        cli = _get_v2_client()
        if code:
            df = cli.get_fin_summary(code=params["code"] or "", date_yyyymmdd=_fmt_date(date) or "")
        elif from_date or to_date:
            df = cli.get_fin_summary_range(
                start_dt=_fmt_date(from_date) or "20080707",
                end_dt=_fmt_date(to_date) or _fmt_date(date) or datetime.now().date().isoformat(),
            )
        else:
            df = cli.get_fin_summary(code=params["code"] or "", date_yyyymmdd=_fmt_date(date) or "")
        return df.to_dict("records")
    try:
        return get("/fins/statements", params=params, paginate=True)  # type: ignore[return-value]
    except Exception as e:
        logger.warning("J-Quants V1 statements failed; trying V2: %s", e)
        cli = _get_v2_client()
        if code:
            df = cli.get_fin_summary(code=params["code"] or "", date_yyyymmdd=_fmt_date(date) or "")
        elif from_date or to_date:
            df = cli.get_fin_summary_range(
                start_dt=_fmt_date(from_date) or "20080707",
                end_dt=_fmt_date(to_date) or _fmt_date(date) or datetime.now().date().isoformat(),
            )
        else:
            df = cli.get_fin_summary(code=params["code"] or "", date_yyyymmdd=_fmt_date(date) or "")
        return df.to_dict("records")


def get_weekly_margin_interest(
    code: str | None = None,
    date: str | date | None = None,
    from_date: str | date | None = None,
    to_date: str | date | None = None,
) -> list[dict]:
    params = {
        "code": f"{normalize_code(code)}0" if code else None,
        "date": _fmt_date(date),
        "from": _fmt_date(from_date),
        "to": _fmt_date(to_date),
    }
    if _USE_V2:
        cli = _get_v2_client()
        return cli.get_mkt_margin_interest(
            code=params["code"] or "",
            date_yyyymmdd=_fmt_date(date) or "",
            from_yyyymmdd=_fmt_date(from_date) or "",
            to_yyyymmdd=_fmt_date(to_date) or "",
        ).to_dict("records")
    return get("/markets/weekly_margin_interest", params=params, paginate=True)  # type: ignore[return-value]


def get_daily_margin_interest(
    code: str | None = None,
    date: str | date | None = None,
    from_date: str | date | None = None,
    to_date: str | date | None = None,
) -> list[dict]:
    params = {
        "code": f"{normalize_code(code)}0" if code else None,
        "date": _fmt_date(date),
        "from": _fmt_date(from_date),
        "to": _fmt_date(to_date),
    }
    if _USE_V2:
        cli = _get_v2_client()
        if not code and not date and (from_date or to_date):
            return cli.get_mkt_margin_alert_range(
                start_dt=_fmt_date(from_date) or "20170101",
                end_dt=_fmt_date(to_date) or datetime.now().date().isoformat(),
            ).to_dict("records")
        return cli.get_mkt_margin_alert(
            code=params["code"] or "",
            date_yyyymmdd=_fmt_date(date) or "",
            from_yyyymmdd=_fmt_date(from_date) or "",
            to_yyyymmdd=_fmt_date(to_date) or "",
        ).to_dict("records")
    return get("/markets/daily_margin_interest", params=params, paginate=True)  # type: ignore[return-value]


def get_short_selling(
    sector33code: str | None = None,
    date: str | date | None = None,
    from_date: str | date | None = None,
    to_date: str | date | None = None,
) -> list[dict]:
    params = {
        "sector33code": sector33code,
        "date": _fmt_date(date),
        "from": _fmt_date(from_date),
        "to": _fmt_date(to_date),
    }
    if _USE_V2:
        cli = _get_v2_client()
        if not sector33code and not date and (from_date or to_date):
            return cli.get_mkt_short_ratio_range(
                start_dt=_fmt_date(from_date) or "20170101",
                end_dt=_fmt_date(to_date) or datetime.now().date().isoformat(),
            ).to_dict("records")
        return cli.get_mkt_short_ratio(
            sector_33_code=sector33code or "",
            date_yyyymmdd=_fmt_date(date) or "",
            from_yyyymmdd=_fmt_date(from_date) or "",
            to_yyyymmdd=_fmt_date(to_date) or "",
        ).to_dict("records")
    return get("/markets/short_selling", params=params, paginate=True)  # type: ignore[return-value]
