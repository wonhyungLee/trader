from __future__ import annotations

import argparse
import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests

from src.autotrade.engine_adapter import recommend_daytrade_plan
from src.autotrade.info_loader import AutoTradeInfo, load_autotrade_info
from src.autotrade.payloads import build_limit_order, infer_exchange, infer_quote_currency
from src.autotrade.price_feed import fetch_current_price_us
from src.storage.sqlite_store import SQLiteStore, normalize_code
from src.utils.config import load_settings
from src.utils.notifier import maybe_notify
from src.utils.project_root import ensure_repo_root


LIST_SELECTED = "SELECTED"
LIST_EXIT = "EXIT"


@dataclass(frozen=True)
class AutoTradeConfig:
    enabled: bool
    send_enabled: bool
    db_path: str
    info_path: str
    webhook_url: str
    password: str
    kis_number: str
    default_amount: int
    poll_interval_sec: int
    optimize: bool
    optimize_lookback_bars: Optional[int]


def utc_ts() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


def load_autotrade_config(settings: dict) -> AutoTradeConfig:
    cfg = settings.get("autotrade", {}) or {}
    enabled = bool(cfg.get("enabled", False))
    send_enabled = bool(cfg.get("send_enabled", False))
    db_path = str((settings.get("database", {}) or {}).get("path", "data/market_data.db"))
    info_path = str(cfg.get("info_path") or "").strip()
    webhook_url = str(cfg.get("webhook_url") or "").strip()
    password = str(cfg.get("password") or "").strip()
    kis_number = str(cfg.get("kis_number") or "").strip()
    default_amount = int(cfg.get("default_amount") or 1)
    poll_interval_sec = int(cfg.get("poll_interval_sec") or 60)
    optimize = bool(cfg.get("optimize", True))
    lookback_raw = cfg.get("optimize_lookback_bars")
    try:
        optimize_lookback_bars = int(lookback_raw) if lookback_raw is not None else None
    except Exception:
        optimize_lookback_bars = None

    if info_path:
        info = load_autotrade_info(info_path)
        if not webhook_url and info.webhook_url:
            webhook_url = info.webhook_url
        if _is_placeholder(password) and info.password:
            password = info.password
        if _is_placeholder(kis_number) and info.kis_number:
            kis_number = info.kis_number

    return AutoTradeConfig(
        enabled=enabled,
        send_enabled=send_enabled,
        db_path=db_path,
        info_path=info_path,
        webhook_url=webhook_url,
        password=password,
        kis_number=kis_number,
        default_amount=max(1, default_amount),
        poll_interval_sec=max(10, poll_interval_sec),
        optimize=optimize,
        optimize_lookback_bars=optimize_lookback_bars,
    )


def _is_placeholder(value: str) -> bool:
    text = str(value or "").strip()
    return bool(text) and text.startswith("${") and text.endswith("}")


def _enrich_symbol(store: SQLiteStore, code: str) -> Tuple[str, str, str]:
    """Return (name, market, excd) for the code using watchlist/universe members."""
    code = normalize_code(code)
    row = store.conn.execute(
        "SELECT name, market, excd FROM autotrade_watchlist WHERE code=?",
        (code,),
    ).fetchone()
    name = (row[0] if row else "") or ""
    market = (row[1] if row else "") or ""
    excd = (row[2] if row else "") or ""
    if name and excd:
        return (str(name), str(market), str(excd))

    u = store.conn.execute(
        "SELECT name, market, excd FROM universe_members WHERE code=?",
        (code,),
    ).fetchone()
    if u:
        name = name or (u[0] or "")
        market = market or (u[1] or "")
        excd = excd or (u[2] or "")

    return (str(name or ""), str(market or ""), str(excd or ""))


def _ensure_plan_and_queue_for_code(store: SQLiteStore, cfg: AutoTradeConfig, code: str) -> Optional[str]:
    latest_date = store.last_price_date(code)
    if not latest_date:
        return None

    code = normalize_code(code)
    existing = store.conn.execute(
        "SELECT 1 FROM autotrade_plans WHERE asof_date=? AND code=?",
        (latest_date, code),
    ).fetchone()
    plan_row = None
    if existing:
        plan_row = store.conn.execute(
            "SELECT entry_price, target_price, stop_price, confidence, status, plan_json FROM autotrade_plans WHERE asof_date=? AND code=?",
            (latest_date, code),
        ).fetchone()

    if plan_row:
        entry_price = plan_row[0]
        target_price = plan_row[1]
        stop_price = plan_row[2]
        confidence = plan_row[3]
        status = plan_row[4] or ""
        plan_json = plan_row[5] or ""
    else:
        rec = recommend_daytrade_plan(
            db_path=cfg.db_path,
            code=code,
            optimize=cfg.optimize,
            optimize_lookback_bars=cfg.optimize_lookback_bars,
        )
        if not rec.get("ok"):
            logging.info("[autotrade] recommend failed for %s: %s", code, rec.get("error"))
            return None
        snap = rec.get("snapshot") or {}
        asof_date = str(snap.get("date") or latest_date)
        plan = rec.get("plan") or {}
        entry_price = plan.get("entry_price")
        target_price = plan.get("target_price")
        stop_price = plan.get("stop_price")
        confidence = rec.get("confidence")
        status = rec.get("status") or ""
        plan_json = json.dumps(rec, ensure_ascii=False, default=str)
        store.upsert_autotrade_plan(
            asof_date=asof_date,
            code=code,
            entry_price=float(entry_price) if entry_price is not None else None,
            target_price=float(target_price) if target_price is not None else None,
            stop_price=float(stop_price) if stop_price is not None else None,
            confidence=float(confidence) if confidence is not None else None,
            status=str(status),
            plan_json=plan_json,
        )
        latest_date = asof_date

    # Queue payloads (idempotent upsert)
    name, market, excd = _enrich_symbol(store, code)
    exchange = infer_exchange(excd, code)
    quote = infer_quote_currency(code)
    order_name = f"{name or code} 매매"

    info = AutoTradeInfo(webhook_url=cfg.webhook_url, password=cfg.password, kis_number=cfg.kis_number)
    if not info.webhook_url or not info.password or not info.kis_number:
        logging.warning("[autotrade] missing webhook config (url/password/kis_number). url=%s", bool(info.webhook_url))
        return latest_date

    if entry_price is not None:
        payload = build_limit_order(
            password=info.password,
            exchange=exchange,
            base=code,
            quote=quote,
            side="buy",
            amount=cfg.default_amount,
            price=float(entry_price),
            order_name=order_name,
            kis_number=info.kis_number,
        )
        store.upsert_autotrade_queue(
            asof_date=latest_date,
            code=code,
            side="BUY",
            trigger_price=float(payload.get("price") or 0),
            trigger_rule="<=",
            webhook_url=info.webhook_url,
            payload_json=json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=str),
        )

    if target_price is not None:
        payload = build_limit_order(
            password=info.password,
            exchange=exchange,
            base=code,
            quote=quote,
            side="sell",
            amount=cfg.default_amount,
            price=float(target_price),
            order_name=order_name,
            kis_number=info.kis_number,
        )
        store.upsert_autotrade_queue(
            asof_date=latest_date,
            code=code,
            side="SELL",
            trigger_price=float(payload.get("price") or 0),
            trigger_rule=">=",
            webhook_url=info.webhook_url,
            payload_json=json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=str),
        )

    # Store any missing metadata back to watchlist row for UI.
    store.upsert_autotrade_watchlist(
        code,
        name=name,
        market=market,
        excd=excd,
        list_type=(store.conn.execute("SELECT list_type FROM autotrade_watchlist WHERE code=?", (code,)).fetchone() or ["SELECTED"])[0],
        enabled=True,
    )

    return latest_date


def _latest_queue_asof_per_code(store: SQLiteStore) -> Dict[str, str]:
    rows = store.conn.execute("SELECT code, MAX(asof_date) FROM autotrade_queue GROUP BY code").fetchall()
    out: Dict[str, str] = {}
    for r in rows:
        if r and r[0] and r[1]:
            out[str(r[0]).upper()] = str(r[1])
    return out


def _list_dispatch_candidates(store: SQLiteStore) -> List[Dict[str, Any]]:
    # Only consider latest asof_date per code to avoid stale PENDINGs.
    sql = """
    WITH latest AS (
      SELECT code, MAX(asof_date) AS max_asof
      FROM autotrade_queue
      GROUP BY code
    )
    SELECT q.id, q.asof_date, q.code, q.side, q.trigger_price, q.trigger_rule,
           q.webhook_url, q.payload_json, q.status,
           w.list_type, w.enabled, w.name, w.market, w.excd
    FROM autotrade_queue q
    JOIN latest l
      ON q.code = l.code AND q.asof_date = l.max_asof
    JOIN autotrade_watchlist w
      ON q.code = w.code
    WHERE q.status='PENDING' AND w.enabled=1
    ORDER BY q.asof_date DESC, q.code ASC, q.side ASC, q.id ASC
    """
    rows = store.conn.execute(sql).fetchall()
    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "id": r[0],
                "asof_date": r[1],
                "code": str(r[2]).upper(),
                "side": str(r[3]).upper(),
                "trigger_price": r[4],
                "trigger_rule": r[5],
                "webhook_url": r[6],
                "payload_json": r[7],
                "status": r[8],
                "list_type": str(r[9]).upper() if r[9] else "",
                "enabled": int(r[10] or 0),
                "name": r[11] or "",
                "market": r[12] or "",
                "excd": r[13] or "",
            }
        )
    return out


def _should_dispatch(list_type: str, side: str) -> bool:
    side = str(side or "").upper()
    list_type = str(list_type or "").upper()
    if side == "BUY":
        return list_type == LIST_SELECTED
    if side == "SELL":
        return list_type == LIST_EXIT
    return False


def _trigger_met(rule: str, price: float, trigger_price: float) -> bool:
    if rule == "<=":
        return price <= trigger_price
    if rule == ">=":
        return price >= trigger_price
    return False


def _claim_pending(store: SQLiteStore, order_id: int) -> bool:
    now = utc_ts()
    cur = store.conn.execute(
        """
        UPDATE autotrade_queue
        SET status='SENDING',
            attempt_count=COALESCE(attempt_count, 0) + 1,
            last_attempt_at=?,
            updated_at=?
        WHERE id=? AND status='PENDING'
        """,
        (now, now, int(order_id)),
    )
    store.conn.commit()
    return int(cur.rowcount or 0) == 1


def _mark_result(
    store: SQLiteStore,
    *,
    order_id: int,
    status: str,
    http_status: Optional[int] = None,
    response_text: str = "",
    error_text: str = "",
) -> None:
    now = utc_ts()
    store.conn.execute(
        """
        UPDATE autotrade_queue
        SET status=?,
            sent_at=CASE WHEN ?='SENT' THEN ? ELSE sent_at END,
            response_text=?,
            last_error=?,
            updated_at=?
        WHERE id=?
        """,
        (
            str(status).upper(),
            str(status).upper(),
            now,
            (response_text or "")[:8000],
            (error_text or "")[:8000],
            now,
            int(order_id),
        ),
    )
    store.conn.commit()

    # Best-effort event log
    row = store.conn.execute(
        "SELECT asof_date, code, side, webhook_url, payload_json FROM autotrade_queue WHERE id=?",
        (int(order_id),),
    ).fetchone()
    if row:
        store.insert_autotrade_event(
            ts=now,
            asof_date=str(row[0]),
            code=str(row[1]),
            side=str(row[2]),
            status=str(status).upper(),
            http_status=http_status,
            webhook_url=str(row[3] or ""),
            payload_json=str(row[4] or ""),
            response_text=response_text or "",
            error_text=error_text or "",
        )


def _send_webhook(url: str, payload: Dict[str, Any]) -> Tuple[bool, Optional[int], str]:
    resp = requests.post(url, json=payload, timeout=(4, 12))
    ok = bool(resp.ok)
    body = (resp.text or "").strip()
    return ok, int(resp.status_code), body


def _to_float(value) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(str(value).replace(",", "").strip())
    except Exception:
        return None


def _fetch_current_prices_kr(settings: dict, codes: List[str]) -> Dict[str, Dict[str, Any]]:
    """Fetch KR current prices via KIS multi-price (best-effort)."""
    out: Dict[str, Dict[str, Any]] = {}
    if not codes:
        return out
    try:
        from src.brokers.kis_broker import KISBroker
        broker = KISBroker(settings)
    except Exception as exc:
        logging.warning("[autotrade] KISBroker unavailable: %s", exc)
        return out

    now = utc_ts()
    # KIS multi endpoint supports up to 30 symbols.
    for i in range(0, len(codes), 30):
        batch = [str(c).strip().zfill(6) for c in codes[i : i + 30] if str(c).strip()]
        if not batch:
            continue
        try:
            res = broker.get_multi_price(batch)
            outputs = res.get("output") or res.get("output1") or []
            if not isinstance(outputs, list):
                continue
            for rec in outputs:
                code = rec.get("inter_shrn_iscd") or rec.get("stck_shrn_iscd") or rec.get("iscd") or rec.get("code")
                if not code:
                    continue
                code = str(code).strip().zfill(6)
                price = rec.get("inter2_prpr") or rec.get("stck_prpr") or rec.get("prpr") or rec.get("price")
                out[code] = {"code": code, "price": _to_float(price), "asof": now, "source": "kis"}
        except Exception as exc:
            logging.warning("[autotrade] KR price batch failed (%s..): %s", batch[0], exc)
    return out


def run_cycle(store: SQLiteStore, settings: dict, cfg: AutoTradeConfig, *, dry_run: bool = False) -> None:
    if not cfg.enabled:
        return

    watch = store.list_autotrade_watchlist(enabled_only=True)
    for row in watch:
        code = str(row["code"] or "").strip().upper()
        if not code:
            continue
        _ensure_plan_and_queue_for_code(store, cfg, code)

    candidates = _list_dispatch_candidates(store)
    if not candidates:
        return

    # Price fetch once per code
    by_code: Dict[str, List[Dict[str, Any]]] = {}
    for ev in candidates:
        by_code.setdefault(ev["code"], []).append(ev)

    all_codes = list(by_code.keys())
    kr_codes = [c for c in all_codes if str(c).isdigit()]
    us_codes = [c for c in all_codes if not str(c).isdigit()]
    price_map: Dict[str, Dict[str, Any]] = {}
    if kr_codes:
        price_map.update(_fetch_current_prices_kr(settings, kr_codes))
    for c in us_codes:
        price_map[c] = fetch_current_price_us(c)

    for code, items in by_code.items():
        price_rec = price_map.get(code) or {}
        price = price_rec.get("price")
        if price is None:
            continue

        for it in items:
            if not _should_dispatch(it.get("list_type", ""), it.get("side", "")):
                continue
            tp = it.get("trigger_price")
            if tp is None:
                continue
            try:
                tp_f = float(tp)
                price_f = float(price)
            except Exception:
                continue
            if not _trigger_met(str(it.get("trigger_rule") or ""), price_f, tp_f):
                continue

            if not cfg.send_enabled or dry_run:
                logging.info("[autotrade] DRY send %s %s at price=%s trigger=%s%s", code, it.get("side"), price_f, it.get("trigger_rule"), tp_f)
                continue

            if not _claim_pending(store, int(it["id"])):
                continue

            payload_raw = it.get("payload_json") or ""
            url = str(it.get("webhook_url") or "").strip()
            if not url:
                _mark_result(store, order_id=int(it["id"]), status="ERROR", error_text="webhook_url_missing")
                continue
            try:
                payload = json.loads(payload_raw) if payload_raw else {}
            except Exception:
                payload = {}
            try:
                ok, http_status, body = _send_webhook(url, payload)
                if ok:
                    _mark_result(store, order_id=int(it["id"]), status="SENT", http_status=http_status, response_text=body)
                    maybe_notify(settings, f"[autotrade] SENT {code} {it.get('side')} trigger={it.get('trigger_rule')}{tp_f} price={price_f} http={http_status}")
                else:
                    _mark_result(store, order_id=int(it["id"]), status="ERROR", http_status=http_status, response_text=body, error_text=f"http_{http_status}")
            except Exception as exc:
                _mark_result(store, order_id=int(it["id"]), status="ERROR", error_text=str(exc))


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="autotrade_worker", description="Webhook auto-trade worker (plan -> queue -> dispatch).")
    p.add_argument("--once", action="store_true", help="Run one cycle and exit")
    p.add_argument("--dry-run", action="store_true", help="Do not send webhook even if send_enabled=true")
    return p


def main() -> None:
    ensure_repo_root()
    os.makedirs("logs", exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.FileHandler("logs/autotrade_worker.log"), logging.StreamHandler()],
    )

    args = build_parser().parse_args()
    settings = load_settings()
    cfg = load_autotrade_config(settings)
    if not cfg.enabled:
        raise SystemExit("autotrade disabled (config.settings.yaml autotrade.enabled=false)")

    store = SQLiteStore(cfg.db_path)
    try:
        while True:
            try:
                run_cycle(store, settings, cfg, dry_run=bool(args.dry_run))
            except Exception:
                logging.exception("[autotrade] cycle failed")
            if args.once:
                break
            time.sleep(float(cfg.poll_interval_sec))
    finally:
        try:
            store.conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
