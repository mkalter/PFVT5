"""Utilities voor alerts websocket broadcasting en parsing."""

from __future__ import annotations

import asyncio
import json
import re
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple

from fastapi import WebSocket

# Globale container voor verbonden WebSocket clients
WS_CLIENTS: Set[WebSocket] = set()

# Queue waarin alerts geplaatst worden om naar clients te sturen
ALERTS_QUEUE: "asyncio.Queue[dict]" = asyncio.Queue()

# Optionele HMS lookup functie zodat deze module zelfstandig meldingen kan verrijken
HMS_LOOKUP: Optional[Callable[[Any], Tuple[Optional[str], str]]] = None


def configure_hms_lookup(func: Optional[Callable[[Any], Tuple[Optional[str], str]]]) -> None:
    """Registreer lookup functie voor HMS codes."""

    global HMS_LOOKUP
    HMS_LOOKUP = func


def _jsonable(value: Any) -> Any:
    """Zorg dat waarde JSON-serialiseerbaar wordt."""

    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(v) for v in value]
    return str(value)


def _normalize_int(value: Any) -> Optional[int]:
    """Zet waarde om naar int wanneer mogelijk."""

    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        try:
            return int(float(s))
        except (ValueError, TypeError):
            return None
    return None


def _normalize_hms_code(code: Any) -> Optional[str]:
    """Normaliseer HMS code naar 8-digit hex."""

    if code in (None, "", 0, "0"):
        return None
    s = str(code).strip()
    if not s:
        return None
    try:
        if s.startswith("0x") or s.startswith("0X"):
            return f"{int(s, 16):08X}"
        if re.fullmatch(r"[0-9A-Fa-f]{1,8}", s):
            return s.upper().rjust(8, "0")
        return f"{int(s, 10):08X}"
    except (ValueError, TypeError):
        cleaned = re.sub(r"[^0-9A-Fa-f]", "", s)
        if not cleaned:
            return None
        return cleaned.upper().rjust(8, "0")[:8]


def _lookup_hms(code: Any) -> Tuple[Optional[str], Optional[str]]:
    """Gebruik geregistreerde lookup om melding te verrijken."""

    normalized = _normalize_hms_code(code)
    if not normalized:
        return None, None
    if HMS_LOOKUP is None:
        return None, normalized
    try:
        msg, hex_code = HMS_LOOKUP(normalized)
    except Exception:
        return None, normalized
    return msg, hex_code or normalized


def _extract_hms_records(value: Any) -> Iterable[Any]:
    """Itereer over mogelijke HMS records in willekeurige structuur."""

    stack: List[Any] = [value]
    seen: Set[int] = set()
    while stack:
        current = stack.pop()
        if isinstance(current, (list, tuple, set)):
            obj_id = id(current)
            if obj_id in seen:
                continue
            seen.add(obj_id)
            stack.extend(list(current))
            continue
        if isinstance(current, dict):
            obj_id = id(current)
            if obj_id in seen:
                continue
            seen.add(obj_id)
            if any(k in current for k in ("ecode", "error_code", "err_code", "code", "hms_code")):
                yield current
            else:
                for key in ("items", "item", "list", "records", "errors", "alerts", "hms", "error", "data"):
                    if key in current:
                        stack.append(current[key])
        else:
            yield current


def _extract_print_error_records(value: Any) -> Iterable[Any]:
    """Itereer over print_error records."""

    stack: List[Any] = [value]
    seen: Set[int] = set()
    while stack:
        current = stack.pop()
        if isinstance(current, (list, tuple, set)):
            obj_id = id(current)
            if obj_id in seen:
                continue
            seen.add(obj_id)
            stack.extend(list(current))
            continue
        if isinstance(current, dict):
            obj_id = id(current)
            if obj_id in seen:
                continue
            seen.add(obj_id)
            if any(k in current for k in ("error_code", "code", "id")):
                yield current
            else:
                for key in ("items", "item", "list", "records", "errors", "alerts", "data"):
                    if key in current:
                        stack.append(current[key])
        else:
            yield current


def collect_hms_errors(
    device_id: str,
    hms_val: Any,
    *,
    printer_name: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Extraheer HMS fouten inclusief lookup beschrijving."""

    results: List[Dict[str, Any]] = []
    if hms_val in (None, "", [], {}):
        return results

    for record in _extract_hms_records(hms_val):
        raw = _jsonable(record)
        if isinstance(record, dict):
            code = (
                record.get("ecode")
                or record.get("error_code")
                or record.get("err_code")
                or record.get("code")
                or record.get("hms_code")
            )
            message = record.get("message") or record.get("msg") or record.get("intro")
            module = (
                record.get("module")
                or record.get("module_name")
                or record.get("source")
                or record.get("src")
            )
            severity = _normalize_int(
                record.get("level")
                or record.get("severity")
                or record.get("priority")
                or record.get("pri")
            )
        else:
            code = record
            message = None
            module = None
            severity = None

        lookup_msg, normalized_code = _lookup_hms(code)
        if not normalized_code and not message:
            continue

        result: Dict[str, Any] = {
            "type": "hms",
            "device_id": device_id,
            "code": normalized_code,
            "message": message or lookup_msg,
            "raw": raw,
        }
        if printer_name:
            result["printer_name"] = printer_name
        if module:
            result["module"] = str(module)
        if severity is not None:
            result["severity"] = severity
        if lookup_msg and message:
            result["lookup"] = lookup_msg
        results.append(result)

    return results


def collect_print_error_codes(
    device_id: str,
    print_error_val: Any,
    *,
    printer_name: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Extraheer print_error codes."""

    results: List[Dict[str, Any]] = []
    if print_error_val in (None, "", [], {}):
        return results

    for record in _extract_print_error_records(print_error_val):
        raw = _jsonable(record)
        if isinstance(record, dict):
            base_code = record.get("error_code") or record.get("code") or record.get("id")
            sub_code = record.get("sub_code") or record.get("subcode")
            message = record.get("message") or record.get("msg") or record.get("description")
            module = (
                record.get("module")
                or record.get("module_name")
                or record.get("source")
                or record.get("src")
            )
            severity = _normalize_int(
                record.get("level")
                or record.get("severity")
                or record.get("priority")
                or record.get("pri")
            )
        else:
            base_code = record
            sub_code = None
            message = None
            module = None
            severity = None

        if base_code in (None, "") and message is None:
            continue

        if base_code in (None, ""):
            code_str = None
        else:
            code_str = str(base_code)
            if sub_code not in (None, ""):
                code_str = f"{code_str}:{sub_code}"

        result: Dict[str, Any] = {
            "type": "print_error",
            "device_id": device_id,
            "code": code_str,
            "message": message,
            "raw": raw,
        }
        if printer_name:
            result["printer_name"] = printer_name
        if module:
            result["module"] = str(module)
        if severity is not None:
            result["severity"] = severity
        results.append(result)

    return results


def collect_alert_snapshot(
    device_id: str,
    *,
    printer_name: Optional[str] = None,
    hms_val: Any = None,
    print_error_val: Any = None,
) -> Optional[Dict[str, Any]]:
    """Maak een samengestelde snapshot met HMS en print_error info."""

    hms_errors = collect_hms_errors(device_id, hms_val, printer_name=printer_name)
    print_errors = collect_print_error_codes(device_id, print_error_val, printer_name=printer_name)

    if not hms_errors and not print_errors:
        return None

    snapshot: Dict[str, Any] = {
        "device_id": device_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "hms": hms_errors,
        "print_errors": print_errors,
    }
    if printer_name:
        snapshot["printer_name"] = printer_name
    return snapshot


def enqueue_alert_snapshot(snapshot: Dict[str, Any], *, loop: Optional[asyncio.AbstractEventLoop] = None) -> None:
    """Plaats alert snapshot in de queue (thread-safe)."""

    if not snapshot:
        return

    try:
        current_loop = asyncio.get_running_loop()
    except RuntimeError:
        current_loop = None

    target_loop = loop or current_loop
    if target_loop and target_loop.is_running():
        if target_loop is current_loop:
            target_loop.create_task(ALERTS_QUEUE.put(snapshot))
        else:
            asyncio.run_coroutine_threadsafe(ALERTS_QUEUE.put(snapshot), target_loop)
    else:
        try:
            ALERTS_QUEUE.put_nowait(snapshot)
        except Exception:
            pass


async def alerts_broadcaster() -> None:
    """Broadcast alerts naar alle WebSocket clients."""
    while True:
        msg = await ALERTS_QUEUE.get()
        dead = []
        payload = json.dumps(msg, separators=(",", ":"))
        for ws in list(WS_CLIENTS):
            try:
                await ws.send_text(payload)
            except Exception:
                dead.append(ws)
        for ws in dead:
            WS_CLIENTS.discard(ws)
            try:
                await ws.close()
            except Exception:
                pass
