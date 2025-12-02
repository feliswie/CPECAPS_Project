from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

RUNS_DIR = Path("storage") / "runs"
LATEST_POINTER = RUNS_DIR / "latest.json"

DEVICE_ALIASES = [
    "device_id",
    "device id",
    "device nos",
    "device nos.",
    "device number",
    "iscout device id",
    "ivm/iscout device id",
]
DATE_ALIASES = [
    "last_sighted_date",
    "last sighted date",
    "last sighted",
    "last seen",
    "last disarmed date",
    "disarm date",
    "begin journey date",
]
LOCATION_ALIASES = [
    "last_sighted_location",
    "last sighted location",
    "last disarmed area",
    "destination",
    "location",
]
CODE_ALIASES = [
    "location_code",
    "location code",
    "site code",
    "loc code",
    "location",
]


def _ensure_directories() -> None:
    RUNS_DIR.mkdir(parents=True, exist_ok=True)


def _normalize_header(value) -> str:
    if value is None:
        return ""
    text = value if isinstance(value, str) else str(value)
    text = text.strip()
    if not text:
        return ""
    return " ".join(text.replace("_", " ").lower().split())


def _find_column(columns: Dict[str, str], aliases: List[str]) -> Optional[str]:
    for alias in aliases:
        normalized = _normalize_header(alias)
        if normalized in columns:
            return columns[normalized]
    return None


def _format_datetime(value) -> str:
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return ""
    return ts.strftime("%Y-%m-%d %H:%M:%S")


def extract_device_rows(workbook_path: Path) -> List[Dict[str, str]]:
    _ensure_directories()
    if not workbook_path.exists():
        return []

    xls = pd.ExcelFile(workbook_path)
    sheet_name = "MAIN" if "MAIN" in xls.sheet_names else xls.sheet_names[0]
    df = xls.parse(sheet_name=sheet_name)
    df = df.dropna(how="all")

    column_lookup = {_normalize_header(col): col for col in df.columns}
    device_col = _find_column(column_lookup, DEVICE_ALIASES)
    if not device_col:
        raise ValueError("Unable to find a device identifier column in MAIN sheet.")

    date_col = _find_column(column_lookup, DATE_ALIASES)
    location_col = _find_column(column_lookup, LOCATION_ALIASES)
    code_col = _find_column(column_lookup, CODE_ALIASES)

    devices: List[Dict[str, str]] = []
    for _, row in df.iterrows():
        raw_device = row.get(device_col)
        if pd.isna(raw_device):
            continue
        device_value = str(raw_device).strip()
        if not device_value:
            continue

        entry = {
            "Device_ID": device_value,
            "Last_Sighted_Date": _format_datetime(row.get(date_col)) if date_col else "",
            "Last_Sighted_Location": str(row.get(location_col)).strip() if location_col and not pd.isna(row.get(location_col)) else "",
            "Location_Code": str(row.get(code_col)).strip() if code_col and not pd.isna(row.get(code_col)) else "",
        }
        devices.append(entry)

    return devices


def _parse_device_datetime(value: str) -> Optional[datetime]:
    if not value:
        return None
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return None
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts.tz_convert(timezone.utc)


def compute_alerts(devices: List[Dict[str, str]]) -> Dict[str, List[Dict[str, object]]]:
    now = datetime.now(timezone.utc)
    alerts = {"urgent": [], "soft": []}

    for device in devices:
        ts = _parse_device_datetime(device.get("Last_Sighted_Date", ""))
        if not ts:
            continue
        days_inactive = (now - ts).days
        alert_entry = {
            "Device_ID": device.get("Device_ID"),
            "Last_Sighted": ts.strftime("%Y-%m-%d"),
            "Days_Inactive": days_inactive,
            "Location": device.get("Last_Sighted_Location", ""),
        }
        if days_inactive >= 7:
            alerts["urgent"].append(alert_entry)
        elif days_inactive >= 3:
            alerts["soft"].append(alert_entry)

    return alerts


def compute_stats(devices: List[Dict[str, str]], alerts: Dict[str, List[Dict[str, object]]]) -> Dict[str, object]:
    now = datetime.now(timezone.utc)
    ts = now.isoformat()
    active = 0
    for device in devices:
        dt = _parse_device_datetime(device.get("Last_Sighted_Date", ""))
        if dt and (now - dt).days < 3:
            active += 1
    return {
        "generated_at": ts,
        "total_devices": len(devices),
        "active_devices": active,
        "urgent_count": len(alerts.get("urgent", [])),
        "soft_count": len(alerts.get("soft", [])),
    }


def _write_json(path: Path, payload: object) -> None:
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def _read_json(path: Path, default):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return default


def persist_run(buffer, filename: str) -> Dict[str, object]:
    _ensure_directories()
    run_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    run_dir = RUNS_DIR / run_id
    run_dir.mkdir(parents=True, exist_ok=False)

    result_path = run_dir / filename
    buffer.seek(0)
    result_path.write_bytes(buffer.read())

    devices = extract_device_rows(result_path)
    alerts = compute_alerts(devices)
    stats = compute_stats(devices, alerts)

    _write_json(run_dir / "devices.json", devices)
    _write_json(run_dir / "alerts.json", alerts)
    _write_json(run_dir / "stats.json", stats)

    meta = {
        "run_id": run_id,
        "filename": filename,
        "result_path": str(result_path),
        "generated_at": stats["generated_at"],
        "row_count": len(devices),
    }
    _write_json(run_dir / "meta.json", meta)
    _write_json(LATEST_POINTER, {"run_id": run_id})

    buffer.seek(0)
    return meta


def _latest_run_dir() -> Optional[Path]:
    pointer = _read_json(LATEST_POINTER, None)
    if not pointer:
        return None
    run_id = pointer.get("run_id")
    if not run_id:
        return None
    run_dir = RUNS_DIR / run_id
    if not run_dir.exists():
        return None
    return run_dir


def load_latest_devices() -> List[Dict[str, str]]:
    run_dir = _latest_run_dir()
    if not run_dir:
        return []
    return _read_json(run_dir / "devices.json", [])


def load_latest_alerts() -> Dict[str, List[Dict[str, object]]]:
    run_dir = _latest_run_dir()
    if not run_dir:
        return {"urgent": [], "soft": []}
    return _read_json(run_dir / "alerts.json", {"urgent": [], "soft": []})


def load_latest_stats() -> Dict[str, object]:
    run_dir = _latest_run_dir()
    if not run_dir:
        return {
            "generated_at": None,
            "total_devices": 0,
            "active_devices": 0,
            "urgent_count": 0,
            "soft_count": 0,
        }
    return _read_json(run_dir / "stats.json", {
        "generated_at": None,
        "total_devices": 0,
        "active_devices": 0,
        "urgent_count": 0,
        "soft_count": 0,
    })


def load_latest_meta() -> Optional[Dict[str, object]]:
    run_dir = _latest_run_dir()
    if not run_dir:
        return None
    return _read_json(run_dir / "meta.json", None)


def get_latest_result_path() -> Optional[Path]:
    meta = load_latest_meta()
    if not meta:
        return None
    path = Path(meta.get("result_path", ""))
    if path.exists():
        return path
    return None