#!/usr/bin/env python3
"""
Queue Service v3.2 - Production Ready with Auto Re-Assignment

CHANGES vs v3.1:
- ✅ Automatic job re-assignment after tag cleanup
- ✅ Manual trigger endpoint /api/queue/reassign
- ✅ Smart pending job detection
- ✅ Prevents stuck PENDING jobs
"""

import os, re, json, uuid, pathlib, subprocess, logging, shutil, zipfile, tempfile
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Tuple, Any, Set

from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import sqlite3
import uvicorn
from urllib.request import urlopen, Request
from urllib.error import URLError
import asyncio

# Import upload manager + matching helpers
from printer_uploader import get_upload_manager, files_match, normalize_filename

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s"
)
logger = logging.getLogger("queue_service")

# ============ Config ============
QUEUE_HOST = os.environ.get("QUEUE_HOST", "0.0.0.0")
QUEUE_PORT = int(os.environ.get("QUEUE_PORT", "8010"))
DATA_DIR = pathlib.Path(os.environ.get("DATA_DIR", "./data")).resolve()
QUEUE_DIR = DATA_DIR / "queue"
DB_PATH = DATA_DIR / "queue.db"

FARM_API_BASE = os.environ.get("FARM_API_BASE", "http://127.0.0.1:8000")

# Bambu Studio CLI
BAMBU_BIN_ENV = os.environ.get("BAMBUSTUDIO_CLI", r"C:\\Program Files\\Bambu Studio\\bambu-studio.exe")

BASE_DIR = pathlib.Path(__file__).parent.resolve()
PROFILES_DIR = BASE_DIR / "profiles"
MACHINE_JSON = os.environ.get("BAMBU_MACHINE_JSON", str(PROFILES_DIR / "machine.json"))
PROCESS_JSON = os.environ.get("BAMBU_PROCESS_JSON", str(PROFILES_DIR / "process.json"))
FILAMENT_JSON = os.environ.get("BAMBU_FILAMENT_JSON", str(PROFILES_DIR / "filament.json"))
LOCAL_TZ = os.environ.get("LOCAL_TZ", "Europe/Amsterdam")

AUTO_UPLOAD = os.environ.get("AUTO_UPLOAD", "true").lower() in ("true", "1", "yes")

TERMINAL_STATES = {"COMPLETED", "FAILED", "CANCELLED"}
SERVER_API = os.getenv("SERVER_API", "http://127.0.0.1:8000").rstrip("/")

# ============ Utilities ============
def ensure_dirs():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    QUEUE_DIR.mkdir(parents=True, exist_ok=True)

def db_conn():
    con = sqlite3.connect(DB_PATH, timeout=30.0)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute("PRAGMA synchronous=NORMAL;")
    con.execute("PRAGMA busy_timeout=30000;")
    con.isolation_level = "IMMEDIATE"
    return con

def _table_has_column(cur: sqlite3.Cursor, table: str, column: str) -> bool:
    try:
        rows = cur.execute(f"PRAGMA table_info({table})").fetchall()
        return any((r[1] if isinstance(r, tuple) else r["name"]).lower() == column.lower() for r in rows)
    except Exception:
        return False

def init_db():
    con = db_conn()
    cur = con.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS queue_jobs (
            id TEXT PRIMARY KEY,
            device_id TEXT,
            printer_model TEXT,
            filename TEXT NOT NULL,
            filepath TEXT NOT NULL,
            size_bytes INTEGER NOT NULL,
            status TEXT NOT NULL,
            job_tag TEXT,
            created_at TEXT NOT NULL,
            filament_mm REAL DEFAULT 0,
            filament_g  REAL DEFAULT 0,
            error TEXT,
            uploaded_at TEXT,
            started_at TEXT,
            completed_at TEXT,
            retry_count INTEGER DEFAULT 0
        )
        """
    )

    migrations = [
        ("job_tag", "ALTER TABLE queue_jobs ADD COLUMN job_tag TEXT"),
        ("filament_mm", "ALTER TABLE queue_jobs ADD COLUMN filament_mm REAL DEFAULT 0"),
        ("filament_g", "ALTER TABLE queue_jobs ADD COLUMN filament_g REAL DEFAULT 0"),
        ("error", "ALTER TABLE queue_jobs ADD COLUMN error TEXT"),
        ("uploaded_at", "ALTER TABLE queue_jobs ADD COLUMN uploaded_at TEXT"),
        ("started_at", "ALTER TABLE queue_jobs ADD COLUMN started_at TEXT"),
        ("completed_at", "ALTER TABLE queue_jobs ADD COLUMN completed_at TEXT"),
        ("retry_count", "ALTER TABLE queue_jobs ADD COLUMN retry_count INTEGER DEFAULT 0"),
    ]

    for col, ddl in migrations:
        if not _table_has_column(cur, "queue_jobs", col):
            try:
                cur.execute(ddl)
                logger.info(f"Added column: {col}")
            except Exception as e:
                logger.warning(f"Could not add column {col}: {e}")

    con.commit()
    con.close()

def now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def safe_name(name: str) -> str:
    name = name or "upload"
    return re.sub(r"[^A-Za-z0-9._-]+", "_", name)

def _norm_tag(s: str) -> str:
    return re.sub(r"[^a-z0-9_-]+", "-", (s or "").strip().lower())

# ============ Job Management ============
def job_insert(
    filename: str,
    filepath: pathlib.Path,
    size: int,
    job_tag: Optional[str],
    filament_mm: float = 0.0,
    filament_g: float = 0.0,
) -> Dict:
    """Create new job with status PENDING"""
    job_id = f"job-{uuid.uuid4().hex[:12]}"
    created = now_iso()
    normalized_tag = _norm_tag(job_tag or "") if job_tag else None

    con = db_conn()
    cur = con.cursor()
    cur.execute(
        """
        INSERT INTO queue_jobs
        (id, device_id, filename, filepath, size_bytes, status, job_tag,
         created_at, filament_mm, filament_g, retry_count)
        VALUES (?, 'auto', ?, ?, ?, 'PENDING', ?, ?, ?, ?, 0)
        """,
        (
            job_id,
            filename,
            str(filepath),
            int(size),
            normalized_tag,
            created,
            float(filament_mm),
            float(filament_g),
        ),
    )
    con.commit()
    con.close()

    logger.info(f"Job {job_id} created (tag={normalized_tag}, status=PENDING)")

    return {
        "id": job_id,
        "device_id": "auto",
        "filename": filename,
        "filepath": str(filepath),
        "size_bytes": int(size),
        "status": "PENDING",
        "job_tag": normalized_tag,
        "created_at": created,
        "filament_mm": float(filament_mm),
        "filament_g": float(filament_g),
        "retry_count": 0,
    }

def job_list(
    device_id: Optional[str] = None, tag: Optional[str] = None, status: Optional[str] = None
) -> List[Dict]:
    con = db_conn()
    cur = con.cursor()

    conditions = []
    params: List[Any] = []

    if device_id:
        conditions.append("device_id=?")
        params.append(device_id)
    if tag:
        norm_tag = _norm_tag(tag)
        if norm_tag:
            conditions.append("job_tag=?")
            params.append(norm_tag)
        else:
            conditions.append("job_tag IS NULL")
    if status:
        conditions.append("status=?")
        params.append(status)

    where = " AND ".join(conditions) if conditions else "1=1"
    query = f"SELECT * FROM queue_jobs WHERE {where} ORDER BY created_at DESC"

    rows = cur.execute(query, params).fetchall()
    con.close()
    return [dict(r) for r in rows]

def job_get(job_id: str) -> Optional[Dict]:
    con = db_conn()
    cur = con.cursor()
    row = cur.execute("SELECT * FROM queue_jobs WHERE id=?", (job_id,)).fetchone()
    con.close()
    return dict(row) if row else None

def _cleanup_tag_if_no_jobs(tag: str) -> None:
    """Verwijder printer-tag als er geen jobs meer zijn voor deze tag."""
    if not tag:
        return
    try:
        con = db_conn()
        cur = con.cursor()
        total = cur.execute("SELECT COUNT(*) FROM queue_jobs WHERE job_tag=?", (tag,)).fetchone()[0]
        con.close()
    except Exception as e:
        logger.warning(f"Tag cleanup check mislukt voor '{tag}': {e}")
        return

    if total == 0:
        try:
            import requests
            resp = requests.delete(f"{SERVER_API}/api/config/printer-tags/{tag}", timeout=5)
            if resp.ok:
                logger.info(f"Tag cleanup (no jobs left): '{tag}' verwijderd.")
            else:
                logger.warning(f"Tag cleanup: kon '{tag}' niet verwijderen: HTTP {resp.status_code}")
        except Exception as e:
            logger.warning(f"Tag cleanup request fout voor '{tag}': {e}")

def job_delete(job_id: str) -> bool:
    con = db_conn()
    cur = con.cursor()
    row = cur.execute("SELECT filepath, job_tag FROM queue_jobs WHERE id=?", (job_id,)).fetchone()
    if not row:
        con.close()
        return False
    filepath = pathlib.Path(row["filepath"]) if isinstance(row, sqlite3.Row) else pathlib.Path(row[0])
    job_tag = (row["job_tag"] if isinstance(row, sqlite3.Row) else row[1]) or None
    cur.execute("DELETE FROM queue_jobs WHERE id=?", (job_id,))
    con.commit()
    con.close()
    try:
        filepath.unlink(missing_ok=True)
        logger.info(f"Deleted job {job_id}")
    except Exception as e:
        logger.warning(f"Could not delete file for job {job_id}: {e}")
    
    try:
        _cleanup_tag_if_no_jobs(job_tag)
    except Exception as e:
        logger.warning(f"job_delete: tag-cleanup fout voor '{job_tag}': {e}")
    return True

def queue_stats() -> Dict:
    """Queue statistics per status"""
    con = db_conn()
    cur = con.cursor()
    rows = cur.execute(
        """
        SELECT status, COUNT(*) as cnt
        FROM queue_jobs
        GROUP BY status
        """
    ).fetchall()

    per_device_rows = cur.execute(
        """
        SELECT device_id, COUNT(*) as cnt
        FROM queue_jobs
        WHERE status IN ('READY','UPLOADING','PRINTING')
          AND device_id NOT IN ('', 'auto')
        GROUP BY device_id
        """
    ).fetchall()
    con.close()

    stats = {r["status"]: r["cnt"] for r in rows}
    total = sum(stats.values())

    per_device = {r["device_id"]: r["cnt"] for r in per_device_rows if r["device_id"]}

    return {
        "total": total,
        "pending": stats.get("PENDING", 0),
        "ready": stats.get("READY", 0),
        "uploading": stats.get("UPLOADING", 0),
        "uploaded": stats.get("UPLOADED", 0),
        "printing": stats.get("PRINTING", 0),
        "completed": stats.get("COMPLETED", 0),
        "failed": stats.get("FAILED", 0),
        "by_status": stats,
        "per_device": per_device,
    }

# ============ Timezone helpers ============
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

def _local_tz():
    if ZoneInfo is None:
        return timezone.utc
    try:
        return ZoneInfo(LOCAL_TZ)
    except Exception:
        return timezone.utc

def _to_utc_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def _range_start_iso(range_key: str) -> Optional[str]:
    """Get ISO8601 UTC start time for given local range"""
    rk = (range_key or "day").lower()
    now_local = datetime.now(_local_tz())

    if rk == "day":
        start_local = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    elif rk == "week":
        start_of_week = now_local - timedelta(days=now_local.weekday())
        start_local = start_of_week.replace(hour=0, minute=0, second=0, microsecond=0)
    elif rk == "month":
        start_local = now_local.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    else:
        return None

    return _to_utc_iso(start_local)

def _filament_summary(range_key: str = "day") -> Dict[str, Any]:
    """Filament usage summary for given time range"""
    start_iso = _range_start_iso(range_key)
    only_printed = os.environ.get("FILAMENT_ONLY_PRINTED", "false").lower() in ("1", "true", "yes")

    status_filter = ("COMPLETED",) if only_printed else (
        "PENDING", "READY", "UPLOADING", "UPLOADED", "PRINTING", "COMPLETED"
    )

    where_parts: List[str] = [
        "status IN (" + ",".join("?" for _ in status_filter) + ")"
    ]
    params: List[Any] = list(status_filter)

    if start_iso:
        where_parts.append("COALESCE(completed_at, created_at) >= ?")
        params.append(start_iso)

    where = " AND ".join(where_parts)
    params_tuple = tuple(params)

    con = db_conn()
    cur = con.cursor()

    total_row = cur.execute(
        f"SELECT COALESCE(SUM(filament_mm),0) AS mm, COALESCE(SUM(filament_g),0) AS g FROM queue_jobs WHERE {where}",
        params_tuple,
    ).fetchone()

    per_rows = cur.execute(
        f"SELECT device_id, COALESCE(SUM(filament_mm),0) AS mm, COALESCE(SUM(filament_g),0) AS g FROM queue_jobs WHERE {where} AND device_id NOT IN ('', 'auto') GROUP BY device_id",
        params_tuple,
    ).fetchall()
    con.close()

    name_map = {p["device_id"]: (p.get("name") or p["device_id"]) for p in get_printers_config()}

    return {
        "range": (range_key or "day").lower(),
        "printed": {
            "mm": float(total_row["mm"] if total_row else 0.0),
            "g": float(total_row["g"] if total_row else 0.0),
        },
        "per_printer": [
            {
                "device_id": r["device_id"],
                "name": name_map.get(r["device_id"], r["device_id"]),
                "mm": float(r["mm"]),
                "g": float(r["g"]),
            }
            for r in per_rows
        ],
    }

# ============ External API ============
def fetch_json(url: str, timeout: float = 3.0):
    try:
        req = Request(url, headers={"User-Agent": "printfarm-queue/3.2"})
        with urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8"))
    except Exception as e:
        logger.warning(f"Could not fetch {url}: {e}")
        return None

def get_printers_config() -> List[Dict]:
    url = f"{FARM_API_BASE.rstrip('/')}/api/config/printers"
    data = fetch_json(url) or []
    out = []
    for p in data if isinstance(data, list) else []:
        tags = p.get("tags", [])
        if isinstance(tags, str):
            tags = [t.strip() for t in tags.split(",") if t.strip()]
        tags = [_norm_tag(t) for t in tags if _norm_tag(t)]
        out.append(
            {
                "device_id": p.get("device_id", ""),
                "name": p.get("name", ""),
                "model": p.get("model", ""),
                "ip": p.get("ip", ""),
                "lan_access_code": p.get("lan_access_code", ""),
                "autoprint": bool(p.get("autoprint")),
                "tags": tags,
            }
        )
    return [p for p in out if p["device_id"]]

def known_printer_tags() -> Set[str]:
    """Return set of all configured printer tags (lowercase)."""
    tags: Set[str] = set()
    for printer in get_printers_config():
        for tag in printer.get("tags", []) or []:
            t = _norm_tag(tag)
            if t:
                tags.add(t)
    return tags

def get_printer_status() -> List[Dict]:
    """Get real-time printer status from Farm API"""
    url = f"{FARM_API_BASE.rstrip('/')}/api/printers"
    data = fetch_json(url) or []
    return data if isinstance(data, list) else []

# ============ Filament Calculation (Bambu CLI) ============
def _collect_numeric_values(obj: Any, keys_like: set, bucket: List[float]) -> None:
    if isinstance(obj, dict):
        for k, v in obj.items():
            lk = str(k).lower()
            if any(kw in lk for kw in keys_like):
                if isinstance(v, (int, float)):
                    bucket.append(float(v))
                elif isinstance(v, (list, tuple)):
                    for it in v:
                        if isinstance(it, (int, float)):
                            bucket.append(float(it))
            _collect_numeric_values(v, keys_like, bucket)
    elif isinstance(obj, (list, tuple)):
        for it in obj:
            _collect_numeric_values(it, keys_like, bucket)

def parse_export_dir(export_dir: pathlib.Path) -> Dict[str, Any]:
    grams, lengths = [], []
    files = list(export_dir.rglob("*.json"))
    for p in files:
        try:
            j = json.loads(p.read_text(encoding="utf-8", errors="ignore"))
        except Exception:
            continue
        _collect_numeric_values(j, {"weight"}, grams)
        _collect_numeric_values(j, {"length", "filament_mm"}, lengths)

    grams = [g for g in grams if 0.01 <= g <= 10_000_000]
    lengths = [l for l in lengths if 0.1 <= l <= 10_000_000]

    return {
        "ok": True,
        "json_files": len(files),
        "total_grams": sum(grams) if grams else 0.0,
        "total_mm": sum(lengths) if lengths else 0.0,
    }

def parse_3mf_fallback(three_mf: pathlib.Path) -> Dict[str, Any]:
    if not three_mf.exists():
        return {"ok": False, "error": f"3MF niet gevonden: {three_mf}"}
    grams, lengths, scanned = [], [], 0
    try:
        with zipfile.ZipFile(three_mf, "r") as z:
            for name in z.namelist():
                lname = name.lower()
                if not (lname.endswith(".json") or lname.endswith(".config")):
                    continue
                scanned += 1
                try:
                    with z.open(name) as f:
                        text = f.read().decode("utf-8", errors="ignore")
                    try:
                        j = json.loads(text)
                        _collect_numeric_values(j, {"weight"}, grams)
                        _collect_numeric_values(j, {"length", "filament_mm"}, lengths)
                    except Exception:
                        for line in text.splitlines():
                            ll = line.lower()
                            m = re.search(r"([-+]?\d+(\.\d+)?)", line)
                            if not m:
                                continue
                            val = float(m.group(1))
                            if "weight" in ll and 0.01 <= val <= 10_000_000:
                                grams.append(val)
                            if ("length" in ll or "filament_mm" in ll) and 0.1 <= val <= 10_000_000:
                                lengths.append(val)
                except Exception:
                    pass
    except Exception as e:
        logger.warning(f"3MF parse error: {e}")

    return {
        "ok": True,
        "files_scanned": scanned,
        "total_grams": sum(grams) if grams else 0.0,
        "total_mm": sum(lengths) if lengths else 0.0,
    }

def run_bambu_cli(
    studio_exe: str, stl_path: str, machine_json: str, process_json: str, filament_json: str
) -> Dict[str, Any]:
    tmp_root = pathlib.Path(tempfile.mkdtemp(prefix="bambu_cli_"))
    export_dir = tmp_root / "export"
    export_dir.mkdir(parents=True, exist_ok=True)
    out_3mf = export_dir / "__tmp__.3mf"

    cmd = [
        studio_exe,
        "--slice",
        "0",
        "--debug",
        "2",
        "--export-3mf",
        str(out_3mf),
        "--export-slicedata",
        str(export_dir),
        "--arrange",
        "1",
        "--orient",
        "1",
        "--load-settings",
        f"{machine_json};{process_json}",
        "--load-filaments",
        filament_json,
        stl_path,
    ]

    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, shell=False, timeout=300)
        return {
            "code": proc.returncode,
            "tmp_root": str(tmp_root),
            "export_dir": str(export_dir),
            "out_3mf": str(out_3mf),
        }
    except subprocess.TimeoutExpired:
        return {
            "code": -1,
            "tmp_root": str(tmp_root),
            "export_dir": str(export_dir),
            "out_3mf": str(out_3mf),
        }

def compute_filament_usage(
    input_file: pathlib.Path, output_dir: pathlib.Path
) -> Tuple[float, float, Optional[pathlib.Path]]:
    logger.info(f"Computing filament usage for: {input_file.name}")

    if not os.path.exists(BAMBU_BIN_ENV):
        logger.warning(f"Bambu Studio not found at: {BAMBU_BIN_ENV}")
        return (0.0, 0.0, None)

    if not all(os.path.exists(p) for p in [MACHINE_JSON, PROCESS_JSON, FILAMENT_JSON]):
        logger.warning("Profile files not complete")
        return (0.0, 0.0, None)

    result = run_bambu_cli(BAMBU_BIN_ENV, str(input_file), MACHINE_JSON, PROCESS_JSON, FILAMENT_JSON)

    export_dir = pathlib.Path(result["export_dir"])
    grams, mm, best_3mf = 0.0, 0.0, None

    three_mf_files = list(export_dir.glob("*.3mf"))

    if three_mf_files:
        for mf in three_mf_files:
            fb = parse_3mf_fallback(mf)
            g, m = fb.get("total_grams", 0.0), fb.get("total_mm", 0.0)
            if g > grams:
                grams, best_3mf = g, mf
            if m > mm:
                mm = m
            if grams > 0 or mm > 0:
                break

    if grams <= 0 and mm <= 0:
        parsed = parse_export_dir(export_dir)
        grams = parsed.get("total_grams", 0.0)
        mm = parsed.get("total_mm", 0.0)

    saved_3mf = None
    if best_3mf and best_3mf.exists():
        saved_name = f"{input_file.stem}-{uuid.uuid4().hex[:6]}.3mf"
        saved_3mf = output_dir / saved_name
        try:
            shutil.copy2(best_3mf, saved_3mf)
            logger.info(f"Saved 3MF: {saved_3mf.name}")
        except Exception as e:
            logger.error(f"Could not save 3MF: {e}")
            saved_3mf = None

    try:
        shutil.rmtree(result["tmp_root"], ignore_errors=True)
    except Exception:
        pass

    if grams > 0 or mm > 0:
        logger.info(f"Filament: {mm:.1f}mm / {grams:.2f}g")

    return (float(mm), float(grams), saved_3mf)

# ============ 🆕 RE-ASSIGNMENT LOGIC ============
async def _reassign_pending_jobs():
    """
    Wijs PENDING jobs toe aan vrijgekomen printers.
    Wordt aangeroepen na tag cleanup.
    """
    try:
        manager = get_upload_manager()
        
        con = db_conn()
        cur = con.cursor()
        pending = cur.execute("""
            SELECT id, job_tag FROM queue_jobs 
            WHERE status='PENDING' 
              AND (device_id IS NULL OR device_id='' OR device_id='auto')
            ORDER BY created_at ASC
            LIMIT 20
        """).fetchall()
        con.close()
        
        if not pending:
            return
        
        logger.info(f"🔄 Re-assigning {len(pending)} pending jobs...")
        
        # Gebruik bestaande assignment logica
        await asyncio.to_thread(manager.process_pending_jobs)
        
        logger.info(f"✅ Re-assignment voltooid")
        
    except Exception as e:
        logger.error(f"Re-assignment fout: {e}", exc_info=True)

# ============ Reconciliation ============
def _printer_status_map() -> Dict[str, Dict[str, Any]]:
    """Get printer status as a dict keyed by device_id"""
    data = get_printer_status()
    return {p.get("device_id", ""): p for p in data if isinstance(p, dict) and p.get("device_id")}

async def _reconcile_once():
    """
    Smart reconciliation using file matching + automatic re-assignment
    """
    printers = _printer_status_map()
    if not printers:
        return

    con = db_conn()
    cur = con.cursor()
    now = now_iso()

    rows = cur.execute(
        """
        SELECT id, device_id, filename, status, started_at, completed_at, job_tag
        FROM queue_jobs
        WHERE status IN ('READY','UPLOADING','UPLOADED','PRINTING')
          AND device_id NOT IN ('', 'auto')
        ORDER BY COALESCE(started_at, uploaded_at, created_at) DESC
        """
    ).fetchall()

    updates: List[Tuple[str, Tuple[Any, ...]]] = []
    tags_to_check: Set[Tuple[str, str]] = set()

    OPEN_STATES = {"PENDING", "READY", "UPLOADING", "UPLOADED", "PRINTING"}

    for r in rows:
        jid = r["id"]
        dev = r["device_id"]
        fname = r["filename"]
        jstat = (r["status"] or "").upper()
        jtag = _norm_tag(r.get("job_tag") or "")

        p = printers.get(dev)
        if not p:
            continue

        pstat = (p.get("status") or "").upper()
        pfile = p.get("file") or ""

        match = files_match(fname, pfile)

        if jstat in ("UPLOADED", "READY", "UPLOADING"):
            if pstat == "RUNNING" and match:
                if jstat != "PRINTING":
                    updates.append(
                        (
                            "UPDATE queue_jobs SET status='PRINTING', error=NULL, started_at=COALESCE(started_at, ?) WHERE id=?",
                            (now, jid),
                        )
                    )
                    logger.info(
                        f"Reconcile: Job {jid} -> PRINTING (printer running matched file)"
                    )

        elif jstat == "PRINTING":
            if pstat == "FINISH" and match:
                updates.append(
                    (
                        "UPDATE queue_jobs SET status='COMPLETED', completed_at=COALESCE(completed_at, ?) WHERE id=?",
                        (now, jid),
                    )
                )
                logger.info(f"Reconcile: Job {jid} -> COMPLETED (printer finished)")
                if jtag:
                    tags_to_check.add((jtag, dev))

            elif pstat == "FAILED":
                updates.append(
                    (
                        "UPDATE queue_jobs SET status='FAILED', error='Printer reported FAILED', completed_at=COALESCE(completed_at, ?) WHERE id=?",
                        (now, jid),
                    )
                )
                logger.info(f"Reconcile: Job {jid} -> FAILED (printer failed)")
                if jtag:
                    tags_to_check.add((jtag, dev))

            elif pstat == "IDLE" and not match:
                if r["started_at"]:
                    try:
                        started = datetime.fromisoformat(r["started_at"].replace("Z", "+00:00"))
                        elapsed = (
                            datetime.utcnow().replace(tzinfo=started.tzinfo) - started
                        ).total_seconds()
                        if elapsed > 30:
                            updates.append(
                                (
                                    "UPDATE queue_jobs SET status='FAILED', error='Job cancelled or stopped', completed_at=COALESCE(completed_at, ?) WHERE id=?",
                                    (now, jid),
                                )
                            )
                            logger.info(
                                f"Reconcile: Job {jid} -> FAILED (cancelled/stopped)"
                            )
                            if jtag:
                                tags_to_check.add((jtag, dev))
                    except Exception:
                        pass

    # Apply updates
    if updates:
        for sql, params in updates:
            try:
                cur.execute(sql, params)
            except Exception as e:
                logger.error(f"Reconcile update error: {e}")

        con.commit()
        logger.info(f"Reconciliation completed: {len(updates)} updates")

        # 🆕 Tag cleanup for tags that might be done now
        try:
            import requests
            cur2 = con.cursor()
            for (job_tag, _dev) in set(tags_to_check):
                if not job_tag:
                    continue
                qs = (
                    "SELECT COUNT(*) FROM queue_jobs "
                    "WHERE job_tag=? AND status IN (" + ",".join("?" * len(OPEN_STATES)) + ")"
                )
                params = (job_tag, *OPEN_STATES)
                open_cnt = cur2.execute(qs, params).fetchone()[0]

                if open_cnt == 0:
                    try:
                        resp = requests.delete(
                            f"{SERVER_API}/api/config/printer-tags/{job_tag}", timeout=5
                        )
                        if resp.ok:
                            logger.info(
                                f"🏷️ Tag '{job_tag}' verwijderd (geen open jobs meer)"
                            )
                        else:
                            logger.warning(
                                f"Kon tag '{job_tag}' niet verwijderen: HTTP {resp.status_code}"
                            )
                    except Exception as e:
                        logger.warning(f"Kon tag '{job_tag}' niet verwijderen: {e}")
        except Exception as e:
            logger.error(f"Tag-cleanup error: {e}")

        # 🆕 TRIGGER RE-ASSIGNMENT AFTER TAG CLEANUP
        if tags_to_check:
            try:
                logger.info("🔄 Triggering re-assignment after tag cleanup...")
                await _reassign_pending_jobs()
            except Exception as e:
                logger.error(f"Re-assignment trigger fout: {e}")

    # Global sweep: remove tags with no open jobs
    try:
        _cleanup_tags_without_open_jobs(con)
    except Exception as e:
        logger.error(f"Tag-cleanup error (global sweep): {e}")

    con.close()

async def _reconcile_loop():
    """Background task for continuous reconciliation"""
    await asyncio.sleep(2.0)

    while True:
        try:
            await _reconcile_once()
        except Exception as e:
            logger.error(f"Reconcile error: {e}", exc_info=True)

        await asyncio.sleep(3.0)

def _cleanup_tags_without_open_jobs(con) -> None:
    """
    Remove printer-tags for which there are NO open jobs left in the queue.
    """
    import requests

    OPEN_STATES = ("PENDING", "READY", "UPLOADING", "UPLOADED", "PRINTING")
    TERMINAL_STATES = ("COMPLETED", "FAILED")

    sql = (
        "SELECT job_tag "
        "FROM queue_jobs "
        "WHERE job_tag IS NOT NULL "
        "GROUP BY job_tag "
        "HAVING "
        "  SUM(CASE WHEN status IN (" + ",".join(["?"] * len(OPEN_STATES)) + ") THEN 1 ELSE 0 END) = 0 "
        "  AND SUM(CASE WHEN status IN (" + ",".join(["?"] * len(TERMINAL_STATES)) + ") THEN 1 ELSE 0 END) > 0"
    )

    cur = con.cursor()
    rows = cur.execute(sql, (*OPEN_STATES, *TERMINAL_STATES)).fetchall()
    tags = [(r[0] if not isinstance(r, dict) else r["job_tag"]) for r in rows]

    if not tags:
        return

    for tag in sorted({_norm_tag(t) for t in tags if t}):
        url = f"{SERVER_API}/api/config/printer-tags/{tag}"
        try:
            resp = requests.delete(url, timeout=5)
            if resp.ok:
                logger.info(f"Tag cleanup: '{tag}' verwijderd (geen open jobs meer).")
            else:
                logger.warning(
                    f"Tag cleanup: HTTP {resp.status_code} voor '{tag}'"
                )
        except Exception as e:
            logger.warning(f"Tag cleanup: request fout voor '{tag}': {e}")

# ============ FastAPI App ============
app = FastAPI(title="Printfarm Queue Service", version="3.2.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class QueueListResp(BaseModel):
    items: List[Dict]

# ============ Health Check ============
@app.get("/health")
def health_check():
    """Health check endpoint"""
    try:
        con = db_conn()
        cur = con.cursor()
        cur.execute("SELECT COUNT(*) FROM queue_jobs")
        count = cur.fetchone()[0]
        con.close()

        printers = get_printers_config()

        return {
            "status": "healthy",
            "database": "ok",
            "jobs_count": count,
            "printers_configured": len(printers),
            "auto_upload": AUTO_UPLOAD,
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        raise HTTPException(500, f"Health check failed: {e}")

# ============ Queue Endpoints ============
@app.get("/api/queue", response_model=QueueListResp)
def api_queue_list(
    device_id: Optional[str] = Query(None),
    tag: Optional[str] = Query(None),
    status: Optional[str] = Query(None),
):
    """List queue jobs with optional filters"""
    return {"items": job_list(device_id, tag, status)}

@app.get("/api/queue/stats")
def api_queue_stats():
    """Queue statistics per status"""
    return queue_stats()

@app.get("/api/filament/summary")
def api_filament_summary(range: str = Query("day")):
    """Filament usage summary endpoint"""
    return _filament_summary(range)

@app.get("/api/queue/filament_summary")
def api_queue_filament_summary_compat(range: str = Query("day")):
    """Compatibility endpoint for older UI"""
    return _filament_summary(range)

@app.get("/api/queue/{job_id}")
def api_queue_get(job_id: str):
    """Get specific job"""
    job = job_get(job_id)
    if not job:
        raise HTTPException(404, "Job not found")
    return job

@app.delete("/api/queue/{job_id}")
def api_queue_delete(job_id: str):
    """Delete job from queue"""
    if not job_delete(job_id):
        raise HTTPException(404, "Job not found")
    return {"ok": True}

@app.delete("/api/queue/by-tag/{tag}")
def api_queue_delete_by_tag(tag: str):
    """Verwijder ALLE jobs met deze tag uit de wachtrij"""
    t = _norm_tag(tag)
    con = db_conn()
    cur = con.cursor()
    rows = cur.execute("SELECT id, filepath FROM queue_jobs WHERE job_tag=?", (t,)).fetchall()
    cur.execute("DELETE FROM queue_jobs WHERE job_tag=?", (t,))
    con.commit()
    con.close()
    
    for r in rows or []:
        try:
            fp = pathlib.Path(r["filepath"]) if isinstance(r, sqlite3.Row) else pathlib.Path(r[1])
            fp.unlink(missing_ok=True)
        except Exception:
            pass
    
    try:
        _cleanup_tag_if_no_jobs(t)
    except Exception as e:
        logger.warning(f"delete_by_tag: tag-cleanup fout voor '{t}': {e}")
    
    return {"ok": True, "removed": len(rows or [])}

@app.post("/api/queue/{job_id}/retry")
def api_queue_retry(job_id: str):
    """Retry failed job"""
    job = job_get(job_id)
    if not job:
        raise HTTPException(404, "Job not found")

    if job["status"] != "FAILED":
        raise HTTPException(400, "Only failed jobs can be retried")

    con = db_conn()
    cur = con.cursor()
    cur.execute(
        """
        UPDATE queue_jobs
        SET status='PENDING', device_id='auto', error=NULL, retry_count=0
        WHERE id=?
        """,
        (job_id,),
    )
    con.commit()
    con.close()

    logger.info(f"Job {job_id} manually retried")

    return {"ok": True, "message": "Job queued for retry"}

# 🆕 MANUAL RE-ASSIGNMENT TRIGGER
@app.post("/api/queue/reassign")
async def api_queue_reassign():
    """
    Trigger immediate re-assignment van pending jobs.
    Nuttig na tag cleanup of handmatige interventies.
    """
    try:
        await _reassign_pending_jobs()
        return {"ok": True, "message": "Re-assignment triggered"}
    except Exception as e:
        logger.error(f"Manual re-assignment fout: {e}")
        raise HTTPException(500, f"Re-assignment failed: {e}")

@app.post("/api/queue/upload")
async def api_queue_upload(
    file: UploadFile = File(...),
    job_tag: Optional[str] = Form(None),
):
    """Upload a 3D model to the queue"""
    if job_tag:
        job_tag = _norm_tag(job_tag) or None
        if job_tag:
            known_tags = known_printer_tags()
            if job_tag not in known_tags:
                logger.warning(
                    f"Onbekende tag '{job_tag}', omzetting naar None (untagged)."
                )
                job_tag = None

    up_dir = QUEUE_DIR / "uploads"
    up_dir.mkdir(parents=True, exist_ok=True)

    original = file.filename or "upload"
    safe = safe_name(original)
    tmp_path = up_dir / f"tmp_{uuid.uuid4().hex[:8]}_{safe}"

    size = 0
    with tmp_path.open("wb") as f:
        while True:
            chunk = await file.read(1024 * 1024)
            if not chunk:
                break
            f.write(chunk)
            size += len(chunk)

    ext = tmp_path.suffix.lower()
    if ext not in (".stl", ".obj", ".3mf"):
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass
        raise HTTPException(400, "Only .stl, .obj or .3mf files are supported")

    if ext in (".stl", ".obj") and not os.path.exists(BAMBU_BIN_ENV):
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass
        raise HTTPException(
            400,
            "STL/OBJ vereisen Bambu Studio CLI op deze server.",
        )

    fil_mm, fil_g, generated_3mf = compute_filament_usage(tmp_path, up_dir)

    if generated_3mf and generated_3mf.exists():
        final_path = generated_3mf
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass
    elif ext == ".3mf":
        final_path = up_dir / f"{tmp_path.stem}-{uuid.uuid4().hex[:6]}.3mf"
        tmp_path.replace(final_path)
    else:
        logger.error("No 3MF generated")
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass
        raise HTTPException(500, "Could not generate 3MF")

    final_size = final_path.stat().st_size

    job = job_insert(
        filename=final_path.name,
        filepath=final_path,
        size=final_size or size,
        job_tag=job_tag,
        filament_mm=fil_mm,
        filament_g=fil_g,
    )

    logger.info(
        f"Job {job['id']} uploaded (tag={job_tag}, {fil_mm:.1f}mm, {fil_g:.2f}g)"
    )

    return {
        "ok": True,
        "job": job,
        "message": "Job added to queue. Will be automatically assigned to an IDLE printer.",
    }

# ============ Printer Endpoints ============
@app.get("/api/printers/available")
def api_printers_available(
    tag: Optional[str] = Query(None),
    include_disabled: bool = Query(False),
):
    """Get available printers with current status"""
    try:
        cfg = get_printers_config()
        status_data = get_printer_status()
        status_map = {p["device_id"]: p for p in status_data if isinstance(p, dict)}

        con = db_conn()
        cur = con.cursor()
        rows = cur.execute(
            """
            SELECT device_id, COUNT(*) as cnt
            FROM queue_jobs
            WHERE status IN ('READY','UPLOADING','PENDING','PRINTING')
            GROUP BY device_id
            """
        ).fetchall()
        con.close()
        queue_loads = {r["device_id"]: r["cnt"] for r in rows}

        result = []
        for p in cfg:
            if tag:
                tag_lower = _norm_tag(tag)
                printer_tags = [_norm_tag(t) for t in (p.get("tags", []) or [])]
                if tag_lower not in printer_tags:
                    continue

            if not include_disabled and not bool(p.get("autoprint")):
                continue

            status_info = status_map.get(p["device_id"], {})
            result.append(
                {
                    "device_id": p["device_id"],
                    "name": p.get("name", ""),
                    "model": p.get("model", ""),
                    "autoprint": bool(p.get("autoprint")),
                    "tags": p.get("tags", []),
                    "status": status_info.get("status", "UNKNOWN"),
                    "progress": status_info.get("progress", 0),
                    "file": status_info.get("file", "-"),
                    "queue_load": queue_loads.get(p["device_id"], 0),
                    "has_credentials": bool(p.get("ip") and p.get("lan_access_code")),
                }
            )

        def sort_key(x):
            prio = {"IDLE": 0, "FINISH": 1, "NO_CONN": 2, "PAUSE": 3, "RUNNING": 4, "FAILED": 5}
            return (prio.get(x["status"], 9), x["queue_load"], x["name"].lower())

        result.sort(key=sort_key)

        return {"printers": result}

    except Exception as e:
        logger.error(f"Error fetching printers: {e}", exc_info=True)
        return {"printers": []}

@app.get("/api/queue/waiting")
def api_queue_waiting():
    """Number of jobs waiting for an IDLE printer"""
    con = db_conn()
    cur = con.cursor()

    pending = cur.execute(
        "SELECT COUNT(*) as cnt FROM queue_jobs WHERE status='PENDING'"
    ).fetchone()["cnt"]

    ready = cur.execute(
        "SELECT COUNT(*) as cnt FROM queue_jobs WHERE status='READY'"
    ).fetchone()["cnt"]

    tag_rows = cur.execute(
        """
        SELECT job_tag, COUNT(*) as cnt
        FROM queue_jobs
        WHERE status='PENDING' AND job_tag IS NOT NULL
        GROUP BY job_tag
        """
    ).fetchall()

    con.close()

    return {
        "pending_assignment": pending,
        "ready_for_upload": ready,
        "by_tag": {r["job_tag"]: r["cnt"] for r in tag_rows},
    }

# ============ Startup & Shutdown ============
@app.on_event("startup")
async def startup_event():
    logger.info("Queue Service v3.2 starting up...")
    ensure_dirs()
    init_db()

    if AUTO_UPLOAD:
        logger.info("Starting upload manager (dynamic assignment mode)...")
        manager = get_upload_manager()
        manager.start()
    else:
        logger.info("Auto-upload is disabled")

    logger.info("Starting reconciliation loop...")
    app.state.reconcile_task = asyncio.create_task(_reconcile_loop())

    logger.info("✅ Queue Service v3.2 ready (with auto re-assignment)")

@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Queue Service shutting down...")

    if AUTO_UPLOAD:
        manager = get_upload_manager()
        manager.stop()

    task = getattr(app.state, "reconcile_task", None)
    if task:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    logger.info("Queue Service stopped")

# ============ Main ============
def main():
    ensure_dirs()
    init_db()

    logger.info(f"Queue Service v3.2 starting on {QUEUE_HOST}:{QUEUE_PORT}")
    logger.info(f"Dynamic assignment: {AUTO_UPLOAD}")
    logger.info(f"Data directory: {DATA_DIR}")

    uvicorn.run(app, host=QUEUE_HOST, port=QUEUE_PORT)

if __name__ == "__main__":
    main()