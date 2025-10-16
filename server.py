#!/usr/bin/env python3
"""
Bambu PrintFarm - Production Ready Backend
Version: 2.0.0

Een robuuste, productie-klare backend voor het beheren van meerdere Bambu Lab 3D-printers.
Ondersteunt MQTT-communicatie, AMS-beheer en realtime alerts.
"""

import os
import json
import ssl
import subprocess
import threading
import time
import pathlib
import re
import asyncio
import mimetypes
import signal
import sys
from dataclasses import dataclass
from typing import Dict, Any, Optional, List, Tuple, Set
from datetime import datetime, timezone
from contextlib import contextmanager
import logging
from logging.handlers import RotatingFileHandler
from fastapi import APIRouter, HTTPException
import json, re

from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse, StreamingResponse
from alerts_service import (
    WS_CLIENTS,
    alerts_broadcaster,
    collect_alert_snapshot,
    configure_hms_lookup,
    enqueue_alert_snapshot,
)
from pydantic import BaseModel, Field, validator
import sqlite3
import uvicorn
import paho.mqtt.client as mqtt
import urllib.request
from pydantic import BaseModel, Field
from fastapi import HTTPException
import os



# ========== CONFIGURATIE ==========
APP_HOST = os.environ.get("APP_HOST", "0.0.0.0")
APP_PORT = int(os.environ.get("APP_PORT", "8000"))
DATA_DIR = pathlib.Path(os.environ.get("DATA_DIR", "./data")).resolve()
DB_PATH = DATA_DIR / "printfarm.db"
WEB_DIR = pathlib.Path(os.environ.get("WEB_DIR", "./web")).resolve()
LOG_DIR = DATA_DIR / "logs"

BASE_DIR = pathlib.Path(__file__).resolve().parent

STATE_STALE_SECONDS = int(os.environ.get("STATE_STALE_SECONDS", "120"))

HMS_JSON_URL = os.environ.get(
    "HMS_JSON_URL",
    "https://raw.githubusercontent.com/bambulab/BambuStudio/master/resources/hms/hms_en_094.json"
)
HMS_REFRESH_SEC = int(os.environ.get("HMS_REFRESH_SEC", "21600"))
STABLE_EMPTY_SEC = 5.0
LOCAL_TZ = os.environ.get("LOCAL_TZ", "Europe/Amsterdam")

# MIME types
mimetypes.add_type("application/vnd.apple.mpegurl", ".m3u8")
mimetypes.add_type("application/x-mpegURL", ".m3u8")
mimetypes.add_type("video/mp2t", ".ts")
mimetypes.add_type("image/jpeg", ".jpg")

DEFAULT_ITEMCODE = os.environ.get("DEFAULT_ITEMCODE", "").strip()


# ========== LOGGING SETUP ==========
def setup_logging():
    """Configureer gestructureerd logging met rotatie"""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    
    # Root logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # Console handler
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console_fmt = logging.Formatter(
        '%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console.setFormatter(console_fmt)
    logger.addHandler(console)
    
    # File handler met rotatie (10MB per bestand, max 5 backups)
    file_handler = RotatingFileHandler(
        LOG_DIR / "printfarm.log",
        maxBytes=10*1024*1024,
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setLevel(logging.DEBUG)
    file_fmt = logging.Formatter(
        '%(asctime)s [%(levelname)s] %(name)s [%(funcName)s:%(lineno)d]: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(file_fmt)
    logger.addHandler(file_handler)
    
    # Error log (alleen errors en hoger)
    error_handler = RotatingFileHandler(
        LOG_DIR / "errors.log",
        maxBytes=10*1024*1024,
        backupCount=3,
        encoding='utf-8'
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(file_fmt)
    logger.addHandler(error_handler)
    
    return logger

logger = setup_logging()


class ManualOrderIn(BaseModel):
    ordernr: str = Field(..., min_length=3, max_length=100)
    # Als je HTML itemcode meestuurt:
    itemcode: str | None = Field(default=None, min_length=3, max_length=50)
    aantal: int = Field(..., ge=1, le=100)
    
# ========== DATABASE ==========
class DatabaseManager:
    """Thread-safe database manager met connection pooling"""
    
    def __init__(self, db_path: pathlib.Path):
        self.db_path = db_path
        self._local = threading.local()
    
    @contextmanager
    def get_connection(self):
        """Context manager voor database connecties"""
        if not hasattr(self._local, 'conn') or self._local.conn is None:
            self._local.conn = sqlite3.connect(str(self.db_path), timeout=30.0)
            self._local.conn.row_factory = sqlite3.Row
            self._local.conn.execute("PRAGMA journal_mode=WAL;")
            self._local.conn.execute("PRAGMA synchronous=NORMAL;")
            self._local.conn.execute("PRAGMA foreign_keys=ON;")
            self._local.conn.execute("PRAGMA busy_timeout=30000;")
        
        try:
            yield self._local.conn
            self._local.conn.commit()
        except Exception:
            self._local.conn.rollback()
            raise
    
    def init_schema(self):
        """Initialiseer database schema met migraties"""
        with self.get_connection() as conn:
            cur = conn.cursor()
            
            # Printers tabel
            cur.execute("""
                CREATE TABLE IF NOT EXISTS printers (
                    device_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    model TEXT NOT NULL,
                    ip TEXT,
                    lan_access_code TEXT,
                    cloud_user_id TEXT,
                    cloud_access_token TEXT,
                    autoprint INTEGER NOT NULL DEFAULT 1,
                    tags TEXT NOT NULL DEFAULT '[]',
                    auto_ams_black_asacf INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL DEFAULT (datetime('now')),
                    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
                )
            """)
            
            # States tabel
            cur.execute("""
                CREATE TABLE IF NOT EXISTS states (
                    device_id TEXT PRIMARY KEY,
                    payload TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY (device_id) REFERENCES printers(device_id) ON DELETE CASCADE
                )
            """)
            
            # Events tabel
            cur.execute("""
                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    device_id TEXT NOT NULL,
                    ts TEXT NOT NULL,
                    type TEXT NOT NULL,
                    meta TEXT NOT NULL DEFAULT '{}',
                    FOREIGN KEY (device_id) REFERENCES printers(device_id) ON DELETE CASCADE
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_events_device_ts ON events(device_id, ts)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_events_type ON events(type)")
            
            # Alerts tabel
            cur.execute("""
                CREATE TABLE IF NOT EXISTS alerts (
                    id TEXT PRIMARY KEY,
                    device_id TEXT NOT NULL,
                    printer_name TEXT,
                    code TEXT,
                    message TEXT,
                    severity INTEGER NOT NULL,
                    state TEXT NOT NULL,
                    module TEXT,
                    count INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    last_seen TEXT NOT NULL,
                    resolved_at TEXT,
                    raw TEXT,
                    FOREIGN KEY (device_id) REFERENCES printers(device_id) ON DELETE CASCADE
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_alerts_device ON alerts(device_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_alerts_state ON alerts(state)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_alerts_created ON alerts(created_at)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_alerts_code ON alerts(code)")
            
            # --- UI state lock (authoritative)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS ui_state_locks (
                    device_id  TEXT PRIMARY KEY,
                    status     TEXT NOT NULL,
                    reason     TEXT,
                    locked_at  TEXT NOT NULL,
                    FOREIGN KEY (device_id) REFERENCES printers(device_id) ON DELETE CASCADE
                )
            """)
            
            conn.commit()
            logger.info("Database schema geïnitialiseerd")

db_manager = DatabaseManager(DB_PATH)

# ========== UTILITY FUNCTIES ==========
def now_ts() -> float:
    """Huidige timestamp in seconden"""
    return time.time()

def ts_iso() -> str:
    """Huidige timestamp in ISO format (UTC)"""
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def to_epoch(s: Optional[str]) -> Optional[int]:
    """Converteer ISO datetime string naar epoch timestamp"""
    if not s:
        return None
    try:
        if s.endswith("Z"):
            return int(datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp())
        if "T" in s:
            return int(datetime.fromisoformat(s).replace(tzinfo=timezone.utc).timestamp())
        dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except Exception as e:
        logger.warning(f"Kon timestamp niet converteren: {s} - {e}")
        return None

def ensure_dirs():
    """Zorg dat alle benodigde directories bestaan"""
    for d in [DATA_DIR, WEB_DIR, LOG_DIR]:
        d.mkdir(parents=True, exist_ok=True)
    logger.info("Directories geverifieerd")

_TAG_SANITIZE_RE = re.compile(r"[^a-z0-9_-]+")


def _normalize_tag(tag: Any) -> str:
    """Normaliseer één tag naar lowercase en vervang vreemde tekens door '-'."""
    raw = str(tag or "").strip().lower()
    if not raw:
        return ""
    return _TAG_SANITIZE_RE.sub("-", raw)


def _parse_tags(raw: Any) -> List[str]:
    """Parse willekeurige tagrepresentaties naar een ruwe lijst."""
    if isinstance(raw, list):
        return [str(x) for x in raw]
    if isinstance(raw, str):
        try:
            data = json.loads(raw)
            if isinstance(data, list):
                return [str(x) for x in data]
        except Exception:
            pass
        return [s.strip() for s in raw.split(",")]
    return []


def normalize_tags(val: Any) -> List[str]:
    """Normaliseer tags naar lowercase lijst zonder duplicaten (maximaal 1 tag)."""
    tags = _parse_tags(val)

    out: List[str] = []
    seen = set()
    for t in tags:
        s = _normalize_tag(t)
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
        if len(out) >= 1:
            break
    return out


def _remove_tag_from_other_printers(conn: sqlite3.Connection, owner: str, tag: Optional[str]) -> None:
    """Verwijder tag bij andere printers zodat tags uniek blijven."""
    if not tag:
        return

    cur = conn.cursor()
    cur.execute("SELECT device_id, tags FROM printers WHERE device_id<>?", (owner,))
    for row in cur.fetchall():
        device_id = row["device_id"] if isinstance(row, sqlite3.Row) else row[0]
        raw_tags = row["tags"] if isinstance(row, sqlite3.Row) else row[1]
        tags = _parse_tags(raw_tags)
        normalized_existing = [_normalize_tag(t) for t in tags if _normalize_tag(t)]
        new_tags = [t for t in normalized_existing if t != tag]
        if new_tags != normalized_existing:
            cur.execute(
                "UPDATE printers SET tags=?, updated_at=datetime('now') WHERE device_id=?",
                (json.dumps(new_tags), device_id),
            )

def hex_rgba_to_css(hex8: str) -> str:
    """Converteer 8-digit hex (RRGGBBAA) naar CSS hex (#RRGGBB)"""
    h = (hex8 or "").strip().upper()
    if not re.fullmatch(r"[0-9A-F]{8}", h):
        return "#000000"
    return "#" + h[:6]

# ========== HMS CATALOG ==========
class HMSCatalog:
    """HMS error code lookup met automatische refresh"""
    
    def __init__(self):
        self.map: Dict[str, str] = {}
        self.version: Optional[str] = None
        self.loaded_at: Optional[float] = None
        self.lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self.cache_path = DATA_DIR / "hms_cache.json"
    
    def normalize_and_load(self, data: Any) -> Tuple[Dict[str, str], Optional[str]]:
        """Parse HMS JSON naar code -> message mapping"""
        version = None
        entries = []
        
        if isinstance(data, dict) and "data" in data:
            version = str(data.get("ver") or data["data"].get("ver") or "")
            dev = data["data"].get("device_error") or {}
            lang = dev.get("en") or dev.get("EN") or dev.get("En")
            if isinstance(lang, list):
                entries = lang
        
        if not entries and isinstance(data, dict) and "device_error" in data:
            version = str(data.get("ver") or data["device_error"].get("ver") or "")
            lang = data["device_error"].get("en") or data["device_error"].get("EN")
            if isinstance(lang, list):
                entries = lang
        
        if not entries and isinstance(data, list):
            entries = data
        
        mapping: Dict[str, str] = {}
        for it in entries:
            if not isinstance(it, dict):
                continue
            code = str(it.get("ecode") or it.get("code") or "").strip().upper()
            intro = str(it.get("intro") or it.get("message") or "").strip()
            if code:
                mapping[code] = intro
        
        return mapping, version
    
    def load_from_disk(self):
        """Laad gecachete HMS data van disk"""
        if not self.cache_path.exists():
            logger.debug("Geen HMS cache gevonden")
            return
        try:
            with self.cache_path.open("r", encoding="utf-8") as f:
                raw = json.load(f)
            mapping, version = self.normalize_and_load(raw)
            with self.lock:
                if mapping:
                    self.map = mapping
                    self.version = version
                    self.loaded_at = now_ts()
            logger.info(f"HMS cache geladen: {len(mapping)} codes, versie {version}")
        except Exception as e:
            logger.error(f"Fout bij laden HMS cache: {e}")
    
    def save_to_disk(self, raw: Any):
        """Sla HMS data op naar disk"""
        try:
            with self.cache_path.open("w", encoding="utf-8") as f:
                json.dump(raw, f, ensure_ascii=False)
            logger.debug("HMS cache opgeslagen")
        except Exception as e:
            logger.error(f"Fout bij opslaan HMS cache: {e}")
    
    def download_and_reload(self) -> bool:
        """Download fresh HMS data en reload"""
        try:
            req = urllib.request.Request(HMS_JSON_URL, headers={"User-Agent": "printfarm/2.0"})
            with urllib.request.urlopen(req, timeout=10) as r:
                data = json.loads(r.read().decode("utf-8"))
        except Exception as e:
            logger.warning(f"HMS download mislukt: {e}")
            return False
        
        mapping, version = self.normalize_and_load(data)
        if not mapping:
            logger.warning("HMS download bevatte geen geldige data")
            return False
        
        self.save_to_disk(data)
        with self.lock:
            self.map = mapping
            self.version = version
            self.loaded_at = now_ts()
        logger.info(f"HMS data geüpdatet: {len(mapping)} codes, versie {version}")
        return True
    
    def refresh(self) -> bool:
        """Refresh HMS data (download of gebruik cache)"""
        ok = self.download_and_reload()
        if not ok:
            self.load_from_disk()
            ok = bool(self.map)
        return ok
    
    def start_background_refresh(self):
        """Start achtergrond thread voor periodieke refresh"""
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._refresh_loop, daemon=True, name="HMS-Refresh")
        self._thread.start()
        logger.info("HMS achtergrond refresh gestart")
    
    def stop_background_refresh(self):
        """Stop achtergrond refresh thread"""
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)
        logger.info("HMS achtergrond refresh gestopt")
    
    def _refresh_loop(self):
        """Achtergrond loop voor periodieke HMS refresh"""
        try:
            self.refresh()
        except Exception as e:
            logger.error(f"Initiële HMS refresh mislukt: {e}")
        
        while not self._stop.is_set():
            for _ in range(HMS_REFRESH_SEC):
                if self._stop.is_set():
                    return
                time.sleep(1)
            try:
                self.refresh()
            except Exception as e:
                logger.error(f"HMS refresh mislukt: {e}")
    
    def lookup(self, code_any: Any) -> Tuple[Optional[str], str]:
        """Zoek error message voor gegeven code"""
        if code_any in (None, "", 0, "0"):
            return (None, "00000000")
        
        s = str(code_any).strip().upper()
        hex_code = None
        try:
            if s.startswith("0X"):
                hex_code = f"{int(s, 16):08X}"
            elif re.fullmatch(r"[0-9A-F]{8}", s):
                hex_code = s
            else:
                hex_code = f"{int(s, 10):08X}"
        except Exception:
            s2 = re.sub(r"[^0-9A-F]", "", s)
            hex_code = (s2[:8] if s2 else "0").rjust(8, "0")
        
        with self.lock:
            msg = self.map.get(hex_code)
        return (msg, hex_code)

hms = HMSCatalog()
configure_hms_lookup(hms.lookup)


# ========== SERVICE PROCESS MANAGER ==========
class ServiceProcessManager:
    """Beheer achtergrondservices die parallel aan de API moeten draaien."""

    def __init__(self):
        self.services = {
            "printer_uploader": BASE_DIR / "printer_uploader.py",
            "slicer": BASE_DIR / "slicer.py",
            "queue_service": BASE_DIR / "queue_service.py",
        }
        self.processes: Dict[str, subprocess.Popen] = {}
        self.log_files: Dict[str, Any] = {}
        self.lock = threading.Lock()
        self.monitor_thread: Optional[threading.Thread] = None
        self.stop_event = threading.Event()
        self.last_start: Dict[str, float] = {}
        self.restart_backoff = 5.0

    def _start_service(self, name: str, path: pathlib.Path) -> bool:
        if self.stop_event.is_set():
            return False

        if name in self.processes:
            proc = self.processes.get(name)
            if proc and proc.poll() is None:
                return True
            self._finalize_service(name)

        if not path.exists():
            logger.error(f"Service {name} niet gevonden op {path}")
            return False

        try:
            LOG_DIR.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            logger.warning(f"Kon logmap niet maken voor service {name}: {e}")

        log_file = None
        stdout_target = None
        stderr_target = None
        try:
            log_path = LOG_DIR / f"{name}.log"
            log_file = open(log_path, "a", buffering=1, encoding="utf-8")
            log_file.write(f"[{datetime.utcnow().isoformat()}Z] Service start\n")
            stdout_target = log_file
            stderr_target = log_file
        except Exception as e:
            logger.error(f"Kon logbestand niet openen voor service {name}: {e}")
            log_file = None

        cmd = [sys.executable, str(path)]
        try:
            proc = subprocess.Popen(
                cmd,
                stdout=stdout_target,
                stderr=stderr_target,
                cwd=str(path.parent),
                env=os.environ.copy(),
            )
        except Exception as e:
            if log_file:
                try:
                    log_file.write(f"[{datetime.utcnow().isoformat()}Z] Startfout: {e}\n")
                except Exception:
                    pass
                log_file.close()
            logger.error(f"Kon service {name} niet starten: {e}")
            return False

        self.processes[name] = proc
        if log_file:
            self.log_files[name] = log_file
        self.last_start[name] = time.time()
        logger.info(f"Service {name} gestart (pid={proc.pid})")
        return True

    def _finalize_service(self, name: str):
        proc = self.processes.pop(name, None)
        log_file = self.log_files.pop(name, None)
        if log_file:
            try:
                log_file.write(f"[{datetime.utcnow().isoformat()}Z] Service gestopt\n")
            except Exception:
                pass
            log_file.close()
        self.last_start.pop(name, None)
        return proc

    def start_all(self):
        with self.lock:
            self.stop_event.clear()
            for name, path in self.services.items():
                self._start_service(name, path)
            if not self.monitor_thread or not self.monitor_thread.is_alive():
                self.monitor_thread = threading.Thread(
                    target=self._monitor_loop,
                    name="service-monitor",
                    daemon=True,
                )
                self.monitor_thread.start()

    def stop_service(self, name: str, timeout: float = 20.0):
        with self.lock:
            proc = self.processes.get(name)

        if not proc:
            return

        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                logger.warning(f"Force kill van service {name} (timeout)")
                proc.kill()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    logger.error(f"Kon service {name} niet killen")

        with self.lock:
            self._finalize_service(name)

        logger.info(f"Service {name} gestopt")

    def stop_all(self, timeout: float = 20.0):
        self.stop_event.set()
        monitor = None
        with self.lock:
            monitor = self.monitor_thread
            self.monitor_thread = None
        if monitor and monitor.is_alive():
            monitor.join(timeout=timeout)

        with self.lock:
            names = list(self.processes.keys())

        for name in names:
            self.stop_service(name, timeout=timeout)

        self.last_start.clear()

    def _monitor_loop(self):
        logger.info("Service monitor gestart")
        try:
            while not self.stop_event.wait(5.0):
                for name, path in list(self.services.items()):
                    if self.stop_event.is_set():
                        break

                    restart_needed = False
                    exit_code = None
                    with self.lock:
                        proc = self.processes.get(name)
                        if proc is None:
                            restart_needed = True
                        else:
                            poll_res = proc.poll()
                            if poll_res is not None:
                                exit_code = poll_res
                                self._finalize_service(name)
                                restart_needed = True

                    if not restart_needed or self.stop_event.is_set():
                        continue

                    if exit_code is not None:
                        logger.warning(
                            f"Service {name} onverwacht gestopt (exit={exit_code}); poging tot herstart"
                        )

                    delay = max(0.0, self.restart_backoff - (time.time() - self.last_start.get(name, 0.0)))
                    if delay > 0 and self.stop_event.wait(delay):
                        break

                    with self.lock:
                        if self.stop_event.is_set():
                            break
                        if not self._start_service(name, path):
                            logger.error(f"Automatische herstart van service {name} mislukt")
        finally:
            logger.info("Service monitor gestopt")


service_manager = ServiceProcessManager()

# ========== MQTT BROKER CONFIG ==========
@dataclass
class BrokerConfig:
    is_cloud: bool = False
    host: str = ""
    port: int = 8883
    username: str = ""
    password: str = ""
    tls_insecure: bool = True

# ========== PRINTER CLIENT ==========
class PrinterClient:
    """MQTT client voor individuele printer"""
    
    def __init__(self, cfg: dict, on_report):
        self.cfg = cfg
        self.device_id = cfg["device_id"]
        self.on_report = on_report
        self.client = mqtt.Client(
            client_id=f"printfarm-{self.device_id}",
            clean_session=True
        )
        self.seq = 0
        self.seq_lock = threading.Lock()
        self.connected = threading.Event()
        self.stop_evt = threading.Event()
        self.thread: Optional[threading.Thread] = None
        
        # ACK tracking
        self._pending_lock = threading.Lock()
        self._pending: Dict[str, Tuple[threading.Event, str, str]] = {}
        self._responses: Dict[str, dict] = {}
    
    def _broker(self) -> BrokerConfig:
        """Bepaal broker config (LAN of Cloud)"""
        if self.cfg.get("ip") and self.cfg.get("lan_access_code"):
            return BrokerConfig(
                False,
                self.cfg["ip"],
                8883,
                "bblp",
                self.cfg["lan_access_code"],
                True
            )
        if self.cfg.get("cloud_user_id") and self.cfg.get("cloud_access_token"):
            return BrokerConfig(
                True,
                "us.mqtt.bambulab.com",
                8883,
                f"u_{self.cfg['cloud_user_id']}",
                self.cfg["cloud_access_token"],
                False
            )
        raise RuntimeError("Geen geldige broker config (LAN of Cloud)")
    
    def _on_connect(self, client, userdata, flags, rc):
        """MQTT connect callback"""
        if rc == 0:
            topic = f"device/{self.device_id}/report"
            client.subscribe(topic, qos=1)
            self.connected.set()
            self.send_pushall()
            logger.info(f"MQTT verbonden: {self.device_id}")
        else:
            self.connected.clear()
            logger.warning(f"MQTT connect mislukt: {self.device_id}, rc={rc}")
    
    def _on_message(self, client, userdata, msg):
        """MQTT message callback"""
        try:
            payload = json.loads(msg.payload.decode("utf-8", errors="ignore"))
        except Exception as e:
            logger.warning(f"Ongeldige MQTT payload voor {self.device_id}: {e}")
            return
        
        # Doorsturen naar manager
        try:
            self.on_report(self.device_id, payload)
        except Exception as e:
            logger.error(f"Fout bij verwerken report voor {self.device_id}: {e}", exc_info=True)
        
        # ACK tracking
        try:
            for typ, block in payload.items():
                if not isinstance(block, dict):
                    continue
                seq = str(block.get("sequence_id", "") or "")
                cmd = str(block.get("command", "") or "")
                if not seq or not cmd:
                    continue
                
                with self._pending_lock:
                    pending = self._pending.get(seq)
                    if pending:
                        ev, exp_typ, exp_cmd = pending
                        if typ == exp_typ and cmd == exp_cmd:
                            self._responses[seq] = block
                            ev.set()
        except Exception as e:
            logger.debug(f"ACK tracking fout voor {self.device_id}: {e}")
    
    def _on_disconnect(self, client, userdata, rc):
        """MQTT disconnect callback"""
        self.connected.clear()
        if rc != 0:
            logger.warning(f"MQTT onverwacht verbroken: {self.device_id}, rc={rc}")
    
    def start(self):
        """Start MQTT client thread"""
        if self.thread and self.thread.is_alive():
            return
        self.stop_evt.clear()
        self.thread = threading.Thread(
            target=self._run,
            daemon=True,
            name=f"MQTT-{self.device_id}"
        )
        self.thread.start()
        logger.info(f"MQTT client gestart voor {self.device_id}")
    
    def stop(self):
        """Stop MQTT client"""
        self.stop_evt.set()
        try:
            self.client.disconnect()
        except Exception:
            pass
        if self.thread:
            self.thread.join(timeout=5)
        logger.info(f"MQTT client gestopt voor {self.device_id}")
    
    def _run(self):
        """MQTT client main loop"""
        broker = self._broker()
        self.client.username_pw_set(broker.username, broker.password)
        
        ctx = ssl.create_default_context()
        if broker.tls_insecure:
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
        self.client.tls_set_context(ctx)
        
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.client.on_disconnect = self._on_disconnect
        
        retry_count = 0
        max_retries = 10
        
        while not self.stop_evt.is_set() and retry_count < max_retries:
            try:
                self.client.connect(broker.host, broker.port, keepalive=60)
                self.client.loop_start()
                retry_count = 0  # Reset bij succesvolle connect
                
                while not self.stop_evt.is_set():
                    time.sleep(1.0)
                break
            except Exception as e:
                retry_count += 1
                logger.error(f"MQTT connect fout {self.device_id} (poging {retry_count}): {e}")
                time.sleep(min(30, 3 * retry_count))
            finally:
                self.client.loop_stop()
        
        if retry_count >= max_retries:
            logger.error(f"MQTT client opgegeven na {max_retries} pogingen: {self.device_id}")
    
    def _next_seq(self) -> str:
        """Genereer uniek sequence ID"""
        with self.seq_lock:
            self.seq += 1
            return str(self.seq)
    
    def _publish(self, cmd: dict, qos: int = 1):
        """Publiceer command naar printer"""
        if not self.connected.is_set():
            raise RuntimeError("MQTT niet verbonden")
        topic = f"device/{self.device_id}/request"
        payload = json.dumps(cmd, separators=(",", ":"))
        self.client.publish(topic, payload, qos=qos)
    
    def _register_waiter(self, seq: str, typ: str, cmd: str) -> threading.Event:
        """Registreer ACK waiter"""
        ev = threading.Event()
        with self._pending_lock:
            self._pending[seq] = (ev, typ, cmd)
        return ev
    
    def _wait_for_response(self, seq: str, timeout: float = 3.0) -> Optional[dict]:
        """Wacht op ACK response"""
        with self._pending_lock:
            tup = self._pending.get(seq)
        if not tup:
            return None
        
        ev, _, _ = tup
        if not ev.wait(timeout):
            with self._pending_lock:
                self._pending.pop(seq, None)
            return None
        
        with self._pending_lock:
            resp = self._responses.pop(seq, None)
            self._pending.pop(seq, None)
        return resp
    
    def _request_with_ack(self, typ: str, body: dict, qos: int = 1, timeout: float = 3.0) -> Optional[dict]:
        """Stuur command en wacht op ACK"""
        seq = self._next_seq()
        body = dict(body or {})
        body["sequence_id"] = seq
        cmd = str(body.get("command", ""))
        waiter = self._register_waiter(seq, typ, cmd)
        
        # Double publish voor betrouwbaarheid
        self._publish({typ: body}, qos=qos)
        time.sleep(0.08)
        self._publish({typ: body}, qos=qos)
        
        return self._wait_for_response(seq, timeout=timeout)
    
    def send_pushall(self):
        """Vraag volledige status update"""
        cmd = {
            "pushing": {
                "sequence_id": self._next_seq(),
                "command": "pushall",
                "version": 1,
                "push_target": 1
            }
        }
        self._publish(cmd, qos=0)
    
    def pause(self):
        """Pauzeer print"""
        self._publish({
            "print": {
                "sequence_id": self._next_seq(),
                "command": "pause",
                "param": ""
            }
        }, qos=1)
    
    def resume(self):
        """Hervat print"""
        self._publish({
            "print": {
                "sequence_id": self._next_seq(),
                "command": "resume",
                "param": ""
            }
        }, qos=1)
    
    def stop_print(self):
        """Stop print"""
        self._publish({
            "print": {
                "sequence_id": self._next_seq(),
                "command": "stop",
                "param": ""
            }
        }, qos=1)
    
    def set_led(self, node: str = "chamber_light", mode: str = "on", timeout: float = 3.0) -> Optional[dict]:
        """Schakel LED (met ACK)"""
        mode = str(mode).lower()
        if mode not in ("on", "off", "flashing"):
            mode = "on"
        
        body = {
            "command": "ledctrl",
            "led_node": node,
            "led_mode": mode,
            "led_on_time": 500,
            "led_off_time": 500,
            "loop_times": 1,
            "interval_time": 1000
        }
        return self._request_with_ack("system", body, qos=1, timeout=timeout)
    
    def ams_filament_setting(self, ams_id: int, tray_id: int, tray_type: str, tray_color_hex: str):
        """Stel AMS tray in"""
        hex_clean = tray_color_hex.strip().lstrip("#")
        if len(hex_clean) == 6:
            hex_clean += "FF"
        elif len(hex_clean) != 8:
            hex_clean = "000000FF"
        
        mats = {
            "PLA": (190, 240),
            "ASA": (240, 270),
            "ASA-CF": (250, 280),
            "ABS": (230, 260),
            "PETG": (220, 250)
        }
        tmin, tmax = mats.get(tray_type.upper(), (190, 260))
        
        cmd = {
            "print": {
                "sequence_id": self._next_seq(),
                "command": "ams_filament_setting",
                "ams_id": int(ams_id),
                "tray_id": int(tray_id),
                "tray_info_idx": "",
                "tray_color": hex_clean.upper(),
                "nozzle_temp_min": int(tmin),
                "nozzle_temp_max": int(tmax),
                "tray_type": tray_type
            }
        }
        self._publish(cmd, qos=1)

# ========== MANAGER ==========
class Manager:
    """Centrale manager voor alle printers en status tracking"""
    
    def __init__(self):
        self.clients: Dict[str, PrinterClient] = {}
        self.clients_lock = threading.Lock()
        self.job_active: Dict[str, bool] = {}
        self.ams_state: Dict[str, Dict[int, Dict[str, Any]]] = {}
        self.ams_lock = threading.Lock()
        self._light_locks: Dict[str, threading.Lock] = {}
        self._light_locks_guard = threading.Lock()
    
    def _get_light_lock(self, device_id: str) -> threading.Lock:
        """Thread-safe light command lock per device"""
        with self._light_locks_guard:
            lk = self._light_locks.get(device_id)
            if lk is None:
                lk = threading.Lock()
                self._light_locks[device_id] = lk
            return lk
    
    def start_for_all(self):
        """Start MQTT clients voor alle printers"""
        with db_manager.get_connection() as conn:
            cur = conn.cursor()
            rows = cur.execute("SELECT * FROM printers").fetchall()
        
        for row in rows:
            p = dict(row)
            p["autoprint"] = bool(p.get("autoprint", 1))
            p["auto_ams_black_asacf"] = bool(p.get("auto_ams_black_asacf", 1))
            p["tags"] = normalize_tags(p.get("tags", "[]"))
            try:
                self.ensure_client(p["device_id"], p)
            except Exception as e:
                logger.error(f"Fout bij starten client {p['device_id']}: {e}")
    
    def ensure_client(self, device_id: str, cfg: Optional[dict] = None):
        """Zorg dat MQTT client actief is voor printer"""
        with self.clients_lock:
            if device_id in self.clients:
                if cfg:
                    self.clients[device_id].cfg = cfg
                return
            
            if not cfg:
                with db_manager.get_connection() as conn:
                    cur = conn.cursor()
                    row = cur.execute("SELECT * FROM printers WHERE device_id=?", (device_id,)).fetchone()
                if not row:
                    raise HTTPException(404, "Printer niet gevonden")
                cfg = dict(row)
                cfg["autoprint"] = bool(cfg.get("autoprint", 1))
                cfg["auto_ams_black_asacf"] = bool(cfg.get("auto_ams_black_asacf", 1))
                cfg["tags"] = normalize_tags(cfg.get("tags", "[]"))
            
            cli = PrinterClient(cfg, self._on_report)
            self.clients[device_id] = cli
            cli.start()
    
    def drop_client(self, device_id: str):
        """Stop en verwijder MQTT client"""
        with self.clients_lock:
            cli = self.clients.pop(device_id, None)
        if cli:
            try:
                cli.stop()
            except Exception as e:
                logger.error(f"Fout bij stoppen client {device_id}: {e}")
    
    def _tray_has_filament(self, tr: dict) -> bool:
        """Detecteer of tray filament bevat"""
        if not isinstance(tr, dict):
            return False
        
        ttype = str(tr.get("tray_type") or "").strip().upper()
        if ttype and ttype not in ("", "N/A", "NA", "NONE"):
            return True
        
        if str(tr.get("tag_uid") or "").strip():
            return True
        
        try:
            if float(tr.get("remain", 0) or 0) > 0:
                return True
        except (ValueError, TypeError):
            pass
        
        return False
    
    def note_manual_ams(self, device_id: str, tray_id: int):
        """Markeer handmatige AMS wijziging"""
        now = now_ts()
        with self.ams_lock:
            d = self.ams_state.setdefault(device_id, {})
            rec = d.setdefault(int(tray_id), {
                "had": None,
                "last_manual": 0.0,
                "manual_lock": False,
                "empty_since": 0.0,
                "_lock_seen": 0.0,
                "emptied_after_manual": False
            })
            rec["last_manual"] = now
            rec["manual_lock"] = True
            rec["emptied_after_manual"] = False
            rec["_lock_seen"] = now
        logger.debug(f"Handmatige AMS wijziging: {device_id} tray {tray_id}")
    
    def _on_report(self, device_id: str, payload: dict):
        """Verwerk MQTT report van printer"""
        kpi_dirty = False
        try:
            # Sla state op
            with db_manager.get_connection() as conn:
                cur = conn.cursor()
                cur.execute("""
                    INSERT INTO states (device_id, payload, updated_at)
                    VALUES (?, ?, datetime('now'))
                    ON CONFLICT(device_id) DO UPDATE SET 
                        payload=excluded.payload, 
                        updated_at=datetime('now')
                """, (device_id, json.dumps(payload)))
            
            # --- UI lock cleanup bij echte job start (RUNNING/PRINTING)
            try:
                pr = payload.get("print", payload)
                gstate = str(pr.get("gcode_state", "IDLE")).upper()
                if gstate in ("RUNNING", "PRINTING"):
                    ui_lock_clear(device_id)  # nieuwe job -> lock weg
            except Exception:
                pass

            # Verwerk events (job start/finish/fail)
            kpi_dirty = self._process_events(device_id, payload)

            # Verwerk alerts (HMS + print_error)
            self._process_alerts(device_id, payload)
            
            # Auto AMS label (zwart ASA-CF)
            self._process_auto_ams(device_id, payload)
            
            # (Geen auto-clear van UI lock hier; dat doen we op RUNNING/PRINTING)

        except Exception as e:
            logger.error(f"Fout bij verwerken report {device_id}: {e}", exc_info=True)
        finally:
            notify_printer_update(kpis=kpi_dirty)
    
    def _process_events(self, device_id: str, payload: dict) -> bool:
        """Detecteer en log job events."""
        dirty_kpis = False
        try:
            p = payload.get("print", payload)
            gcode_state = str(p.get("gcode_state", "IDLE")).upper()
            
            # Job start detectie
            if gcode_state in ("RUNNING", "PRINTING"):
                if not self.job_active.get(device_id):
                    self.job_active[device_id] = True
                    file_name = p.get("subtask_name") or p.get("gcode_file") or "unknown"
                    with db_manager.get_connection() as conn:
                        cur = conn.cursor()
                        cur.execute(
                            "INSERT INTO events (device_id, ts, type, meta) VALUES (?, datetime('now'), ?, ?)",
                            (device_id, "job_start", json.dumps({"file": file_name}))
                        )
                    logger.info(f"Job gestart: {device_id} - {file_name}")
            
            # Job finish detectie
            elif gcode_state in ("FINISH", "FINISHED", "SUCCESS"):
                if self.job_active.get(device_id):
                    self.job_active[device_id] = False
                    file_name = p.get("subtask_name") or p.get("gcode_file") or "unknown"
                    filament_g = float(p.get("gcode_weight", 0) or 0)
                    with db_manager.get_connection() as conn:
                        cur = conn.cursor()
                        cur.execute(
                            "INSERT INTO events (device_id, ts, type, meta) VALUES (?, datetime('now'), ?, ?)",
                            (device_id, "job_finish", json.dumps({"file": file_name, "filament_g": filament_g}))
                        )
                    logger.info(f"Job voltooid: {device_id} - {file_name}")
                    dirty_kpis = True

            # Job fail detectie
            elif gcode_state in ("FAILED", "FAIL"):
                if self.job_active.get(device_id):
                    self.job_active[device_id] = False
                    file_name = p.get("subtask_name") or p.get("gcode_file") or "unknown"
                    with db_manager.get_connection() as conn:
                        cur = conn.cursor()
                        cur.execute(
                            "INSERT INTO events (device_id, ts, type, meta) VALUES (?, datetime('now'), ?, ?)",
                            (device_id, "job_fail", json.dumps({"file": file_name}))
                        )
                    logger.warning(f"Job mislukt: {device_id} - {file_name}")
                    dirty_kpis = True

        except Exception as e:
            logger.error(f"Fout bij verwerken events {device_id}: {e}")
        return dirty_kpis
    
    def _process_alerts(self, device_id: str, payload: dict):
        """Verwerk HMS alerts en print errors"""
        try:
            p = payload.get("print", payload)

            # HMS alerts
            hms_val = p.get("hms")
            printer_name: Optional[str] = None
            with self.clients_lock:
                cli = self.clients.get(device_id)
                if cli:
                    name = cli.cfg.get("name")
                    if name:
                        printer_name = str(name)

            print_error = p.get("print_error")

            snapshot = collect_alert_snapshot(
                device_id,
                printer_name=printer_name,
                hms_val=hms_val,
                print_error_val=print_error,
            )
            if snapshot:
                loop = getattr(app.state, "loop", None)
                enqueue_alert_snapshot(snapshot, loop=loop)

            # Print error
            if print_error is not None:
                self._sync_print_error(device_id, print_error)

        except Exception as e:
            logger.error(f"Fout bij verwerken alerts {device_id}: {e}")
    
    def _sync_hms_alerts(self, device_id: str, hms_val: Any):
        """Synchroniseer HMS alerts naar database (placeholder/ingekort)"""
        pass
    
    def _sync_print_error(self, device_id: str, print_error_val: Any):
        """Synchroniseer print error alert (placeholder/ingekort)"""
        pass
    
    def _process_auto_ams(self, device_id: str, payload: dict):
        """Auto-label zwart ASA-CF bij vullen tray"""
        try:
            with db_manager.get_connection() as conn:
                cur = conn.cursor()
                row = cur.execute("SELECT auto_ams_black_asacf FROM printers WHERE device_id=?", (device_id,)).fetchone()
            
            if not row or not bool(row["auto_ams_black_asacf"]):
                return
            
            cli = self.clients.get(device_id)
            if not cli:
                return
            
            pr_block = payload.get("print", payload)
            ams_block = pr_block.get("ams") or {}
            units = ams_block.get("ams") if isinstance(ams_block, dict) else []
            
            if not (isinstance(units, list) and units):
                return
            
            now = now_ts()
            
            with self.ams_lock:
                dev_state = self.ams_state.setdefault(device_id, {})
                
                for unit in units:
                    trays = unit.get("tray") or []
                    if not isinstance(trays, list):
                        continue
                    
                    for tray in trays:
                        if "id" not in tray:
                            continue
                        
                        try:
                            tray_id = int(tray.get("id", 0))
                        except (ValueError, TypeError):
                            continue
                        
                        rec = dev_state.setdefault(tray_id, {
                            "had": None,
                            "last_manual": 0.0,
                            "manual_lock": False,
                            "empty_since": 0.0,
                            "_lock_seen": 0.0,
                            "emptied_after_manual": False
                        })
                        
                        # Check handmatige lock
                        last_manual = float(rec.get("last_manual", 0.0))
                        lock_seen = float(rec.get("_lock_seen", 0.0))
                        
                        if last_manual > lock_seen:
                            rec["manual_lock"] = True
                            rec["emptied_after_manual"] = False
                            rec["_lock_seen"] = last_manual
                        
                        # Huidige status
                        had_now = self._tray_has_filament(tray)
                        prev_had = rec.get("had")
                        
                        # Update lege timer
                        if not had_now:
                            if not rec.get("empty_since"):
                                rec["empty_since"] = now
                            
                            empty_duration = now - float(rec.get("empty_since", 0.0))
                            if empty_duration >= STABLE_EMPTY_SEC:
                                rec["manual_lock"] = False
                                rec["emptied_after_manual"] = True
                        else:
                            rec["empty_since"] = 0.0
                        
                        rec["had"] = bool(had_now)
                        
                        # Auto-label condities
                        if prev_had is None:
                            continue
                        if not (prev_had is False and had_now is True):
                            continue
                        if rec.get("manual_lock", False):
                            continue
                        if not rec.get("emptied_after_manual", True):
                            continue
                        
                        # Check of al correct
                        current_type = str(tray.get("tray_type") or "").strip().upper()
                        current_color = hex_rgba_to_css(tray.get("tray_color", "000000FF")).upper()
                        
                        if current_type == "ASA-CF" and current_color == "#000000":
                            continue
                        
                        # Voer auto-label uit
                        try:
                            ams_id = 0
                            try:
                                ams_id = int(unit.get("id", 0))
                            except (ValueError, TypeError):
                                pass
                            
                            cli.ams_filament_setting(
                                ams_id=ams_id,
                                tray_id=tray_id,
                                tray_type="ASA-CF",
                                tray_color_hex="000000"
                            )
                            logger.info(f"Auto-label uitgevoerd: {device_id} tray {tray_id} -> ASA-CF zwart")
                        
                        except Exception as e:
                            logger.debug(f"Auto-label fout {device_id} tray {tray_id}: {e}")
        
        except Exception as e:
            logger.error(f"Fout bij auto AMS verwerking {device_id}: {e}")
    
    def ui_printer_list(self) -> List[dict]:
        """Genereer printer lijst voor UI (respecteert UI-lock)"""
        out = []
        
        with db_manager.get_connection() as conn:
            cur = conn.cursor()
            printers = cur.execute("SELECT * FROM printers ORDER BY name").fetchall()
        
        for p_row in printers:
            p = dict(p_row)
            device_id = p["device_id"]
            
            # Haal state op
            with db_manager.get_connection() as conn:
                cur = conn.cursor()
                st_row = cur.execute("SELECT payload, updated_at FROM states WHERE device_id=?", (device_id,)).fetchone()
            
            payload = None
            updated_at = None
            if st_row:
                try:
                    payload = json.loads(st_row["payload"])
                except Exception:
                    pass
                updated_at = st_row["updated_at"]
            
            # Parse status
            status, prg, rem_mins, file_name, nozzle, bed, ams = self._parse_status(payload)
            light = self._payload_chamber_light_on(payload) or False
            
            # Check stale
            stale = True
            if updated_at:
                epoch = to_epoch(updated_at)
                if epoch and (now_ts() - epoch <= STATE_STALE_SECONDS):
                    stale = False
            
            # Check connected
            cli = self.clients.get(device_id)
            is_conn = (cli.connected.is_set() if cli else False) or (not stale)
            if not is_conn:
                status = "NO_CONN"
                prg = 0.0
                rem_mins = 0
                file_name = "-"
                nozzle = 0.0
                bed = 0.0
                light = False

            # --- Authoritatieve UI-lock toepassen
            try:
                lock = ui_lock_get(device_id)
            except Exception:
                lock = None
            if lock:
                lock_status = str(lock.get("status") or "").upper()
                if lock_status:
                    status = lock_status
                    if lock_status == "IDLE":
                        prg = 0.0
                        rem_mins = 0
                        file_name = "-"

            out.append({
                "device_id": device_id,
                "name": p["name"],
                "model": p["model"] or "X1 Carbon",
                "status": status,
                "progress": prg,
                "remaining_time": rem_mins,
                "file": file_name or "-",
                "nozzle_temp": nozzle,
                "bed_temp": bed,
                "ams": ams,
                "autoprint": bool(p.get("autoprint", 1)),
                "tags": normalize_tags(p.get("tags", "[]")),
                "light_on": bool(light),
                "chamber_light": bool(light),
                "lights": {"chamber": bool(light)}
            })
        
        return out
    
    def _parse_status(self, payload: Optional[dict]) -> Tuple[str, float, int, str, float, float, Optional[dict]]:
        """Parse status uit payload"""
        if not payload:
            return ("IDLE", 0.0, 0, "-", 0.0, 0.0, None)
        
        p = payload.get("print", {}) if "print" in payload else payload
        gcode_state = str(p.get("gcode_state", "IDLE")).upper()
        
        if gcode_state in ("RUNNING", "PRINTING"):
            status = "RUNNING"
        elif gcode_state in ("PAUSE", "PAUSED"):
            status = "PAUSE"
        elif gcode_state in ("FINISH", "FINISHED", "SUCCESS"):
            status = "FINISH"
        elif gcode_state in ("FAILED", "FAIL"):
            status = "FAILED"
        else:
            status = "IDLE"
        
        progress = float(p.get("mc_percent", 0) or 0.0)
        rem = int(p.get("mc_remaining_time", 0) or 0)
        if rem > 24 * 60 * 3:
            rem //= 60
        
        file_name = p.get("subtask_name") or p.get("gcode_file") or "-"
        nozzle = float(p.get("nozzle_temper", 0) or 0.0)
        bed = float(p.get("bed_temper", 0) or 0.0)
        
        ams_dict = None
        ams_block = p.get("ams", {}) or {}
        if ams_block:
            trays = []
            try:
                for ams_unit in ams_block.get("ams", []):
                    ams_id = int(ams_unit.get("id", 0))
                    for t in ams_unit.get("tray", []):
                        if "id" not in t:
                            continue
                        tid = int(t.get("id", 0))
                        empty = not self._tray_has_filament(t)
                        ttype = (t.get("tray_type") or "N/A")
                        color_hex = hex_rgba_to_css(t.get("tray_color", "000000FF"))
                        trays.append({
                            "id": tid,
                            "type": "-" if empty else ttype,
                            "color": color_hex,
                            "empty": bool(empty),
                            "ams_id": ams_id
                        })
            except Exception:
                pass
            if trays:
                ams_dict = {"trays": trays}
        
        return (status, progress, rem, file_name, nozzle, bed, ams_dict)
    
    def _payload_chamber_light_on(self, payload: Optional[dict]) -> Optional[bool]:
        """Extract chamber light status uit payload"""
        if not payload:
            return None
        p = payload.get("print", payload)
        lights = p.get("lights_report")
        if isinstance(lights, list):
            for it in lights:
                try:
                    if str(it.get("node", "")).lower() == "chamber_light":
                        return str(it.get("mode", "")).lower() != "off"
                except Exception:
                    pass
        return None
    
    def cleanup(self):
        """Cleanup alle clients en resources"""
        logger.info("Manager cleanup gestart")
        with self.clients_lock:
            device_ids = list(self.clients.keys())
        
        for device_id in device_ids:
            try:
                self.drop_client(device_id)
            except Exception as e:
                logger.error(f"Fout bij cleanup client {device_id}: {e}")

manager = Manager()

# ========== FASTAPI APP ==========
app = FastAPI(
    title="Bambu PrintFarm Backend",
    version="2.0.0",
    description="Productie-klare backend voor Bambu Lab 3D-printer beheer"
)
app.state.loop = None

# ========== PRINTER STATUS STREAM (SSE) ==========
PRINTER_STREAM_SUBSCRIBERS: Set["asyncio.Queue[dict]"] = set()
PRINTER_STREAM_LOCK = threading.Lock()


def _format_sse(data: dict) -> str:
    """Serialize payload naar SSE event."""
    try:
        payload = json.dumps(data, separators=(",", ":"))
    except Exception as e:
        logger.error(f"Kon SSE payload niet serialiseren: {e}")
        payload = "{}"
    return f"data: {payload}\n\n"


def _register_printer_stream_queue() -> "asyncio.Queue[dict]":
    q: "asyncio.Queue[dict]" = asyncio.Queue(maxsize=1)
    with PRINTER_STREAM_LOCK:
        PRINTER_STREAM_SUBSCRIBERS.add(q)
    return q


def _unregister_printer_stream_queue(q: "asyncio.Queue[dict]") -> None:
    with PRINTER_STREAM_LOCK:
        PRINTER_STREAM_SUBSCRIBERS.discard(q)


def notify_printer_update(*, kpis: bool = False) -> None:
    """Scheduleer realtime UI update voor alle SSE-clients."""
    loop = getattr(app.state, "loop", None)
    if loop is None:
        return

    with PRINTER_STREAM_LOCK:
        queues = list(PRINTER_STREAM_SUBSCRIBERS)

    flag = bool(kpis)

    def _push(q: "asyncio.Queue[dict]") -> None:
        merged = flag
        try:
            if q.full():
                try:
                    existing = q.get_nowait()
                except asyncio.QueueEmpty:
                    existing = None
                if isinstance(existing, dict):
                    merged = merged or bool(existing.get("kpis"))
            q.put_nowait({"kpis": merged})
        except Exception as e:
            logger.debug(f"Kon printer stream update niet plaatsen: {e}")

    for q in queues:
        loop.call_soon_threadsafe(_push, q)

router = APIRouter()


def _remove_tag_from_printer_rows(conn: sqlite3.Connection, normalized_tag: str) -> Optional[str]:
    """Verwijder de genormaliseerde tag uit de eerste printer die 'm bevat."""
    cur = conn.cursor()
    cur.execute("SELECT device_id, tags FROM printers")

    removed_from: Optional[str] = None
    for row in cur.fetchall():
        device_id = row["device_id"] if isinstance(row, sqlite3.Row) else row[0]
        tags = _parse_tags(row["tags"] if isinstance(row, sqlite3.Row) else row[1])
        normalized_existing = [_normalize_tag(x) for x in tags if _normalize_tag(x)]
        new_tags = [x for x in normalized_existing if x != normalized_tag]
        if new_tags != normalized_existing:
            cur.execute(
                "UPDATE printers SET tags=?, updated_at=strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE device_id=?",
                (json.dumps(new_tags), device_id),
            )
            removed_from = device_id
            break

    return removed_from


@router.delete("/api/config/printer-tags/{tag}")
def remove_printer_tag_endpoint(tag: str):
    """Verwijder een tag van de printer die 'm draagt. Idempotent."""
    t = _normalize_tag(tag)
    if not t:
        raise HTTPException(400, "Tag mag niet leeg zijn")

    attempts = 5
    delay = 0.1
    last_error: Optional[Exception] = None
    removed_from: Optional[str] = None

    for attempt in range(attempts):
        try:
            with db_manager.get_connection() as conn:
                removed_from = _remove_tag_from_printer_rows(conn, t)
            break
        except sqlite3.OperationalError as e:
            msg = str(e).lower()
            if "locked" in msg or "busy" in msg:
                last_error = e
                time.sleep(delay * (attempt + 1))
                continue
            raise HTTPException(500, f"Kon tag '{t}' niet verwijderen: {e}")
        except Exception as e:  # pragma: no cover - defensief
            raise HTTPException(500, f"Kon tag '{t}' niet verwijderen: {e}")
    else:
        raise HTTPException(503, f"Kon tag '{t}' niet verwijderen: database bezet") from last_error

    if removed_from:
        logger.info(f"Tag '{t}' verwijderd van printer {removed_from}")
        notify_printer_update()
    else:
        logger.debug(f"Tag '{t}' niet aangetroffen voor verwijdering")

    return {"removed_from": removed_from, "tag": t}

# ====== router aanmelden ======
app.include_router(router)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

# Global exception handler
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"Onverwachte fout: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"detail": "Interne serverfout"}
    )

# Health check endpoint
@app.get("/health")
def health_check():
    return {
        "status": "healthy",
        "version": "2.0.0",
        "timestamp": ts_iso()
    }

# ========== PYDANTIC MODELS ==========
class PrinterIn(BaseModel):
    device_id: str = Field(..., min_length=3, max_length=100)
    name: str = Field(..., min_length=1, max_length=200)
    model: str = Field(default="X1 Carbon", max_length=100)
    ip: Optional[str] = Field(None, max_length=45)
    lan_access_code: Optional[str] = Field(None, max_length=100)
    cloud_user_id: Optional[str] = Field(None, max_length=100)
    cloud_access_token: Optional[str] = Field(None, max_length=500)
    autoprint: bool = True
    tags: List[str] = Field(default_factory=list)
    
    @validator('tags')
    def validate_tags(cls, v):
        return normalize_tags(v)
    
    @validator('device_id', 'name')
    def no_special_chars(cls, v):
        if not re.match(r'^[a-zA-Z0-9_\-\s]+$', v):
            raise ValueError('Alleen alfanumerieke tekens, spaties, - en _ toegestaan')
        return v

class AutoPrintToggle(BaseModel):
    autoprint: bool

class AmsAutoLabelToggle(BaseModel):
    enabled: bool

class AmsSettingIn(BaseModel):
    ams_id: int = Field(default=0, ge=0, le=3)
    tray_id: int = Field(..., ge=0, le=3)
    tray_type: str = Field(..., max_length=50)
    tray_color: Optional[str] = Field(default="#000000", max_length=9)
    
    @validator('tray_color')
    def validate_color(cls, v):
        if not re.match(r'^#?[0-9A-Fa-f]{6}$', v):
            raise ValueError('Kleur moet hex format zijn (#RRGGBB)')
        return v

# ========== UI-LOCK HELPERS ==========
def ui_lock_set(device_id: str, status: str, reason: str = ""):
    status = (status or "").upper()
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO ui_state_locks (device_id, status, reason, locked_at)
            VALUES (?, ?, ?, datetime('now'))
            ON CONFLICT(device_id) DO UPDATE SET
              status=excluded.status,
              reason=excluded.reason,
              locked_at=excluded.locked_at
        """, (device_id, status, reason))
    notify_printer_update()

def ui_lock_get(device_id: str) -> Optional[dict]:
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        row = cur.execute("SELECT device_id, status, reason, locked_at FROM ui_state_locks WHERE device_id=?",
                          (device_id,)).fetchone()
    if not row:
        return None
    try:
        return dict(row)
    except Exception:
        return {"device_id": row[0], "status": row[1], "reason": row[2], "locked_at": row[3]}

def ui_lock_clear(device_id: str):
    with db_manager.get_connection() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM ui_state_locks WHERE device_id=?", (device_id,))

# ========== CONFIG ENDPOINTS ==========
@app.post("/api/orders/manual")
def create_manual_order(body: ManualOrderIn):
    try:
        # Hergebruik exact dezelfde pipeline als je AFAS script
        from afas_to_queue import (
            LOCAL_STL_MAP, engrave_text_in_stl, queue_upload_file,
            choose_or_get_assigned_printer, _normalize_tag, ensure_dir, config,
            log_print_upload, init_order_completion, update_order_completion
        )
    except Exception as e:
        raise HTTPException(500, f"Kan AFAS pipeline niet importeren: {e}")

    ordernr = body.ordernr.strip()
    itemcode = (body.itemcode or DEFAULT_ITEMCODE).strip()
    aantal = int(body.aantal)

    if not itemcode:
        raise HTTPException(400, "itemcode ontbreekt (stel DEFAULT_ITEMCODE in of stuur 'itemcode' mee).")

    bases = LOCAL_STL_MAP.get(itemcode)
    if not bases:
        raise HTTPException(400, f"Onbekende itemcode: {itemcode}")

    order_tag = _normalize_tag(ordernr)
    total_files = len(bases) * aantal

    # Printer kiezen (of bestaande tag gebruiken) + order tracking initialiseren
    device_id = choose_or_get_assigned_printer(order_tag, itemcode, ordernr, total_files)
    ensure_dir(config.output_dir)
    init_order_completion(order_tag, itemcode, ordernr, device_id, total_files)

    uploaded = 0
    failed = 0
    files_out: list[str] = []

    for _ in range(aantal):  # aantal = hoeveel sets
        for fname in bases:
            src = os.path.join(config.local_stl_base_dir, fname)
            if not os.path.isfile(src):
                failed += 1
                update_order_completion(order_tag, failed=1)
                continue

            try:
                # Graveer order_tag in STL en upload naar queue
                out = engrave_text_in_stl(
                    src, order_tag,
                    txt_size=config.text_size_mm,
                    cut_height=config.depth_mm,
                    y_shift=-20.0,
                    font="Arial",
                    mirror_x=True,
                    output_dir=config.output_dir
                )
                resp = queue_upload_file(out, order_tag)
                job_id = (resp.get("job") or {}).get("id")

                log_print_upload(
                    order_tag=order_tag,
                    itemcode=itemcode,
                    ordernr=ordernr,
                    filename=os.path.basename(out),
                    filepath=out,
                    size=os.path.getsize(out),
                    job_id=job_id,
                    device_id=device_id
                )
                update_order_completion(order_tag, completed=1)
                uploaded += 1
                files_out.append(os.path.basename(out))
            except Exception as e:
                failed += 1
                update_order_completion(order_tag, failed=1)
                # (optioneel) loggen

    return {
        "ok": uploaded > 0,
        "ordernr": ordernr,
        "order_tag": order_tag,
        "itemcode": itemcode,
        "device_id": device_id,
        "uploaded": uploaded,
        "failed": failed,
        "files": files_out
    }

@app.get("/api/config/printers")
def cfg_list():
    """Lijst alle printers"""
    try:
        with db_manager.get_connection() as conn:
            cur = conn.cursor()
            rows = cur.execute("SELECT * FROM printers ORDER BY name").fetchall()
        
        printers = []
        for row in rows:
            p = dict(row)
            p["autoprint"] = bool(p.get("autoprint", 1))
            p["auto_ams_black_asacf"] = bool(p.get("auto_ams_black_asacf", 1))
            p["tags"] = normalize_tags(p.get("tags", "[]"))
            printers.append(p)
        
        return printers
    except Exception as e:
        logger.error(f"Fout bij ophalen printers: {e}")
        raise HTTPException(500, "Kon printers niet ophalen")

@app.post("/api/config/printers")
def cfg_add(p: PrinterIn):
    """Voeg nieuwe printer toe"""
    try:
        normalized_tags = normalize_tags(p.tags)

        with db_manager.get_connection() as conn:
            cur = conn.cursor()
            existing = cur.execute("SELECT device_id FROM printers WHERE device_id=?", (p.device_id,)).fetchone()

            if existing:
                raise HTTPException(409, "Printer met dit device ID bestaat al")

            cur.execute("""
                INSERT INTO printers (device_id, name, model, ip, lan_access_code,
                                     cloud_user_id, cloud_access_token, autoprint, tags, auto_ams_black_asacf)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
            """, (p.device_id, p.name, p.model, p.ip, p.lan_access_code,
                  p.cloud_user_id, p.cloud_access_token, 1 if p.autoprint else 0,
                  json.dumps(normalized_tags)))

            _remove_tag_from_other_printers(conn, p.device_id, normalized_tags[0] if normalized_tags else None)

        cfg = p.dict()
        cfg["tags"] = normalized_tags
        cfg["auto_ams_black_asacf"] = True
        manager.ensure_client(p.device_id, cfg)

        notify_printer_update()

        logger.info(f"Printer toegevoegd: {p.device_id} ({p.name})")
        return {"ok": True, "device_id": p.device_id}
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Fout bij toevoegen printer: {e}", exc_info=True)
        raise HTTPException(500, "Kon printer niet toevoegen")

@app.put("/api/config/printers/{device_id}")
def cfg_update(device_id: str, p: PrinterIn):
    """Update bestaande printer"""
    if device_id != p.device_id:
        raise HTTPException(400, "Device ID kan niet gewijzigd worden")
    
    try:
        normalized_tags = normalize_tags(p.tags)

        with db_manager.get_connection() as conn:
            cur = conn.cursor()
            existing = cur.execute("SELECT auto_ams_black_asacf FROM printers WHERE device_id=?", (device_id,)).fetchone()

            if not existing:
                raise HTTPException(404, "Printer niet gevonden")

            cur.execute("""
                UPDATE printers
                SET name=?, model=?, ip=?, lan_access_code=?, cloud_user_id=?,
                    cloud_access_token=?, autoprint=?, tags=?, updated_at=datetime('now')
                WHERE device_id=?
            """, (p.name, p.model, p.ip, p.lan_access_code, p.cloud_user_id,
                  p.cloud_access_token, 1 if p.autoprint else 0,
                  json.dumps(normalized_tags), device_id))

            _remove_tag_from_other_printers(conn, device_id, normalized_tags[0] if normalized_tags else None)

            preserve_flag = bool(existing["auto_ams_black_asacf"])

        manager.drop_client(device_id)
        cfg = p.dict()
        cfg["tags"] = normalized_tags
        cfg["auto_ams_black_asacf"] = preserve_flag
        manager.ensure_client(device_id, cfg)

        notify_printer_update()

        logger.info(f"Printer geüpdatet: {device_id}")
        return {"ok": True}
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Fout bij updaten printer: {e}", exc_info=True)
        raise HTTPException(500, "Kon printer niet updaten")

@app.patch("/api/config/printers/{device_id}/autoprint")
def cfg_toggle_autoprint(device_id: str, body: AutoPrintToggle):
    """Toggle autoprint voor printer"""
    try:
        with db_manager.get_connection() as conn:
            cur = conn.cursor()
            result = cur.execute(
                "UPDATE printers SET autoprint=? WHERE device_id=?",
                (1 if body.autoprint else 0, device_id)
            )
            if result.rowcount == 0:
                raise HTTPException(404, "Printer niet gevonden")
        
        logger.info(f"Autoprint {'aan' if body.autoprint else 'uit'}: {device_id}")
        return {"ok": True, "autoprint": body.autoprint}
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Fout bij toggle autoprint: {e}")
        raise HTTPException(500, "Kon autoprint niet wijzigen")

@app.patch("/api/config/printers/{device_id}/auto_ams_black_asacf")
def cfg_toggle_auto_ams(device_id: str, body: AmsAutoLabelToggle):
    """Toggle auto AMS zwart ASA-CF"""
    try:
        with db_manager.get_connection() as conn:
            cur = conn.cursor()
            result = cur.execute(
                "UPDATE printers SET auto_ams_black_asacf=? WHERE device_id=?",
                (1 if body.enabled else 0, device_id)
            )
            if result.rowcount == 0:
                raise HTTPException(404, "Printer niet gevonden")
        
        logger.info(f"Auto AMS label {'aan' if body.enabled else 'uit'}: {device_id}")
        return {"ok": True, "auto_ams_black_asacf": body.enabled}
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Fout bij toggle auto AMS: {e}")
        raise HTTPException(500, "Kon auto AMS niet wijzigen")

@app.delete("/api/config/printers/{device_id}")
def cfg_delete(device_id: str):
    """Verwijder printer"""
    try:
        manager.drop_client(device_id)

        with db_manager.get_connection() as conn:
            cur = conn.cursor()
            cur.execute("DELETE FROM printers WHERE device_id=?", (device_id,))

        notify_printer_update()

        logger.info(f"Printer verwijderd: {device_id}")
        return {"ok": True}
    
    except Exception as e:
        logger.error(f"Fout bij verwijderen printer: {e}", exc_info=True)
        raise HTTPException(500, "Kon printer niet verwijderen")

# ========== PRINTER CONTROL ENDPOINTS ==========
@app.get("/api/printers")
def api_printers():
    """Lever UI-lijst; UI-lock is hierin al toegepast."""
    return manager.ui_printer_list()

@app.get("/api/stats")
def api_stats(range: str = Query("day", regex="^(day|week|month|total)$")):
    """Statistieken over periode"""
    try:
        with db_manager.get_connection() as conn:
            cur = conn.cursor()
            printers = cur.execute("SELECT COUNT(*) AS c FROM printers").fetchone()["c"]
            
            params: List[Any] = []
            if range == "day":
                where = "ts >= ?"
                try:
                    from zoneinfo import ZoneInfo
                    tz = ZoneInfo(LOCAL_TZ)
                except Exception:
                    tz = timezone.utc
                now_local = datetime.now(tz)
                start_local = datetime(now_local.year, now_local.month, now_local.day, 0, 0, 0, tzinfo=tz)
                start_utc = start_local.astimezone(timezone.utc)
                params = [start_utc.strftime("%Y-%m-%d %H:%M:%S")]
            elif range == "week":
                where = "ts >= datetime('now','-7 day')"
            elif range == "month":
                where = "ts >= datetime('now','-30 day')"
            else:
                where = "1=1"
            
            fails = cur.execute(f"SELECT COUNT(*) AS c FROM events WHERE type='job_fail' AND {where}", params).fetchone()["c"]
            finishes = cur.execute(f"SELECT COUNT(*) AS c FROM events WHERE type='job_finish' AND {where}", params).fetchone()["c"]
            rows = cur.execute(f"SELECT meta FROM events WHERE type='job_finish' AND {where}", params).fetchall()
        
        filament_g = 0.0
        for r in rows:
            try:
                m = json.loads(r["meta"])
                filament_g += float(m.get("filament_g", 0.0))
            except Exception:
                pass
        
        spools = filament_g / 1000.0 / 0.75 if filament_g > 0 else 0.0
        succ_rate = (finishes / (finishes + fails) * 100.0) if (finishes + fails) > 0 else None
        
        running = sum(1 for p in manager.ui_printer_list() if p["status"] == "RUNNING")
        
        return {
            "total": printers,
            "running": running,
            "fails": fails,
            "filament_kg": filament_g / 1000.0,
            "spools": spools,
            "success_rate": succ_rate
        }
    
    except Exception as e:
        logger.error(f"Fout bij ophalen stats: {e}")
        raise HTTPException(500, "Kon statistieken niet ophalen")

@app.post("/api/printers/{device_id}/pause_resume")
def api_pause_resume(device_id: str):
    """Pauzeer of hervat print"""
    try:
        manager.ensure_client(device_id)
        cli = manager.clients.get(device_id)
        if not cli:
            raise HTTPException(500, "MQTT client niet beschikbaar")
        
        with db_manager.get_connection() as conn:
            cur = conn.cursor()
            st_row = cur.execute("SELECT payload FROM states WHERE device_id=?", (device_id,)).fetchone()
        
        payload = None
        if st_row:
            try:
                payload = json.loads(st_row["payload"])
            except Exception:
                pass
        
        status = manager._parse_status(payload)[0]
        
        if status == "PAUSE":
            cli.resume()
            logger.info(f"Print hervat: {device_id}")
        elif status == "RUNNING":
            cli.pause()
            logger.info(f"Print gepauzeerd: {device_id}")
        else:
            raise HTTPException(400, f"Kan niet pauzeren/hervatten in status {status}")
        
        return {"ok": True, "action": "resume" if status == "PAUSE" else "pause"}
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Fout bij pause/resume {device_id}: {e}")
        raise HTTPException(500, "Kon print niet pauzeren/hervatten")

@app.post("/api/printers/{device_id}/stop")
def api_stop(device_id: str):
    """
    Stop print en zet een persistente UI-lock naar IDLE.
    Lock blijft actief over refresh/reboot en gaat pas weg bij RUNNING/PRINTING.
    """
    manager.ensure_client(device_id)
    cli = manager.clients.get(device_id)
    if not cli:
        raise HTTPException(500, "MQTT client niet beschikbaar")

    # Probeer te stoppen; maar hoe dan ook zetten we de UI-lock
    try:
        cli.stop_print()
    except Exception as e:
        logger.warning(f"Stop commando had een fout: {e} (UI-lock wordt alsnog gezet)")

    ui_lock_set(device_id, "IDLE", "manual-stop-soft-idle")
    logger.info(f"UI-lock gezet: {device_id} -> IDLE (manual-stop-soft-idle)")
    return {"ok": True, "forced_idle": True}

@app.post("/api/printers/{device_id}/light/{mode}")
def api_light_set(device_id: str, mode: str):
    """Stel kamer licht in"""
    mode = str(mode).lower()
    if mode not in ("on", "off", "flashing"):
        raise HTTPException(400, "Mode moet 'on', 'off' of 'flashing' zijn")
    
    try:
        manager.ensure_client(device_id)
        cli = manager.clients.get(device_id)
        if not cli:
            raise HTTPException(500, "MQTT client niet beschikbaar")
        
        lk = manager._get_light_lock(device_id)
        with lk:
            try:
                ack = cli.set_led("chamber_light", mode, timeout=1.5)
            except Exception as e:
                raise HTTPException(500, f"Licht schakelen mislukt: {e}")
            
            if not ack or str(ack.get("result", "")).lower() not in ("success", "ok", "succeed"):
                try:
                    ack = cli.set_led("chamber_light", mode, timeout=1.0)
                except Exception:
                    raise HTTPException(504, "Geen bevestiging van printer")
                if not ack or str(ack.get("result", "")).lower() not in ("success", "ok", "succeed"):
                    raise HTTPException(504, "Geen bevestiging van printer")
            
            logger.info(f"Licht geschakeld {mode}: {device_id}")
            return {"ok": True, "mode": mode}
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Fout bij light set {device_id}: {e}")
        raise HTTPException(500, "Kon licht niet schakelen")

@app.post("/api/printers/{device_id}/light/toggle")
def api_light_toggle(device_id: str):
    """Toggle kamer licht"""
    try:
        manager.ensure_client(device_id)
        cli = manager.clients.get(device_id)
        if not cli:
            raise HTTPException(500, "MQTT client niet beschikbaar")
        
        lk = manager._get_light_lock(device_id)
        with lk:
            with db_manager.get_connection() as conn:
                cur = conn.cursor()
                st_row = cur.execute("SELECT payload FROM states WHERE device_id=?", (device_id,)).fetchone()
            
            cur_on_opt = None
            if st_row:
                try:
                    payload = json.loads(st_row["payload"])
                    cur_on_opt = manager._payload_chamber_light_on(payload)
                except Exception:
                    pass
            
            target = "off" if cur_on_opt is True else "on"
            
            try:
                ack = cli.set_led("chamber_light", target, timeout=1.5)
            except Exception as e:
                raise HTTPException(500, f"Lamp toggle mislukt: {e}")
            
            if not ack or str(ack.get("result", "")).lower() not in ("success", "ok", "succeed"):
                try:
                    ack = cli.set_led("chamber_light", target, timeout=1.0)
                except Exception:
                    raise HTTPException(504, "Geen bevestiging van printer")
                if not ack or str(ack.get("result", "")).lower() not in ("success", "ok", "succeed"):
                    raise HTTPException(504, "Geen bevestiging van printer")
            
            logger.info(f"Licht getoggled naar {target}: {device_id}")
            return {"ok": True, "mode": target}
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Fout bij light toggle {device_id}: {e}")
        raise HTTPException(500, "Kon licht niet togglen")

# ========== AMS ENDPOINTS ==========
@app.post("/api/printers/{device_id}/ams/filament_setting")
def api_ams_set(device_id: str, body: AmsSettingIn):
    """Stel AMS tray in"""
    try:
        manager.ensure_client(device_id)
        cli = manager.clients.get(device_id)
        if not cli:
            raise HTTPException(500, "MQTT client niet beschikbaar")
        
        cli.ams_filament_setting(body.ams_id, body.tray_id, body.tray_type, body.tray_color or "#000000")
        manager.note_manual_ams(device_id, int(body.tray_id))
        
        logger.info(f"AMS tray ingesteld: {device_id} tray {body.tray_id} -> {body.tray_type}")
        return {"ok": True}
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Fout bij AMS set {device_id}: {e}")
        raise HTTPException(500, "Kon AMS tray niet instellen")

# ========== CAMERA COMPATIBILITEIT ==========
@app.get("/api/stream/printers")
async def api_stream_printers(request: Request):
    """Realtime status updates via Server-Sent Events."""

    queue = _register_printer_stream_queue()

    async def event_generator():
        try:
            try:
                initial = await asyncio.to_thread(manager.ui_printer_list)
            except Exception as e:
                logger.error(f"Kon printerlijst niet laden voor SSE start: {e}")
                initial = []
            yield _format_sse({"printers": initial})

            while True:
                try:
                    msg = await asyncio.wait_for(queue.get(), timeout=25.0)
                except asyncio.TimeoutError:
                    if await request.is_disconnected():
                        break
                    yield ": keep-alive\n\n"
                    continue

                if await request.is_disconnected():
                    break

                try:
                    printers = await asyncio.to_thread(manager.ui_printer_list)
                except Exception as e:
                    logger.error(f"Kon printerlijst niet laden voor SSE update: {e}")
                    continue

                payload = {"printers": printers}
                if isinstance(msg, dict) and msg.get("kpis"):
                    payload["kpis"] = True
                yield _format_sse(payload)
        finally:
            _unregister_printer_stream_queue(queue)

    headers = {
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
    }
    return StreamingResponse(event_generator(), media_type="text/event-stream", headers=headers)

@app.post("/api/printers/{device_id}/snapshot/start")
def api_snapshot_start(device_id: str):
    """Geef nette melding dat snapshotten niet meer beschikbaar is."""
    logger.info("Snapshot request voor %s, maar camera is uitgeschakeld", device_id)
    return JSONResponse(
        status_code=200,
        content={
            "ok": False,
            "camera_enabled": False,
            "message": "Camera functionaliteit is uitgeschakeld",
        },
    )

# ========== ALERTS ENDPOINTS (ingekort) ==========
@app.get("/api/alerts")
def api_alerts(
    device_id: Optional[str] = Query(None),
    q: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=2000),
    state: Optional[str] = Query(None, regex="^(open|closed|all)$"),
    range: Optional[str] = Query(None),
):
    """Lijst alerts met filtering (placeholder)."""
    return {"items": [], "total": 0}

@app.get("/api/alerts/summary")
def api_alerts_summary(
    device_id: Optional[str] = Query(None),
    range: Optional[str] = Query(None),
):
    """Alert samenvatting (placeholder)."""
    return {"open": 0, "today": 0, "unique_codes": 0}

# ========== WEBSOCKET ==========
@app.websocket("/ws/alerts")
async def ws_alerts(ws: WebSocket):
    """WebSocket voor realtime alerts"""
    await ws.accept()
    WS_CLIENTS.add(ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        WS_CLIENTS.discard(ws)
        try:
            await ws.close()
        except Exception:
            pass

# ========== LIFECYCLE EVENTS ==========
@app.on_event("startup")
async def startup_event():
    """Applicatie startup"""
    logger.info("=== Bambu PrintFarm Backend Start ===")
    logger.info(f"Versie: 2.0.0")
    logger.info(f"Data directory: {DATA_DIR}")
    logger.info(f"Database: {DB_PATH}")

    try:
        app.state.loop = asyncio.get_running_loop()
    except RuntimeError:
        app.state.loop = None
    
    # Init database
    try:
        db_manager.init_schema()
    except Exception as e:
        logger.error(f"Database initialisatie mislukt: {e}", exc_info=True)
        sys.exit(1)
    
    # Start HMS catalog
    try:
        hms.load_from_disk()
        hms.start_background_refresh()
    except Exception as e:
        logger.error(f"HMS catalog start mislukt: {e}")
    
    # Start MQTT clients
    try:
        manager.start_for_all()
    except Exception as e:
        logger.error(f"Manager start mislukt: {e}", exc_info=True)
    
    # Start WebSocket broadcaster
    app.state.alerts_task = asyncio.create_task(alerts_broadcaster())

    # Start achtergrondservices
    try:
        service_manager.start_all()
    except Exception as e:
        logger.error(f"Kon achtergrondservices niet starten: {e}")

    logger.info("=== Backend succesvol gestart ===")

@app.on_event("shutdown")
async def shutdown_event():
    """Applicatie shutdown"""
    logger.info("=== Bambu PrintFarm Backend Shutdown ===")

    app.state.loop = None
    
    # Stop WebSocket broadcaster
    task = getattr(app.state, "alerts_task", None)
    if task:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Fout bij stoppen WebSocket broadcaster: {e}")
    
    # Stop HMS catalog
    try:
        hms.stop_background_refresh()
    except Exception as e:
        logger.error(f"Fout bij stoppen HMS catalog: {e}")
    
    # Stop manager en clients
    try:
        manager.cleanup()
    except Exception as e:
        logger.error(f"Fout bij manager cleanup: {e}")

    # Stop achtergrondservices
    try:
        service_manager.stop_all()
    except Exception as e:
        logger.error(f"Fout bij stoppen achtergrondservices: {e}")
    
    logger.info("=== Backend succesvol gestopt ===")

# Mount static files (laatste, catch-all)
if WEB_DIR.exists():
    app.mount("/", StaticFiles(directory=str(WEB_DIR), html=True), name="web")

# ========== SIGNAL HANDLERS ==========
def signal_handler(signum, frame):
    """Graceful shutdown op SIGTERM/SIGINT"""
    logger.info(f"Signal {signum} ontvangen, shutdown wordt gestart...")
    sys.exit(0)

signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)

# ========== MAIN ==========
def main():
    """Main entry point"""
    ensure_dirs()
    
    # Configuratie logging
    log_level = os.environ.get("LOG_LEVEL", "INFO").upper()
    logging.getLogger().setLevel(getattr(logging, log_level, logging.INFO))
    
    logger.info(f"Starting Bambu PrintFarm Backend op {APP_HOST}:{APP_PORT}")
    
    # Uvicorn configuratie
    uvicorn_config = {
        "app": app,
        "host": APP_HOST,
        "port": APP_PORT,
        "log_level": log_level.lower(),
        "access_log": True,
        "server_header": False,
        "date_header": False,
        "forwarded_allow_ips": "*",  # Voor reverse proxy ondersteuning
        "proxy_headers": True
    }
    
    # Optioneel: SSL/TLS configuratie
    ssl_cert = os.environ.get("SSL_CERT")
    ssl_key = os.environ.get("SSL_KEY")
    if ssl_cert and ssl_key:
        if pathlib.Path(ssl_cert).exists() and pathlib.Path(ssl_key).exists():
            uvicorn_config["ssl_certfile"] = ssl_cert
            uvicorn_config["ssl_keyfile"] = ssl_key
            logger.info("SSL/TLS ingeschakeld")
        else:
            logger.warning("SSL certificaat bestanden niet gevonden, SSL uitgeschakeld")
    
    uvicorn.run(**uvicorn_config)

if __name__ == "__main__":
    main()
