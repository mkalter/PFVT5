#!/usr/bin/env python3
"""
Printer Uploader Service v4.0 - Production Ready

FIXES:
- Unified file matching logic
- Atomic job assignment with locks
- Improved database concurrency
- Print retry logic with backoff
- Removed duplicate reconciliation (delegated to queue_service)
- Better error handling and logging
"""

import os
import ssl
import logging
import pathlib
import threading
import time
import json
import sqlite3
import socket
import io
import random
from typing import Optional, Dict, List, Any
from ftplib import FTP_TLS, FTP
from dataclasses import dataclass
from urllib.request import urlopen, Request
from datetime import datetime

# ====== MQTT ======
try:
    import paho.mqtt.client as mqtt
except Exception:
    mqtt = None

logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s"
)
logger = logging.getLogger("uploader")

# ------------ Config ------------
DATA_DIR = pathlib.Path(os.environ.get("DATA_DIR", "./data")).resolve()
DB_PATH = DATA_DIR / "queue.db"
FARM_API_BASE = os.environ.get("FARM_API_BASE", "http://127.0.0.1:8000")
UPLOAD_CHECK_INTERVAL = int(os.environ.get("UPLOAD_CHECK_INTERVAL", "10"))
MAX_UPLOAD_RETRIES = int(os.environ.get("UPLOAD_MAX_RETRIES", "3"))
MAX_PRINT_RETRIES = int(os.environ.get("MAX_PRINT_RETRIES", "3"))

# Configuratie
AUTO_PRINT_USE_AMS = os.environ.get("AUTO_PRINT_USE_AMS", "1").strip().lower() not in ("0", "false", "no")
AUTO_PRINT_UPLOAD_DIRS = os.environ.get("AUTO_PRINT_UPLOAD_DIRS", "cache")
ASSIGN_TO_FINISH = os.environ.get("ASSIGN_TO_FINISH", "false").lower() in ("true", "1")

FTPS_USERNAME = "bblp"

# ------------- Dataclasses -------------
@dataclass
class PrinterConfig:
    device_id: str
    ip: str
    lan_access_code: str
    name: str = ""
    tags: List[str] = None
    autoprint: bool = True

    def __post_init__(self):
        if self.tags is None:
            self.tags = []

@dataclass
class PrinterStatus:
    device_id: str
    status: str   # IDLE, RUNNING, PAUSE, FINISH, FAILED, NO_CONN
    progress: float
    file: str

# ------------- Shared Utilities -------------
def _tls_ctx() -> ssl.SSLContext:
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx

def _boost_sock(s):
    if not s:
        return
    try:
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    except Exception:
        pass
    try:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1024 * 1024)
    except Exception:
        pass

def _now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def normalize_filename(filename: str) -> str:
    """Normalize filename for comparison (no path, no extension, lowercase)"""
    if not filename:
        return ""
    base = os.path.basename(filename)
    name_no_ext = os.path.splitext(base)[0]
    return name_no_ext.strip().lower()

def files_match(job_filename: str, printer_filename: str) -> bool:
    """
    Unified file matching logic used across all services.
    Compares normalized filenames (case-insensitive, no extension).
    """
    if not job_filename or not printer_filename:
        return False
    
    job_norm = normalize_filename(job_filename)
    printer_norm = normalize_filename(printer_filename)
    
    if not job_norm or not printer_norm or job_norm == '-' or printer_norm == '-':
        return False
    
    # Exact match preferred
    if job_norm == printer_norm:
        return True
    
    # Fallback: substring match (for cases like "model_v2" vs "model_v2_plate_1")
    return job_norm in printer_norm or printer_norm in job_norm

# ------------- MQTT client -------------
class BambuMQTTClient:
    def __init__(self, device_id: str, ip: str, lan_access_code: str, timeout: float = 30.0):
        if mqtt is None:
            raise RuntimeError("paho-mqtt is niet geïnstalleerd. Voer 'pip install paho-mqtt' uit.")
        self.device_id = device_id
        self.ip = ip
        self.lan_access_code = lan_access_code
        self.timeout = timeout

        self._client = mqtt.Client(
            client_id=f"printfarm-uploader-{device_id}-{random.randint(1000, 9999)}",
            clean_session=True
        )

        ctx = _tls_ctx()
        self._client.tls_set_context(ctx)
        self._client.username_pw_set("bblp", self.lan_access_code)

        self._connected = threading.Event()
        self._result_event = threading.Event()
        self._target_seq: Optional[str] = None
        self._target_cmd: Optional[str] = None
        self._result_payload: Optional[Dict[str, Any]] = None

        self._client.on_connect = self._on_connect
        self._client.on_message = self._on_message
        self._client.on_disconnect = self._on_disconnect

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            topic = f"device/{self.device_id}/report"
            client.subscribe(topic, qos=1)
            self._connected.set()
            logger.debug(f"[MQTT] Connected + subscribed to {topic}")
        else:
            logger.error(f"[MQTT] Connect failed rc={rc}")

    def _on_disconnect(self, client, userdata, rc):
        logger.debug(f"[MQTT] Disconnected rc={rc}")

    def _on_message(self, client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode("utf-8", errors="ignore"))
        except Exception:
            return
        for key in ("print", "system", "camera", "xcam", "info", "pushing", "upgrade", "mc_print"):
            node = payload.get(key)
            if not isinstance(node, dict):
                continue
            seq = str(node.get("sequence_id", ""))
            cmd = str(node.get("command", ""))
            if self._target_seq and self._target_cmd:
                if seq == self._target_seq and cmd == self._target_cmd:
                    self._result_payload = node
                    self._result_event.set()
                    return

    def __enter__(self):
        self._client.connect(self.ip, 8883, keepalive=15)
        self._client.loop_start()
        if not self._connected.wait(timeout=self.timeout):
            raise TimeoutError("MQTT connect timeout")
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            self._client.loop_stop()
        finally:
            try:
                self._client.disconnect()
            except Exception:
                pass

    @staticmethod
    def _seq() -> str:
        return str(int(time.time() * 1000) % 1_000_000_000)

    def send_print_project_file(
        self,
        remote_relpath: str,
        plate: int = 1,
        use_ams: Optional[bool] = None,
        ams_mapping: Optional[List[int]] = None,
        timelapse: Optional[bool] = None,
        bed_levelling: bool = True,
        flow_cali: bool = True,
        vibration_cali: bool = True,
        layer_inspect: bool = True,
        gcode_param: Optional[str] = None,
    ) -> Dict[str, Any]:
        seq = self._seq()
        self._target_seq = seq
        self._target_cmd = "project_file"
        self._result_payload = None
        self._result_event.clear()

        url = f"ftp:///{remote_relpath}"
        param = gcode_param or f"Metadata/plate_{max(1, int(plate))}.gcode"

        payload = {
            "print": {
                "sequence_id": seq,
                "command": "project_file",
                "project_id": "0",
                "profile_id": "0",
                "task_id": "0",
                "subtask_id": "0",
                "subtask_name": "",
                "param": param,
                "file": "",
                "url": url,
                "md5": "",
                "timelapse": bool(timelapse) if timelapse is not None else True,
                "bed_type": "auto",
                "bed_levelling": bool(bed_levelling),
                "flow_cali": bool(flow_cali),
                "vibration_cali": bool(vibration_cali),
                "layer_inspect": bool(layer_inspect),
                "use_ams": bool(use_ams) if use_ams is not None else False,
            }
        }
        if ams_mapping is not None:
            payload["print"]["ams_mapping"] = ams_mapping
            payload["print"]["use_ams"] = True

        topic = f"device/{self.device_id}/request"
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        logger.info(f"[MQTT] → {topic} :: project_file url={url} param={param}")
        self._client.publish(topic, body, qos=1)

        if not self._result_event.wait(timeout=self.timeout):
            raise TimeoutError("Geen bevestiging ontvangen voor print.project_file")

        return self._result_payload or {}

# ------------- FTPS helpers -------------
class _ImplicitFTP_TLS(FTP_TLS):
    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.context = _tls_ctx()
        self._sock = None
        self.af = socket.AF_INET
        self._epsv = False

    @property
    def sock(self):
        return self._sock

    @sock.setter
    def sock(self, value):
        if value is not None and not isinstance(value, ssl.SSLSocket):
            value = self.context.wrap_socket(value)
        self._sock = value
        _boost_sock(self._sock)

    def ntransfercmd(self, cmd, rest=None):
        conn, size = FTP.ntransfercmd(self, cmd, rest)
        if self._prot_p:
            conn = self.context.wrap_socket(
                conn, server_hostname=self.host,
                session=getattr(self.sock, "session", None)
            )
            _boost_sock(conn)
        return conn, size

# ------------- FTPS uploader -------------
class FTPSUploader:
    def __init__(self, printer: PrinterConfig):
        self.printer = printer

    def upload_file(
        self,
        local_path: pathlib.Path,
        remote_filename: Optional[str] = None,
        preferred_dirs: Optional[List[str]] = None,
    ) -> Optional[str]:
        if not local_path.exists():
            logger.error(f"Lokaal bestand niet gevonden: {local_path}")
            return None

        base = (remote_filename or local_path.name)
        if not base.lower().endswith(".3mf"):
            base += ".3mf"

        env_dirs = [d.strip().strip("/") for d in AUTO_PRINT_UPLOAD_DIRS.split(",") if d.strip()]
        pref = [d.strip().strip("/") for d in (preferred_dirs or []) if d and d.strip()]
        candidates: List[str] = [f"{d}/{base}" for d in (pref + env_dirs)]
        candidates.append(base)

        with open(local_path, "rb") as f:
            payload = f.read()

        file_size_mb = len(payload) / 1024 / 1024
        logger.info(f"Upload {local_path.name} → {self.printer.name} ({self.printer.ip}) [{file_size_mb:.2f} MB]")

        methods = [self._upload_implicit_990, self._upload_explicit_21, self._upload_plain_21]
        errors = []
        for remote_rel in candidates:
            for m in methods:
                try:
                    if m(remote_rel, payload):
                        logger.info(f"✓ Upload gelukt via {m.__name__} als '{remote_rel}'")
                        return remote_rel
                except Exception as e:
                    errors.append(f"{m.__name__}::{remote_rel}: {e}")

        logger.error("Alle upload methoden gefaald: " + " | ".join(errors))
        return None

    def _upload_implicit_990(self, remote_relpath: str, payload: bytes) -> bool:
        BLOCK = 262144
        TIMEOUT = 20
        ftps = _ImplicitFTP_TLS()
        ftps.connect(host=self.printer.ip, port=990, timeout=TIMEOUT)
        _boost_sock(ftps.sock)
        ftps.login(FTPS_USERNAME, self.printer.lan_access_code)
        ftps.prot_p()
        ftps.set_pasv(True)
        try: ftps.voidcmd("TYPE I")
        except Exception: pass
        bio = io.BytesIO(payload); bio.seek(0)
        resp = ftps.storbinary(f"STOR {remote_relpath}", bio, blocksize=BLOCK)
        try: ftps.quit()
        except Exception: ftps.close()
        return '226' in resp or '250' in resp

    def _upload_explicit_21(self, remote_relpath: str, payload: bytes) -> bool:
        BLOCK = 262144
        TIMEOUT = 20
        ftps = FTP_TLS()
        ftps.context = _tls_ctx()
        ftps.af = socket.AF_INET
        ftps._epsv = False
        ftps.connect(self.printer.ip, 21, timeout=TIMEOUT)
        _boost_sock(ftps.sock)
        ftps.auth()
        ftps.login(FTPS_USERNAME, self.printer.lan_access_code)
        ftps.prot_p()
        ftps.set_pasv(True)
        try: ftps.voidcmd("TYPE I")
        except Exception: pass
        bio = io.BytesIO(payload); bio.seek(0)
        resp = ftps.storbinary(f"STOR {remote_relpath}", bio, blocksize=BLOCK)
        try: ftps.quit()
        except Exception: ftps.close()
        return '226' in resp or '250' in resp

    def _upload_plain_21(self, remote_relpath: str, payload: bytes) -> bool:
        BLOCK = 262144
        TIMEOUT = 15
        ftp = FTP()
        ftp.af = socket.AF_INET
        ftp._epsv = False
        ftp.connect(self.printer.ip, 21, timeout=TIMEOUT)
        _boost_sock(ftp.sock)
        ftp.login(FTPS_USERNAME, self.printer.lan_access_code)
        ftp.set_pasv(True)
        try: ftp.voidcmd("TYPE I")
        except Exception: pass
        bio = io.BytesIO(payload); bio.seek(0)
        resp = ftp.storbinary(f"STOR {remote_relpath}", bio, blocksize=BLOCK)
        try: ftp.quit()
        except Exception: ftp.close()
        return '226' in resp or '250' in resp

# ------------- Upload Manager -------------
class UploadManager:
    def __init__(self):
        self.running = False
        self.thread = None
        self.printers_cache: Dict[str, PrinterConfig] = {}
        self.status_cache: Dict[str, PrinterStatus] = {}
        self.cache_time = 0
        self.cache_ttl = 10

    # --- DB ---
    def db_conn(self):
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        con = sqlite3.connect(DB_PATH, timeout=30.0)
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA journal_mode=WAL;")
        con.execute("PRAGMA busy_timeout=30000;")
        con.isolation_level = "IMMEDIATE"
        return con

    # --- Farm API fetches ---
    def fetch_printers_config(self, force: bool = False) -> List[PrinterConfig]:
        """Haal printer-config op uit Farm API (incl. autoprint)."""
        now = time.time()
        if not force and now - self.cache_time < self.cache_ttl and self.printers_cache:
            return list(self.printers_cache.values())

        try:
            url = f"{FARM_API_BASE.rstrip('/')}/api/config/printers"
            req = Request(url, headers={"User-Agent": "printfarm-uploader/4.0"})
            with urlopen(req, timeout=5) as r:
                data = json.loads(r.read().decode("utf-8"))

            self.printers_cache.clear()
            for p in data if isinstance(data, list) else []:
                device_id = p.get("device_id")
                ip = p.get("ip")
                lan_code = p.get("lan_access_code")
                if device_id and ip and lan_code:
                    tags = p.get("tags", [])
                    if isinstance(tags, str):
                        tags = [t.strip() for t in tags.split(",") if t.strip()]
                    self.printers_cache[device_id] = PrinterConfig(
                        device_id=device_id,
                        ip=ip,
                        lan_access_code=lan_code,
                        name=p.get("name", "") or "",
                        tags=tags or [],
                        autoprint=bool(p.get("autoprint", True)),
                    )

            self.cache_time = now
            logger.info(f"Cached {len(self.printers_cache)} printer configs")
            return list(self.printers_cache.values())

        except Exception as e:
            logger.error(f"Kon printers niet ophalen: {e}")
            return list(self.printers_cache.values())

    def fetch_printer_status(self) -> Dict[str, PrinterStatus]:
        """Haal real-time printer status op van Farm API."""
        try:
            url = f"{FARM_API_BASE.rstrip('/')}/api/printers"
            req = Request(url, headers={"User-Agent": "printfarm-uploader/4.0"})
            with urlopen(req, timeout=5) as r:
                data = json.loads(r.read().decode("utf-8"))

            self.status_cache.clear()
            for p in data if isinstance(data, list) else []:
                device_id = p.get("device_id")
                if device_id:
                    self.status_cache[device_id] = PrinterStatus(
                        device_id=device_id,
                        status=p.get("status", "IDLE"),
                        progress=float(p.get("progress", 0)),
                        file=p.get("file", "-")
                    )
            return self.status_cache

        except Exception as e:
            logger.error(f"Kon printer status niet ophalen: {e}")
            return self.status_cache

    # --- Queue helpers ---
    def get_pending_jobs(self) -> List[Dict]:
        """Haal unassigned jobs op (device_id=NULL/''/'auto')."""
        con = self.db_conn()
        cur = con.cursor()
        rows = cur.execute("""
            SELECT * FROM queue_jobs
            WHERE status='PENDING'
              AND (device_id IS NULL OR device_id='' OR device_id='auto')
            ORDER BY created_at ASC
            LIMIT 100
        """).fetchall()
        con.close()
        return [dict(r) for r in rows]

    def get_assigned_ready_jobs(self) -> List[Dict]:
        """Haal jobs op die al toegewezen zijn en klaar voor upload/print."""
        con = self.db_conn()
        cur = con.cursor()
        rows = cur.execute("""
            SELECT * FROM queue_jobs
            WHERE status='READY'
              AND device_id IS NOT NULL
              AND device_id!=''
              AND device_id!='auto'
            ORDER BY created_at ASC
            LIMIT 50
        """).fetchall()
        con.close()
        return [dict(r) for r in rows]

    def update_job_status(self, job_id: str, status: str, error: Optional[str] = None):
        """Zet status en vul relevante timestamps."""
        now = _now_iso()
        fields = ["status=?"]
        params: List[Any] = [status]

        if error is not None:
            fields.append("error=?")
            params.append(error)

        if status == "UPLOADED":
            fields.append("uploaded_at=?")
            params.append(now)
        elif status == "PRINTING":
            fields.append("started_at=COALESCE(started_at, ?)")
            params.append(now)
        elif status in ("COMPLETED", "FAILED"):
            fields.append("completed_at=?")
            params.append(now)

        sql = f"UPDATE queue_jobs SET {', '.join(fields)} WHERE id=?"
        params.append(job_id)

        con = self.db_conn()
        cur = con.cursor()
        cur.execute(sql, params)
        con.commit()
        con.close()

    def requeue_job(self, job_id: str):
        """Zet job terug naar PENDING en maak device_id 'auto'."""
        con = self.db_conn()
        cur = con.cursor()
        cur.execute(
            "UPDATE queue_jobs SET status='PENDING', device_id='auto', error=NULL WHERE id=?", 
            (job_id,)
        )
        con.commit()
        con.close()
        logger.info(f"Job {job_id} teruggezet naar PENDING")

    def assign_job_to_device(self, job_id: str, device_id: str) -> bool:
        """Atomic job assignment with conflict detection"""
        con = self.db_conn()
        cur = con.cursor()
        
        try:
            # Check if printer already has active jobs
            existing = cur.execute("""
                SELECT id FROM queue_jobs 
                WHERE device_id=? 
                  AND status IN ('READY','UPLOADING','PRINTING')
                  AND id != ?
                LIMIT 1
            """, (device_id, job_id)).fetchone()
            
            if existing:
                logger.warning(f"Printer {device_id} already busy with job {existing['id']}")
                con.close()
                return False
            
            # Atomic update: only if job is still PENDING
            cur.execute("""
                UPDATE queue_jobs 
                SET device_id=?, status='READY' 
                WHERE id=? AND status='PENDING'
            """, (device_id, job_id))
            
            success = cur.rowcount > 0
            con.commit()
            
            if success:
                logger.info(f"Job {job_id} toegewezen aan printer {device_id}")
            else:
                logger.warning(f"Job {job_id} kon niet toegewezen worden (niet meer PENDING)")
            
            return success
            
        except Exception as e:
            logger.error(f"Assignment error: {e}")
            con.rollback()
            return False
        finally:
            con.close()


    def set_printer_single_tag(self, device_id: str, tag: str) -> bool:
        """Zet op de farm-server exact één tag op de printer (single-tag policy)."""
        if not tag:
            return False
        try:
            import requests, json
            # Haal huidige config op
            base = os.getenv("SERVER_API", "http://127.0.0.1:8000").rstrip("/")
            r = requests.get(f"{base}/api/config/printers", timeout=5)
            if not r.ok:
                logger.warning(f"Kon printers config niet ophalen voor tag-set: HTTP {r.status_code}")
                return False
            data = r.json() or []
            pr = None
            for p in data:
                if (p or {}).get("device_id") == device_id:
                    pr = p
                    break
            if not pr:
                logger.warning(f"Printer {device_id} niet gevonden voor tag-set")
                return False

            # Overschrijf tags met precies één genormaliseerde tag
            t = (tag or "").strip().lower()
            pr_out = {
                "device_id": pr.get("device_id"),
                "name": pr.get("name") or "",
                "model": pr.get("model") or "",
                "ip": pr.get("ip") or "",
                "lan_access_code": pr.get("lan_access_code") or "",
                "cloud_user_id": pr.get("cloud_user_id"),
                "cloud_access_token": pr.get("cloud_access_token"),
                "autoprint": bool(pr.get("autoprint", True)),
                "tags": [t],
            }
            r2 = requests.put(f"{base}/api/config/printers/{device_id}", json=pr_out, timeout=5)
            if r2.ok:
                logger.info(f"Tag '{t}' gezet op printer {device_id}")
                # Refresh lokale cache zodat toewijzing meteen consistent is
                try:
                    self.fetch_printers_config(force=True)
                except Exception:
                    pass
                return True
            logger.warning(f"Kon tag niet zetten op printer {device_id}: HTTP {r2.status_code} — {r2.text[:200]}")
            return False
        except Exception as e:
            logger.warning(f"Fout bij set_printer_single_tag: {e}")
            return False
    # --- selectie van printer ---
    def has_tag(self, printer: PrinterConfig, tag: str) -> bool:
        if not tag:
            return False
        tag_lower = tag.strip().lower()
        return tag_lower in [t.strip().lower() for t in printer.tags]

    def choose_idle_printer(self, job_tag: Optional[str]) -> Optional[str]:
        """
        Kies een vrije printer (autoprint=True) met optionele tag.
        Kandidaten: IDLE en optioneel FINISH (afhankelijk van config).
        """
        printers = self.fetch_printers_config()
        statuses = self.fetch_printer_status()
        if not printers:
            logger.warning("Geen printers beschikbaar")
            return None

        con = self.db_conn()
        cur = con.cursor()
        rows = cur.execute("""
            SELECT device_id, COUNT(*) as cnt
            FROM queue_jobs
            WHERE status IN ('READY','UPLOADING','PRINTING')
            GROUP BY device_id
        """).fetchall()
        con.close()
        queue_loads = {r["device_id"]: r["cnt"] for r in rows}

        # Define eligible statuses
        eligible_statuses = ["IDLE"]
        if ASSIGN_TO_FINISH:
            eligible_statuses.append("FINISH")

        
        candidates_match = []
        candidates_empty = []
        for p in printers:
            if not p.autoprint:
                continue
            status = statuses.get(p.device_id)
            if not status:
                continue
            if status.status not in eligible_statuses:
                continue
            load = queue_loads.get(p.device_id, 0)
            tags_norm = [str(t or "").strip().lower() for t in (p.tags or []) if str(t or "").strip()]
            if job_tag:
                jt = job_tag.strip().lower()
                if jt in tags_norm:
                    candidates_match.append({"printer": p, "load": load, "status": status})
                elif len(tags_norm) == 0:
                    candidates_empty.append({"printer": p, "load": load, "status": status})
                else:
                    # Andere tag aanwezig → laat staan (wordt opgeruimd zodra er geen jobs meer zijn)
                    continue
            else:
                # Jobs zonder tag → alleen printers zonder tag gebruiken
                if len(tags_norm) == 0:
                    candidates_empty.append({"printer": p, "load": load, "status": status})

        candidates = candidates_match if candidates_match else candidates_empty

        if not candidates:
            if job_tag:
                logger.info(f"Geen vrije autoprint-printer gevonden met tag '{job_tag}'")
            else:
                logger.info("Geen vrije autoprint-printers beschikbaar")
            return None

        candidates.sort(key=lambda x: (x["load"], x["printer"].name.lower()))
        return candidates[0]["printer"].device_id


    # --- pending -> ready ---
    def process_pending_jobs(self):
        jobs = self.get_pending_jobs()
        if not jobs:
            return
        logger.info(f"Processing {len(jobs)} pending jobs voor assignment...")
        for job in jobs:
            device_id = self.choose_idle_printer(job.get("job_tag"))
            if device_id:
                self.assign_job_to_device(job["id"], device_id)

    # --- upload + print ---
    def process_upload_job(self, job: Dict) -> bool:
        job_id = job["id"]
        device_id = job["device_id"]
        filepath = pathlib.Path(job["filepath"])

        logger.info(f"Processing job {job_id} → printer {device_id}")

        # Verse configs
        self.fetch_printers_config(force=True)

        printer = self.printers_cache.get(device_id)
        if not printer:
            logger.error(f"Printer {device_id} niet gevonden in configuratie")
            self.update_job_status(job_id, "FAILED", "Printer niet gevonden")
            return False

        # Check autoprint
        if not printer.autoprint:
            logger.warning(f"Printer {device_id} heeft autoprint=FALSE → job {job_id} terug in wachtrij")
            self.requeue_job(job_id)
            return False

        if not filepath.exists():
            logger.error(f"Bestand niet gevonden: {filepath}")
            self.update_job_status(job_id, "FAILED", "Bestand niet gevonden")
            return False

        # Check printer status
        statuses = self.fetch_printer_status()
        status = statuses.get(device_id)
        eligible_statuses = ["IDLE"]
        if ASSIGN_TO_FINISH:
            eligible_statuses.append("FINISH")
            
        if status and status.status not in eligible_statuses:
            logger.info(f"Printer {device_id} niet meer vrij (status={status.status}) → requeue")
            self.requeue_job(job_id)
            return False

        # Upload
        self.update_job_status(job_id, "UPLOADING")

        uploader = FTPSUploader(printer)
        retries = 0
        last_error = None
        remote_relpath: Optional[str] = None

        preferred_dirs: List[str] = []
        if job.get("remote_dir"):
            preferred_dirs.append(str(job["remote_dir"]))

        base_name = pathlib.Path(job["filepath"]).name

        while retries < MAX_UPLOAD_RETRIES:
            try:
                remote_relpath = uploader.upload_file(
                    local_path=filepath,
                    remote_filename=base_name,
                    preferred_dirs=preferred_dirs
                )
                if remote_relpath:
                    logger.info(f"✓ Job {job_id} geüpload als {remote_relpath}")
                    break
                else:
                    last_error = "Upload returned failure status"
            except Exception as e:
                last_error = str(e)
                logger.warning(f"Upload poging {retries + 1}/{MAX_UPLOAD_RETRIES} mislukt: {e}")
            retries += 1
            if retries < MAX_UPLOAD_RETRIES:
                time.sleep(5)

        if not remote_relpath:
            logger.error(f"✗ Job {job_id} upload mislukt na {MAX_UPLOAD_RETRIES} pogingen")
            self.update_job_status(job_id, "FAILED", f"Upload mislukt: {last_error}")
            return False

        # Upload OK
        self.update_job_status(job_id, "UPLOADED")

        # Start print with retry logic
        return self._start_print_with_retry(job, printer, remote_relpath)

    def _start_print_with_retry(self, job: Dict, printer: PrinterConfig, remote_relpath: str) -> bool:
        """Start print with retry logic and exponential backoff"""
        job_id = job["id"]
        
        # Get current retry count
        con = self.db_conn()
        cur = con.cursor()
        row = cur.execute(
            "SELECT COALESCE(retry_count, 0) as retry_count FROM queue_jobs WHERE id=?", 
            (job_id,)
        ).fetchone()
        current_retry = row["retry_count"] if row else 0
        con.close()
        
        if current_retry >= MAX_PRINT_RETRIES:
            logger.error(f"Job {job_id} heeft max print retries bereikt ({MAX_PRINT_RETRIES})")
            self.update_job_status(job_id, "FAILED", f"Max print retries bereikt ({MAX_PRINT_RETRIES})")
            return False
        
        # Parse job parameters
        use_ams: Optional[bool] = None
        ams_mapping: Optional[List[int]] = None
        plate = int(job.get("plate", 1) or 1)
        gcode_param = job.get("gcode_param") or None

        if "use_ams" in job and job["use_ams"] is not None:
            v = job["use_ams"]
            use_ams = (str(v).strip().lower() not in ("0", "false", "no"))
        else:
            use_ams = AUTO_PRINT_USE_AMS

        if "ams_mapping" in job and job["ams_mapping"]:
            try:
                if isinstance(job["ams_mapping"], str):
                    ams_mapping = json.loads(job["ams_mapping"])
                elif isinstance(job["ams_mapping"], (list, tuple)):
                    ams_mapping = list(job["ams_mapping"])
            except Exception:
                logger.warning(f"Job {job_id}: ongeldige ams_mapping, genegeerd")
                ams_mapping = None

        # Try to start print
        try:
            with BambuMQTTClient(printer.device_id, printer.ip, printer.lan_access_code, timeout=30.0) as mq:
                report = mq.send_print_project_file(
                    remote_relpath=remote_relpath,
                    plate=plate,
                    use_ams=use_ams,
                    ams_mapping=ams_mapping,
                    timelapse=True,
                    bed_levelling=True,
                    flow_cali=True,
                    vibration_cali=True,
                    layer_inspect=True,
                    gcode_param=gcode_param,
                )

            logger.debug("project_file report: %s", json.dumps(report, indent=2))
            result = (report or {}).get("result", "").lower()
            
            if result == "success":
                logger.info(f"✓ Job {job_id} gestart met print.project_file (printer={printer.device_id})")
                self.update_job_status(job_id, "PRINTING")
                
                # Reset retry count on success
                con = self.db_conn()
                cur = con.cursor()
                cur.execute("UPDATE queue_jobs SET retry_count=0 WHERE id=?", (job_id,))
                con.commit()
                con.close()
                
                return True
            else:
                reason = (report or {}).get("reason", "onbekende fout")
                logger.error(f"✗ Print start voor job {job_id} mislukt: {reason}")
                
                # Increment retry count
                con = self.db_conn()
                cur = con.cursor()
                cur.execute(
                    "UPDATE queue_jobs SET retry_count = retry_count + 1 WHERE id=?", 
                    (job_id,)
                )
                con.commit()
                con.close()
                
                # Retry or fail
                if current_retry + 1 < MAX_PRINT_RETRIES:
                    backoff = min(30, 5 * (2 ** current_retry))  # Exponential backoff: 5s, 10s, 20s, 30s
                    logger.info(f"Retry {current_retry + 1}/{MAX_PRINT_RETRIES} voor job {job_id} over {backoff}s")
                    time.sleep(backoff)
                    self.update_job_status(job_id, "READY")  # Retry
                else:
                    self.update_job_status(job_id, "FAILED", f"Print start mislukt na {MAX_PRINT_RETRIES} pogingen: {reason}")
                
                return False

        except TimeoutError as e:
            logger.error(f"✗ Print start timeout voor job {job_id}: {e}")
            
            # Increment retry
            con = self.db_conn()
            cur = con.cursor()
            cur.execute("UPDATE queue_jobs SET retry_count = retry_count + 1 WHERE id=?", (job_id,))
            con.commit()
            con.close()
            
            if current_retry + 1 < MAX_PRINT_RETRIES:
                backoff = min(30, 5 * (2 ** current_retry))
                logger.info(f"Retry {current_retry + 1}/{MAX_PRINT_RETRIES} voor job {job_id} over {backoff}s")
                time.sleep(backoff)
                self.update_job_status(job_id, "READY")
            else:
                self.update_job_status(job_id, "FAILED", f"Print start timeout na {MAX_PRINT_RETRIES} pogingen")
            
            return False
            
        except Exception as e:
            logger.error(f"✗ Print start fout voor job {job_id}: {e}", exc_info=True)
            
            # Increment retry
            con = self.db_conn()
            cur = con.cursor()
            cur.execute("UPDATE queue_jobs SET retry_count = retry_count + 1 WHERE id=?", (job_id,))
            con.commit()
            con.close()
            
            if current_retry + 1 < MAX_PRINT_RETRIES:
                backoff = min(30, 5 * (2 ** current_retry))
                logger.info(f"Retry {current_retry + 1}/{MAX_PRINT_RETRIES} voor job {job_id} over {backoff}s")
                time.sleep(backoff)
                self.update_job_status(job_id, "READY")
            else:
                self.update_job_status(job_id, "FAILED", f"Print start fout na {MAX_PRINT_RETRIES} pogingen: {e}")
            
            return False

    # --- main loop ---
    def run_loop(self):
        logger.info("Upload manager started (v4.0 - Production Ready)")
        self.fetch_printers_config(force=True)
        
        consecutive_empty = 0  # NIEUW: track lege cycli
        
        while self.running:
            try:
                # Refresh caches
                self.fetch_printers_config()
                self.fetch_printer_status()

                # Process pending jobs (kan nieuwe assignments maken)
                pending_before = len(self.get_pending_jobs())
                self.process_pending_jobs()
                pending_after = len(self.get_pending_jobs())
                
                # Process ready jobs
                ready_jobs = self.get_assigned_ready_jobs()
                if ready_jobs:
                    logger.info(f"Processing {len(ready_jobs)} READY jobs...")
                    consecutive_empty = 0
                    
                for job in ready_jobs:
                    if not self.running:
                        break
                    self.process_upload_job(job)
                
                # NIEUW: Als er geen werk was, tel lege cycli
                if not ready_jobs and pending_before == pending_after and pending_after > 0:
                    consecutive_empty += 1
                    
                    # Na 3 lege cycli: force re-evaluation
                    if consecutive_empty >= 3:
                        logger.info("Triggering re-evaluation na idle periode...")
                        self._force_reevaluate_pending()
                        consecutive_empty = 0
                else:
                    consecutive_empty = 0

            except Exception as e:
                logger.error(f"Fout in upload loop: {e}", exc_info=True)

            time.sleep(UPLOAD_CHECK_INTERVAL)

        logger.info("Upload manager stopped")

    def _force_reevaluate_pending(self):
        """
        Force re-evaluatie van PENDING jobs.
        Nuttig als printers vrijkomen maar jobs nog niet assigned zijn.
        """
        try:
            # Reset device_id voor PENDING jobs die te lang wachten
            con = self.db_conn()
            cur = con.cursor()
            
            # Jobs die langer dan 30 sec PENDING zijn
            cur.execute("""
                UPDATE queue_jobs 
                SET device_id='auto'
                WHERE status='PENDING' 
                  AND device_id IS NOT NULL
                  AND device_id != ''
                  AND device_id != 'auto'
                  AND datetime(created_at) < datetime('now', '-30 seconds')
            """)
            
            reset_count = cur.rowcount
            con.commit()
            con.close()
            
            if reset_count > 0:
                logger.info(f"Reset {reset_count} stuck PENDING jobs voor re-assignment")
                # Trigger nieuwe assignment
                self.process_pending_jobs()
                
        except Exception as e:
            logger.error(f"Force re-evaluate fout: {e}")

    def start(self):
        if self.running:
            logger.warning("Upload manager is al gestart")
            return
        self.running = True
        self.thread = threading.Thread(target=self.run_loop, daemon=True)
        self.thread.start()
        logger.info("Upload manager thread gestart")

    def stop(self):
        self.running = False
        if self.thread:
            self.thread.join(timeout=10)
        logger.info("Upload manager gestopt")

# ------------- Singleton + main -------------
_upload_manager: Optional[UploadManager] = None

def get_upload_manager() -> UploadManager:
    global _upload_manager
    if _upload_manager is None:
        _upload_manager = UploadManager()
    return _upload_manager

def main():
    logger.info("Starting Printer Uploader Service v4.0 (Production Ready)")
    if mqtt is None:
        logger.error("paho-mqtt ontbreekt. Installeer met: pip install paho-mqtt")
        return
    
    manager = get_upload_manager()
    manager.start()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutdown signal ontvangen")
        manager.stop()

if __name__ == "__main__":
    main()