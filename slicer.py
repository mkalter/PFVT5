# slicer.py
import os, subprocess, uuid
from pathlib import Path
from typing import Dict, Optional, List, Tuple

class SliceError(Exception):
    pass

# --- Locaties om te zoeken ---
PROFILE_DIRS: List[Path] = [
    Path.cwd() / "profiles",
    Path("profiles"),
]
EXE_SEARCH_DIRS: List[Path] = [
    Path("C:/Program Files/Bambu Studio"),
    Path("C:/Program Files (x86)/Bambu Studio"),
    Path.home() / "AppData/Local/Programs/BambuStudio",
    Path.cwd(),
]

# ---- Helpers ----
def _exists_file(p: Path) -> bool:
    """Alleen TRUE als het een bestaand REGULIER BESTAND is (geen map, geen leeg pad)."""
    try:
        return bool(p and str(p).strip() and Path(p).is_file())
    except Exception:
        return False

def _find_file(name_or_path: str) -> Path:
    """Zoek bestand: absolute/relative, anders doorzoek profiel/exe directories en PATH."""
    if not name_or_path:
        # Belangrijk: leeg pad blijft leeg; wordt door _exists_file als False gezien.
        return Path("")
    expanded = os.path.expandvars(os.path.expanduser(name_or_path))
    p = Path(expanded)
    if p.is_absolute():
        return p
    if Path(expanded).exists():
        return Path(expanded)
    for d in PROFILE_DIRS + EXE_SEARCH_DIRS:
        cand = d / expanded
        if cand.exists():
            return cand
    return Path(expanded)

def _guess_exe(cfg_exe: Optional[str]) -> Path:
    # 1) meegegeven
    if cfg_exe:
        p = _find_file(cfg_exe)
        if _exists_file(p): return p
    # 2) ENV
    env = os.getenv("BAMBUSTUDIO_EXE")
    if env:
        p = _find_file(env)
        if _exists_file(p): return p
    # 3) vaste paden Windows
    for abs_path in (
        r"C:\Program Files\Bambu Studio\bambu-studio.exe",
        r"C:\Program Files (x86)\Bambu Studio\bambu-studio.exe",
    ):
        p = Path(abs_path)
        if _exists_file(p): return p
    # 4) namen in PATH of dirs
    for name in ("bambu-studio.exe", "BambuStudio.exe", "bambu-studio", "BambuStudio"):
        p = _find_file(name)
        if _exists_file(p): return p
    return _find_file(cfg_exe or "bambu-studio.exe")

def _run(cmd: List[str], timeout: int = 900) -> str:
    try:
        proc = subprocess.run(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, check=False, timeout=timeout
        )
    except FileNotFoundError as e:
        raise SliceError(f"Executable not found: {e}")
    except subprocess.TimeoutExpired:
        raise SliceError("Slicing timed out")

    out = (proc.stdout or "").strip()
    if proc.returncode != 0:
        raise SliceError(out or f"Process exited with code {proc.returncode}")
    return out

# ---- Autodetect profielen in ./profiles ----
def _model_keywords(model: str) -> List[str]:
    m = (model or "").lower()
    if m == "x1c":
        return ["x1c", "x1", "x1-carbon", "x1_carbon"]
    return ["p1s", "p1"]  # default naar P1S

def _score_name(fname: str, kind: str, model_kw: List[str]) -> int:
    """Scoreer bestandsnaam op relevantie: kind + model keywords."""
    name = fname.lower()
    score = 0
    # soort
    if kind == "machine" and "machine" in name: score += 5
    if kind == "process" and ("process" in name or "proc" in name): score += 5
    if kind == "filament" and ("filament" in name or "fil" in name): score += 5
    # model
    for kw in model_kw:
        if kw in name: score += 3
    # algemene voorkeur
    if name.endswith(".json"): score += 1
    return score

def _auto_find_profiles(model: str) -> Tuple[Path, Path, Path]:
    """Zoek machine/process/filament JSONs in ./profiles (beste match op naam)."""
    candidates: List[Path] = []
    for d in PROFILE_DIRS:
        if d.exists():
            candidates.extend([p for p in d.glob("*.json") if p.is_file()])

    if not candidates:
        raise SliceError("Geen JSON-profielen gevonden in ./profiles")

    model_kw = _model_keywords(model)

    def best(kind: str) -> Path:
        best_p, best_s = None, -1
        for p in candidates:
            s = _score_name(p.name, kind, model_kw)
            if s > best_s:
                best_p, best_s = p, s
        if not best_p or best_s <= 0:
            raise SliceError(f"Geen geschikt {kind}_json gevonden in ./profiles")
        return best_p

    return best("machine"), best("process"), best("filament")

def _printer_name_from_model(model: str | None) -> str:
    return "Bambu X1 Carbon" if (model or "").strip().upper() == "X1C" else "Bambu P1S"

# ---- Publieke entrypoint ----
def slice_stl_to_3mf_bambu(stl_path: Path, out_dir: Path, config: Dict) -> Path:
    """
    Slice STL/OBJ naar 3MF met Bambu Studio CLI.

    config (alles optioneel):
      - exe: pad/naam van bambu-studio cli
      - machine_json / process_json / filament_json: expliciete paden
      - arrange: int (default 1)
      - orient: bool (default True)
      - printer_model: "X1C" of "P1S" — gebruikt voor fallback en autodetect

    Werking:
      1) Als ALLE 3 expliciete JSON-paden geldig zijn (bestanden!), gebruik ze.
      2) Anders probeer automatisch te vinden in ./profiles (model-aware).
      3) Als dat niet lukt, val terug op --printer "<model-naam>" (zonder JSON).
    """
    out_dir = Path(out_dir); out_dir.mkdir(parents=True, exist_ok=True)
    stl_path = Path(stl_path).resolve()
    if not _exists_file(stl_path):
        raise SliceError(f"STL not found: {stl_path}")

    out_3mf = out_dir / f"{stl_path.stem}-{uuid.uuid4().hex[:8]}.3mf"
    exe = _guess_exe(config.get("exe"))
    if not _exists_file(exe):
        raise SliceError(f"Bambu Studio executable not found: {exe}")

    # 1) expliciet
    machine = _find_file(config.get("machine_json", ""))
    process = _find_file(config.get("process_json", ""))
    filament = _find_file(config.get("filament_json", ""))

    have_all_explicit = _exists_file(machine) and _exists_file(process) and _exists_file(filament)

    # 2) autodetect in profiles
    have_all = False
    if not have_all_explicit:
        try:
            machine, process, filament = _auto_find_profiles(config.get("printer_model") or "P1S")
            have_all = True
        except SliceError:
            have_all = False
    else:
        have_all = True

    arrange = str(int(config.get("arrange", 1)))
    orient_flag = bool(config.get("orient", True))

    cmd: List[str] = [str(exe), "--orient", ("1" if orient_flag else "0"), "--arrange", arrange]

    if have_all:
        # Volledige set profielen beschikbaar (ALLEMAAL echte bestanden)
        cmd += ["--load-settings", f"{machine};{process}", "--load-filaments", f"{filament}"]
    else:
        # Fallback: geen JSONs laden, alleen printernaam
        cmd += ["--printer", _printer_name_from_model(config.get("printer_model"))]

    cmd += ["--slice", "0", "--export-3mf", str(out_3mf), str(stl_path)]

    _run(cmd)

    if not out_3mf.exists() or not out_3mf.is_file():
        raise SliceError("Bambu Studio produced no 3MF")
    if out_3mf.stat().st_size == 0:
        raise SliceError("3MF file is empty")

    return out_3mf
