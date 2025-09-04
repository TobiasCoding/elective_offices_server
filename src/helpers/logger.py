from config.config import LOGS_DIR
from pathlib import Path
from src.helpers.utils import _now_iso

def _log_key(year: str, election_category: str, phase: str, office: str) -> Path:
    safe = f"{year}__{election_category}__{phase}__{office}".replace("/", "-")
    return LOGS_DIR / f"{safe}.log"

def log_append(year: str, election_category: str, phase: str, office: str, msg: str) -> None:
    p = _log_key(year, election_category, phase, office)
    with p.open("a", encoding="utf-8") as f:
        f.write(f"[{_now_iso()}] {msg}\n")

def read_log(year: str, election_category: str, phase: str, office: str) -> str:
    p = _log_key(year, election_category, phase, office)
    if not p.exists():
        return "(sin logs)"
    return p.read_text(encoding="utf-8", errors="replace")


