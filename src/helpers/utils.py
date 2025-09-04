import datetime
from typing import Iterable


def _now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _norm(s: str) -> str:
    # Normalización simple para comparar nombres
    s = s.lower().strip()
    repl = {
        "á": "a", "é": "e", "í": "i", "ó": "o", "ú": "u",
        "ü": "u", "ñ": "n",
    }
    for k, v in repl.items():
        s = s.replace(k, v)
    # espacios múltiples -> uno
    s = " ".join(s.split())
    return s


def _iter_dicts(items: Iterable) -> Iterable[dict]:
    """Itera sobre dicts en el iterable."""
    for it in items:
        if isinstance(it, dict):
            yield it