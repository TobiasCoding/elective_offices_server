from config.config import DB_PATH
from fastapi import HTTPException
import json

def load_db() -> dict:
    if not DB_PATH.exists():
        return {}
    try:
        return json.loads(DB_PATH.read_text(encoding="utf-8"))
    except Exception as e:
        raise HTTPException(500, f"db.json corrupto: {e}")

def save_db(db: dict) -> None:
    DB_PATH.write_text(json.dumps(db, ensure_ascii=False, indent=2), encoding="utf-8")


from pathlib import Path
from typing import Iterable, Dict, List

from src.helpers.utils import _norm, _iter_dicts


def _load_jsonl(path: Path) -> List[dict]:
    """Loader local para evitar imports circulares."""
    items: List[dict] = []
    if not path.exists():
        raise HTTPException(400, f"Falta archivo requerido: {path}")
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            items.append(json.loads(line))
    return items

def _token_match(a: str, b: str) -> bool:
    """Match flexible: exacto, substring o solapamiento de tokens."""
    if not a or not b:
        return False
    if a == b or a in b or b in a:
        return True
    at, bt = set(a.split()), set(b.split())
    if not at or not bt:
        return False
    overlap = len(at & bt)
    # pide al menos la mitad de los tokens (redondeo hacia abajo) de la más corta
    threshold = max(1, min(len(at), len(bt)) // 2)
    return overlap >= threshold


# --- get_seats: versión final con logs print() ---
import json
from pathlib import Path
from typing import Iterable, Dict, List

def get_seats(
    db: dict,
    rows: List[dict],
    year: str,
    election_category: str,
    phase: str,
    office: str,
) -> int:
    """
    Lee `cantidad_cargos` desde el JSONL cuyo path relativo está en:
      db[year]["categories"][election_category]["seats_jsonl"]

    Reglas:
      - Match ESTRICTO por office: item['nombre_cargo'] == office (normalizado).
      - Match por category: si el item trae 'category', debe coincidir con election_category.
      - Usa contexto territorial (tipo/nombre escala) si existe en rows.
      - Búsqueda por capas con for embebidos:
          1) exigir category y contexto
          2) exigir category y SIN contexto
          3) SIN category y con contexto
          4) SIN category y SIN contexto
      - Si no hay coincidencias positivas, fallback a meta['seats'] o 0.
    """
    # print(f"[GET_SEATS] year={year} category={election_category} phase={phase} office={office}")

    # --- Navegación segura del nodo de categoría ---
    cat_node = db.get(year, {}).get("categories", {}).get(election_category, {}) or {}
    meta = cat_node.get("phases", {}).get(phase, {}).get("entries", {}).get(office, {}) or {}
    seats_rel = cat_node.get("seats_jsonl")
    # print(f"[GET_SEATS] seats_jsonl (relativo) = {seats_rel}")

    # --- Helpers de normalización / contexto ---
    def _norm(s: str) -> str:
        if not isinstance(s, str):
            return ""
        s = s.lower().strip()
        for a, b in {"á":"a","é":"e","í":"i","ó":"o","ú":"u","ü":"u","ñ":"n"}.items():
            s = s.replace(a, b)
        return " ".join(s.split())

    def _mode(values: Iterable[str]) -> str | None:
        freq: Dict[str, int] = {}
        for v in values:
            if not v:
                continue
            v = _norm(v)
            freq[v] = freq.get(v, 0) + 1
        return max(freq, key=freq.get) if freq else None

    # Contexto territorial predominante (si existe en rows)
    ctx_tipo   = _mode([r.get("tipo_escala_territorial")   for r in rows if isinstance(r, dict)])
    ctx_nombre = _mode([r.get("nombre_escala_territorial") for r in rows if isinstance(r, dict)])
    # print(f"[GET_SEATS] ctx_tipo={ctx_tipo} ctx_nombre={ctx_nombre}")

    office_n   = _norm(office)
    category_n = _norm(election_category)

    # --- Resolver base_dir (relativo a DB_PATH si existe) ---
    if "DB_PATH" in globals() and globals()["DB_PATH"]:
        base_dir = Path(globals()["DB_PATH"]).resolve().parent
        # print(f"[GET_SEATS] base_dir por DB_PATH = {base_dir}")
    elif "DATA_DIR" in globals() and globals()["DATA_DIR"]:
        base_dir = Path(globals()["DATA_DIR"]).resolve()
        # print(f"[GET_SEATS] base_dir por DATA_DIR = {base_dir}")
    else:
        base_dir = Path.cwd()
        # print(f"[GET_SEATS] base_dir por defecto (cwd) = {base_dir}")

    # --- Cargar JSONL tolerando 'NaN' ---
    seats_items: List[dict] = []
    if seats_rel:
        seats_path = Path(seats_rel).resolve()
        try:
            raw = seats_path.read_text(encoding="utf-8", errors="ignore").splitlines()
            # print(f"[GET_SEATS] líneas leídas = {len(raw)}")
            parsed = 0
            for ln in raw:
                ln = ln.strip()
                if not ln:
                    continue
                # JSON válido: NaN → null
                ln = ln.replace(": NaN", ": null").replace(":NaN", ": null")
                try:
                    obj = json.loads(ln)
                except Exception as e:
                    # Línea inválida, continuar
                    continue
                if isinstance(obj, dict):
                    seats_items.append(obj)
                    parsed += 1
            # print(f"[GET_SEATS] items parseados = {parsed}")
        except Exception as e:
            # print(f"[GET_SEATS][ERROR] No se pudo leer/parsear {seats_rel}: {e}")
            seats_items = []
    else:
        print("[GET_SEATS][WARN] seats_jsonl no definido en db.json")

    # --- Predicados de match (sin alias) ---
    def _match_office(item: dict) -> bool:
        match = _norm(str(item.get("nombre_cargo", ""))) == office_n
        return match

    def _match_category(item: dict) -> bool:
        if "category" in item:
            return _norm(str(item.get("category", ""))) == category_n
        return True  # si no trae category, solo se filtrará cuando no se exija category

    def _match_ctx(item: dict) -> bool:
        if not ctx_tipo and not ctx_nombre:
            return True
        ok = True
        if ctx_tipo:
            ok = ok and (_norm(str(item.get("tipo_escala_territorial", ""))) == ctx_tipo)
        if ctx_nombre:
            ok = ok and (_norm(str(item.get("nombre_escala_territorial", ""))) == ctx_nombre)
        return ok

    # Métrica rápida: cuántos matchean office
    office_candidates = sum(1 for it in seats_items if _match_office(it))
    # print(f"[GET_SEATS] candidatos por office={office_candidates}")

    # --- Búsqueda por capas con for embebidos ---
    # require_category in (True, False)
    #   require_ctx in (True, False)
    for require_category in (True, False):
        for require_ctx in (True, False):
            seats_sum = 0
            tested = 0
            kept = 0
            # print(f"[GET_SEATS] capa require_category={require_category} require_ctx={require_ctx}")
            for it in seats_items:                      # 1er nivel: recorrer items
                if not _match_office(it):               # office siempre requerido
                    continue

                tested += 1

                if require_category and not _match_category(it):
                    continue

                if require_ctx and not _match_ctx(it):
                    continue

                kept += 1
                try:
                    seats_sum += int(it.get("cantidad_cargos", 0) or 0)
                except Exception:
                    # print("[GET_SEATS][WARN] cantidad_cargos inválido, item saltado")
                    continue

            # print(f"[GET_SEATS] capa tested={tested} kept={kept} seats_sum={seats_sum}")
            if seats_sum > 0:
                # print(f"[GET_SEATS] -> seats={seats_sum} (match por capas)")
                return seats_sum

    # --- Fallback a meta['seats'] o 0 ---
    try:
        fallback = int(meta.get("seats", 0) or 0)
    except Exception:
        fallback = 0
    # print(f"[GET_SEATS] fallback meta['seats']={fallback}")
    return fallback
