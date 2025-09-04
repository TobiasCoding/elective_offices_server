from typing import Dict, List, Tuple, Iterable
from src.helpers.db import load_db, save_db
from fastapi import HTTPException
import json
from config.config import DATA_DIR, CATEGORIES_PATH
from src.helpers.utils import _now_iso
from src.helpers.logger import log_append
from pathlib import Path

# ============================================================
# CÁLCULO PRINCIPAL
# ============================================================

from src.helpers.db import get_seats

def do_calc(year: str, election_category: str, phase: str, office: str) -> dict:
    """
    Calcula y persiste el bloque 'calc.all' en el JSON preprocesado.

    CAMBIO CLAVE:
    - 'method' se toma SIEMPRE desde config/categories.jsonl (clave 'method' para el par
      (category=election_category, office=office)), validando que la fase ('paso'/'general'/'balotaje')
      sea aplicable (boolean true en esa línea). El parámetro 'method' recibido se ignora.

    Estructura final en payload['calc']['all']:
      {
        "positive": { "<group_id>": votos, ..., "all": total_positivo },
        "impugnado": N, "recurrido": N, "comando": N, "en_blanco": N, "nulo": N,
        "all": total_de_todos_los_tipos,
        "seats": { "method": "...", "by_group": {...}, "meta": {...} }   # sólo si hay método soportado
      }
    """
    print(f"[CALC] year={year} category={election_category} phase={phase} office={office}")

    # ---------- 0) Resolver método desde categories.jsonl (sin fallbacks) ----------
    # Cargamos categories.jsonl y buscamos la línea exacta por categoría y office.
    # Además, exigimos que la fase indicada sea aplicable (boolean true).
    phase_key = (phase or "").strip().lower()
    if phase_key not in ("paso", "general", "balotaje"):
        raise HTTPException(400, "Fase inválida: debe ser PASO, GENERAL o BALOTAJE")

    try:
        # Lectura estricta del JSONL (sin depender de otros módulos)
        items = []
        with CATEGORIES_PATH.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                items.append(json.loads(line))
    except FileNotFoundError:
        raise HTTPException(400, f"No existe categories.jsonl en: {CATEGORIES_PATH}")
    except json.JSONDecodeError as e:
        raise HTTPException(400, f"categories.jsonl inválido: {e}")

    matched = None
    for it in items:
        if not isinstance(it, dict):
            continue
        if it.get("category") == election_category and it.get("office") == office:
            matched = it
            break
    if matched is None:
        raise HTTPException(400, "Categoría/Cargo no está definido en categories.jsonl")

    if not bool(matched.get(phase_key, False)):
        raise HTTPException(400, f"{phase} no aplica para este cargo según categories.jsonl")

    method = matched.get("method")
    if not isinstance(method, str) or not method.strip():
        raise HTTPException(400, "Método ausente o inválido en categories.jsonl para este cargo")

    # ---------- 1) Cargar metadatos de db.json y archivo preprocesado ----------
    db = load_db()
    try:
        cat_node = db[year]["categories"][election_category]
        meta = cat_node["phases"][phase]["entries"][office]
    except KeyError:
        raise HTTPException(404, "Entrada no encontrada en db.json")

    pre_rel = (meta or {}).get("preprocessed_json")
    if not pre_rel:
        raise HTTPException(400, "Falta preprocesado (no existe preprocessed_json en db.json)")

    pre_path = (DATA_DIR / pre_rel).resolve()
    if not pre_path.exists():
        raise HTTPException(400, f"Archivo preprocesado no encontrado: {pre_path}")

    try:
        payload = json.loads(pre_path.read_text(encoding="utf-8"))
    except Exception as e:
        raise HTTPException(500, f"Error leyendo preprocessed_json: {e}")

    rows = payload.get("rows", [])

    # ---------- 2) Agregaciones de votos ----------
    positive_by_group, others_totals = _aggregate_votes(rows)
    total_positive = sum(positive_by_group.values())
    positive_block = positive_by_group | {"all": total_positive}
    total_all = total_positive + sum(others_totals.values())
    all_block = {"positive": positive_block} | others_totals
    all_block["all"] = total_all

    # ---------- 3) Resolver 'seats' (según jsonl o meta) ----------
    # print(f"tipo_cargo: {election_category}")
    # print(f"office: {office}")
    seats = get_seats(db, rows, year, election_category, phase, office)

    # ---------- 4) Asignación según método tomado de categories.jsonl ----------
    seats_info = {}
    method_lc = method.strip().lower()

    print(f"INPUT:\nmethod_lc: {method_lc}\npositive_by_group: {positive_by_group}\nseats: {seats}")

    if method_lc in ("d-hont", "dhont", "d'hont"):
        by_group, extra_meta = _alloc_dhondt(positive_by_group, seats)
        seats_info = {"method": "d-hont", "by_group": by_group, "meta": extra_meta}

    elif method_lc == "hare":
        by_group, extra_meta = _alloc_hare(positive_by_group, seats)
        seats_info = {"method": "hare", "by_group": by_group, "meta": extra_meta}

    elif method_lc == "lista-incompleta":
        by_group, extra_meta = _alloc_lista_incompleta(positive_by_group, seats)
        seats_info = {"method": "lista-incompleta", "by_group": by_group, "meta": extra_meta}

    elif method_lc in ("mayoria-simple", "mayoría-simple"):
        by_group, extra_meta = _alloc_mayoria_simple(positive_by_group, seats)
        seats_info = {"method": "mayoria-simple", "by_group": by_group, "meta": extra_meta}

    elif method_lc in ("balotaje", "ballotage"):
        by_group, extra_meta = _eval_balotaje(positive_by_group, seats)
        seats_info = {"method": "balotaje", "by_group": by_group, "meta": extra_meta}

    else:
        raise HTTPException(400, f"Método no soportado: {method}")

    print(f"OUTPUT:\nby_group: {by_group}\nextra_meta: {extra_meta}")

    if seats_info:
        all_block["seats"] = seats_info

    # ---------- 5) Persistir resultado ----------
    if "calc" not in payload or not isinstance(payload["calc"], dict):
        payload["calc"] = {}
    payload["calc"]["all"] = all_block

    pre_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    # Marcar último cálculo en db.json
    meta["last_calc"] = _now_iso()
    save_db(db)

    log_append(
        year, election_category, phase, office,
        f"Calc OK -> {pre_path.name} (rows={len(rows)}, groups={len(positive_by_group)}, method={method}, seats={seats})"
    )

    return {
        "ok": True,
        "stage": "calc",
        "year": year,
        "category": election_category,
        "phase": phase,
        "office": office,
        "method": method,
        "groups": len(positive_by_group),
        "totals": others_totals,
        "seats": seats,
        "assignment": seats_info,
    }


# ============================================================
# AGREGACIÓN DE VOTOS
# ============================================================

def _aggregate_votes(rows: List[dict]) -> Tuple[Dict[str, int], Dict[str, int]]:
    """
    Devuelve:
      - positive_by_group: votos POSITIVO (vote_type=0) por group_id
      - others_totals: totales globales de impugnado/recurrido/comando/en_blanco/nulo
    """
    positive_by_group: Dict[str, int] = {}
    type_names = {1: "impugnado", 2: "recurrido", 3: "comando", 5: "en_blanco", 6: "nulo"}
    others_totals = {name: 0 for name in type_names.values()}

    for r in rows:
        vt = r.get("vote_type")
        vn = int(r.get("vote_number") or 0)

        if vt == 0:
            gid = str(r.get("group_id") or "0")
            positive_by_group[gid] = positive_by_group.get(gid, 0) + max(0, vn)
        else:
            name = type_names.get(vt)
            if name:
                others_totals[name] += max(0, vn)

    return positive_by_group, others_totals


# ============================================================
# MÉTODOS DE ASIGNACIÓN
# ============================================================

def _alloc_dhondt(votes: Dict[str, int], seats: int) -> Tuple[Dict[str, int], dict]:
    """
    D'Hondt (promedios más altos).
    """
    # print(f"votes: {votes}")
    # print(f"seats: {seats}")
    result = {gid: 0 for gid in votes.keys()}
    if seats <= 0 or not votes:
        return result, {"seats": seats, "note": "sin asignación (seats<=0 o sin votos)"}

    quotients: List[Tuple[float, str, int]] = []
    for gid, v in votes.items():
        for d in range(1, seats + 1):
            quotients.append((v / d if d else 0.0, gid, d))
    quotients.sort(key=lambda x: (x[0], votes[x[1]], x[1]), reverse=True)

    picks = quotients[:seats]
    for _, gid, _ in picks:
        result[gid] = result.get(gid, 0) + 1

    meta = {
        "seats": seats,
        "total_votes": sum(votes.values()),
        "picks_preview": [{"gid": gid, "q": float(f"{q:.6f}"), "d": d} for q, gid, d in picks],
    }
    return result, meta


def _alloc_hare(votes: Dict[str, int], seats: int) -> Tuple[Dict[str, int], dict]:
    """
    Hare (cuota Hare + restos mayores).
    """
    result = {gid: 0 for gid in votes.keys()}
    total = sum(votes.values())
    if seats <= 0 or total <= 0 or not votes:
        return result, {"seats": seats, "total_votes": total, "note": "sin asignación"}

    quota = total / seats
    assigned = 0
    remainders: List[Tuple[float, str]] = []

    for gid, v in votes.items():
        s = int(v // quota)
        result[gid] = s
        assigned += s
        remainders.append((v - s * quota, gid))

    remaining = seats - assigned
    remainders.sort(key=lambda x: (x[0], votes[x[1]], x[1]), reverse=True)
    for _, gid in remainders[:max(0, remaining)]:
        result[gid] += 1

    meta = {
        "seats": seats,
        "total_votes": total,
        "quota_hare": quota,
        "assigned_floor": assigned,
        "remaining_after_floor": max(0, remaining),
    }
    return result, meta


def _alloc_lista_incompleta(votes: Dict[str, int], seats: int) -> Tuple[Dict[str, int], dict]:
    """
    Lista incompleta (regla simple/estándar):
      - seats == 3: 2 para 1ra, 1 para 2da.
      - seats == 2: 2 para 1ra.
      - seats == 1: 1 para 1ra.
      - otro: aprox. 2/3 a la primera, resto a la segunda.
    """
    result = {gid: 0 for gid in votes.keys()}
    if seats <= 0 or not votes:
        return result, {"seats": seats, "note": "sin asignación"}

    ordered = sorted(votes.items(), key=lambda kv: (kv[1], kv[0]), reverse=True)
    first_gid, _ = ordered[0]
    second_gid = ordered[1][0] if len(ordered) > 1 else None

    if seats == 3:
        result[first_gid] = 2
        if second_gid:
            result[second_gid] = 1
    elif seats == 2:
        result[first_gid] = 2
    elif seats == 1:
        result[first_gid] = 1
    else:
        first_share = int(round(seats * (2.0 / 3.0)))
        first_share = max(1, min(seats, first_share))
        second_share = seats - first_share
        result[first_gid] = first_share
        if second_gid and second_share > 0:
            result[second_gid] = second_share

    meta = {"seats": seats, "ranking": [gid for gid, _ in ordered[:2]], "rule": "2/1 o 2/3-1/3"}
    return result, meta


def _alloc_mayoria_simple(votes: Dict[str, int], seats: int) -> Tuple[Dict[str, int], dict]:
    """
    Mayoría simple (winner-takes-all).
    """
    result = {gid: 0 for gid in votes.keys()}
    if seats <= 0 or not votes:
        return result, {"seats": seats, "note": "sin asignación"}
    winner = max(votes.items(), key=lambda kv: (kv[1], kv[0]))[0]
    result[winner] = seats
    meta = {"seats": seats, "winner": winner}
    return result, meta


def _eval_balotaje(votes: Dict[str, int], seats: int) -> Tuple[Dict[str, int], dict]:
    """
    Balotaje (reglas AR):
      - Gana en 1ra vuelta si: pct>=45% o (pct>=40% y ventaja>=10).
      - Si no, requiere balotaje (no asigna aquí).
      - Para ejecutivos, si gana en 1ra, asigna 1 (o 'seats' si >0).
    """
    result = {gid: 0 for gid in votes.keys()}
    total = sum(votes.values())
    if total <= 0 or not votes:
        return result, {"seats": seats, "note": "sin asignación (sin votos)"}

    ordered = sorted(votes.items(), key=lambda kv: (kv[1], kv[0]), reverse=True)
    first_gid, first_votes = ordered[0]
    second_gid, second_votes = (ordered[1] if len(ordered) > 1 else (None, 0))

    p1 = (first_votes / total) * 100.0 if total else 0.0
    p2 = (second_votes / total) * 100.0 if total else 0.0
    lead = p1 - p2

    win_first_round = (p1 >= 45.0) or (p1 >= 40.0 and lead >= 10.0)

    meta = {
        "total_positive": total,
        "first": {"gid": first_gid, "votes": first_votes, "pct": p1},
        "second": {"gid": second_gid, "votes": second_votes, "pct": p2} if second_gid else None,
        "lead_points": lead,
        "win_first_round": win_first_round,
        "requires_runoff": not win_first_round,
        "rule": "45% o 40%+10",
    }

    target_seats = seats if seats > 0 else 1
    if win_first_round:
        result[first_gid] = target_seats
    else:
        result = {gid: 0 for gid in votes.keys()}  # se definirá tras el balotaje

    return result, meta


