from typing import Dict, List, Tuple, Iterable
from src.helpers.db import load_db, save_db
from fastapi import HTTPException
import json
from config.config import DATA_DIR
from src.helpers.utils import _now_iso
from src.helpers.logger import log_append

# ============================================================
# CÁLCULO PRINCIPAL
# ============================================================

def do_calc(year: str, election_category: str, phase: str, office: str, method: str | None = None) -> dict:
    """
    Calcula:
      - POSITIVO (vote_type=0): suma por group_id
      - Resto (impugnado/recurrido/comando/en_blanco/nulo): totales globales (sin desglosar por grupo)
      - Según 'method', asigna bancas/representantes por group_id y lo guarda en calc.all.seats

    Además:
      - La cantidad de cargos a distribuir (seats) se resuelve así, en orden:
        1) Si meta['seats'] > 0 en db.json (entries -> office), se usa ese valor.
        2) Si no, se busca en el JSONL apuntado por category['seats_jsonl'] (config/db.json):
           * Si una línea trae "seats": N, se suma ese N cuando el nombre del cargo coincida.
           * Si no hay "seats", se cuenta 1 por cada línea cuyo "nombre_cargo" coincida.
           * Coincidencia por nombre normalizado con candidatos: meta.get("nombre_cargo"), meta.get("office_name"), 'office'.
    Estructura final en el preprocesado:
        payload["calc"]["all"] = {
            "positive": { "<group_id>": votos, ..., "all": total_positivo },
            "impugnado": N, "recurrido": N, "comando": N, "en_blanco": N, "nulo": N,
            "all": total_de_todos_los_tipos,
            "seats": { "method": "...", "by_group": {...}, "meta": {...} }   # si method provisto
        }
    """
    print(f"[CALC] year={year} category={election_category} phase={phase} office={office} method={method}")

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

    # Cargar preprocesado existente
    payload = json.loads(pre_path.read_text(encoding="utf-8"))
    rows = payload.get("rows", [])

    # --- 1) Agregaciones de votos ---
    positive_by_group, others_totals = _aggregate_votes(rows)

    total_positive = sum(positive_by_group.values())
    positive_block = positive_by_group | {"all": total_positive}
    total_all = total_positive + sum(others_totals.values())
    all_block = {"positive": positive_block} | others_totals
    all_block["all"] = total_all

    # --- 2) Resolver seats desde db.json o seats_jsonl ---
    seats = _resolve_seats_from_jsonl_or_meta(db, year, election_category, phase, office, rows)

    # --- 3) Asignación de bancas según método (opcional) ---
    seats_info = {}
    if method:
        method_lc = method.strip().lower()

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
            seats_info = {"method": method, "by_group": {}, "meta": {"error": "método no soportado"}}

    if seats_info:
        all_block["seats"] = seats_info

    # --- 4) Persistir resultado de cálculo ---
    if "calc" not in payload or not isinstance(payload["calc"], dict):
        payload["calc"] = {}
    payload["calc"]["all"] = all_block

    pre_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    # Marca de último cálculo
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
        "assignment": seats_info or None,
    }


# ============================================================
# RESOLVER SEATS DESDE JSONL O META
# ============================================================

def _resolve_seats_from_jsonl_or_meta(db: dict, year: str, election_category: str, phase: str, office: str, rows: List[dict]) -> int:
    """
    1) Usa meta['seats'] si existe (>0).
    2) Si no, busca category['seats_jsonl'] y calcula:
       - Si las líneas del JSONL tienen "seats": N, suma N para las que matcheen el cargo.
       - Si no traen "seats", cuenta 1 por línea que matchee.
    * Match por nombre de cargo normalizado.
    * Candidatos para nombre de cargo: meta['nombre_cargo'], meta['office_name'], 'office' (parámetro).
    """
    seats = 0
    try:
        meta = db[year]["categories"][election_category]["phases"][phase]["entries"][office]
    except KeyError:
        return 0

    # 1) meta['seats']
    raw_seats = meta.get("seats")
    try:
        if raw_seats is not None:
            s = int(raw_seats)
            if s > 0:
                return s
    except Exception:
        pass

    # 2) seats_jsonl a nivel categoría
    try:
        cat_node = db[year]["categories"][election_category]
    except KeyError:
        return 0
    seats_jsonl_rel = (cat_node or {}).get("seats_jsonl")
    if not seats_jsonl_rel:
        return 0

    seats_path = (DATA_DIR / seats_jsonl_rel).resolve()
    if not seats_path.exists():
        return 0

    try:
        items = load_jsonl(seats_path)  # lista de dicts
    except Exception:
        return 0

    # nombres candidatos de cargo a matchear
    candidates = [
        str(meta.get("nombre_cargo") or "").strip(),
        str(meta.get("office_name") or "").strip(),
        str(office or "").strip(),
    ]
    candidates = [c for c in candidates if c]
    if not candidates:
        return 0

    cand_norm = {_norm(c) for c in candidates}

    total = 0
    for it in _iter_dicts(items):
        nombre_cargo = _norm(str(it.get("nombre_cargo") or ""))
        if not nombre_cargo:
            continue
        if nombre_cargo in cand_norm:
            # Si el jsonl provee 'seats' explícito, usarlo; si no, sumar 1 por línea.
            val = it.get("seats")
            try:
                if val is not None:
                    total += int(val)
                else:
                    total += 1
            except Exception:
                total += 1

    return max(0, total)


def _iter_dicts(items: Iterable) -> Iterable[dict]:
    for it in items:
        if isinstance(it, dict):
            yield it


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
