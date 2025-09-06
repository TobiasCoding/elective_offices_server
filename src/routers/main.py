from __future__ import annotations

from src.calc.calc import do_calc
import os, json, hashlib, csv, io, datetime
from pathlib import Path
from typing import Dict, List, Tuple

import httpx
import pandas as pd
from fastapi import FastAPI, APIRouter, Request, Form, Query, HTTPException, UploadFile, File
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from src.helpers.db import load_db, save_db
from src.helpers.logger import log_append
from src.helpers.utils import _now_iso

# ================= Paths & bootstrap =================
from config.config import *

# ================= App =================
app = FastAPI(title="Elective Office")

app.mount("/elective_office/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
router = APIRouter(prefix="/elective_office")

# ================= Utils =================
@app.get("/favicon.ico")
async def favicon():
    return RedirectResponse("/static/favicon.ico")


# --- NUEVO: Normalización simple de nombres para mapear cargos ---
def _norm_office_name(s: str) -> str:
    if not isinstance(s, str):
        return ""
    s = s.lower().strip()
    repl = {"á":"a","é":"e","í":"i","ó":"o","ú":"u","ü":"u","ñ":"n"}
    for k,v in repl.items(): s = s.replace(k, v)
    return " ".join(s.split())

# --- NUEVO: mapa office -> idCargo (según estándar provisto) ---
# 1=presidente, 2=senadores, 3=diputados, 4=gobernador,
# 5=senadores nacionales, 6=diputados provinciales, 7=intendente,
# 8=parlamentario mercosur nacional, 9=parlamentario mercosur regional
_OFFICE_TO_IDCARGO: dict[str,int] = {
    # Nacional
    "presidente y vicepresidente": 1,
    "presidente": 1,
    "senador nacional": 5,
    "senadores nacionales": 5,
    "diputado nacional": 3,
    "diputados nacionales": 3,
    "parlamentario mercosur distrito nacional": 8,
    "parlamentario del mercosur distrito nacional": 8,
    "parlamentario mercosur nacional": 8,
    "parlamentario mercosur distrito regional": 9,
    "parlamentario del mercosur distrito regional": 9,
    "parlamentario mercosur regional": 9,

    # Provincial (BA)
    "gobernador y vicegobernador": 4,
    "gobernador": 4,
    "senador provincial": 2,
    "senadores": 2,
    "diputado provincial": 6,
    "diputados provinciales": 6,
    "intendente": 7,
}

def _office_to_idcargo(office: str) -> int | None:
    key = _norm_office_name(office)
    return _OFFICE_TO_IDCARGO.get(key)

# --- NUEVO: mapa phase -> idEleccion ---
# 1=PASO, 2=GENERALES, 3=BALOTAJE
def _phase_to_ideleccion(phase: str) -> int | None:
    p = (phase or "").strip().upper()
    return {"PASO":1, "GENERAL":2, "GENERALES":2, "BALOTAJE":3}.get(p)

# --- NUEVO: builder de json_url (endpoint JSON, no CSV) ---
def build_json_url(year: str | int, phase: str, office: str) -> str | None:
    """
    Estructura:
      https://resultados.mininterior.gob.ar/api/resultado/totalizado?a%C3%B1o={year}&recuento=Provisorio&idEleccion={1|2|3}&idCargo={...}&idDistrito=2
    - año: variable
    - recuento: Provisorio
    - idDistrito: 2 (fijo)
    - idEleccion: por phase (PASO=1, GENERALES=2, BALOTAJE=3)
    - idCargo: por office (mapa arriba)
    """
    try:
        y = int(str(year).strip())
    except Exception:
        return None
    ide = _phase_to_ideleccion(phase)
    idc = _office_to_idcargo(office)
    if not ide or not idc:
        return None
    # 'año' debe ir URL-encoded; dejamos el carácter tal cual porque FastAPI/clients lo codifican al pedir
    return (
        f"https://resultados.mininterior.gob.ar/api/resultado/totalizado?"
        f"a%C3%B1o={y}&recuento=Provisorio&idEleccion={ide}&idCargo={idc}&idDistrito=2"
    )

def excel_bytes_to_records(data: bytes) -> List[dict]:
    """
    Convierte la primera hoja del Excel a una lista de dicts (records).
    - Normaliza columnas a str strip()
    - Convierte NaN a None
    """
    df = pd.read_excel(io.BytesIO(data), dtype=object)
    df.columns = [str(c).strip() for c in df.columns]
    df = df.where(pd.notnull(df), None)
    return df.to_dict(orient="records")


def load_jsonl(path: Path) -> List[dict]:
    items: List[dict] = []
    if not path.exists():
        raise HTTPException(400, f"Falta archivo requerido: {path}")
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                items.append(json.loads(line))
            except json.JSONDecodeError as e:
                raise HTTPException(400, f"JSONL inválido en {path}: {e}")
    return items


def ensure_year(db: dict, year: str) -> None:
    if year not in db:
        db[year] = {"categories": {}}

def ensure_category(db: dict, year: str, election_category: str) -> None:
    ensure_year(db, year)
    cats = db[year]["categories"]
    if election_category not in cats:
        cats[election_category] = {"seats_excel": None, "phases": {}}

def ensure_phase(db: dict, year: str, election_category: str, phase: str) -> None:
    ensure_category(db, year, election_category)
    phases = db[year]["categories"][election_category]["phases"]
    if phase not in phases:
        phases[phase] = {"entries": {}}  # office -> {method,url,seats,base_sha256,last_processed,last_calc,result}

def detect_method(category_items: List[dict], methods: List[dict], election_category: str, office: str, phase: str) -> Tuple[str, str]:
    # Busca la fila en categories.jsonl y valida que esa oficina aplique a la fase
    matched = None
    for it in category_items:
        if it.get("category") == election_category and it.get("office") == office:
            matched = it
            break
    if not matched:
        raise HTTPException(400, "Categoría/Cargo no está definido en categories.jsonl")

    key = phase.lower()
    if key not in ("paso", "general", "balotaje"):
        raise HTTPException(400, "Fase inválida")
    if not bool(matched.get(key, False)):
        raise HTTPException(400, f"{phase} no aplica para este cargo")

    method_name = matched.get("method")
    desc = None
    for m in methods:
        if m.get("method") == method_name:
            desc = m.get("description")
            break
    if desc is None:
        raise HTTPException(400, f"Método '{method_name}' no definido en method.jsonl")
    return method_name, desc

# ================= Cálculos =================
def calc_dhont(seats: int, votes: Dict[str, int]) -> Dict[str, int]:
    if seats <= 0:
        raise HTTPException(400, "'seats' debe ser > 0 para D'Hondt")
    quotients: List[Tuple[str, float]] = []
    for p, v in votes.items():
        if v < 0:
            raise HTTPException(400, "Votos negativos no permitidos")
        for d in range(1, seats + 1):
            quotients.append((p, v / d))
    quotients.sort(key=lambda x: x[1], reverse=True)
    top = quotients[:seats]
    result: Dict[str, int] = {}
    for p, _ in top:
        result[p] = result.get(p, 0) + 1
    return result

def calc_hare(seats: int, votes: Dict[str, int]) -> Dict[str, int]:
    if seats <= 0:
        raise HTTPException(400, "'seats' debe ser > 0 para Hare")
    total_valid = sum(max(0, v) for v in votes.values())
    if total_valid <= 0:
        raise HTTPException(400, "Total de votos válidos debe ser > 0 para Hare")
    quota = total_valid / seats
    alloc = {p: int(v // quota) for p, v in votes.items()}
    assigned = sum(alloc.values())
    rests = sorted(((p, (v - alloc[p] * quota)) for p, v in votes.items()), key=lambda x: x[1], reverse=True)
    for _ in range(seats - assigned):
        if not rests:
            break
        p, _r = rests.pop(0)
        alloc[p] = alloc.get(p, 0) + 1
    return alloc

def calc_lista_incompleta(votes: Dict[str, int]) -> Dict[str, int]:
    ordered = sorted(votes.items(), key=lambda x: x[1], reverse=True)
    if not ordered:
        return {}
    if len(ordered) == 1:
        return {ordered[0][0]: 3}
    return {ordered[0][0]: 2, ordered[1][0]: 1}

# ================= Preprocesamiento =================
def sha256_of_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def download_csv(url: str) -> Tuple[str, bytes]:
    if not url:
        raise HTTPException(400, "URL requerida")
    try:
        with httpx.stream("GET", url, follow_redirects=True, timeout=60) as r:
            r.raise_for_status()
            buf = io.BytesIO()
            for chunk in r.iter_bytes():
                buf.write(chunk)
            data = buf.getvalue()
    except httpx.HTTPError as e:
        raise HTTPException(400, f"Descarga falló: {e}")
    digest = sha256_of_bytes(data)
    out_path = FILES_DIR / f"{digest}.csv"
    out_path.write_bytes(data)
    return digest, data

def parse_votes_from_csv(data: bytes) -> List[dict]:
    txt = data.decode("utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(txt))
    rows: List[dict] = []
    required = {"votos_tipo", "votos_cantidad", "agrupacion_id", "agrupacion_nombre", "cargo_nombre"}
    missing = required - set([c.strip() for c in (reader.fieldnames or [])])
    if missing:
        raise HTTPException(400, f"CSV inválido: faltan columnas {sorted(missing)}")
    for raw in reader:
        try:
            if (raw.get("votos_tipo") or "").upper() != "POSITIVO":
                continue
            votos = int(float(raw.get("votos_cantidad") or 0))
            rows.append({
                "cargo_nombre": (raw.get("cargo_nombre") or "").strip(),
                "agrupacion_id": (raw.get("agrupacion_id") or "").strip(),
                "agrupacion_nombre": (raw.get("agrupacion_nombre") or "").strip(),
                "votos": max(0, votos),
            })
        except Exception:
            continue
    if not rows:
        raise HTTPException(400, "CSV no contiene votos POSITIVO válidos")
    return rows

def write_jsonl(path: Path, items: List[dict]) -> None:
    with path.open("w", encoding="utf-8") as f:
        for it in items:
            f.write(json.dumps(it, ensure_ascii=False) + "\n")

# ================= Views =================
# agrega al inicio del archivo si no está
# main.py
import json
from pathlib import Path
from fastapi import Request
from src.helpers.utils import _now_iso
# asumo que ya existe load_db()

def _norm(s: str) -> str:
    return (
        (s or "")
        .strip()
        .upper()
        .replace("Á","A").replace("É","E").replace("Í","I").replace("Ó","O").replace("Ú","U")
        .replace("Ü","U").replace("Ñ","N")
    )

# Mapea nombres de "office" del config a aliases posibles en el JSONL
OFFICE_ALIASES = {
    "PRESIDENTE Y VICEPRESIDENTE": ["PRESIDENTE Y VICEPRESIDENTE"],
    "DIPUTADO NACIONAL": ["DIPUTADO NACIONAL", "DIPUTADOS NACIONALES"],
    "SENADOR NACIONAL": ["SENADOR NACIONAL", "SENADORES NACIONALES"],
    "PARLAMENTARIO MERCOSUR DISTRITO NACIONAL": ["PARLAMENTARIO MERCOSUR DISTRITO NACIONAL"],
    "PARLAMENTARIO MERCOSUR DISTRITO REGIONAL": ["PARLAMENTARIO MERCOSUR DISTRITO REGIONAL"],
    # extendé acá si agregás más offices
}

def _iter_jsonl(jsonl_path: Path):
    with jsonl_path.open("r", encoding="utf-8") as fh:
        for line in fh:
            s = line.strip()
            if not s:
                continue
            yield json.loads(s)

def _resolve_seats_for_office(lines, office: str) -> int:
    """
    Suma 'cantidad_cargos' de líneas cuyo nombre_cargo matchee el 'office'.
    Filtra por escala 'ARGENTINA' o 'PROVINCIA DE BUENOS AIRES' (según datos provistos).
    No hace fallback: si no hay match, retorna 0.
    """
    officeN = _norm(office)
    aliases = OFFICE_ALIASES.get(officeN, [officeN])

    total = 0
    for row in lines:
        cargoN = _norm(row.get("nombre_cargo", ""))
        escalaN = _norm(row.get("nombre_escala_territorial", ""))
        if not cargoN:
            continue

        if not any(cargoN == a or a in cargoN for a in aliases):
            continue

        # Filtrado de escala (ajusta si tu JSONL usa otra nomenclatura)
        if escalaN not in ("ARGENTINA", "PROVINCIA DE BUENOS AIRES"):
            continue

        cc = row.get("cantidad_cargos", 0)
        if isinstance(cc, (int, float)) and cc > 0:
            total += int(cc)
    return total

def build_seats_ctx(db: dict) -> dict:
    """
    Devuelve un dict:
      { "{year}::{category}::{office}": seats_int, ... }
    Resuelve seats leyendo el JSONL definido en la categoría (seats_jsonl).
    """
    seats_ctx = {}
    for year, ydata in (db or {}).items():
        cats = (ydata.get("categories") or {})
        for cat, cdata in cats.items():
            seats_jsonl = (cdata or {}).get("seats_jsonl")
            if not seats_jsonl:
                continue
            jsonl_path = Path(seats_jsonl)
            # Leemos todas las líneas una sola vez por categoría
            lines = list(_iter_jsonl(jsonl_path))

            phases = (cdata.get("phases") or {})
            for _, pdata in phases.items():
                for office, _meta in (pdata.get("entries") or {}).items():
                    key = f"{year}::{cat}::{office}"
                    seats_ctx[key] = _resolve_seats_for_office(lines, office)
    return seats_ctx

@router.get("/")
async def home(request: Request):
    db = load_db()

    # Construcción de filas para la tabla
    rows = []
    years_set = set()
    phases_set = set()

    for year, ydata in (db or {}).items():
        years_set.add(year)
        cats = (ydata.get("categories") or {})
        for cat, cdata in cats.items():
            phases = (cdata.get("phases") or {})
            for ph, pdata in phases.items():
                phases_set.add(ph)
                for office, meta in (pdata.get("entries") or {}).items():
                    rows.append({
                        "year": year,
                        "election_type": ph,               # phase
                        "category": cat,
                        "office": office,
                        "method": (meta or {}).get("method"),
                        "url": (meta or {}).get("url"),
                        "json_url": (meta or {}).get("json_url"),
                        "seats": (meta or {}).get("seats"),  # puede venir null; el front usará seats_ctx
                        "base_sha256": (meta or {}).get("base_sha256"),
                        "last_calc": (meta or {}).get("last_calc"),
                        "result": (meta or {}).get("result"),
                        "preprocessed_json": (meta or {}).get("preprocessed_json"),
                    })

    rows.sort(key=lambda r: (r["year"], r["category"], r["election_type"], r["office"]))
    years = sorted(years_set)
    phases = sorted(phases_set)

    # Resolvemos y serializamos el arreglo de seats
    seats_ctx = build_seats_ctx(db)
    seats_ctx_json = json.dumps(seats_ctx, ensure_ascii=False)

    print("seats_ctx_json=", seats_ctx_json)

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "title": "Inicio",
            "rows": rows,
            "years": years,
            "phases": phases,
            "now": _now_iso(),
            "seats_ctx_json": seats_ctx_json,
        },
    )


# AGREGAR (debajo de otras rutas /elective_office)
@router.get("/api/calc")
async def api_get_calc(
    year: str,
    election_category: str,
    phase: str,
    office: str,
):
    """
    Devuelve {"calc": ...} leyendo el archivo preprocessed_json registrado
    en db.json para esa entrada (si existe); si no hay, devuelve 404 o calc vacío.
    """
    db = load_db()
    try:
        meta = db[year]["categories"][election_category]["phases"][phase]["entries"][office]
    except KeyError:
        raise HTTPException(404, "Entrada no encontrada en db.json")

    pre_rel = (meta or {}).get("preprocessed_json")
    if not pre_rel:
        raise HTTPException(404, "No hay preprocessed_json aún para esta entrada")

    pre_path = (DATA_DIR / pre_rel).resolve()
    if not pre_path.exists():
        raise HTTPException(404, f"Archivo preprocesado no encontrado: {pre_path}")

    try:
        payload = json.loads(pre_path.read_text(encoding="utf-8"))
    except Exception as e:
        raise HTTPException(500, f"Error leyendo preprocessed_json: {e}")

    # Si no tiene 'calc', devolvemos calc vacío coherente
    calc = payload.get("calc")

    # === NUEVO: cargar mapa group_id -> group_name desde groups_jsonl ===
    groups_map: dict[str, str] = {}
    try:
        # groups_jsonl se define al nivel de la categoría en db.json
        groups_rel = (db[year]["categories"][election_category] or {}).get("groups_jsonl")
        if groups_rel:
            groups_path = (DATA_DIR / groups_rel).resolve()
            if groups_path.exists():
                # Soportamos dos formatos de línea:
                # - {"group_id": "20132", "group_name": "JUNTOS POR EL CAMBIO"}
                # - {"20132": "JUNTOS POR EL CAMBIO"}
                for it in load_jsonl(groups_path):
                    if isinstance(it, dict):
                        if "group_id" in it and "group_name" in it:
                            gid = str(it.get("group_id") or "").strip()
                            gnm = str(it.get("group_name") or "").strip()
                            if gid:
                                groups_map[gid] = gnm
                        elif len(it) == 1:
                            k, v = next(iter(it.items()))
                            gid = str(k).strip()
                            gnm = str(v or "").strip()
                            if gid:
                                groups_map[gid] = gnm
    except Exception:
        # Si falla, seguimos devolviendo calc sin nombres (fallback silencioso)
        groups_map = {}

    return JSONResponse({"calc": calc, "groups": groups_map})

@router.get("/config")
async def config_view(
    request: Request,
    flt_year: str | None = Query(None),
    flt_category: str | None = Query(None),
    flt_phase: str | None = Query(None),
    year: str | None = Query(None),
    election_category: str | None = Query(None),
    phase: str | None = Query(None),
    office: str | None = Query(None),
):
    db = load_db()
    # Cargamos definiciones (si faltan, lanzan 400 explícito)
    cats = load_jsonl(CATEGORIES_PATH)
    methods = load_jsonl(METHODS_PATH)

    # Puede ser {} la primera vez
    years = sorted(db.keys()) if isinstance(db, dict) else []
    attached_categories: list[dict] = []
    phases_summary: list[dict] = []
    url_entries: list[dict] = []

    for y in years:
        ydata = db.get(y) or {}
        # ✅ usa .get con dict vacío por defecto
        categories = (ydata.get("categories") or {})
        for cat, cdata in categories.items():
            cdata = cdata or {}
            attached_categories.append({
                "year": y,
                "category": cat,
                "excel": cdata.get("seats_excel"),
            })
            phases_map = (cdata.get("phases") or {})
            phases = sorted(phases_map.keys())
            phases_summary.append({"year": y, "category": cat, "phases": phases})
            for ph, pdata in phases_map.items():
                entries_map = (pdata or {}).get("entries") or {}
                for off, meta in entries_map.items():
                    url_entries.append({
                        "year": y,
                        "category": cat,
                        "phase": ph,
                        "office": off,
                        "method": (meta or {}).get("method"),
                        "url": (meta or {}).get("url"),
                    })

    # Filtros en tabla CRUD
    if flt_year:
        url_entries = [e for e in url_entries if e["year"] == flt_year]
    if flt_category:
        url_entries = [e for e in url_entries if e["category"] == flt_category]
    if flt_phase:
        url_entries = [e for e in url_entries if e["phase"] == flt_phase]

    # Logs (opcional si se pasa combinación)
    log_text = None
    if year and election_category and phase and office:
        log_text = read_log(year, election_category, phase, office)

    return templates.TemplateResponse(
        "config.html",
        {
            "request": request,
            "title": "Config",
            "years": years,
            "attached_categories": attached_categories,
            "phases_summary": phases_summary,
            "url_entries": sorted(
                url_entries,
                key=lambda x: (x["year"], x["category"], x["phase"], x["office"]),
            ),
            "flt_year": flt_year,
            "flt_category": flt_category,
            "flt_phase": flt_phase,
            "log_text": log_text,
            "now": _now_iso(),
        },
    )


# --------- Pasos 1-3 ----------
@router.post("/config/create_year")
async def create_year(year: str = Form(...)):
    year = (year or "").strip()
    if not year.isdigit() or len(year) != 4:
        raise HTTPException(400, "Año inválido")
    db = load_db()
    ensure_year(db, year)
    save_db(db)
    return RedirectResponse("/elective_office/config", status_code=303)

@router.post("/config/rename_year")
async def rename_year(old_year: str = Form(...), new_year: str = Form(...)):
    db = load_db()
    if old_year not in db:
        raise HTTPException(404, "Año a renombrar no existe")
    if new_year in db:
        raise HTTPException(400, "El año nuevo ya existe")
    # mover estructura completa del año bajo la nueva clave
    db[new_year] = db.pop(old_year)
    save_db(db)
    return RedirectResponse("/elective_office/config", status_code=303)

# Helper local para borrar sin romper si no existe
def _safe_unlink(p: Path) -> None:
    try:
        if p.is_file():
            p.unlink()
    except Exception:
        pass

@router.post("/config/delete_year")
async def delete_year(year: str = Form(...)):
    year = (year or "").strip()
    db = load_db()
    if year not in db:
        raise HTTPException(404, "Año no existe")

    # Recorrer categorías de ese año y borrar sus archivos seats_excel/seats_json
    year_data = db.get(year) or {}
    categories = (year_data.get("categories") or {})
    for cat_data in categories.values():
        if not cat_data:
            continue
        seats_excel_rel = cat_data.get("seats_excel")
        seats_json_rel  = cat_data.get("seats_json")
        if seats_excel_rel:
            excel_path = (DATA_DIR / seats_excel_rel).resolve()
            if DATA_DIR in excel_path.parents or excel_path == DATA_DIR:
                _safe_unlink(excel_path)
        if seats_json_rel:
            json_path = (DATA_DIR / seats_json_rel).resolve()
            if DATA_DIR in json_path.parents or json_path == DATA_DIR:
                _safe_unlink(json_path)

    # Finalmente borrar el año del DB
    del db[year]
    save_db(db)
    return RedirectResponse("/elective_office/config", status_code=303)

def excel_bytes_to_jsonl_rows(data: bytes) -> list[dict]:
    """
    Lee la primera hoja del Excel y devuelve filas con SOLO las columnas requeridas:
    tipo_escala_territorial | nombre_escala_territorial | numero_seccion_electoral | tipo_cargo | nombre_cargo

    - Normaliza encabezados (strip)
    - Convierte NaN a None
    - numero_seccion_electoral intenta int; si falla, deja None
    """
    import io
    import pandas as pd

    required = [
        "tipo_escala_territorial",
        "nombre_escala_territorial",
        "numero_seccion_electoral",
        "tipo_cargo",
        "nombre_cargo",
        "cantidad_cargos",
    ]

    df = pd.read_excel(io.BytesIO(data), dtype=object)
    df.columns = [str(c).strip() for c in df.columns]
    df = df.where(pd.notnull(df), None)

    missing = [c for c in required if c not in df.columns]
    if missing:
        raise HTTPException(400, f"Excel inválido: faltan columnas {missing}")

    # Nos quedamos solo con las columnas requeridas (en ese orden)
    df = df[required].copy()

    # numero_seccion_electoral -> int si se puede, si no None
    def _to_int_or_none(x):
        if x is None:
            return None
        try:
            # Acepta "123", 123.0, etc.
            n = int(float(str(x).strip()))
            return n
        except Exception:
            return None

    df["numero_seccion_electoral"] = df["numero_seccion_electoral"].map(_to_int_or_none)

    # A dicts orient="records"
    return df.to_dict(orient="records")


@router.post("/config/attach_category")
async def attach_category(
    year: str = Form(...),
    election_category: str = Form(...),
    seats_excel: UploadFile = File(...),
):
    year = (year or "").strip()
    election_category = (election_category or "").strip()
    if election_category not in ("Nacional", "Provincial"):
        raise HTTPException(400, "Categoría debe ser Nacional o Provincial")
    if not seats_excel.filename.lower().endswith((".xlsx", ".xls")):
        raise HTTPException(400, "Adjunte un Excel (.xlsx/.xls)")

    # 1) Persistir Excel original
    data = await seats_excel.read()
    excel_path = FILES_DIR / f"{year}__{election_category}.xlsx"
    excel_path.write_bytes(data)

    # 2) Transformar a JSONL y guardar en preprocessed_data/
    digest = sha256_of_bytes(data)
    jsonl_name = f"{year}__{election_category}__seats__{digest}.jsonl"
    jsonl_path = PRE_DIR / jsonl_name
    try:
        rows = excel_bytes_to_jsonl_rows(data)  # <- valida y normaliza las 5 columnas
    except Exception as e:
        raise HTTPException(400, f"Error leyendo/validando Excel: {e}")

    # Escribe una línea por item
    write_jsonl(jsonl_path, rows)

    # 3) Actualizar db.json (ruta del Excel y seats_jsonl)
    db = load_db()
    ensure_category(db, year, election_category)
    cat = db[year]["categories"][election_category]
    cat["seats_excel"]  = str(excel_path.relative_to(DATA_DIR))
    cat["seats_jsonl"]  = str(jsonl_path.relative_to(DATA_DIR))  # nuevo campo; sustituye seats_json
    # si querés, eliminar el viejo si existiera:
    if "seats_json" in cat:
        del cat["seats_json"]
    save_db(db)

    return RedirectResponse("/elective_office/config", status_code=303)


@router.post("/config/create_phase")
async def create_phase(
    year: str = Form(...),
    election_category: str = Form(...),
    phase: str = Form(...),
):
    year = (year or "").strip()
    election_category = (election_category or "").strip()
    phase = (phase or "").strip().upper()
    if phase not in ("PASO", "GENERAL", "BALOTAJE"):
        raise HTTPException(400, "Fase inválida")

    # Debe existir la categoría y su excel
    db = load_db()
    ensure_category(db, year, election_category)
    seats_excel = db[year]["categories"][election_category].get("seats_excel")
    if not seats_excel:
        raise HTTPException(400, "Debe adjuntar primero el Excel de cargos para la categoría")

    ensure_phase(db, year, election_category, phase)

    # Autogenerar entradas según categories.jsonl (compatibles con la fase solicitada)
    cats = load_jsonl(CATEGORIES_PATH)
    methods = load_jsonl(METHODS_PATH)
    entries = db[year]["categories"][election_category]["phases"][phase]["entries"]

    # Creamos oficinas sólo si aplican a esta fase y no existen aún.
    for it in cats:
        if it.get("category") != election_category:
            continue
        if not bool(it.get(phase.lower(), False)):
            continue
        office = it.get("office")
        if office in entries:
            continue
        method_name, _desc = detect_method(cats, methods, election_category, office, phase)

        # NUEVO: calcular json_url según estándares
        computed_json_url = build_json_url(year, phase, office)

        entries[office] = {
            "method": method_name,
            "url": None,
            "json_url": computed_json_url,   # <-- NUEVO CAMPO
            "seats": None,
            "base_sha256": None,
            "last_processed": None,
            "last_calc": None,
            "result": None,
        }


    save_db(db)
    return RedirectResponse("/elective_office/config", status_code=303)

# Endpoint para recomputar json_url masivamente
@router.post("/config/rebuild_json_urls")
async def rebuild_json_urls(
    year: str = Form(...),
    election_category: str = Form(...),
    phase: str | None = Form(None),
):
    """
    Recalcula y guarda 'json_url' para todas las entries del contexto indicado.
    Si 'phase' es None, procesa todas las fases de la categoría.
    """
    db = load_db()
    try:
        cat_node = db[year]["categories"][election_category]
    except KeyError:
        raise HTTPException(404, "Contexto inexistente en db.json")

    phases = [phase] if phase else list((cat_node.get("phases") or {}).keys())
    updated = 0

    for ph in phases:
        pnode = (cat_node.get("phases") or {}).get(ph) or {}
        entries = pnode.get("entries") or {}
        for office, meta in entries.items():
            new_url = build_json_url(year, ph, office)
            meta["json_url"] = new_url
            updated += 1

    save_db(db)
    return RedirectResponse(
        f"/elective_office/config?flt_year={year}&flt_category={election_category}" + (f"&flt_phase={phase}" if phase else ""),
        status_code=303,
    )


# --------- 4) CRUD URL ----------
@router.post("/config/upsert_url")
async def upsert_url(
    year: str = Form(...),
    election_category: str = Form(...),
    phase: str = Form(...),
    office: str = Form(...),
    url: str = Form(None),
):
    db = load_db()
    try:
        meta = db[year]["categories"][election_category]["phases"][phase]["entries"][office]
    except KeyError:
        raise HTTPException(404, "Entrada no encontrada; asegúrese de haber creado la fase")

    meta["url"] = (url or "").strip() or None
    save_db(db)
    return RedirectResponse("/elective_office/config", status_code=303)

# --------- Parse, preprocess & Calc (adaptados a nuevo esquema) ----------
# ===== Helpers stub para orquestación (por ahora solo prints) =====
def do_parse(year: str, election_category: str, phase: str, office: str) -> bool:
    """
    Descarga y valida el archivo CSV asociado a la entrada indicada.
    - Guarda el CSV en files/<sha256>.csv
    - Actualiza db.json con la ruta relativa y el sha256
    - Devuelve True si se parseó correctamente
    """
    print(f"[PARSE] year={year} category={election_category} phase={phase} office={office}")
    db = load_db()
    try:
        meta = db[year]["categories"][election_category]["phases"][phase]["entries"][office]
        print(f"Entrada encontrada: {meta}")
    except KeyError:
        print(f"Entrada no encontrada en db.json")
        raise HTTPException(404, "Entrada no encontrada en db.json")

    url = meta.get("url")
    if not url:
        print(f"No hay URL registrada para esta entrada")
        raise HTTPException(400, "No hay URL registrada para esta entrada")

    # 1) Descargar archivo
    print(f"Descargando archivo: {url}")
    digest, data = download_csv(url)

    # 2) Guardar archivo en files/
    print(f"Guardando archivo en files/{digest}.csv")
    csv_path = FILES_DIR / f"{digest}.csv"
    csv_path.write_bytes(data)
    print(f"Archivo guardado en files/{digest}.csv")

    # 3) Intentar parsear (si falla, se lanza HTTPException)
    print(f"Intentando parsear archivo")
    _ = parse_votes_from_csv(data)
    print(f"Parseo exitoso")

    # 4) Actualizar db.json con info del archivo
    print(f"Actualizando db.json")
    meta["base_sha256"] = digest
    meta["last_processed"] = _now_iso()
    meta["file_path"] = str(csv_path.relative_to(DATA_DIR))
    print(f"Guardando db.json: {meta}")
    save_db(db)
    print(f"Parseo exitoso")
    return True


def do_preprocess(year: str, election_category: str, phase: str, office: str) -> dict:
    """
    Lee el CSV referenciado en db.json (base_sha256 / file_path) y genera un JSON normalizado en:
    preprocessed_data/<year>__<category>__<phase>__<office>__<sha256>.json

    Cambios requeridos:
    - En 'rows' guarda sólo 'group_id' (NO 'group_name').
    - Mapea 'vote_type' a código numérico según especificación.
    - Copia campos: seccionprovincial_id, seccion_id, circuito_id, mesa_id, mesa_electores.
    - Usa/crea mapeo de grupos en preprocessed_data/groups_{año}_{nacional|provincial}.jsonl.
      * La ruta relativa se guarda en db.json al mismo nivel que 'seats_excel' bajo la categoría.
      * Si aparecen nuevos group_id -> se agregan (group_id como clave, group_name como valor) y
        se reescribe el jsonl al finalizar.
    """
    print(f"[PREPROCESS] year={year} category={election_category} phase={phase} office={office}")

    # --- 1) Resolver metadata desde db.json ---
    db = load_db()
    try:
        meta = db[year]["categories"][election_category]["phases"][phase]["entries"][office]
    except KeyError:
        raise HTTPException(404, "Entrada no encontrada en db.json")

    digest = (meta or {}).get("base_sha256")
    rel_csv = (meta or {}).get("file_path")
    if not digest or not rel_csv:
        raise HTTPException(400, "Falta realizar el parseo previo (base_sha256/file_path ausentes)")

    csv_path = (DATA_DIR / rel_csv).resolve()
    if not csv_path.exists():
        raise HTTPException(400, f"CSV base no encontrado: {csv_path}")

    # --- 2) Preparar ruta de groups_jsonl por año+tipo (nacional/provincial) ---
    # Normalizamos categoría para decidir 'nacional' o 'provincial'
    cat_lc = (election_category or "").strip().lower()
    if "nac" in cat_lc:
        cat_slug = "nacional"
    elif "prov" in cat_lc:
        cat_slug = "provincial"
    else:
        # fallback conservador
        cat_slug = cat_lc.replace(" ", "_") or "categoria"

    groups_fname = f"groups_{year}_{cat_slug}.jsonl"
    groups_path = (PRE_DIR / groups_fname).resolve()  # absoluto
    groups_rel = str(Path("preprocessed_data") / groups_fname)  # relativo a DATA_DIR

    # Garantizar que la ruta quede guardada en db.json a nivel de la categoría
    cat_node = db.setdefault(year, {}).setdefault("categories", {}).setdefault(election_category, {})
    if cat_node.get("groups_jsonl") != groups_rel:
        cat_node["groups_jsonl"] = groups_rel
        save_db(db)

    # --- 3) Cargar mapeo de grupos existente desde el jsonl específico ---
    groups_map: dict[str, str] = {}
    if groups_path.exists():
        try:
            # Soportamos líneas en dos formatos:
            # - {"group_id": "20132", "group_name": "JUNTOS POR EL CAMBIO"}
            # - {"20132": "JUNTOS POR EL CAMBIO"}  (clave única por línea)
            items = load_jsonl(groups_path)
            for it in items:
                if isinstance(it, dict):
                    if "group_id" in it and "group_name" in it:
                        gid = str(it.get("group_id") or "").strip()
                        gnm = str(it.get("group_name") or "").strip()
                        if gid:
                            groups_map[gid] = gnm
                    elif len(it) == 1:
                        k, v = next(iter(it.items()))
                        gid = str(k).strip()
                        gnm = str(v or "").strip()
                        if gid:
                            groups_map[gid] = gnm
        except Exception:
            # No abortamos el preprocesado si el jsonl está corrupto; seguimos con vacío.
            groups_map = {}

    # --- 4) Leer CSV y normalizar filas ---
    data = csv_path.read_bytes()
    txt = data.decode("utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(txt))

    required = {"votos_tipo", "votos_cantidad", "agrupacion_id", "agrupacion_nombre"}
    missing = required - set([c.strip() for c in (reader.fieldnames or [])])
    if missing:
        raise HTTPException(400, f"CSV inválido: faltan columnas {sorted(missing)}")

    # Columnas adicionales a copiar tal cual
    extra_cols = ["seccionprovincial_id", "seccion_id", "circuito_id", "mesa_id", "mesa_electores"]

    # Mapeo de tipos de voto -> código (None si vacío o no reconocido)
    def map_vote_type(raw: str | None) -> int | None:
        if not raw:
            return None
        t = str(raw).strip().upper()
        if t in ("", "UNDEFINED", "NULL", "NONE"):
            return None
        # Especificación conocida:
        # 0 POSITIVO, 1 IMPUGNADO, 2 RECURRIDO, 3 COMANDO, (4 también figuraba como POSITIVO en una variante)
        # Opcionalmente contemplamos EN BLANCO (5) y NULO (6) si existiesen en la fuente.
        if t == "POSITIVO":
            return 0
        if t == "IMPUGNADO":
            return 1
        if t == "RECURRIDO":
            return 2
        if t == "COMANDO":
            return 3
        if t == "EN BLANCO":
            return 5
        if t == "NULO":
            return 6
        return None

    rows: list[dict] = []
    pending_groups: dict[str, str] = {}  # nuevos group_id->group_name detectados

    for raw_row in reader:
        try:
            # Código de tipo de voto
            vt_code = map_vote_type(raw_row.get("votos_tipo"))

            # Cantidad de votos (entero no negativo)
            try:
                vn = int(float(raw_row.get("votos_cantidad") or 0))
            except Exception:
                vn = 0
            vn = max(0, vn)

            # Normalización de group_id y registro del nombre
            gid_raw = raw_row.get("agrupacion_id")
            gid = str(gid_raw if gid_raw is not None else "").strip()
            # Si viene 'undefined'/vacío, se establece a "0" (pedido previo)
            if gid.lower() in ("", "undefined", "null", "none"):
                gid = "0"

            gnm = str(raw_row.get("agrupacion_nombre") or "").strip()

            # Registrar nuevos grupos para persistir (si tenemos nombre)
            if gid and gnm and (gid not in groups_map) and (gid not in pending_groups):
                pending_groups[gid] = gnm

            # Construir fila final SOLO con group_id + extras
            out_row = {
                "vote_type": vt_code,
                "vote_number": vn,
                "group_id": gid,  # solo id
            }
            for col in extra_cols:
                val = raw_row.get(col)
                if val is None:
                    out_row[col] = None
                else:
                    sval = str(val).strip()
                    out_row[col] = sval if sval != "" else None

            rows.append(out_row)
        except Exception:
            # fila corrupta: continuar
            continue

    if not rows:
        raise HTTPException(400, "CSV sin filas válidas para preprocesar")

    # --- 5) Persistir JSON preprocesado ---
    out_name = f"{year}__{election_category}__{phase}__{office}__{digest}.json"
    out_path = PRE_DIR / out_name
    payload = {
        "year": year,
        "category": election_category,
        "phase": phase,
        "office": office,
        "source": {"sha256": digest, "csv_path": str(csv_path.relative_to(DATA_DIR))},
        "rows": rows,
    }
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    # --- 6) Actualizar db.json con referencia al preprocesado (y groups_jsonl ya seteado) ---
    meta["preprocessed_json"] = str(out_path.relative_to(DATA_DIR))
    meta["last_preprocess"] = _now_iso()
    # 'groups_jsonl' ya quedó en cat_node al inicio; persistimos todo:
    save_db(db)

    # --- 7) Actualizar/crear preprocessed_data/groups_{año}_{tipo}.jsonl con nuevos group_id detectados ---
    if pending_groups:
        # Mezclar con los existentes
        groups_map.update(pending_groups)

        # Formato de salida: {group_id: group_name} por línea
        lines: list[dict] = [{gid: groups_map[gid]} for gid in sorted(groups_map.keys(), key=lambda x: (len(x), x))]

        # Asegurar directorio
        groups_path.parent.mkdir(parents=True, exist_ok=True)
        write_jsonl(groups_path, lines)

    log_append(year, election_category, phase, office, f"Preprocess OK -> {out_path.name} ({len(rows)} filas)")
    return {
        "ok": True,
        "stage": "preprocess",
        "output": str(out_path.relative_to(DATA_DIR)),
        "rows": len(rows),
        "new_groups": len(pending_groups),
        "groups_jsonl": groups_rel,
    }





@router.post("/config/parse")
async def parse(
    year: str = Form(...),
    election_category: str = Form(...),
    phase: str = Form(...),
    office: str = Form(...),
):
    """
    Preprocesamiento: primero parsea el CSV, luego continúa con el stub.
    """
    if not do_parse(year, election_category, phase, office):
        raise HTTPException(400, "Parseo fallido")
    return RedirectResponse(
        f"/elective_office/config?flt_year={year}&flt_category={election_category}&flt_phase={phase}",
        status_code=303,
    )

@router.post("/config/preprocess")
async def preprocess(
    year: str = Form(...),
    election_category: str = Form(...),
    phase: str = Form(...),
    office: str = Form(...),
):
    """
    Ahora usa do_preprocess(...) (stub con print). 
    Mantengo Redirect para volver al panel.
    """
    _ = do_preprocess(year, election_category, phase, office)
    return RedirectResponse(
        f"/elective_office/config?flt_year={year}&flt_category={election_category}&flt_phase={phase}",
        status_code=303,
    )

@router.post("/config/calc")
async def calc(
    year: str = Form(...),
    election_category: str = Form(...),
    phase: str = Form(...),
    office: str = Form(...),
):
    """
    Ahora usa do_calc(...) (stub con print). 
    Por ahora method se deja en None; se podrá inferir/validar luego.
    """
    _ = do_calc(year, election_category, phase, office)

    return RedirectResponse(
        f"/elective_office/config?flt_year={year}&flt_category={election_category}&flt_phase={phase}",
        status_code=303,
    )


from fastapi import Body
from fastapi.responses import JSONResponse

@router.post("/config/preprocess_and_calc")
async def preprocess_and_calc(payload: dict = Body(...)):
    """
    Acepta JSON con:
      {
        "year": "2025",
        "election_type": "PASO",     # -> phase
        "category": "Nacional",      # -> election_category
        "office": "diputados",
        "method": "d-hont"           # opcional
      }
    Ejecuta do_preprocess(...) y luego do_calc(...). Por ahora solo prints.
    """
    year = str(payload.get("year", "")).strip()
    phase = str(payload.get("election_type", "")).strip()          # mapea a 'phase'
    election_category = str(payload.get("category", "")).strip()   # mapea a 'election_category'
    office = str(payload.get("office", "")).strip()

    if not (year and phase and election_category and office):
        return JSONResponse({"ok": False, "error": "Parámetros requeridos: year, election_type, category, office"}, status_code=400)

    pre_res = do_preprocess(year, election_category, phase, office)
    calc_res = do_calc(year, election_category, phase, office)
    return JSONResponse({"ok": True, "preprocess": pre_res, "calc": calc_res})



# Redirect raíz
@app.get("/")
async def root():
    return RedirectResponse("/elective_office/")



# Renombrar fase
@router.post("/config/rename_phase")
async def rename_phase(
    year: str = Form(...),
    election_category: str = Form(...),
    phase_old: str = Form(...),
    phase_new: str = Form(...),
):
    year = (year or "").strip()
    election_category = (election_category or "").strip()
    po = (phase_old or "").strip().upper()
    pn = (phase_new or "").strip().upper()
    if pn not in ("PASO", "GENERAL", "BALOTAJE"):
        raise HTTPException(400, "Fase nueva inválida")

    db = load_db()
    try:
        cats = db[year]["categories"]
        phases = cats[election_category]["phases"]
    except KeyError:
        raise HTTPException(404, "Ruta (año/categoría) inexistente")

    if po not in phases:
        raise HTTPException(404, "Fase a renombrar no existe")
    if pn in phases:
        raise HTTPException(400, "La fase destino ya existe")

    phases[pn] = phases.pop(po)
    save_db(db)
    return RedirectResponse("/elective_office/config", status_code=303)


# NUEVO: eliminar fase
@router.post("/config/delete_phase")
async def delete_phase(
    year: str = Form(...),
    election_category: str = Form(...),
    phase: str = Form(...),
):
    year = (year or "").strip()
    election_category = (election_category or "").strip()
    phase = (phase or "").strip().upper()

    db = load_db()
    try:
        phases = db[year]["categories"][election_category]["phases"]
    except KeyError:
        raise HTTPException(404, "Ruta (año/categoría) inexistente")

    if phase not in phases:
        raise HTTPException(404, "Fase no existe")
    del phases[phase]
    save_db(db)
    return RedirectResponse("/elective_office/config", status_code=303)


# Helper local para borrar sin romper si no existe
def _safe_unlink(p: Path) -> None:
    try:
        if p.is_file():
            p.unlink()
    except Exception:
        # opcional: loggear si querés
        pass

# Eliminar categoría completa + sus archivos seats_excel/seats_json
@router.post("/config/delete_category")
async def delete_category(
    year: str = Form(...),
    election_category: str = Form(...),
):
    year = (year or "").strip()
    election_category = (election_category or "").strip()

    db = load_db()
    try:
        cats = db[year]["categories"]
    except KeyError:
        raise HTTPException(404, "Año inexistente")

    cat = cats.get(election_category)
    if cat is None:
        raise HTTPException(404, "Categoría no existe en ese año")

    # Borrar archivos asociados si están registrados (paths guardados relativos a DATA_DIR)
    seats_excel_rel = (cat or {}).get("seats_excel")
    seats_json_rel  = (cat or {}).get("seats_json")

    if seats_excel_rel:
        excel_path = (DATA_DIR / seats_excel_rel).resolve()
        # Evitar escapes fuera de DATA_DIR
        if DATA_DIR in excel_path.parents or excel_path == DATA_DIR:
            _safe_unlink(excel_path)

    if seats_json_rel:
        json_path = (DATA_DIR / seats_json_rel).resolve()
        if DATA_DIR in json_path.parents or json_path == DATA_DIR:
            _safe_unlink(json_path)

    # Finalmente, quitar la categoría del DB
    del cats[election_category]
    save_db(db)
    return RedirectResponse("/elective_office/config", status_code=303)


# Registrar router (al final para incluir todas las rutas)
app.include_router(router)
