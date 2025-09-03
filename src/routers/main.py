from __future__ import annotations

import os, json, hashlib, csv, io, datetime
from pathlib import Path
from typing import Dict, List, Tuple

import httpx
import pandas as pd
from fastapi import FastAPI, APIRouter, Request, Form, Query, HTTPException, UploadFile, File
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

# ================= Paths & bootstrap =================
from config.config import *

# ================= App =================
app = FastAPI(title="Elective Office")
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
router = APIRouter(prefix="/elective_office")

# ================= Utils =================
def _now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")

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

def load_db() -> dict:
    if not DB_PATH.exists():
        return {}
    try:
        return json.loads(DB_PATH.read_text(encoding="utf-8"))
    except Exception as e:
        raise HTTPException(500, f"db.json corrupto: {e}")

def save_db(db: dict) -> None:
    DB_PATH.write_text(json.dumps(db, ensure_ascii=False, indent=2), encoding="utf-8")

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
@router.get("/")
async def home(request: Request):
    db = load_db()
    # Salida resumida para usuario final (index)
    rows = []
    for year, ydata in db.items():
        cats = (ydata.get("categories") or {})
        for cat, cdata in cats.items():
            phases = (cdata.get("phases") or {})
            for ph, pdata in phases.items():
                for office, meta in (pdata.get("entries") or {}).items():
                    rows.append({
                        "year": year,
                        "election_type": ph,
                        "category": cat,
                        "office": office,
                        "method": meta.get("method"),
                        "url": meta.get("url"),
                        "base_sha256": meta.get("base_sha256"),
                        "last_calc": meta.get("last_calc"),
                        "result": meta.get("result"),
                    })
    rows.sort(key=lambda r: (r["year"], r["category"], r["election_type"], r["office"]))
    return templates.TemplateResponse("index.html", {"request": request, "title": "Inicio", "rows": rows, "now": _now_iso()})

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

    # 2) Transformar a JSON y guardar en preprocessed_data/
    digest = sha256_of_bytes(data)
    json_name = f"{year}__{election_category}__seats__{digest}.json"
    json_path = PRE_DIR / json_name
    try:
        records = excel_bytes_to_records(data)   # <- usa el helper nuevo
    except Exception as e:
        raise HTTPException(400, f"Error leyendo Excel: {e}")

    json_path.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")

    # 3) Actualizar db.json (conserva ruta del Excel y agrega seats_json)
    db = load_db()
    ensure_category(db, year, election_category)
    cat = db[year]["categories"][election_category]
    cat["seats_excel"] = str(excel_path.relative_to(DATA_DIR))
    cat["seats_json"]  = str(json_path.relative_to(DATA_DIR))  # NUEVO campo de referencia
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
        entries[office] = {
            "method": method_name,
            "url": None,
            "seats": None,            # se puede completar manualmente o derivar del Excel en otra etapa
            "base_sha256": None,
            "last_processed": None,
            "last_calc": None,
            "result": None,
        }

    save_db(db)
    return RedirectResponse("/elective_office/config", status_code=303)

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

# --------- Preprocess & Calc (adaptados a nuevo esquema) ----------
# ===== Helpers stub para orquestación (por ahora solo prints) =====
def do_preprocess(year: str, election_category: str, phase: str, office: str) -> dict:
    """
    Stub de preprocesamiento. Más adelante acá irá la lógica real.
    """
    print(f"[PREPROCESS] year={year} category={election_category} phase={phase} office={office}")
    return {"ok": True, "stage": "preprocess", "year": year, "category": election_category, "phase": phase, "office": office}

def do_calc(year: str, election_category: str, phase: str, office: str, method: str | None = None) -> dict:
    """
    Stub de cálculo. Más adelante acá irá la lógica real.
    """
    print(f"[CALC] year={year} category={election_category} phase={phase} office={office} method={method}")
    return {"ok": True, "stage": "calc", "year": year, "category": election_category, "phase": phase, "office": office, "method": method}


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
    _ = do_calc(year, election_category, phase, office, method=None)

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
    method = payload.get("method")

    if not (year and phase and election_category and office):
        return JSONResponse({"ok": False, "error": "Parámetros requeridos: year, election_type, category, office"}, status_code=400)

    pre_res = do_preprocess(year, election_category, phase, office)
    calc_res = do_calc(year, election_category, phase, office, method=method)
    return JSONResponse({"ok": True, "preprocess": pre_res, "calc": calc_res})



# Redirect raíz
@app.get("/")
async def root():
    return RedirectResponse("/elective_office/")


@app.get("/favicon.ico")
async def favicon():
    return RedirectResponse("/static/favicon.ico")


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
