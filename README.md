# README.md

# Elective Office Server (FastAPI)

Servidor FastAPI para gestionar URLs de resultados electorales, preprocesar CSVs y calcular distribución de cargos (D'Hondt, Hare, Lista Incompleta, Mayoría Simple) con UI mínima.

## Estructura

```
|-- README.md
|-- requirements.txt
|-- src
| `-- routers
| `-- main.py
|-- static
| `-- robots.txt
`-- templates
|-- layout.html
|-- index.html
`-- config.html
```

## Instalación

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Ejecutar

```bash
uvicorn src.routers.main:app --reload --port 8004
```

- UI pública: http://localhost:8004/elective_office/
- UI de configuración: http://localhost:8004/elective_office/config

## Directorios de datos (se crean al inicio)

- `config/` → **categories.jsonl**, **method.jsonl** (existentes) y **db.json** (persistencia CRUD)
- `files/` → CSV descargados (nombre: `{sha256}.csv`)
- `preprocessed_data/` → JSONL preprocesados (`{id_file}_{id_election}_{elective_office}.jsonl`)
- `logs/` → bitácoras human‑readable por combinación (año, elección, categoría, cargo)

## Notas

- No hay defaults silenciosos. Los formularios validan campos obligatorios.
- Para **Calc** (D'Hondt/Hare/Lista incompleta) se exige **seats** (bancas) configurado para esa combinación.
