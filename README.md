# Elective Office

Server to calculate distribution of positions (D'Hondt, Hare, Incomplete List, Simple Majority).

## Instalation

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Execute

```bash
uvicorn src.routers.main:app --reload --port 8004
```

- Public UI: http://localhost:8004/elective_office/
- Configuration UI: http://localhost:8004/elective_office/config

## File with data on elective offices

**Columns:** tipo_escala_territorial, nombre_escala_territorial, numero_seccion_electoral, tipo_cargo, nombre_cargo, cantidad_cargos

**Example:** `datos_cargos_electivos_2023.xlsx`
