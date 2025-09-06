# Elective Office

Servidor FastAPI para calcular distribución de cargos (D'Hondt, Hare, Lista Incompleta, Mayoría Simple).

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

## Archivo con datos de cargos electivos

**Columnas:** tipo_escala_territorial, nombre_escala_territorial, numero_seccion_electoral, tipo_cargo, nombre_cargo, cantidad_cargos

**Ejemplo:** `datos_cargos_electivos_2023.xlsx`
