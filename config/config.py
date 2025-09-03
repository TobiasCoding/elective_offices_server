# =============== Paths & bootstrap ===============
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR
CONFIG_DIR = DATA_DIR / "config"
FILES_DIR = DATA_DIR / "files"
PRE_DIR = DATA_DIR / "preprocessed_data"
LOGS_DIR = DATA_DIR / "logs"
TEMPLATES_DIR = DATA_DIR / "templates"
STATIC_DIR = DATA_DIR / "static"

for d in (CONFIG_DIR, FILES_DIR, PRE_DIR, LOGS_DIR):
    d.mkdir(parents=True, exist_ok=True)

DB_PATH = CONFIG_DIR / "db.json"  # {year}->{election_id}->{category}->{office}->{url, method, seats, base_sha256, last_processed, last_calc, result}
CATEGORIES_PATH = CONFIG_DIR / "categories.jsonl"  # ya existe
METHODS_PATH = CONFIG_DIR / "method.jsonl"        # ya existe