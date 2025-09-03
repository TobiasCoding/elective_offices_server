# =============== Paths & bootstrap ===============
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
print("BASE_DIR=", BASE_DIR)
DATA_DIR = BASE_DIR
print("DATA_DIR=", DATA_DIR)
CONFIG_DIR = DATA_DIR / "config"
print("CONFIG_DIR=", CONFIG_DIR)
FILES_DIR = DATA_DIR / "files"
print("FILES_DIR=", FILES_DIR)
PRE_DIR = DATA_DIR / "preprocessed_data"
print("PRE_DIR=", PRE_DIR)
LOGS_DIR = DATA_DIR / "logs"
print("LOGS_DIR=", LOGS_DIR)
TEMPLATES_DIR = DATA_DIR / "templates"
print("TEMPLATES_DIR=", TEMPLATES_DIR)
STATIC_DIR = DATA_DIR / "static"
print("STATIC_DIR=", STATIC_DIR)

for d in (CONFIG_DIR, FILES_DIR, PRE_DIR, LOGS_DIR):
    d.mkdir(parents=True, exist_ok=True)

DB_PATH = CONFIG_DIR / "db.json"  # {year}->{election_id}->{category}->{office}->{url, method, seats, base_sha256, last_processed, last_calc, result}
print("DB_PATH=", DB_PATH)
CATEGORIES_PATH = CONFIG_DIR / "categories.jsonl"  # ya existe
print("CATEGORIES_PATH=", CATEGORIES_PATH)
METHODS_PATH = CONFIG_DIR / "method.jsonl"        # ya existe
print("METHODS_PATH=", METHODS_PATH)