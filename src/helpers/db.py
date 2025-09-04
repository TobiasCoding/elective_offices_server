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