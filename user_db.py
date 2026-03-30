"""
user_db.py
고객 회원 관리 (SQLite)
"""

import os
import sqlite3
import logging
from datetime import datetime
from werkzeug.security import generate_password_hash, check_password_hash
from data_manager import get_path

logger = logging.getLogger(__name__)

_DB_PATH = os.path.join(get_path("db"), "users.db")


def _conn() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(_DB_PATH), exist_ok=True)
    conn = sqlite3.connect(_DB_PATH, check_same_thread=False, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    conn = _conn()
    try:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL UNIQUE,
                password_hash TEXT NOT NULL,
                name TEXT DEFAULT '',
                phone TEXT DEFAULT '',
                role TEXT NOT NULL DEFAULT 'customer',
                created_at TEXT DEFAULT (datetime('now','localtime'))
            );
            CREATE INDEX IF NOT EXISTS idx_username ON users(username);
        """)
        conn.commit()
        logger.info(f"회원 DB 초기화 완료: {_DB_PATH}")
    finally:
        conn.close()


def create_user(username: str, password: str, name: str = "", phone: str = "") -> bool:
    conn = _conn()
    try:
        conn.execute(
            "INSERT INTO users (username, password_hash, name, phone) VALUES (?,?,?,?)",
            (username, generate_password_hash(password), name, phone),
        )
        conn.commit()
        logger.info(f"회원가입: {username}")
        return True
    except sqlite3.IntegrityError:
        return False
    finally:
        conn.close()


def get_user(username: str):
    conn = _conn()
    try:
        return conn.execute("SELECT * FROM users WHERE username = ?", (username,)).fetchone()
    finally:
        conn.close()


def check_password(user_row, password: str) -> bool:
    return check_password_hash(user_row["password_hash"], password)


def username_exists(username: str) -> bool:
    conn = _conn()
    try:
        row = conn.execute("SELECT 1 FROM users WHERE username = ?", (username,)).fetchone()
        return row is not None
    finally:
        conn.close()
