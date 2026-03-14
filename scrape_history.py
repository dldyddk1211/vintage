"""
scrape_history.py
수집 이력 관리 — 언제, 어떤 사이트/카테고리에서 몇 개 수집했는지 기록
"""

import json
import os
import logging
from datetime import datetime
from data_manager import get_path
from site_config import get_site, get_category

logger = logging.getLogger(__name__)

_HISTORY_PATH = os.path.join(get_path("db"), "scrape_history.json")


def _load() -> list:
    """이력 파일 로드"""
    if os.path.exists(_HISTORY_PATH):
        try:
            with open(_HISTORY_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return []


def _save(history: list):
    """이력 파일 저장"""
    os.makedirs(os.path.dirname(_HISTORY_PATH), exist_ok=True)
    with open(_HISTORY_PATH, "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False, indent=2)


def add_history(site_id: str, category_id: str, product_count: int,
                uploaded_count: int = 0, keyword: str = "", brand: str = ""):
    """수집 이력 추가"""
    site = get_site(site_id)
    cat = get_category(site_id, category_id)

    record = {
        "date": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "site_id": site_id,
        "site_name": site["name"] if site else site_id,
        "category_id": category_id,
        "category_name": cat["name"] if cat else category_id,
        "keyword": keyword or "",
        "brand": brand or "",
        "product_count": product_count,
        "uploaded_count": uploaded_count,
        "status": "수집완료",
    }

    history = _load()
    history.insert(0, record)  # 최신순

    # 최대 200건 보관
    if len(history) > 200:
        history = history[:200]

    _save(history)
    logger.info(f"📝 수집 이력 저장: {record['site_name']} › {record['category_name']} — {product_count}개")
    return record


def update_upload_count(date: str, site_id: str, category_id: str,
                        uploaded_count: int):
    """업로드 완료 수 업데이트"""
    history = _load()
    for h in history:
        if (h["date"] == date and h["site_id"] == site_id
                and h["category_id"] == category_id):
            h["uploaded_count"] = uploaded_count
            h["status"] = "업로드완료" if uploaded_count > 0 else h["status"]
            break
    _save(history)


def get_history(limit: int = 50) -> list:
    """이력 목록 반환 (최신순)"""
    history = _load()
    return history[:limit]
