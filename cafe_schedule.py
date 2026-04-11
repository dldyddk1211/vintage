"""
cafe_schedule.py
카페 업로드 자동 스케줄 관리 — 4개 타임슬롯 (아침/점심/저녁/새벽)
+ 업로드 체크 자동 확인 스케줄
+ 자동 작업 스케줄 (수집/체크/콤보)
"""

import json
import os
import logging
from data_manager import get_path

logger = logging.getLogger(__name__)

_SCHEDULE_PATH = os.path.join(get_path("db"), "cafe_schedule.json")

# 기본 스케줄 설정
DEFAULT_SLOTS = [
    {"id": "morning",   "label": "아침",  "hour": 7,  "minute": 0, "enabled": False, "brand": "ALL", "quantity": 5},
    {"id": "afternoon", "label": "점심",  "hour": 14, "minute": 0, "enabled": False, "brand": "ALL", "quantity": 5},
    {"id": "evening",   "label": "저녁",  "hour": 20, "minute": 0, "enabled": False, "brand": "ALL", "quantity": 5},
    {"id": "night",     "label": "새벽",  "hour": 4,  "minute": 0, "enabled": False, "brand": "ALL", "quantity": 3},
]


def load_schedule() -> list:
    """스케줄 설정 로드"""
    if os.path.exists(_SCHEDULE_PATH):
        try:
            with open(_SCHEDULE_PATH, "r", encoding="utf-8") as f:
                slots = json.load(f)
                if isinstance(slots, list) and len(slots) == 4:
                    return slots
        except Exception:
            pass
    return [dict(s) for s in DEFAULT_SLOTS]


def save_schedule(slots: list):
    """스케줄 설정 저장"""
    os.makedirs(os.path.dirname(_SCHEDULE_PATH), exist_ok=True)
    with open(_SCHEDULE_PATH, "w", encoding="utf-8") as f:
        json.dump(slots, f, ensure_ascii=False, indent=2)
    logger.info(f"📅 스케줄 설정 저장됨: {_SCHEDULE_PATH}")


# ── 빈티지 카페 업로드 자동 스케줄 ──────────────────

_VT_SCHEDULE_PATH = os.path.join(get_path("db"), "vt_cafe_schedule.json")

VT_DEFAULT_SLOTS = [
    {"id": "vt_morning",   "label": "아침",  "hour": 8,  "minute": 0, "enabled": False, "brand": "ALL", "quantity": 3},
    {"id": "vt_afternoon", "label": "점심",  "hour": 13, "minute": 0, "enabled": False, "brand": "ALL", "quantity": 3},
    {"id": "vt_evening",   "label": "저녁",  "hour": 19, "minute": 0, "enabled": False, "brand": "ALL", "quantity": 3},
    {"id": "vt_night",     "label": "새벽",  "hour": 3,  "minute": 0, "enabled": False, "brand": "ALL", "quantity": 2},
]


def load_vt_schedule() -> list:
    if os.path.exists(_VT_SCHEDULE_PATH):
        try:
            with open(_VT_SCHEDULE_PATH, "r", encoding="utf-8") as f:
                slots = json.load(f)
                if isinstance(slots, list) and len(slots) == 4:
                    return slots
        except Exception:
            pass
    return [dict(s) for s in VT_DEFAULT_SLOTS]


def save_vt_schedule(slots: list):
    os.makedirs(os.path.dirname(_VT_SCHEDULE_PATH), exist_ok=True)
    with open(_VT_SCHEDULE_PATH, "w", encoding="utf-8") as f:
        json.dump(slots, f, ensure_ascii=False, indent=2)
    logger.info(f"📅 빈티지 스케줄 저장됨")


# ── 업로드 체크 자동 확인 스케줄 ──────────────────

_CHECK_SCHEDULE_PATH = os.path.join(get_path("db"), "check_schedule.json")

DEFAULT_CHECK_SCHEDULE = {
    "enabled": False,
    "hour": 9,
    "minute": 0,
}


def load_check_schedule() -> dict:
    """업로드 체크 스케줄 설정 로드"""
    if os.path.exists(_CHECK_SCHEDULE_PATH):
        try:
            with open(_CHECK_SCHEDULE_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    return data
        except Exception:
            pass
    return dict(DEFAULT_CHECK_SCHEDULE)


def save_check_schedule(data: dict):
    """업로드 체크 스케줄 설정 저장"""
    os.makedirs(os.path.dirname(_CHECK_SCHEDULE_PATH), exist_ok=True)
    with open(_CHECK_SCHEDULE_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    logger.info(f"📅 체크 스케줄 설정 저장됨: {_CHECK_SCHEDULE_PATH}")


# ── 자동 작업 스케줄 (수집 / 체크 / 콤보) ──────────

_TASK_SCHEDULE_PATH = os.path.join(get_path("db"), "task_schedule.json")

DEFAULT_TASK_SLOTS = [
    {
        "id": "task_scrape",
        "label": "자동 수집",
        "type": "scrape",
        "enabled": False,
        "hour": 8,
        "minute": 0,
        "site_id": "xebio",
        "category_id": "sale",
        "brand_code": "",
        "brand_name": "ALL",
        "keyword": "",
        "pages": "",
    },
    {
        "id": "task_check",
        "label": "자동 체크",
        "type": "check",
        "enabled": False,
        "hour": 10,
        "minute": 0,
        "site_id": "xebio",
        "category_id": "sale",
        "brand_code": "",
        "brand_name": "ALL",
        "keyword": "",
        "pages": "",
    },
    {
        "id": "task_combo",
        "label": "수집+체크",
        "type": "combo",
        "enabled": False,
        "hour": 6,
        "minute": 0,
        "site_id": "xebio",
        "category_id": "sale",
        "brand_code": "",
        "brand_name": "ALL",
        "keyword": "",
        "pages": "",
    },
]


def load_task_schedule() -> list:
    """자동 작업 스케줄 설정 로드"""
    if os.path.exists(_TASK_SCHEDULE_PATH):
        try:
            with open(_TASK_SCHEDULE_PATH, "r", encoding="utf-8") as f:
                slots = json.load(f)
                if isinstance(slots, list) and len(slots) == 3:
                    return slots
        except Exception:
            pass
    return [dict(s) for s in DEFAULT_TASK_SLOTS]


def save_task_schedule(slots: list):
    """자동 작업 스케줄 설정 저장"""
    os.makedirs(os.path.dirname(_TASK_SCHEDULE_PATH), exist_ok=True)
    with open(_TASK_SCHEDULE_PATH, "w", encoding="utf-8") as f:
        json.dump(slots, f, ensure_ascii=False, indent=2)
    logger.info(f"📅 작업 스케줄 설정 저장됨: {_TASK_SCHEDULE_PATH}")
