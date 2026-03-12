"""
notifier.py
텔레그램 알림 모듈
"""

import logging
import requests
from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

logger = logging.getLogger(__name__)

# 런타임 설정 (대시보드에서 변경 가능)
_tg_config = {
    "bot_token": TELEGRAM_BOT_TOKEN,
    "chat_id": TELEGRAM_CHAT_ID,
}


def set_telegram_config(bot_token: str = None, chat_id: str = None):
    """텔레그램 설정 변경"""
    if bot_token is not None:
        _tg_config["bot_token"] = bot_token.strip()
    if chat_id is not None:
        _tg_config["chat_id"] = chat_id.strip()


def get_telegram_config() -> dict:
    """현재 텔레그램 설정 반환 (토큰 마스킹)"""
    token = _tg_config["bot_token"]
    return {
        "bot_token_set": bool(token),
        "bot_token_masked": (token[:8] + "..." + token[-4:]) if len(token) > 12 else ("설정됨" if token else ""),
        "chat_id": _tg_config["chat_id"],
    }


def is_configured() -> bool:
    """텔레그램 설정 완료 여부"""
    return bool(_tg_config["bot_token"] and _tg_config["chat_id"])


def send_telegram(message: str) -> bool:
    """텔레그램 메시지 전송"""
    token = _tg_config["bot_token"]
    chat_id = _tg_config["chat_id"]

    if not token or not chat_id:
        logger.debug("텔레그램 미설정 — 알림 건너뜀")
        return False

    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        resp = requests.post(url, json={
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "HTML",
        }, timeout=10)
        if resp.status_code == 200:
            logger.info(f"텔레그램 전송 완료")
            return True
        else:
            logger.warning(f"텔레그램 전송 실패: {resp.status_code} {resp.text[:100]}")
            return False
    except Exception as e:
        logger.warning(f"텔레그램 전송 오류: {e}")
        return False


def notify_upload_success(product_name: str, idx: int, total: int):
    """업로드 성공 알림"""
    msg = (
        f"✅ <b>카페 업로드 완료</b>\n"
        f"📦 {product_name}\n"
        f"📊 진행: {idx}/{total}"
    )
    send_telegram(msg)


def notify_upload_waiting(product_name: str, idx: int, total: int, delay_min: int):
    """다음 업로드 대기 알림"""
    msg = (
        f"⏳ <b>다음 업로드 대기중</b>\n"
        f"📦 다음: {product_name}\n"
        f"⏱ {delay_min}분 후 업로드\n"
        f"📊 진행: {idx}/{total}"
    )
    send_telegram(msg)


def notify_upload_complete(success: int, total: int):
    """전체 업로드 완료 알림"""
    msg = (
        f"🎉 <b>전체 업로드 완료!</b>\n"
        f"📊 결과: {success}/{total}개 성공"
    )
    send_telegram(msg)


def notify_upload_error(product_name: str, error: str):
    """업로드 오류 알림"""
    msg = (
        f"❌ <b>업로드 오류</b>\n"
        f"📦 {product_name}\n"
        f"💥 {error[:200]}"
    )
    send_telegram(msg)
