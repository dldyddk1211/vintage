"""
notifier.py
텔레그램 알림 모듈
"""

import logging
import os
import requests
from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

logger = logging.getLogger(__name__)

# 런타임 설정 (대시보드에서 변경 가능)
_tg_config = {
    "bot_token": TELEGRAM_BOT_TOKEN,
    "chat_id": TELEGRAM_CHAT_ID,
}


def set_telegram_config(bot_token: str = None, chat_id: str = None):
    """텔레그램 설정 변경 + .env 파일에 영구 저장"""
    if bot_token is not None:
        _tg_config["bot_token"] = bot_token.strip()
    if chat_id is not None:
        _tg_config["chat_id"] = chat_id.strip()
    # .env 파일에 영구 저장
    _save_to_env("TELEGRAM_BOT_TOKEN", _tg_config["bot_token"])
    _save_to_env("TELEGRAM_CHAT_ID", _tg_config["chat_id"])


def _save_to_env(key: str, value: str):
    """키=값을 .env 파일에 저장 (기존 키면 업데이트, 없으면 추가)"""
    env_path = os.path.join(os.path.dirname(__file__), ".env")
    lines = []
    found = False
    if os.path.exists(env_path):
        with open(env_path, encoding="utf-8") as f:
            for line in f:
                if line.strip().startswith(f"{key}="):
                    lines.append(f"{key}={value}\n")
                    found = True
                else:
                    lines.append(line)
    if not found:
        lines.append(f"{key}={value}\n")
    with open(env_path, "w", encoding="utf-8") as f:
        f.writelines(lines)


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


def notify_upload_success(product_name: str, idx: int, total: int, post_url: str = ""):
    """업로드 성공 알림"""
    msg = (
        f"✅ <b>카페 업로드 완료</b>\n"
        f"📦 {product_name}\n"
        f"📊 진행: {idx}/{total}"
    )
    if post_url:
        msg += f"\n🔗 {post_url}"
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


# ── AI API 상태 모니터링 ──────────────────

_ai_api_last_ok = True  # 마지막 체크 시 정상 여부 (중복 알림 방지)

def check_ai_api_and_notify():
    """AI API 상태를 확인하고 문제 시 텔레그램 알림 전송"""
    global _ai_api_last_ok

    if not is_configured():
        return

    try:
        from post_generator import verify_ai_key, get_ai_config
        config = get_ai_config()
        provider = config.get("provider", "none")

        if provider == "none":
            return  # AI 미설정 시 체크 안 함

        result = verify_ai_key()

        if not result.get("ok"):
            # 오류 발생 — 이전에 정상이었으면 알림 전송
            if _ai_api_last_ok:
                msg = (
                    f"🚨 <b>AI API 오류 감지</b>\n"
                    f"🔧 Provider: {result.get('provider', '?')}\n"
                    f"💬 {result.get('message', '알 수 없는 오류')}\n\n"
                    f"⏰ 5분마다 재확인 중..."
                )
                send_telegram(msg)
                logger.warning(f"AI API 오류 감지: {result.get('message')}")
            else:
                # 계속 오류 상태 — 30분마다만 재알림
                import time
                if not hasattr(check_ai_api_and_notify, '_last_repeat'):
                    check_ai_api_and_notify._last_repeat = 0
                now = time.time()
                if now - check_ai_api_and_notify._last_repeat > 1800:  # 30분
                    send_telegram(
                        f"⚠️ <b>AI API 여전히 오류</b>\n"
                        f"🔧 {result.get('provider')}: {result.get('message', '')[:100]}"
                    )
                    check_ai_api_and_notify._last_repeat = now

            _ai_api_last_ok = False
        else:
            # 정상 복구 — 이전에 오류였으면 복구 알림
            if not _ai_api_last_ok:
                send_telegram(
                    f"✅ <b>AI API 정상 복구</b>\n"
                    f"🔧 {result.get('provider')}: 정상 작동 중"
                )
                logger.info(f"AI API 정상 복구: {result.get('provider')}")
            _ai_api_last_ok = True

    except Exception as e:
        logger.warning(f"AI API 체크 오류: {e}")
