"""
cafe_monitor.py
네이버 카페 새 글 감지 → 텔레그램 알림 전송

흐름:
1. 저장된 쿠키로 카페 게시글 목록을 주기적으로 조회
2. 새 글 감지 시 텔레그램으로 제목 + 내용 미리보기 + 링크 전송
3. 텔레그램 message_id ↔ 카페 article_id 매핑 저장 (댓글 연동용)
"""

import json
import os
import time
import logging
import threading
import requests
from datetime import datetime

from config import CAFE_ID, CAFE_URL, CAFE_MENU_ID, NAVER_COOKIE_PATH
from notifier import send_telegram, is_configured

logger = logging.getLogger(__name__)

# ── 상태 ────────────────────────────────────
_monitor_thread = None
_running = False
_check_interval = 180  # 3분 간격

# 게시글 ID ↔ 텔레그램 메시지 ID 매핑
_MAPPING_PATH = os.path.join(os.path.dirname(__file__), "article_tg_map.json")


def _load_mapping() -> dict:
    """매핑 파일 로드"""
    if os.path.exists(_MAPPING_PATH):
        try:
            with open(_MAPPING_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def _save_mapping(mapping: dict):
    """매핑 파일 저장"""
    with open(_MAPPING_PATH, "w", encoding="utf-8") as f:
        json.dump(mapping, f, ensure_ascii=False, indent=2)


def _get_naver_session() -> requests.Session:
    """저장된 쿠키로 네이버 세션 생성"""
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": CAFE_URL,
    })

    if not os.path.exists(NAVER_COOKIE_PATH):
        logger.warning("네이버 쿠키 없음 — 카페 모니터 불가")
        return None

    try:
        with open(NAVER_COOKIE_PATH, "r", encoding="utf-8") as f:
            cookies = json.load(f)
        for c in cookies:
            session.cookies.set(
                c["name"], c["value"],
                domain=c.get("domain", ".naver.com"),
                path=c.get("path", "/"),
            )
        return session
    except Exception as e:
        logger.warning(f"쿠키 로드 실패: {e}")
        return None


def fetch_recent_articles(limit=20) -> list:
    """카페 최근 게시글 목록 조회 (네이버 API)"""
    session = _get_naver_session()
    if not session:
        return []

    try:
        # 네이버 카페 API — 게시글 목록
        api_url = (
            f"https://apis.naver.com/cafe-web/cafe2/ArticleListV2.json"
            f"?search.clubid={CAFE_ID}"
            f"&search.menuid={CAFE_MENU_ID}"
            f"&search.perPage={limit}"
            f"&search.page=1"
        )
        resp = session.get(api_url, timeout=10)
        if resp.status_code != 200:
            logger.warning(f"카페 API 실패: {resp.status_code}")
            return []

        data = resp.json()
        articles = []
        article_list = data.get("message", {}).get("result", {}).get("articleList", [])

        for a in article_list:
            articles.append({
                "article_id": str(a.get("articleId", "")),
                "title": a.get("subject", ""),
                "writer": a.get("nickName", a.get("writerNickname", "")),
                "write_date": a.get("writeDateTimestamp", ""),
                "read_count": a.get("readCount", 0),
                "comment_count": a.get("commentCount", 0),
                "link": f"{CAFE_URL}/{a.get('articleId', '')}",
            })

        return articles

    except Exception as e:
        logger.warning(f"카페 게시글 조회 실패: {e}")
        return []


def fetch_article_content(article_id: str) -> str:
    """게시글 본문 미리보기 (200자)"""
    session = _get_naver_session()
    if not session:
        return ""

    try:
        api_url = (
            f"https://apis.naver.com/cafe-web/cafe-articleapi/v2.1/cafes/{CAFE_ID}/articles/{article_id}"
        )
        resp = session.get(api_url, timeout=10)
        if resp.status_code != 200:
            return ""

        data = resp.json()
        article = data.get("result", {}).get("article", {})
        # contentHtml에서 텍스트 추출
        content = article.get("contentHtml", "") or article.get("content", "")

        # HTML 태그 제거
        import re
        text = re.sub(r'<[^>]+>', '', content)
        text = re.sub(r'&nbsp;', ' ', text)
        text = re.sub(r'\s+', ' ', text).strip()

        return text[:200]

    except Exception as e:
        logger.debug(f"게시글 내용 조회 실패: {e}")
        return ""


def _send_new_article_alert(article: dict) -> int:
    """새 글 알림을 텔레그램으로 전송, message_id 반환"""
    from notifier import _tg_config

    token = _tg_config["bot_token"]
    chat_id = _tg_config["chat_id"]
    if not token or not chat_id:
        return 0

    # 본문 미리보기 가져오기
    preview = fetch_article_content(article["article_id"])
    preview_text = f"\n\n📝 {preview}..." if preview else ""

    msg = (
        f"📬 <b>카페 새 글</b>\n"
        f"━━━━━━━━━━━━━━━\n"
        f"📌 {article['title']}\n"
        f"✍️ {article['writer']}\n"
        f"👁 조회 {article.get('read_count', 0)} · 💬 댓글 {article.get('comment_count', 0)}"
        f"{preview_text}\n\n"
        f"🔗 {article['link']}\n\n"
        f"💡 <i>이 메시지에 답장하면 카페 댓글로 등록됩니다</i>"
    )

    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        resp = requests.post(url, json={
            "chat_id": chat_id,
            "text": msg,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }, timeout=10)

        if resp.status_code == 200:
            result = resp.json()
            message_id = result.get("result", {}).get("message_id", 0)
            logger.info(f"📬 새 글 알림 전송: {article['title']} (tg_msg={message_id})")
            return message_id
        else:
            logger.warning(f"텔레그램 전송 실패: {resp.status_code}")
            return 0
    except Exception as e:
        logger.warning(f"텔레그램 전송 오류: {e}")
        return 0


def _check_new_articles(known_ids: set, log_callback=None) -> set:
    """새 글 체크 & 알림 전송, 업데이트된 known_ids 반환"""
    articles = fetch_recent_articles(20)
    if not articles:
        return known_ids

    mapping = _load_mapping()
    new_ids = set()

    for a in articles:
        aid = a["article_id"]
        new_ids.add(aid)

        if aid not in known_ids and known_ids:  # 첫 실행 시에는 알림 안 보냄
            # 새 글 발견!
            logger.info(f"🆕 새 글 감지: [{aid}] {a['title']}")
            if log_callback:
                log_callback(f"📬 새 글 감지: {a['title']} (by {a['writer']})")

            if is_configured():
                tg_msg_id = _send_new_article_alert(a)
                if tg_msg_id:
                    # 매핑 저장 (텔레그램 메시지 ID → 카페 게시글 ID)
                    mapping[str(tg_msg_id)] = {
                        "article_id": aid,
                        "title": a["title"],
                        "link": a["link"],
                        "alerted_at": datetime.now().isoformat(),
                    }

    # 알려진 ID 업데이트
    known_ids = known_ids | new_ids
    # 최대 500개 유지
    if len(known_ids) > 500:
        known_ids = new_ids

    _save_mapping(mapping)
    return known_ids


def _monitor_loop(log_callback=None):
    """모니터링 루프 (백그라운드 스레드)"""
    global _running
    logger.info("🔔 카페 모니터 시작")
    if log_callback:
        log_callback("🔔 카페 모니터 시작 — 새 글 감지 중...")

    # 첫 실행: 현재 게시글 ID 수집 (기존 글은 알림 안 보냄)
    known_ids = set()
    articles = fetch_recent_articles(20)
    for a in articles:
        known_ids.add(a["article_id"])
    logger.info(f"📋 기존 게시글 {len(known_ids)}개 로드")

    while _running:
        try:
            known_ids = _check_new_articles(known_ids, log_callback)
        except Exception as e:
            logger.warning(f"모니터 체크 오류: {e}")

        # 대기 (1초 단위로 _running 체크)
        for _ in range(_check_interval):
            if not _running:
                break
            time.sleep(1)

    logger.info("🔕 카페 모니터 종료")
    if log_callback:
        log_callback("🔕 카페 모니터 종료")


def start_monitor(log_callback=None, interval=180):
    """카페 모니터 시작"""
    global _monitor_thread, _running, _check_interval
    if _running:
        return False

    _check_interval = interval
    _running = True
    _monitor_thread = threading.Thread(
        target=_monitor_loop, args=(log_callback,), daemon=True
    )
    _monitor_thread.start()
    return True


def stop_monitor():
    """카페 모니터 종료"""
    global _running
    _running = False


def is_monitoring() -> bool:
    """모니터링 중 여부"""
    return _running


def get_article_mapping() -> dict:
    """텔레그램 메시지 ↔ 카페 게시글 매핑 반환"""
    return _load_mapping()
