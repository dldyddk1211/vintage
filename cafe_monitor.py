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

from config import CAFE_ID, CAFE_URL, CAFE_MENU_ID, NAVER_COOKIE_PATH, CAFE_MY_NICKNAME
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
    """게시글 본문 전체 텍스트 가져오기"""
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

        # HTML 태그 → 줄바꿈 보존
        import re
        text = re.sub(r'<br\s*/?>', '\n', content)
        text = re.sub(r'</(p|div|li|tr)>', '\n', text)
        text = re.sub(r'<[^>]+>', '', text)
        text = re.sub(r'&nbsp;', ' ', text)
        text = re.sub(r'&amp;', '&', text)
        text = re.sub(r'&lt;', '<', text)
        text = re.sub(r'&gt;', '>', text)
        text = re.sub(r'\n{3,}', '\n\n', text).strip()

        return text

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

    # 본문 전체 가져오기
    body = fetch_article_content(article["article_id"])

    header = (
        f"📬 <b>카페 새 글</b>\n"
        f"━━━━━━━━━━━━━━━\n"
        f"📌 {article['title']}\n"
        f"✍️ {article['writer']}\n"
        f"👁 조회 {article.get('read_count', 0)} · 💬 댓글 {article.get('comment_count', 0)}\n\n"
    )
    footer = (
        f"\n\n🔗 {article['link']}\n\n"
        f"💡 <i>이 메시지에 답장하면 카페 댓글로 등록됩니다</i>"
    )

    # 텔레그램 메시지 최대 4096자 — 헤더/푸터 제외한 만큼 본문 삽입
    max_body = 4096 - len(header) - len(footer) - 20
    if body:
        if len(body) > max_body:
            body = body[:max_body] + "..."
        body_text = f"📝 <b>본문</b>\n{body}"
    else:
        body_text = ""

    msg = header + body_text + footer

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
            # 내가 쓴 글 또는 알림 제외 닉네임은 건너뜀
            writer = a.get("writer", "")
            _skip_writers = [CAFE_MY_NICKNAME, "더원구매대행"] if CAFE_MY_NICKNAME else ["더원구매대행"]
            if any(nick and nick in writer for nick in _skip_writers):
                logger.info(f"⏭️ 알림 제외 건너뜀: [{aid}] {a['title']} (by {writer})")
                continue

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


async def search_cafe_by_browser(page, keyword: str, nickname: str = "", days: int = 30, log=None) -> dict | None:
    """Playwright 브라우저로 카페 검색 — 카페 검색 URL로 이동 후 결과 파싱

    Returns:
        {"title", "writer", "write_date"} or None
    """
    import asyncio
    import re
    from urllib.parse import quote

    try:
        # 새 카페 검색 URL 형식
        import time as _time
        ts = int(_time.time() * 1000)
        search_url = f"https://cafe.naver.com/f-e/cafes/{CAFE_ID}/menus/0?q={quote(keyword)}&t={ts}"
        if log:
            log(f"      🔍 검색: {keyword}")
        await page.goto(search_url, wait_until="domcontentloaded", timeout=15000)
        await asyncio.sleep(2)

        # 검색 결과 로딩 대기 — 여러 셀렉터 시도
        result_loaded = False
        load_selectors = [
            "a[class*='article']", "a[class*='Article']",
            "div[class*='article']", "li[class*='article']",
            "div[class*='ArticleItem']",
            "div[class*='item_area']", "div[class*='list_area']",
            "ul[class*='list'] li",
            "div[class*='search_list'] li",
            "div[class*='inner_list']",
        ]
        for sel in load_selectors:
            try:
                await page.wait_for_selector(sel, timeout=2000)
                result_loaded = True
                break
            except Exception:
                continue

        # 전체 페이지 텍스트로 폴백 파싱
        page_text = await page.inner_text("body")

        # "검색 결과가 없습니다" 류 체크
        no_result_keywords = ["검색 결과가 없습니다", "게시글이 없습니다", "결과가 없습니다", "No results"]
        for nk in no_result_keywords:
            if nk in page_text:
                return None

        # 디버그: 페이지 텍스트 일부 로그 (최초 500자)
        if log:
            snippet = page_text[:500].replace("\n", " ").strip()
            log(f"      📄 페이지 텍스트(500자): {snippet}")

        # 닉네임 없이 검색한 경우 — 검색 결과에 품번이 있으면 중복으로 판단
        if not nickname:
            if keyword in page_text:
                return {
                    "title": keyword,
                    "writer": "",
                    "write_date": datetime.now().strftime("%Y-%m-%d"),
                }
            return None

        # 닉네임이 페이지에 있는지 전체 텍스트 확인
        if nickname and nickname in page_text:
            # 닉네임 주변 텍스트에서 날짜 추출
            now = datetime.now()
            # 닉네임이 등장하는 모든 위치 검사
            search_text = page_text
            start = 0
            while True:
                idx = search_text.find(nickname, start)
                if idx == -1:
                    break

                # 닉네임 주변 ±500자에서 날짜 검색
                context_start = max(0, idx - 500)
                context_end = min(len(search_text), idx + len(nickname) + 500)
                context = search_text[context_start:context_end]

                # 상대 날짜 ("오늘", "어제", "N분 전", "N시간 전", "방금", "HH:MM")
                write_date = None
                if re.search(r'오늘|방금|분\s*전|시간\s*전', context):
                    write_date = now
                elif '어제' in context:
                    write_date = now - __import__('datetime').timedelta(days=1)
                elif re.search(r'(\d+)일\s*전', context):
                    d_ago = int(re.search(r'(\d+)일\s*전', context).group(1))
                    write_date = now - __import__('datetime').timedelta(days=d_ago)
                elif re.search(r'(?<!\d)\d{1,2}:\d{2}(?!\d)', context):
                    # "13:45" 같은 시간만 표시 = 오늘
                    write_date = now

                # YYYY.MM.DD 형식
                if not write_date:
                    date_match = re.search(r'(\d{4})\.(\d{2})\.(\d{2})', context)
                    if date_match:
                        y, m, d = int(date_match.group(1)), int(date_match.group(2)), int(date_match.group(3))
                        try:
                            write_date = datetime(y, m, d)
                        except ValueError:
                            pass

                # MM.DD. 형식 (올해)
                if not write_date:
                    date_match2 = re.search(r'(\d{2})\.(\d{2})\.', context)
                    if date_match2:
                        month, day = int(date_match2.group(1)), int(date_match2.group(2))
                        try:
                            write_date = datetime(now.year, month, day)
                        except ValueError:
                            pass

                if not write_date:
                    start = idx + 1
                    continue

                diff_days = (now - write_date).days
                if 0 <= diff_days <= days:
                    # 제목 추출 — 닉네임 위쪽 줄에서 찾기
                    before_text = search_text[max(0, idx - 300):idx]
                    lines = [l.strip() for l in before_text.split("\n") if l.strip() and len(l.strip()) > 3]
                    title = lines[-1] if lines else keyword

                    return {
                        "title": title[:50],
                        "writer": nickname,
                        "write_date": write_date.strftime("%Y-%m-%d"),
                    }

                start = idx + 1

        # 닉네임 못 찾음 — 디버그 로그
        if log and nickname:
            if nickname not in page_text:
                log(f"      ❓ '{nickname}' 닉네임이 페이지에 없음")
            else:
                log(f"      ❓ '{nickname}' 있지만 {days}일 이내 날짜 매칭 실패")

        return None

    except Exception as e:
        logger.debug(f"카페 브라우저 검색 오류 ({keyword}): {e}")
        if log:
            log(f"      ⚠️ 검색 오류: {keyword} — {e}")
        return None


async def batch_check_cafe_duplicates(products: list, nickname: str, days: int = 30, log=None, save_callback=None, stop_check=None):
    """빅데이터 DB 선 체크 → 네이버 카페 브라우저 체크

    Args:
        products: 체크할 상품 리스트 (cafe_status 직접 수정됨)
        nickname: 작성자 닉네임
        days: 최근 N일
        log: 로그 콜백
        save_callback: 1건 체크 후 저장 콜백

    Returns:
        (checked, duplicates) 튜플
    """
    import asyncio
    from playwright.async_api import async_playwright

    total = len(products)
    checked = 0
    duplicates = 0

    # ── 1단계: 빅데이터 DB 선 체크 ──
    try:
        from product_db import bulk_check_cafe_status, update_cafe_status
        codes = [p.get("product_code", "") for p in products if p.get("product_code")]
        db_statuses = bulk_check_cafe_status(codes)

        db_hits = 0
        remaining = []
        for prod in products:
            code = prod.get("product_code", "")
            if code and code in db_statuses:
                status = db_statuses[code]
                if status in ("업로드완료", "중복"):
                    prod["cafe_status"] = "중복"
                    checked += 1
                    duplicates += 1
                    db_hits += 1
                    if log:
                        log(f"   🗄️ [{checked}/{total}] {code} — DB에서 중복 확인")
                    if save_callback:
                        try:
                            save_callback()
                        except Exception:
                            pass
                    continue
            remaining.append(prod)

        if db_hits > 0 and log:
            log(f"   📊 빅데이터 DB 체크: {db_hits}개 중복 발견, {len(remaining)}개 카페 검색 필요")
    except Exception as e:
        remaining = list(products)
        if log:
            log(f"   ⚠️ 빅데이터 DB 체크 실패: {e} — 전체 카페 검색 진행")

    if not remaining:
        return checked, duplicates

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,
            slow_mo=200,
            args=["--no-sandbox", "--window-size=1280,900"]
        )
        context = await browser.new_context(
            viewport={"width": 1280, "height": 900},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        )

        # 쿠키 로드
        if os.path.exists(NAVER_COOKIE_PATH):
            try:
                import json as _json
                with open(NAVER_COOKIE_PATH, "r", encoding="utf-8") as f:
                    cookies = _json.load(f)
                naver_cookies = []
                for c in cookies:
                    cookie = {
                        "name": c["name"],
                        "value": c["value"],
                        "domain": c.get("domain", ".naver.com"),
                        "path": c.get("path", "/"),
                    }
                    naver_cookies.append(cookie)
                await context.add_cookies(naver_cookies)
                if log:
                    log("   🍪 네이버 쿠키 로드 완료")
            except Exception as e:
                if log:
                    log(f"   ⚠️ 쿠키 로드 실패: {e}")

        page = await context.new_page()

        try:
            for prod in remaining:
                # 중지 요청 확인
                if stop_check and stop_check():
                    if log:
                        log(f"   ⏹ 중지 요청 — {checked}/{total}에서 중단")
                    break

                code = prod.get("product_code", "")
                if not code:
                    checked += 1
                    if log:
                        log(f"   ⚪ [{checked}/{total}] (품번 없음 — 건너뜀)")
                    continue

                result = await search_cafe_by_browser(page, code, nickname, days, log)
                checked += 1

                if result:
                    prod["cafe_status"] = "중복"
                    prod["cafe_dup_date"] = result["write_date"]
                    duplicates += 1
                    if log:
                        log(f"   🔴 [{checked}/{total}] {code} — {result['write_date']} 등록 ({result['title'][:40]})")
                    # 빅데이터 DB에도 상태 저장
                    try:
                        update_cafe_status(code, "중복")
                    except Exception:
                        pass
                else:
                    if log:
                        log(f"   🟢 [{checked}/{total}] {code}")

                # 1개 체크할 때마다 저장
                if save_callback:
                    try:
                        save_callback()
                    except Exception:
                        pass

                await asyncio.sleep(1)

        finally:
            await browser.close()

    return checked, duplicates
