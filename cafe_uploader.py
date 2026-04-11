"""
cafe_uploader.py
네이버 카페 자동 업로드 (쿠키 기반 로그인)

흐름:
1. 첫 실행 시 브라우저 열림 → 사용자가 수동 로그인 → 쿠키 저장
2. 이후 저장된 쿠키로 자동 로그인 (캡차 없음)
3. 쿠키 만료 시 다시 수동 로그인 요청
"""

import asyncio
import json
import os
import logging
import random
import requests
import tempfile
from datetime import datetime
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout
from notifier import notify_upload_success, notify_upload_waiting, notify_upload_complete, notify_upload_error

# ── 업로드 중지 플래그 ──
_upload_stop_requested = False

# ── 마지막 업로드 실패 이유 (디버깅용) ──
_last_upload_fail_reason = ""

def _set_fail_reason(reason: str):
    global _last_upload_fail_reason
    _last_upload_fail_reason = reason

def request_upload_stop():
    """업로드 중지 요청"""
    global _upload_stop_requested
    _upload_stop_requested = True

def reset_upload_stop():
    """업로드 중지 플래그 초기화"""
    global _upload_stop_requested
    _upload_stop_requested = False

def is_upload_stop_requested():
    """업로드 중지 요청 여부"""
    return _upload_stop_requested

from config import (
    CAFE_URL, CAFE_ID, CAFE_MENU_NAME, CAFE_MENU_ID,
    NAVER_COOKIE_PATH, NAVER_LOGIN_TIMEOUT,
)
from exchange import calc_buying_price, format_price

logger = logging.getLogger(__name__)


# =============================================
# 쿠키 관리
# =============================================

def save_cookies(cookies: list):
    """쿠키를 파일에 저장"""
    with open(NAVER_COOKIE_PATH, "w", encoding="utf-8") as f:
        json.dump(cookies, f, ensure_ascii=False, indent=2)
    logger.info(f"✅ 쿠키 저장 완료: {NAVER_COOKIE_PATH} ({len(cookies)}개)")


def load_cookies(cookie_path: str = None) -> list:
    """저장된 쿠키 불러오기"""
    path = cookie_path or NAVER_COOKIE_PATH
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            cookies = json.load(f)
        logger.info(f"✅ 쿠키 로드 완료: {path} ({len(cookies)}개)")
        return cookies
    except Exception as e:
        logger.warning(f"⚠️ 쿠키 로드 실패 ({path}): {e}")
        return []


def delete_cookies():
    """저장된 쿠키 삭제"""
    if os.path.exists(NAVER_COOKIE_PATH):
        os.remove(NAVER_COOKIE_PATH)
        logger.info("🗑️ 쿠키 삭제 완료")


def has_saved_cookies() -> bool:
    """쿠키 파일 존재 여부"""
    return os.path.exists(NAVER_COOKIE_PATH)


# =============================================
# 네이버 수동 로그인 (쿠키 저장)
# =============================================

async def naver_manual_login(status_callback=None):
    """
    브라우저를 열어 사용자가 직접 네이버 로그인
    로그인 완료 후 쿠키를 저장

    Returns:
        bool: 로그인 성공 여부
    """
    def log(msg):
        logger.info(msg)
        if status_callback:
            status_callback(msg)

    log("🔐 네이버 로그인 브라우저를 엽니다...")
    log(f"   ⏱️ {NAVER_LOGIN_TIMEOUT}초 안에 로그인을 완료해주세요")

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,
            args=["--no-sandbox", "--window-size=500,700"]
        )
        context = await browser.new_context(
            viewport={"width": 500, "height": 700},
            locale="ko-KR",
        )
        page = await context.new_page()

        try:
            # 네이버 로그인 페이지 이동
            await page.goto(
                "https://nid.naver.com/nidlogin.login?url=https://cafe.naver.com/",
                wait_until="domcontentloaded",
                timeout=20000
            )
            log("🌐 네이버 로그인 페이지가 열렸습니다")
            log("   👉 브라우저에서 직접 로그인해주세요!")

            # 로그인 완료 대기 (URL이 cafe.naver.com으로 바뀔 때까지)
            elapsed = 0
            while elapsed < NAVER_LOGIN_TIMEOUT:
                await asyncio.sleep(2)
                elapsed += 2

                current_url = page.url
                # 로그인 성공 판단: nidlogin 페이지를 벗어남
                if "nidlogin" not in current_url and "nid.naver.com" not in current_url:
                    log("✅ 로그인 감지! 쿠키를 저장합니다...")

                    # 쿠키 저장
                    cookies = await context.cookies()
                    save_cookies(cookies)
                    log(f"✅ 쿠키 저장 완료 ({len(cookies)}개)")
                    return True

                # 30초마다 안내 메시지
                if elapsed % 30 == 0 and elapsed < NAVER_LOGIN_TIMEOUT:
                    remaining = NAVER_LOGIN_TIMEOUT - elapsed
                    log(f"   ⏱️ 남은 시간: {remaining}초")

            log("❌ 로그인 시간 초과")
            return False

        except Exception as e:
            log(f"❌ 로그인 오류: {e}")
            return False
        finally:
            await browser.close()


async def naver_manual_login_with_cookie_path(cookie_path: str, status_callback=None, naver_id: str = "", password: str = ""):
    """특정 쿠키 경로로 네이버 로그인 (저장된 계정 있으면 자동 입력)"""
    def log(msg):
        logger.info(msg)
        if status_callback:
            status_callback(msg)

    log(f"🔐 네이버 로그인 브라우저를 엽니다... (쿠키: {cookie_path})")

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,
            args=["--no-sandbox", "--window-size=500,700"]
        )
        context = await browser.new_context(
            viewport={"width": 500, "height": 700},
            locale="ko-KR",
        )
        page = await context.new_page()

        try:
            await page.goto(
                "https://nid.naver.com/nidlogin.login?url=https://cafe.naver.com/",
                wait_until="domcontentloaded",
                timeout=20000
            )
            log("🌐 네이버 로그인 페이지가 열렸습니다")

            # 저장된 아이디/비밀번호가 있으면 자동 입력
            if naver_id and password:
                log(f"   🔑 저장된 계정으로 자동 입력 중: {naver_id}")
                await asyncio.sleep(1)
                try:
                    # 아이디 입력
                    id_input = page.locator("#id")
                    await id_input.click()
                    await asyncio.sleep(0.3)
                    await id_input.fill("")
                    await asyncio.sleep(0.2)
                    # clipboard 방식으로 입력 (네이버 봇 감지 우회)
                    await page.evaluate(f"""() => {{
                        const el = document.getElementById('id');
                        if (el) {{ el.value = '{naver_id}'; el.dispatchEvent(new Event('input', {{bubbles:true}})); }}
                    }}""")
                    await asyncio.sleep(0.5)

                    # 비밀번호 입력
                    pw_input = page.locator("#pw")
                    await pw_input.click()
                    await asyncio.sleep(0.3)
                    await page.evaluate(f"""() => {{
                        const el = document.getElementById('pw');
                        if (el) {{ el.value = '{password}'; el.dispatchEvent(new Event('input', {{bubbles:true}})); }}
                    }}""")
                    await asyncio.sleep(0.5)

                    # 로그인 버튼 클릭
                    login_btn = page.locator("#log\\.login, .btn_login, button[type='submit']").first
                    await login_btn.click()
                    log("   ✅ 자동 입력 완료 — 로그인 버튼 클릭됨")
                    log("   ⏳ 캡차가 뜨면 직접 해결해주세요")
                except Exception as e:
                    log(f"   ⚠️ 자동 입력 실패: {e} — 직접 로그인해주세요")
            else:
                log("   👉 브라우저에서 직접 로그인해주세요!")

            log(f"   ⏱️ {NAVER_LOGIN_TIMEOUT}초 안에 로그인을 완료해주세요")

            elapsed = 0
            while elapsed < NAVER_LOGIN_TIMEOUT:
                await asyncio.sleep(2)
                elapsed += 2

                current_url = page.url
                if "nidlogin" not in current_url and "nid.naver.com" not in current_url:
                    log("✅ 로그인 감지! 쿠키를 저장합니다...")
                    cookies = await context.cookies()
                    with open(cookie_path, "w", encoding="utf-8") as f:
                        json.dump(cookies, f, ensure_ascii=False, indent=2)
                    log(f"✅ 쿠키 저장 완료: {cookie_path} ({len(cookies)}개)")
                    return True

                if elapsed % 30 == 0 and elapsed < NAVER_LOGIN_TIMEOUT:
                    remaining = NAVER_LOGIN_TIMEOUT - elapsed
                    log(f"   ⏱️ 남은 시간: {remaining}초")

            log("❌ 로그인 시간 초과")
            return False

        except Exception as e:
            log(f"❌ 로그인 오류: {e}")
            return False
        finally:
            await browser.close()


# =============================================
# 쿠키 유효성 검증
# =============================================

async def verify_login(context) -> bool:
    """
    저장된 쿠키로 로그인 상태 확인

    Returns:
        bool: 로그인 유효 여부
    """
    page = await context.new_page()
    try:
        await page.goto(
            f"https://cafe.naver.com/f-e/cafes/{CAFE_ID}",
            wait_until="domcontentloaded",
            timeout=15000
        )
        await asyncio.sleep(2)

        # 로그인 상태 확인: 프로필 영역 또는 로그인 버튼 체크
        current_url = page.url
        content = await page.content()

        # 로그인 안 된 경우 로그인 페이지로 리다이렉트되거나 로그인 버튼 표시
        if "nidlogin" in current_url or "login" in current_url:
            return False

        # 로그인된 상태 확인
        if "LogoutButton" in content or "my_info" in content or "gnb_my" in content:
            return True

        # 카페 메인이 정상 로드되면 로그인 상태로 판단
        return "cafe.naver.com" in current_url

    except Exception as e:
        logger.warning(f"로그인 검증 오류: {e}")
        return False
    finally:
        await page.close()


# =============================================
# 메인 업로드 함수
# =============================================

async def _try_find_valid_cookies(log, primary_cookie_path: str = None):
    """
    저장된 계정 쿠키를 순차적으로 확인하여 유효한 쿠키를 찾는다.
    활성 계정 → 계정1 → 계정2 → 계정3 순서로 시도.

    Returns:
        (cookies, cookie_path) 또는 (None, None)
    """
    import os

    # 계정 정보 로드
    accounts_db = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "db", "naver_accounts.json"
    )
    accounts_data = {}
    if os.path.exists(accounts_db):
        try:
            with open(accounts_db, "r", encoding="utf-8") as f:
                accounts_data = json.load(f)
        except Exception:
            pass

    active_slot = accounts_data.get("active", 1)

    # 시도 순서: 활성 슬롯 우선, 나머지 슬롯
    slots_to_try = [active_slot]
    for s in [1, 2, 3]:
        if s not in slots_to_try:
            slots_to_try.append(s)

    for slot in slots_to_try:
        cookie_file = "naver_cookies.json" if slot == 1 else f"naver_cookies_{slot}.json"
        if not os.path.exists(cookie_file):
            continue

        cookies = load_cookies(cookie_file)
        if not cookies:
            continue

        acc_info = accounts_data.get("accounts", {}).get(str(slot), {})
        acc_id = acc_info.get("naver_id", f"슬롯{slot}")
        log(f"   🔍 계정 {slot} ({acc_id}) 쿠키 유효성 검증 중...")

        # 쿠키 유효성 검증
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True, args=["--no-sandbox"])
            context = await browser.new_context()
            await context.add_cookies(cookies)
            is_valid = await verify_login(context)
            await browser.close()

        if is_valid:
            log(f"   ✅ 계정 {slot} ({acc_id}) 쿠키 유효!")
            # 활성 계정이 아닌 경우, 활성 계정 전환
            if slot != active_slot:
                log(f"   🔄 활성 계정을 {slot}번으로 전환합니다")
                accounts_data["active"] = slot
                try:
                    os.makedirs(os.path.dirname(accounts_db), exist_ok=True)
                    with open(accounts_db, "w", encoding="utf-8") as f:
                        json.dump(accounts_data, f, ensure_ascii=False, indent=2)
                    # 기본 쿠키 파일에도 복사
                    if slot != 1:
                        import shutil
                        shutil.copy2(cookie_file, "naver_cookies.json")
                except Exception:
                    pass
            return cookies, cookie_file
        else:
            log(f"   ❌ 계정 {slot} ({acc_id}) 쿠키 만료됨")

    return None, None


async def upload_products(products: list, status_callback=None, max_upload=None, delay_min=8, delay_max=13, on_single_success=None, cookie_path: str = None):
    """
    상품 리스트를 네이버 카페에 업로드

    Args:
        products       : 스크래퍼에서 받은 상품 딕셔너리 리스트
        status_callback: 진행상황 콜백
        max_upload     : 최대 업로드 개수 (None = 전체)
        cookie_path    : 사용할 쿠키 파일 경로 (None = 기본)

    Returns:
        int: 업로드 성공 개수
    """
    def log(msg):
        logger.info(msg)
        if status_callback:
            status_callback(msg)

    # 쿠키 존재 확인
    cookies = load_cookies(cookie_path)
    if not cookies:
        log("⚠️ 지정된 쿠키가 없습니다. 다른 계정 쿠키를 확인합니다...")
        cookies, cookie_path = await _try_find_valid_cookies(log, cookie_path)
        if not cookies:
            log("❌ 유효한 쿠키가 없습니다. '네이버 로그인' 버튼을 눌러주세요")
            return 0

    success_count = 0
    uploaded_codes_session = set()  # 이번 세션에서 업로드 완료된 품번 (즉시 중복 차단용)
    upload_list = products[:max_upload] if max_upload else products

    # ── 업로드 전 품번 리스트 검증 ──
    log(f"📋 업로드 대상 품번 리스트 ({len(upload_list)}개):")
    code_count = {}
    for idx, p_item in enumerate(upload_list, 1):
        code = p_item.get("product_code", "")
        brand = p_item.get("brand_ko") or p_item.get("brand", "")
        name_short = (p_item.get("name_ko") or p_item.get("name", ""))[:25]
        log(f"   {idx}. [{code}] {brand} — {name_short}")
        if code:
            code_count[code] = code_count.get(code, 0) + 1

    # 중복 품번 검출 및 제거 (항상 실행)
    seen_codes = set()
    deduped = []
    for p_item in upload_list:
        code = p_item.get("product_code", "")
        if code and code in seen_codes:
            log(f"   ⚠️ 중복 제거: {code}")
            continue
        if code:
            seen_codes.add(code)
        deduped.append(p_item)
    if len(deduped) < len(upload_list):
        log(f"   🚨 중복 품번 제거: {len(upload_list)}개 → {len(deduped)}개")
    else:
        log(f"   ✅ 중복 품번 없음 — {len(upload_list)}개 업로드 진행")
    upload_list = deduped

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,
            args=["--no-sandbox"]
        )
        context = await browser.new_context(
            viewport={"width": 1280, "height": 900},
            locale="ko-KR",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            permissions=["clipboard-read", "clipboard-write"],
        )

        # 쿠키 로드
        log("🍪 저장된 쿠키로 로그인 중...")
        await context.add_cookies(cookies)

        # 로그인 유효성 검증
        is_valid = await verify_login(context)
        if not is_valid:
            log("⚠️ 현재 쿠키 만료됨 — 다른 계정 쿠키를 순차 확인합니다...")
            await browser.close()

            # 다른 계정 쿠키 순차 시도
            fallback_cookies, fallback_path = await _try_find_valid_cookies(log, cookie_path)
            if not fallback_cookies:
                log("❌ 모든 계정의 쿠키가 만료되었습니다. '네이버 로그인'을 다시 해주세요")
                return 0

            # 유효한 쿠키로 브라우저 재시작
            cookies = fallback_cookies
            cookie_path = fallback_path
            browser = await p.chromium.launch(
                headless=False, args=["--no-sandbox"]
            )
            context = await browser.new_context(
                viewport={"width": 1280, "height": 900},
                locale="ko-KR",
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                permissions=["clipboard-read", "clipboard-write"],
            )
            await context.add_cookies(cookies)

        log("✅ 로그인 확인 완료!")

        page = await context.new_page()

        try:
            # 카페 이동 (f-e 형식 사용)
            cafe_home = f"https://cafe.naver.com/f-e/cafes/{CAFE_ID}"
            log(f"🏠 카페 이동 중: {cafe_home}")
            await page.goto(cafe_home, wait_until="domcontentloaded", timeout=20000)
            await asyncio.sleep(2)

            # 팝업/레이어 닫기
            await close_naver_popups(page, _log=log)

            # 업로드 중지 플래그 초기화
            reset_upload_stop()

            # 상품별 업로드 (실패 시 1회 재시도)
            for i, product in enumerate(upload_list, 1):
                # 중지 요청 확인
                if is_upload_stop_requested():
                    log(f"⏹ 업로드 중지됨 ({success_count}/{len(upload_list)}개 완료)")
                    break

                name_short = (product.get("name_ko") or product.get("name", ""))[:30]
                code = product.get("product_code", "")
                uploaded = False

                # ── 세션 내 중복 즉시 차단 ──
                if code and code in uploaded_codes_session:
                    log(f"   ⏩ [{i}/{len(upload_list)}] 스킵: {name_short} — 이번 세션에서 이미 업로드됨")
                    continue

                # ── 글 작성 직전 DB에서 상태 재확인 (중복 방지) ──
                if code:
                    try:
                        from product_db import get_product_status
                        db_status = get_product_status(code)
                        if db_status and db_status not in ("대기", ""):
                            log(f"   ⏩ [{i}/{len(upload_list)}] 스킵: {name_short} — DB 상태 '{db_status}' (이미 처리됨)")
                            continue
                    except Exception:
                        pass  # DB 조회 실패 시 그냥 진행

                # ── 글 작성 직전 카페 검색으로 실시간 중복 확인 ──
                # 이미 게시된 상품이면 업로드 없이 상태만 "완료"로 변경
                if code:
                    try:
                        from cafe_monitor import search_cafe_by_browser
                        log(f"   🔍 [{i}/{len(upload_list)}] 카페 중복 확인 중: {code}")
                        search_result = await search_cafe_by_browser(page, code, "", days=30)
                        if search_result:
                            log(f"   ✅ [{i}/{len(upload_list)}] 이미 게시됨 → 상태 '완료'로 변경: {name_short}")
                            log(f"      (by {search_result.get('writer', '')}, {search_result.get('write_date', '')})")
                            # DB 상태 업데이트
                            try:
                                from product_db import update_cafe_status
                                update_cafe_status(code, "완료", datetime.now().isoformat())
                                log(f"      📝 DB 상태 → 완료")
                            except Exception as db_err:
                                log(f"      ⚠️ DB 업데이트 실패: {db_err}")
                            # JSON 상태 업데이트
                            if on_single_success:
                                try:
                                    product["cafe_status"] = "완료"
                                    on_single_success(product)
                                    log(f"      📝 JSON 상태 → 완료")
                                except Exception as json_err:
                                    log(f"      ⚠️ JSON 업데이트 실패: {json_err}")
                            uploaded_codes_session.add(code)
                            continue
                        log(f"   ✅ [{i}/{len(upload_list)}] 중복 없음 — 업로드 진행")
                        # 검색 후 글쓰기 페이지로 다시 이동해야 하므로 카페 홈으로 복귀
                        await page.goto(f"https://cafe.naver.com/f-e/cafes/{CAFE_ID}", wait_until="domcontentloaded", timeout=15000)
                        await asyncio.sleep(1)
                    except Exception as e:
                        log(f"   ⚠️ 카페 중복 확인 실패 (업로드 진행): {e}")
                        await page.goto(f"https://cafe.naver.com/f-e/cafes/{CAFE_ID}", wait_until="domcontentloaded", timeout=15000)
                        await asyncio.sleep(1)

                log(f"📤 [{i}/{len(upload_list)}] 업로드 중: {name_short}")
                try:
                    result = await upload_single_product(page, product, log)
                    if result:
                        success_count += 1
                        if code:
                            uploaded_codes_session.add(code)
                        post_url = result if isinstance(result, str) else ""
                        log(f"   ✅ 업로드 성공 ({success_count}개 완료)")
                        notify_upload_success(name_short, success_count, len(upload_list), post_url)
                        try:
                            from product_db import update_cafe_status
                            _code = product.get("product_code", "")
                            if _code:
                                update_cafe_status(_code, "완료", datetime.now().isoformat())
                                log(f"   📝 DB 상태 업데이트: {_code} → 완료")
                        except Exception as db_err:
                            log(f"   ⚠️ DB 상태 업데이트 실패: {db_err}")
                        if on_single_success:
                            try:
                                on_single_success(product)
                                log(f"   📝 JSON 상태 업데이트 완료")
                            except Exception as json_err:
                                log(f"   ⚠️ JSON 상태 업데이트 실패: {json_err}")
                        uploaded = True
                    else:
                        # 실패했지만 실제로 등록됐을 수 있음 → 카페 검색으로 확인
                        fail_reason = _last_upload_fail_reason or "알 수 없는 원인"
                        log(f"   ⚠️ 업로드 실패 [{fail_reason}] — 실제 등록 여부 확인 중...")
                        if code:
                            await asyncio.sleep(3)
                            try:
                                from cafe_monitor import search_cafe_by_browser
                                already = await search_cafe_by_browser(page, code, "", days=1)
                                if already:
                                    log(f"   ✅ 실제로는 등록됨 확인! (결과 확인만 실패)")
                                    success_count += 1
                                    uploaded_codes_session.add(code)
                                    notify_upload_success(name_short, success_count, len(upload_list), "")
                                    try:
                                        from product_db import update_cafe_status
                                        update_cafe_status(code, "완료", datetime.now().isoformat())
                                    except Exception:
                                        pass
                                    if on_single_success:
                                        try:
                                            on_single_success(product)
                                        except Exception:
                                            pass
                                    uploaded = True
                                else:
                                    log(f"   ⛔ 등록 안 됨 확인 — 다음 상품으로 건너뜁니다")
                                    notify_upload_error(name_short, fail_reason)
                                # 카페 홈으로 복귀
                                await page.goto(f"https://cafe.naver.com/f-e/cafes/{CAFE_ID}", wait_until="domcontentloaded", timeout=15000)
                                await asyncio.sleep(1)
                            except Exception as chk_err:
                                log(f"   ⛔ 등록 확인 실패: {chk_err} — 다음 상품으로 건너뜁니다")
                                notify_upload_error(name_short, fail_reason)
                        else:
                            log(f"   ⛔ 품번 없어 확인 불가 — 다음 상품으로 건너뜁니다")
                            notify_upload_error(name_short, fail_reason)
                except Exception as e:
                    log(f"   ⛔ 오류 발생: {e} — 다음 상품으로 건너뜁니다")
                    notify_upload_error(name_short, str(e))

                # 게시글 간 랜덤 딜레이 (8~13분) — 네이버 봇 탐지 방지
                if i < len(upload_list):
                    if is_upload_stop_requested():
                        log(f"⏹ 업로드 중지됨 ({success_count}/{len(upload_list)}개 완료)")
                        break
                    delay_minutes = random.randint(delay_min, delay_max)
                    delay_sec = delay_minutes * 60
                    next_name = (upload_list[i].get("name_ko") or upload_list[i].get("name", ""))[:30]
                    log(f"   ⏳ 다음 게시글까지 {delay_minutes}분 대기...")
                    notify_upload_waiting(next_name, i, len(upload_list), delay_minutes)
                    # 10초 단위로 중지 확인하며 대기
                    for _ in range(delay_sec // 10):
                        if is_upload_stop_requested():
                            log(f"⏹ 대기 중 업로드 중지됨 ({success_count}/{len(upload_list)}개 완료)")
                            break
                        await asyncio.sleep(10)
                    else:
                        await asyncio.sleep(delay_sec % 10)
                        continue
                    break  # for-else: break이면 외부 for도 break

        except Exception as e:
            log(f"❌ 전체 오류: {e}")
            logger.exception(e)
        finally:
            await browser.close()

    log(f"🎉 업로드 완료: 총 {success_count}/{len(upload_list)}개 성공")
    notify_upload_complete(success_count, len(upload_list))
    return success_count


# =============================================
# 팝업/레이어 닫기 (내소식 안내 등)
# =============================================

async def close_naver_popups(pg, _log=None):
    """네이버 카페 팝업/레이어 닫기"""
    try:
        popup_selectors = [
            "button.btn_close",
            "[class*='close']",
            "[class*='Close']",
            "a.btn_close",
            "[aria-label='닫기']",
            ".layer_popup .btn_close",
            ".popup_layer .btn_close",
            "[class*='LayerPopup'] button",
            "[class*='layer_notice'] button",
            "[class*='modal'] button[class*='close']",
        ]
        for sel in popup_selectors:
            try:
                btns = pg.locator(sel)
                count = await btns.count()
                for idx in range(count):
                    btn = btns.nth(idx)
                    if await btn.is_visible():
                        await btn.click()
                        if _log:
                            _log(f"   🔕 팝업 닫기: {sel}")
                        await asyncio.sleep(0.5)
            except Exception:
                continue

        # iframe 내부 팝업도 닫기
        for frame in pg.frames:
            if frame == pg.main_frame:
                continue
            try:
                for sel in ["button.btn_close", "[class*='close']", "[aria-label='닫기']"]:
                    btns = frame.locator(sel)
                    count = await btns.count()
                    for idx in range(count):
                        btn = btns.nth(idx)
                        if await btn.is_visible():
                            await btn.click()
                            if _log:
                                _log(f"   🔕 iframe 팝업 닫기: {sel}")
                            await asyncio.sleep(0.3)
            except Exception:
                continue
    except Exception:
        pass


# =============================================
# 단일 상품 업로드
# =============================================

async def upload_single_product(page, product: dict, log=None) -> bool:
    """상품 하나를 카페 게시글로 작성"""
    def _log(msg):
        if log:
            log(msg)

    try:
        # 가격 계산
        price_info = calc_buying_price(product.get("price_jpy", 0))

        # 게시글 제목 & 내용 생성 (Claude API 우선, 실패 시 기본 템플릿)
        from post_generator import generate_cafe_post, get_detail_image_urls
        post = generate_cafe_post(product, price_info)
        if post is None:
            _log("❌ AI 제목 생성 실패 — 이 상품 건너뜀")
            return False, "AI 제목 생성 실패"
        title = post["title"]
        content = post["content"]
        content_intro = post.get("content_intro", "")
        content_detail = post.get("content_detail", "")
        detail_images = get_detail_image_urls(product)

        # ── 1단계: 글쓰기 페이지로 직접 이동 ──
        # 네이버 카페 URL 형식이 변경될 수 있으므로 여러 형식 시도
        write_urls = [
            f"https://cafe.naver.com/ca-fe/cafes/{CAFE_ID}/articles/write?boardType=L&menuId={CAFE_MENU_ID}",
            f"https://cafe.naver.com/f-e/cafes/{CAFE_ID}/articles/write?menuId={CAFE_MENU_ID}",
        ]
        page_loaded = False
        for try_url in write_urls:
            _log(f"   🌐 글쓰기 페이지 이동: {try_url}")
            try:
                await page.goto(try_url, wait_until="domcontentloaded", timeout=30000)
                await asyncio.sleep(3)

                current_url = page.url
                _log(f"   📍 리다이렉트 결과: {current_url}")

                # 로그인 페이지로 리다이렉트 됐는지 확인
                if "login" in current_url or "nidlogin" in current_url:
                    _log("   ❌ 쿠키 만료 — 재로그인 필요")
                    _set_fail_reason("쿠키 만료 — 재로그인 필요")
                    return False

                # 글쓰기 페이지가 아닌 곳으로 리다이렉트된 경우 (카페 메인 등)
                if "write" not in current_url and "ArticleWrite" not in current_url:
                    _log(f"   ⚠️ 글쓰기 페이지가 아님 — 쿠키 만료 또는 URL 형식 변경")
                    continue

                _log(f"   ✅ 현재 URL: {current_url}")
                # 팝업/레이어 닫기 (내소식 안내 등)
                await close_naver_popups(page, _log=_log)
                page_loaded = True
                break
            except Exception as e:
                _log(f"   ⚠️ 페이지 이동 실패: {e} — 다음 URL 시도")
                continue

        if not page_loaded:
            _log("   ❌ 글쓰기 페이지를 열 수 없습니다 — 쿠키 만료일 가능성이 높습니다. 재로그인 해주세요.")
            _set_fail_reason("글쓰기 페이지 열기 실패 — 재로그인 필요")
            return False

        # ── 2단계: 에디터 렌더링 대기 ──
        # 방법1: iframe 내부에서 찾기
        # 방법2: iframe 없이 메인 페이지에서 직접 찾기 (네이버 UI 변경 대응)
        frame_locator = None
        editor_found = False

        # iframe[name='cafe_main'] 존재 여부 확인
        cafe_main_iframe = await page.query_selector("iframe[name='cafe_main']")
        if cafe_main_iframe:
            _log("   🔍 iframe[name='cafe_main'] 발견 — iframe 내부에서 에디터 검색")
            frame_locator = page.frame_locator("iframe[name='cafe_main']")
            try:
                await frame_locator.locator(
                    "textarea.textarea_input, textarea[placeholder*='제목'], .se-documentTitle .se-text-paragraph"
                ).first.wait_for(timeout=40000)
                _log("   ✅ 에디터 로딩 완료 (iframe#cafe_main)")
                editor_found = True
            except PlaywrightTimeout:
                _log("   ⚠️ iframe 내부 에디터 못 찾음 — 메인 페이지에서 재시도")

        if not editor_found:
            # iframe 없거나 iframe 내부에서 못 찾은 경우 — 메인 페이지에서 직접 검색
            _log("   🔍 메인 페이지에서 에디터 직접 검색 중...")
            try:
                await page.wait_for_selector(
                    "textarea.textarea_input, textarea[placeholder*='제목'], "
                    ".se-documentTitle .se-text-paragraph, "
                    "[contenteditable='true']",
                    timeout=40000
                )
                _log("   ✅ 에디터 로딩 완료 (메인 페이지)")
                frame_locator = None  # 메인 페이지 직접 사용
                editor_found = True
            except PlaywrightTimeout:
                # 디버그: 현재 페이지 구조 로깅
                try:
                    debug_info = await page.evaluate("""() => {
                        const iframes = [...document.querySelectorAll('iframe')].map(f => ({
                            name: f.name, id: f.id, src: (f.src || '').substring(0, 80)
                        }));
                        const frames = window.frames.length;
                        const textareas = document.querySelectorAll('textarea').length;
                        const editables = document.querySelectorAll('[contenteditable]').length;
                        const seEls = document.querySelectorAll('[class*="se-"]').length;
                        return JSON.stringify({iframes, frames, textareas, editables, seEls});
                    }""")
                    _log(f"   🔍 디버그 페이지 구조: {debug_info}")
                except Exception as dbg_err:
                    _log(f"   🔍 디버그 실패: {dbg_err}")
                _log("   ❌ 에디터 로딩 시간 초과 (40초)")
                _set_fail_reason("에디터 로딩 시간 초과 (40초)")
                return False

        # frame_locator가 None이면 page를 직접 사용하는 래퍼
        class _PageAsFrameLocator:
            """page를 frame_locator처럼 사용할 수 있게 하는 래퍼"""
            def __init__(self, pg):
                self._page = pg
            def locator(self, selector):
                return self._page.locator(selector)

        if frame_locator is None:
            frame_locator = _PageAsFrameLocator(page)

        # ── 에디터 본문 영역 높이 확장 ──
        try:
            await frame_locator.locator(".se-content, .se-component-content").first.evaluate(
                """el => {
                    el.style.minHeight = '800px';
                    // 상위 컨테이너도 확장
                    let parent = el.closest('.se-section-text, .se-module-text, .se-editor');
                    if (parent) parent.style.minHeight = '800px';
                }"""
            )
        except Exception:
            pass

        # ── 게시판 선택 (드롭다운에서 게시판 선택) ──
        # URL에 menuId가 포함되어 있으면 게시판이 이미 선택된 상태
        try:
            # 이미 선택된 게시판 이름 확인
            already_selected = False
            selected_board_selectors = [
                "[class*='BoardSelectButton']",
                "[class*='board_name']",
                "a.board_name",
            ]
            for sel in selected_board_selectors:
                try:
                    el = frame_locator.locator(sel).first
                    if await el.count() > 0:
                        board_text = (await el.inner_text()).strip()
                        if CAFE_MENU_NAME in board_text:
                            _log(f"   ✅ 게시판 이미 선택됨: {board_text}")
                            already_selected = True
                            break
                except Exception:
                    continue

            if not already_selected:
                # "게시판을 선택해 주세요" 버튼 찾기
                board_btn = frame_locator.locator(
                    "button:has-text('게시판을 선택해 주세요'), "
                    "button:has-text('게시판 선택'), "
                    "[class*='BoardSelectButton'], "
                    "a.board_name"
                ).first
                if await board_btn.count() > 0:
                    await board_btn.click()
                    await asyncio.sleep(1.5)

                    # 드롭다운 목록 영역 찾기
                    dropdown = frame_locator.locator(
                        "ul[role='listbox'], .select_list, [class*='selectbox'] ul, "
                        "[class*='menu_list'], [class*='board_list'], "
                        "[class*='layer_board'] ul, [class*='select_popup'] ul"
                    ).first

                    # 스크롤하면서 메뉴 항목 찾기
                    found = False
                    for scroll_try in range(15):
                        # 정확한 텍스트 매칭 우선 시도
                        menu_item = None
                        # 1) text= 정확 매칭
                        exact_loc = frame_locator.locator(
                            f"li >> text='{CAFE_MENU_NAME}'"
                        ).first
                        if await exact_loc.count() > 0:
                            menu_item = exact_loc
                        else:
                            # 2) role=option 텍스트 매칭
                            opt_loc = frame_locator.locator(
                                f"[role='option']:has-text('{CAFE_MENU_NAME}')"
                            ).first
                            if await opt_loc.count() > 0:
                                menu_item = opt_loc
                            else:
                                # 3) li 안의 a/span/button 부분 매칭
                                partial_loc = frame_locator.locator(
                                    f"li a:has-text('{CAFE_MENU_NAME}'), "
                                    f"li button:has-text('{CAFE_MENU_NAME}'), "
                                    f"li span:has-text('{CAFE_MENU_NAME}')"
                                ).first
                                if await partial_loc.count() > 0:
                                    menu_item = partial_loc

                        if menu_item and await menu_item.count() > 0:
                            try:
                                await menu_item.scroll_into_view_if_needed()
                                await asyncio.sleep(0.3)
                                await menu_item.click()
                                found = True
                                break
                            except Exception:
                                # Playwright click 실패 시 JS click 시도
                                try:
                                    await menu_item.evaluate("el => el.click()")
                                    found = True
                                    break
                                except Exception:
                                    # dispatchEvent 시도
                                    try:
                                        await menu_item.evaluate(
                                            "el => el.dispatchEvent(new MouseEvent('click', {bubbles:true}))"
                                        )
                                        found = True
                                        break
                                    except Exception as click_err:
                                        _log(f"   ⚠️ 클릭 실패 (시도 {scroll_try}): {click_err}")

                        # 드롭다운 스크롤 다운
                        if await dropdown.count() > 0:
                            await dropdown.evaluate("el => el.scrollTop += 150")
                        else:
                            await board_btn.press("ArrowDown")
                            await board_btn.press("ArrowDown")
                            await board_btn.press("ArrowDown")
                        await asyncio.sleep(0.3)

                    if found:
                        await asyncio.sleep(1)
                        _log(f"   ✅ 게시판 선택: {CAFE_MENU_NAME}")
                    else:
                        _log(f"   ⚠️ '{CAFE_MENU_NAME}' 메뉴 항목을 찾지 못했습니다")
                else:
                    # menuId로 접속했으므로 게시판이 URL에서 이미 지정됨
                    _log(f"   ℹ️ 게시판 선택 버튼 없음 — menuId={CAFE_MENU_ID}로 이미 지정됨")
        except Exception as e:
            _log(f"   ⚠️ 게시판 선택 시도: {e}")

        # ── 3단계: 제목 입력 ──
        title_selectors = [
            "textarea.textarea_input",
            "textarea[placeholder*='제목']",
        ]
        title_filled = False
        for sel in title_selectors:
            try:
                el = frame_locator.locator(sel).first
                if await el.count() > 0:
                    await el.click()
                    await asyncio.sleep(0.5)
                    await el.fill(title)
                    await asyncio.sleep(0.3)
                    val = await el.input_value()
                    if val.strip():
                        title_filled = True
                        _log(f"   ✅ 제목 입력 완료 ({sel}): {val[:30]}")
                        break
                    else:
                        _log(f"   ⚠️ 제목 fill 실패 — keyboard.type 시도")
                        await el.click()
                        await asyncio.sleep(0.3)
                        await el.press("Control+a")
                        await el.type(title, delay=30)
                        await asyncio.sleep(0.3)
                        val2 = await el.input_value()
                        if val2.strip():
                            title_filled = True
                            _log(f"   ✅ 제목 입력 완료 (타이핑): {val2[:30]}")
                            break
            except Exception as e:
                _log(f"   ⚠️ 제목 시도 실패 ({sel}): {e}")
                continue

        if not title_filled:
            _log("   ❌ 제목 입력란을 찾지 못했습니다")
            _set_fail_reason("제목 입력란을 찾지 못함")
            return False

        await asyncio.sleep(1)

        # ── 4단계: 기존 템플릿 끝으로 커서 이동 후 본문 입력 ──
        # 에디터에 미리 등록된 템플릿이 있으므로 끝으로 이동하여 이어서 작성
        try:
            editor_el = None
            for sel in [".se-content", ".se-section-text", "[contenteditable=true]"]:
                try:
                    el = frame_locator.locator(sel).first
                    if await el.count() > 0:
                        editor_el = el
                        break
                except Exception:
                    continue
            if editor_el:
                await editor_el.click()
                await asyncio.sleep(0.3)
                await page.keyboard.press("Control+End")
                await asyncio.sleep(0.3)
                # 빈 줄 추가하여 템플릿과 구분 (2줄 여백)
                # 스마트에디터에서 빈 문단이 확실히 생기도록 공백+Enter 반복
                for _ in range(3):
                    await page.keyboard.press("Enter")
                    await page.keyboard.type(" ", delay=30)
                    await asyncio.sleep(0.1)
                await page.keyboard.press("Enter")
                await asyncio.sleep(0.3)
                _log("   ✅ 기존 템플릿 끝으로 커서 이동 완료")
        except Exception as e:
            _log(f"   ⚠️ 커서 이동 시도: {e}")

        toolbar_locator = await _find_toolbar_locator(page, frame_locator, _log)

        _log(f"   📷 이미지 {len(detail_images)}개 준비됨" + (f" (첫 번째: {detail_images[0][:60]}...)" if detail_images else " — 이미지 없음!"))

        if content_intro and content_detail and detail_images:
            # 인트로 부분 먼저 입력
            _log("   📝 인트로 본문 입력 중...")
            await type_content_to_editor_iframe(page, frame_locator, content_intro, _log, toolbar_locator=toolbar_locator)
            _log("   ⏳ 인트로 안정화 대기 중...")
            await asyncio.sleep(3)

            # 첫 번째 이미지 삽입
            _log(f"   📷 첫 번째 이미지 삽입 [1/{len(detail_images)}]")
            await upload_image_from_url_iframe(page, frame_locator, detail_images[0], _log)
            await asyncio.sleep(2)

            # 상세 부분 이어서 입력
            _log("   📝 상세 본문 입력 중...")
            await type_content_to_editor_iframe(page, frame_locator, content_detail, _log, toolbar_locator=toolbar_locator)
            _log("   ⏳ 본문 안정화 대기 중...")
            await asyncio.sleep(5)

            # 나머지 이미지 업로드
            remaining_images = detail_images[1:]
            if remaining_images:
                for img_idx, img_url in enumerate(remaining_images):
                    _log(f"   📷 이미지 업로드 [{img_idx+2}/{len(detail_images)}]")
                    await upload_image_from_url_iframe(page, frame_locator, img_url, _log)
                    await asyncio.sleep(2)
        else:
            # 분리 불가 시 기존 방식: 전체 본문 → 전체 이미지
            await type_content_to_editor_iframe(page, frame_locator, content, _log, toolbar_locator=toolbar_locator)
            _log("   ⏳ 본문 안정화 대기 중...")
            await asyncio.sleep(5)
            if detail_images:
                for img_idx, img_url in enumerate(detail_images):
                    _log(f"   📷 이미지 업로드 [{img_idx+1}/{len(detail_images)}]")
                    await upload_image_from_url_iframe(page, frame_locator, img_url, _log)
                    await asyncio.sleep(2)

        # ── 6단계: 태그 입력 ──
        tags = post.get("tags", [])
        if tags:
            await input_tags_iframe(frame_locator, tags, _log)

        # ── 7단계: 등록 전 검증 ──────────────────────
        _log("━" * 40)
        _log("   🔍 등록 전 검증 시작...")
        await asyncio.sleep(2)

        verify_ok = True

        # [검증 1] 제목 확인 (비어있거나 일본어 포함 시 차단)
        try:
            title_val = ""
            for sel in ["textarea.textarea_input", "textarea[placeholder*='제목']"]:
                try:
                    el = frame_locator.locator(sel).first
                    if await el.count() > 0:
                        title_val = (await el.input_value()).strip()
                        break
                except Exception:
                    continue
            if title_val:
                # 일본어 잔존 체크
                import re as _re
                has_jp = bool(_re.search(r'[\u3040-\u309F\u30A0-\u30FF\u4E00-\u9FFF]', title_val))
                if has_jp:
                    _log(f"   ❌ [검증] 제목에 일본어가 포함되어 있습니다! → {title_val[:50]}")
                    _log(f"   🔄 [재시도] 제목 재번역 시도...")
                    from post_generator import _gemini_translate_name, _has_japanese
                    new_title = _gemini_translate_name(title_val)
                    if not _has_japanese(new_title):
                        # 제목 필드 다시 입력
                        for sel in ["textarea.textarea_input", "textarea[placeholder*='제목']"]:
                            try:
                                el = frame_locator.locator(sel).first
                                if await el.count() > 0:
                                    await el.fill("")
                                    await asyncio.sleep(0.3)
                                    await el.fill(new_title)
                                    title_val = new_title
                                    _log(f"   ✅ [검증] 제목 재번역 성공: {new_title[:40]}...")
                                    break
                            except Exception:
                                continue
                    else:
                        _log(f"   ❌ [검증] 제목 재번역 후에도 일본어 잔존 — 등록 중단!")
                        verify_ok = False
                else:
                    _log(f"   ✅ [검증] 제목: {title_val[:40]}...")
            else:
                _log(f"   ❌ [검증] 제목이 비어있습니다!")
                verify_ok = False
        except Exception as e:
            _log(f"   ⚠️ [검증] 제목 확인 실패: {e}")

        # [검증 2] 본문 확인 — 에디터 영역 내 텍스트 길이 체크
        try:
            body_text = ""
            body_selectors = [
                ".se-content",
                ".se-component-content",
                "[contenteditable='true']",
                ".editor_content",
                ".se-text-paragraph",
            ]
            for sel in body_selectors:
                try:
                    el = frame_locator.locator(sel).first
                    if await el.count() > 0:
                        body_text = (await el.inner_text()).strip()
                        if len(body_text) > 10:
                            break
                except Exception:
                    continue

            if len(body_text) > 30:
                _log(f"   ✅ [검증] 본문: {len(body_text)}자 입력됨")

                # 구조 검증: 섹션 존재 및 순서 확인 (경고만, 차단하지 않음)
                check_sections = ["🔍 상품 상세 정보", "💎 핵심 구매 포인트"]
                positions = []
                for sec in check_sections:
                    pos = body_text.find(sec)
                    positions.append(pos)
                    if pos == -1:
                        _log(f"   ⚠️ [검증] 누락된 섹션: {sec}")

                # 순서 확인 (찾은 것들만) — 경고만, 등록 중단하지 않음
                found_positions = [(p, s) for p, s in zip(positions, check_sections) if p >= 0]
                if found_positions:
                    is_ordered = all(found_positions[i][0] <= found_positions[i+1][0]
                                   for i in range(len(found_positions)-1))
                    if is_ordered:
                        _log(f"   ✅ [검증] 섹션 순서 정상")
                    else:
                        _log(f"   ⚠️ [검증] 섹션 순서 이상 (계속 진행)")

                # [검증 2-2] 네이버 폼 URL 확인 (에디터 템플릿에 포함되어 있으므로 경고만)
                if "naver.me" in body_text or "네이버 폼" in body_text:
                    _log(f"   ✅ [검증] 네이버 폼 링크 확인됨 (템플릿)")
                else:
                    _log(f"   ⚠️ [검증] 네이버 폼 링크 미확인 (템플릿에 포함되어 있을 수 있음)")

                # [검증 2-1] 본문 일본어 잔존 체크
                import re as _re
                jp_chars = _re.findall(r'[\u3040-\u309F\u30A0-\u30FF\u4E00-\u9FFF]+', body_text)
                if jp_chars:
                    # 한자만 있는 경우는 한국어 한자일 수 있으므로 카타카나/히라가나 기준으로 판단
                    kana_chars = _re.findall(r'[\u3040-\u309F\u30A0-\u30FF]+', body_text)
                    if kana_chars:
                        _log(f"   ❌ [검증] 본문에 일본어(카나) 잔존: {', '.join(kana_chars[:5])}...")
                        _log(f"   ❌ [검증] 번역되지 않은 일본어가 포함된 게시글입니다 — 등록 중단!")
                        verify_ok = False
                    else:
                        _log(f"   ⬚ [검증] 본문 한자 감지 (한국어 한자일 수 있음, 통과)")
                else:
                    _log(f"   ✅ [검증] 본문 일본어 없음 — 번역 OK")

            elif len(body_text) > 0:
                _log(f"   ⚠️ [검증] 본문이 너무 짧습니다 ({len(body_text)}자)")
                verify_ok = False
            else:
                _log(f"   ❌ [검증] 본문이 비어있습니다!")
                verify_ok = False
        except Exception as e:
            _log(f"   ⚠️ [검증] 본문 확인 실패: {e}")

        # [검증 3] 이미지 확인 — 에디터 내 img 태그 수
        try:
            img_count = 0
            img_selectors = [
                "img.se-image-resource",
                ".se-component-image img",
                ".se-image img",
                "img[src*='pstatic']",
                "img[src*='naver']",
            ]
            for sel in img_selectors:
                try:
                    cnt = await frame_locator.locator(sel).count()
                    if cnt > img_count:
                        img_count = cnt
                except Exception:
                    continue

            expected_img = len(detail_images) if detail_images else 0
            if img_count > 0:
                _log(f"   ✅ [검증] 이미지: {img_count}개 삽입됨 (예상: {expected_img}개)")
            elif expected_img > 0:
                _log(f"   ⚠️ [검증] 이미지 0개 — 예상 {expected_img}개인데 삽입 안됨")
                # 이미지 재시도
                _log(f"   🔄 [재시도] 이미지 다시 업로드 시도...")
                retry_success = 0
                for retry_idx, retry_url in enumerate(detail_images[:3]):
                    _log(f"   📷 재시도 이미지 [{retry_idx+1}/{min(len(detail_images),3)}]")
                    await upload_image_from_url_iframe(page, frame_locator, retry_url, _log)
                    await asyncio.sleep(2)

                # 재시도 후 이미지 수 재확인
                img_count_after = 0
                for sel in img_selectors:
                    try:
                        cnt = await frame_locator.locator(sel).count()
                        if cnt > img_count_after:
                            img_count_after = cnt
                    except Exception:
                        continue

                if img_count_after > 0:
                    _log(f"   ✅ [검증] 이미지 재시도 성공: {img_count_after}개 삽입됨")
                else:
                    _log(f"   ❌ [검증] 이미지 재시도 후에도 0개 — 이미지 없는 글은 등록 중단!")
                    verify_ok = False
            else:
                _log(f"   ⚠️ [검증] 원본 상품에 이미지가 없습니다 — 등록 중단!")
                verify_ok = False
        except Exception as e:
            _log(f"   ⚠️ [검증] 이미지 확인 실패: {e}")

        # [검증 4] 태그 확인
        try:
            tag_count = 0
            tag_selectors = [
                ".tag_item", ".se-tag", "[class*='tag_']",
                "li[class*='tag']", "span[class*='tag']",
            ]
            for sel in tag_selectors:
                try:
                    cnt = await frame_locator.locator(sel).count()
                    if cnt > tag_count:
                        tag_count = cnt
                except Exception:
                    continue
            if tag_count > 0:
                _log(f"   ✅ [검증] 태그: {tag_count}개")
            elif tags:
                _log(f"   ⚠️ [검증] 태그가 입력되지 않았을 수 있음")
            else:
                _log(f"   ⬚ [검증] 태그: 없음")
        except Exception as e:
            _log(f"   ⚠️ [검증] 태그 확인 실패: {e}")

        # [검증 5] 게시판 확인
        try:
            board_name = ""
            board_selectors = [
                "a.board_name", "[class*='board_name']",
                "[class*='BoardSelectButton']", "[class*='board_select']",
            ]
            for sel in board_selectors:
                try:
                    el = frame_locator.locator(sel).first
                    if await el.count() > 0:
                        board_name = (await el.inner_text()).strip()
                        if board_name:
                            break
                except Exception:
                    continue
            if board_name:
                _log(f"   ✅ [검증] 게시판: {board_name}")
            else:
                _log(f"   ⬚ [검증] 게시판 이름 확인 불가 (설정: {CAFE_MENU_NAME}, menuId: {CAFE_MENU_ID})")
        except Exception as e:
            _log(f"   ⚠️ [검증] 게시판 확인 실패: {e}")

        # 검증 결과 종합
        if verify_ok:
            _log("   ✅ 검증 완료 — 등록 진행합니다")
        else:
            _log("   ❌ 검증 실패 — 등록 중단! (본문 구조 또는 내용 이상)")
            _log("━" * 40)
            _set_fail_reason("본문 검증 실패 — 구조 또는 내용 이상")
            return False
        _log("━" * 40)

        await asyncio.sleep(1)

        # ── 8단계: 등록 버튼 클릭 ──
        submit_selectors = [
            "a.BaseButton--skinGreen:has-text('등록')",
            "a.BaseButton:has-text('등록')",
            "button:has-text('등록')",
            "a:has-text('등록')",
        ]
        for sel in submit_selectors:
            try:
                el = frame_locator.locator(sel).first
                if await el.count() > 0:
                    await el.click()
                    _log(f"   ✅ 등록 버튼 클릭")

                    # 등록 후 게시글 페이지로 이동 대기 (URL이 /articles/숫자로 변경될 때까지)
                    post_url = ""
                    for wait_i in range(20):  # 최대 20초 대기
                        await asyncio.sleep(1)
                        current_url = page.url
                        if "/articles/" in current_url and "write" not in current_url:
                            post_url = current_url
                            break
                        # 에러 팝업/알림 확인 (내소식 등 무관한 팝업 무시)
                        try:
                            error_el = frame_locator.locator(
                                ".popup_error, [role='alertdialog'], .layer_alert"
                            ).first
                            if await error_el.count() > 0:
                                err_text = (await error_el.inner_text()).strip()[:100]
                                _ignore_keywords = [
                                    "알림을 모두 삭제", "알림 설정", "알림을 확인",
                                    "내소식", "소식", "안내", "레이어",
                                    "공지", "업데이트", "새로운 기능",
                                ]
                                if err_text and not any(kw in err_text for kw in _ignore_keywords):
                                    _log(f"   ❌ 등록 에러 감지: {err_text}")
                                    _set_fail_reason(f"등록 에러: {err_text}")
                                    return False
                                else:
                                    _log(f"   🔕 무관한 팝업 무시: {err_text[:50]}")
                        except Exception:
                            pass

                    if not post_url:
                        # iframe 내부 URL 확인
                        try:
                            for frm in page.frames:
                                if "/articles/" in frm.url and "write" not in frm.url:
                                    post_url = frm.url
                                    break
                        except Exception:
                            pass

                    if post_url:
                        _log(f"   🔗 게시글 URL: {post_url}")
                        return post_url
                    else:
                        _log(f"   ❌ 등록 실패 — 게시글 URL 확인 불가 (20초 대기 후에도 글쓰기 페이지)")
                        _set_fail_reason("등록 버튼 클릭했으나 게시글 페이지로 이동되지 않음")
                        return False
            except Exception:
                continue

        _log("   ❌ 등록 버튼을 찾지 못했습니다")
        _set_fail_reason("등록 버튼을 찾지 못함")
        return False

    except Exception as e:
        logger.error(f"단일 업로드 오류: {e}")
        _set_fail_reason(f"예외 발생: {e}")
        return False


async def select_cafe_menu(frame):
    """카페 게시판 선택"""
    try:
        # 게시판 선택 드롭다운 클릭
        menu_selector = frame.locator(
            "a.board_name, "
            "button[class*='select_board'], "
            "[class*='menu_select']"
        ).first
        if await menu_selector.count() > 0:
            await menu_selector.click()
            await asyncio.sleep(1)

        # 게시판 이름으로 선택
        menu_item = frame.locator(f"text={CAFE_MENU_NAME}").first
        if await menu_item.count() > 0:
            await menu_item.click()
            await asyncio.sleep(1)
            return True
    except Exception:
        pass
    return False


async def _find_toolbar_locator(page, frame_locator, log=None):
    """
    Smart Editor 3 툴바가 어디에 있는지 탐색
    iframe 안 또는 page 레벨에서 탐색하여 올바른 locator 반환
    """
    # 툴바 버튼 테스트 셀렉터 (실제 Smart Editor 3 HTML 기준)
    test_selectors = [
        "button[data-name='font-size']",
        "button[data-name='bold']",
        "button[data-name='line-height']",
        ".se-font-size-code-toolbar-button",
        ".se-bold-toolbar-button",
    ]

    # 1) iframe 안에서 탐색
    for sel in test_selectors:
        try:
            btn = frame_locator.locator(sel).first
            if await btn.count() > 0:
                if log:
                    log(f"   🔧 툴바 발견: iframe 내부 ({sel})")
                return frame_locator
        except Exception:
            continue

    # 2) iframe 밖 page 레벨에서 탐색
    for sel in test_selectors:
        try:
            btn = page.locator(sel).first
            if await btn.count() > 0:
                if log:
                    log(f"   🔧 툴바 발견: page 레벨 ({sel})")
                return page
        except Exception:
            continue

    # 3) 중첩 iframe 탐색 — cafe_main 안의 다른 iframe
    try:
        iframes = frame_locator.locator("iframe")
        count = await iframes.count()
        if log:
            log(f"   🔧 cafe_main 내 iframe 수: {count}")
        for idx in range(count):
            inner_frame = frame_locator.frame_locator(f"iframe >> nth={idx}")
            for sel in test_selectors:
                try:
                    btn = inner_frame.locator(sel).first
                    if await btn.count() > 0:
                        if log:
                            log(f"   🔧 툴바 발견: 중첩 iframe #{idx} ({sel})")
                        return inner_frame
                except Exception:
                    continue
    except Exception as e:
        if log:
            log(f"   ⚠️ 중첩 iframe 탐색 오류: {e}")

    # 4) page 레벨에서 모든 iframe 탐색
    try:
        page_iframes = page.locator("iframe")
        pcount = await page_iframes.count()
        if log:
            log(f"   🔧 page 레벨 iframe 수: {pcount}")
        for idx in range(pcount):
            try:
                pframe = page.frame_locator(f"iframe >> nth={idx}")
                for sel in test_selectors:
                    try:
                        btn = pframe.locator(sel).first
                        if await btn.count() > 0:
                            iframe_name = await page_iframes.nth(idx).get_attribute("name") or ""
                            iframe_id = await page_iframes.nth(idx).get_attribute("id") or ""
                            if log:
                                log(f"   🔧 툴바 발견: page iframe #{idx} name={iframe_name} id={iframe_id} ({sel})")
                            return pframe
                    except Exception:
                        continue
            except Exception:
                continue
    except Exception as e:
        if log:
            log(f"   ⚠️ page iframe 탐색 오류: {e}")

    if log:
        log("   ❌ 툴바를 어디서도 찾지 못했습니다")
    return frame_locator  # fallback


async def set_line_spacing_200(frame_locator, log=None):
    """
    Smart Editor 3 줄간격을 200%로 설정
    에디터 툴바의 줄간격 버튼 → 200% 선택
    """
    try:
        # 먼저 에디터 본문 클릭하여 포커스
        for sel in [".se-content", ".se-section-text", "[contenteditable=true]"]:
            try:
                el = frame_locator.locator(sel).first
                if await el.count() > 0:
                    await el.click()
                    await asyncio.sleep(0.3)
                    break
            except Exception:
                continue

        # 전체 선택 (Ctrl+A) 후 줄간격 적용
        # 줄간격 버튼 찾기
        spacing_btn_selectors = [
            "button[data-name='lineSpacing']",
            "button[aria-label*='줄간격']",
            "button[title*='줄간격']",
            "button[data-command='lineSpacing']",
            "button[class*='line_spacing']",
            "button[class*='lineHeight']",
        ]
        for sel in spacing_btn_selectors:
            try:
                btn = frame_locator.locator(sel).first
                if await btn.count() > 0:
                    await btn.click()
                    await asyncio.sleep(1)

                    # 200% 옵션 선택
                    option_selectors = [
                        "li:has-text('2.0')",
                        "li:has-text('200')",
                        "button:has-text('2.0')",
                        "button:has-text('200')",
                        "[data-value='2.0']",
                        "[data-value='200']",
                    ]
                    for opt_sel in option_selectors:
                        try:
                            opt = frame_locator.locator(opt_sel).first
                            if await opt.count() > 0:
                                await opt.click()
                                await asyncio.sleep(0.5)
                                logger.info("줄간격 200% 설정 완료")
                                if log:
                                    log("   ✅ 줄간격 200% 설정")
                                return
                        except Exception:
                            continue

                    # 드롭다운이 열렸지만 옵션 못 찾은 경우 닫기
                    try:
                        await btn.click()
                        await asyncio.sleep(0.3)
                    except Exception:
                        pass
                    break
            except Exception:
                continue

        logger.warning("줄간격 버튼을 찾지 못했습니다")
        if log:
            log("   ⚠️ 줄간격 설정 실패 — 기본값 사용")
    except Exception as e:
        logger.warning(f"줄간격 설정 오류: {e}")
        if log:
            log(f"   ⚠️ 줄간격 설정 오류: {e}")


async def type_content_to_editor_iframe(page, frame_locator, content: str, log=None, toolbar_locator=None):
    """
    iframe 내 Smart Editor 3 본문 입력
    줄 단위로 타이핑 + Enter (사람이 직접 치는 것처럼)
    toolbar_locator: 툴바가 있는 locator (frame_locator와 다를 수 있음)
    """
    import re
    plain = re.sub(r'<img\s+src="([^"]+)"[^>]*>', '', content)
    plain = re.sub(r'<[^>]+>', '', plain)
    plain = plain.strip()

    editor_selectors = [
        ".se-content",
        ".se-section-text",
        "[class*='se-module-text']",
        "[contenteditable=true]",
    ]

    # 에디터 영역 클릭
    target_el = None
    for sel in editor_selectors:
        try:
            el = frame_locator.locator(sel).first
            if await el.count() > 0:
                await el.click()
                target_el = el
                logger.info(f"에디터 클릭 (iframe): {sel}")
                break
        except Exception:
            continue

    if not target_el:
        if log:
            log("   ⚠️ 에디터 영역을 찾지 못했습니다")
        return

    await asyncio.sleep(0.5)

    # 툴바 locator (frame_locator와 다를 수 있음)
    tb = toolbar_locator if toolbar_locator else frame_locator

    # 패턴 정의
    url_pattern = re.compile(r'^https?://\S+$')
    section_heading_pattern = re.compile(r'^(🔍\s*상품 상세 정보|💎\s*핵심 구매 포인트|⚠️\s*구매 전 확인 사항|👉\s*구매 문의.*진행 방법)$')
    numbered_pattern = re.compile(r'^\d+\.\s')

    # 헤딩 줄 번호 기록 (나중에 서식 적용)
    heading_line_numbers = []

    try:
        lines = plain.split("\n")

        # ════════════════════════════════════════
        # 1단계: 모든 텍스트를 일반 서식으로 천천히 입력
        # ════════════════════════════════════════
        if log:
            log(f"   📝 본문 입력 시작 ({len(lines)}줄)...")

        line_num = 0  # 에디터 내 실제 줄 번호
        for i, line in enumerate(lines):
            stripped = line.strip()

            # 첫 줄 또는 섹션 제목 기록
            is_first_line = (i == 0 and stripped)
            is_heading = (stripped and section_heading_pattern.match(stripped))
            if is_first_line or is_heading:
                heading_line_numbers.append(line_num)

            # URL: 붙여넣기로 삽입 (OG 미리보기 카드 생성)
            if stripped and url_pattern.match(stripped):
                url_inserted = False
                og_detected = False

                # URL/OG 검증 헬퍼
                async def _verify_url_in_editor(url_str, check_og=False):
                    """에디터 본문에 URL이 실제로 들어갔는지 확인"""
                    await asyncio.sleep(1)
                    text_found = False
                    for fr in page.frames:
                        try:
                            txt = await fr.evaluate("document.body?.innerText || ''")
                            if url_str in txt or "naver.me" in txt:
                                text_found = True
                                break
                        except Exception:
                            continue
                    # OG 카드 확인 (href 또는 og-card 요소)
                    og_found = False
                    for fr in page.frames:
                        try:
                            og_found = await fr.evaluate("""(() => {
                                const ogCard = document.querySelector('.se-oglink, .se-module-oglink, [class*=oglink], [data-module=oglink]');
                                if (ogCard) return true;
                                const aTag = document.querySelector("a[href*='naver.me']");
                                if (aTag) return true;
                                return false;
                            })()""")
                            if og_found:
                                break
                        except Exception:
                            continue
                    if check_og:
                        return og_found
                    return text_found or og_found

                # ── 방법 1: clipboard API + Ctrl+V (OG 미리보기 트리거) ──
                try:
                    await page.evaluate(f"navigator.clipboard.writeText('{stripped}')")
                    await asyncio.sleep(0.3)
                    # 에디터에 포커스 확실히
                    await target_el.click()
                    await asyncio.sleep(0.3)
                    await page.keyboard.press("Control+v")
                    await asyncio.sleep(8)  # OG 미리보기 로딩 대기 (넉넉히)
                    if await _verify_url_in_editor(stripped):
                        url_inserted = True
                        og_detected = await _verify_url_in_editor(stripped, check_og=True)
                        if log:
                            og_mark = "✅ OG 미리보기" if og_detected else "⚠️ 텍스트만"
                            log(f"   🔗 URL 붙여넣기 ({og_mark}): {stripped}")
                    else:
                        logger.warning("클립보드 붙여넣기 후 URL 미감지 — 다음 방법 시도")
                except Exception as clip_err:
                    logger.warning(f"클립보드 방법 실패: {clip_err}")

                # ── 방법 2: iframe 내부 clipboard + paste ──
                if not url_inserted:
                    try:
                        for frame in page.frames:
                            try:
                                has_editor = await frame.evaluate("!!document.querySelector('[contenteditable=true]')")
                                if not has_editor:
                                    continue
                                # iframe 내부에서 직접 clipboard 쓰기 + paste 이벤트
                                await frame.evaluate(f"""(async () => {{
                                    const el = document.querySelector("[contenteditable='true']");
                                    if (!el) return;
                                    el.focus();
                                    try {{ await navigator.clipboard.writeText('{stripped}'); }} catch(e) {{}}
                                    document.execCommand('insertText', false, '{stripped}');
                                }})()""")
                                await asyncio.sleep(3)
                                # Enter 키로 URL 자동 감지 트리거
                                await page.keyboard.press("Enter")
                                await asyncio.sleep(8)  # OG 로딩 대기
                                if await _verify_url_in_editor(stripped):
                                    url_inserted = True
                                    og_detected = await _verify_url_in_editor(stripped, check_og=True)
                                    if log:
                                        og_mark = "✅ OG" if og_detected else "⚠️ 텍스트"
                                        log(f"   🔗 URL iframe 삽입 ({og_mark}): {stripped}")
                                    break
                            except Exception:
                                continue
                    except Exception as e2:
                        logger.warning(f"iframe 방법 실패: {e2}")

                # ── 방법 3: 타이핑 + Enter (에디터 URL 자동 감지) ──
                if not url_inserted:
                    try:
                        await target_el.click()
                        await asyncio.sleep(0.3)
                        await target_el.type(stripped, delay=15)
                        await asyncio.sleep(1)
                        # Enter로 URL 자동 감지 트리거
                        await page.keyboard.press("Enter")
                        await asyncio.sleep(8)  # OG 로딩 대기
                        if await _verify_url_in_editor(stripped):
                            url_inserted = True
                            og_detected = await _verify_url_in_editor(stripped, check_og=True)
                            if log:
                                og_mark = "✅ OG" if og_detected else "⚠️ 텍스트"
                                log(f"   🔗 URL 타이핑+Enter ({og_mark}): {stripped}")
                        else:
                            logger.warning("타이핑+Enter 후에도 URL 미감지")
                    except Exception as type_err:
                        logger.warning(f"URL 타이핑 실패: {type_err}")

                # ── 방법 4: dispatchEvent paste ──
                if not url_inserted:
                    try:
                        for frame in page.frames:
                            try:
                                await frame.evaluate(f"""(() => {{
                                    const el = document.querySelector("[contenteditable='true']") || document.querySelector(".se-content");
                                    if (el) {{
                                        el.focus();
                                        const dt = new DataTransfer();
                                        dt.setData('text/plain', '{stripped}');
                                        const evt = new ClipboardEvent('paste', {{clipboardData: dt, bubbles: true, cancelable: true}});
                                        el.dispatchEvent(evt);
                                    }}
                                }})()""")
                                await asyncio.sleep(8)
                                if await _verify_url_in_editor(stripped):
                                    url_inserted = True
                                    og_detected = await _verify_url_in_editor(stripped, check_og=True)
                                    if log:
                                        og_mark = "✅ OG" if og_detected else "⚠️ 텍스트"
                                        log(f"   🔗 URL paste이벤트 ({og_mark}): {stripped}")
                                    break
                            except Exception:
                                continue
                    except Exception as paste_err:
                        logger.warning(f"paste 이벤트 방법 실패: {paste_err}")

                # OG 미리보기 최종 확인 — 텍스트는 있는데 OG가 없으면 재시도
                if url_inserted and not og_detected:
                    if log:
                        log(f"   🔄 OG 미리보기 미생성 — Enter 후 재대기...")
                    try:
                        await page.keyboard.press("Enter")
                        await asyncio.sleep(10)
                        og_detected = await _verify_url_in_editor(stripped, check_og=True)
                        if og_detected and log:
                            log(f"   ✅ OG 미리보기 생성 확인!")
                        elif log:
                            log(f"   ⚠️ OG 미리보기 미생성 — URL 텍스트만 표시됩니다")
                    except Exception:
                        pass

                if not url_inserted and log:
                    log(f"   ❌ URL 삽입 모든 방법 실패: {stripped}")
            elif stripped:
                await target_el.type(stripped, delay=50)
                await asyncio.sleep(0.3)

            if i < len(lines) - 1:
                await target_el.press("Enter")
                await asyncio.sleep(0.2)
                # 다음 줄도 빈 줄이면 추가 Enter (2줄 띄우기)
                if not stripped and i + 1 < len(lines) and not lines[i + 1].strip():
                    await target_el.press("Enter")
                    await asyncio.sleep(0.1)

            line_num += 1

        logger.info(f"본문 입력 완료 ({len(lines)}줄)")
        if log:
            log(f"   ✅ 본문 입력 완료 ({len(lines)}줄)")

        await asyncio.sleep(5)  # 에디터 렌더링 안정화 대기 (충분히)

        # ════════════════════════════════════════
        # 2단계: 헤딩 줄에 서식 적용 (폰트 19 + 볼드)
        # ════════════════════════════════════════
        if heading_line_numbers:
            if log:
                log(f"   🎨 서식 적용 시작 (헤딩 {len(heading_line_numbers)}개)")

            # 맨 위로 이동
            await target_el.press("Control+Home")
            await asyncio.sleep(0.5)

            current_line = 0
            for h_line in heading_line_numbers:
                # 해당 줄까지 이동
                while current_line < h_line:
                    await target_el.press("ArrowDown")
                    await asyncio.sleep(0.05)
                    current_line += 1

                # 줄 전체 선택 (Home → Shift+End)
                await target_el.press("Home")
                await asyncio.sleep(0.1)
                await target_el.press("Shift+End")
                await asyncio.sleep(0.5)

                # 서식 적용
                font_ok = await _set_font_size(page, "19", log)
                await target_el.press("Control+b")
                await asyncio.sleep(0.3)

                if log:
                    log(f"      🎨 줄 {h_line}: 폰트19={'✅' if font_ok else '❌'} + 볼드")

                # 선택 해제
                await target_el.press("End")
                await asyncio.sleep(0.2)

        # ════════════════════════════════════════
        # 3단계: 전체 선택 후 줄간격 200% 적용
        # ════════════════════════════════════════
        await _set_line_spacing(page, target_el, "200", log)

    except Exception as e:
        logger.warning(f"본문 타이핑 실패: {e}")
        if log:
            log(f"   ⚠️ 본문 입력 실패: {e}")


async def _set_line_spacing(page, editor_el, spacing_value: str, log=None):
    """본문 전체 선택 후 줄간격 설정 — page의 모든 frame 탐색"""
    try:
        # 전체 선택 (Ctrl+A)
        await editor_el.press("Control+a")
        await asyncio.sleep(0.5)

        # 줄간격 버튼 탐색 (모든 frame)
        btn = None
        for frame in page.frames:
            try:
                candidate = frame.locator("button[data-name='line-height']").first
                if await candidate.count() > 0:
                    btn = candidate
                    break
                candidate = frame.locator(".se-line-height-toolbar-button").first
                if await candidate.count() > 0:
                    btn = candidate
                    break
            except Exception:
                continue

        if not btn:
            logger.warning("줄간격 버튼 못 찾음")
            if log:
                log("   ❌ 줄간격 버튼을 찾지 못했습니다")
            await editor_el.press("End")
            return

        await btn.click()
        await asyncio.sleep(0.8)

        # 200% 옵션 탐색 (모든 frame)
        opt = None
        for frame in page.frames:
            try:
                candidate = frame.locator(f"button[data-value='{spacing_value}'][data-name='line-height']").first
                if await candidate.count() > 0:
                    opt = candidate
                    break
                candidate = frame.locator(f".se-toolbar-option-line-height-{spacing_value}-button").first
                if await candidate.count() > 0:
                    opt = candidate
                    break
            except Exception:
                continue

        if opt:
            await opt.click()
            await asyncio.sleep(0.5)
            logger.info(f"줄간격 {spacing_value}% 설정 완료")
            if log:
                log(f"   ✅ 줄간격 {spacing_value}% 설정 완료")
        else:
            if log:
                log(f"   ⚠️ 줄간격 {spacing_value}% 옵션 못 찾음")
            await editor_el.press("Escape")

        await editor_el.press("End")
    except Exception as e:
        logger.warning(f"줄간격 설정 오류: {e}")
        if log:
            log(f"   ⚠️ 줄간격 설정 오류: {e}")


async def _set_font_size(page, size: str, log=None) -> bool:
    """Smart Editor 3 폰트 크기 변경 — page의 모든 frame에서 프로퍼티 툴바 탐색"""
    try:
        btn = None
        # page의 모든 frame에서 폰트 크기 버튼 탐색
        for frame in page.frames:
            try:
                candidate = frame.locator("button[data-name='font-size']").first
                if await candidate.count() > 0:
                    try:
                        await candidate.wait_for(state="visible", timeout=1500)
                        btn = candidate
                        break
                    except Exception:
                        pass
            except Exception:
                continue

        if not btn:
            # 클래스명으로 재탐색
            for frame in page.frames:
                try:
                    candidate = frame.locator(".se-font-size-code-toolbar-button").first
                    if await candidate.count() > 0:
                        try:
                            await candidate.wait_for(state="visible", timeout=1000)
                            btn = candidate
                            break
                        except Exception:
                            pass
                except Exception:
                    continue

        if not btn:
            logger.warning("폰트 크기 버튼 못 찾음")
            if log:
                log("      ❌ 폰트 크기 버튼 없음")
            return False

        await btn.click()
        await asyncio.sleep(0.8)

        # 드롭다운에서 사이즈 옵션 선택 (모든 frame 탐색)
        opt = None
        option_selectors = [
            f"button[data-value='{size}'][data-name='font-size']",
            f"[data-value='{size}'][data-role='option']",
            f"[data-value='{size}']",
            f"button.se-font-size-option:has-text('{size}')",
            f".se-property-toolbar-label-select-option button:has-text('{size}')",
            f"button:text-is('{size}')",
        ]
        for frame in page.frames:
            for sel in option_selectors:
                try:
                    candidate = frame.locator(sel).first
                    if await candidate.count() > 0:
                        opt = candidate
                        break
                except Exception:
                    continue
            if opt:
                break

        if opt:
            await opt.click()
            await asyncio.sleep(0.3)
            logger.info(f"폰트 크기 {size} 설정")
            if log:
                log(f"      ✅ 폰트 {size} 적용 완료")
            return True

        if log:
            log(f"      ⚠️ 폰트 {size} 옵션 못 찾음")
        await btn.click()
        await asyncio.sleep(0.3)
        return False
    except Exception as e:
        logger.warning(f"폰트 크기 설정 오류: {e}")
        return False


async def _reset_font_size(page, log=None):
    """폰트 크기를 기본값(13 또는 15)으로 복원"""
    return await _set_font_size(page, "13", log)


async def _toggle_bold(frame_locator, editor_el, on=True):
    """볼드 토글 (Ctrl+B)"""
    try:
        # 키보드 단축키로 볼드 토글
        await editor_el.press("Control+b")
        await asyncio.sleep(0.1)
        logger.info(f"볼드 {'ON' if on else 'OFF'}")
    except Exception as e:
        logger.warning(f"볼드 토글 실패: {e}")


async def _insert_link_on_selection(page, frame_locator, url: str, log=None) -> bool:
    """선택된 텍스트에 링크를 거는 함수 (툴바 링크 버튼 사용)"""
    try:
        link_btn_selectors = [
            "button[data-name='link']",
            ".se-toolbar button.se-link-toolbar-button",
            "button[aria-label*='링크']",
            "button[title*='링크']",
        ]
        for sel in link_btn_selectors:
            try:
                btn = frame_locator.locator(sel).first
                if await btn.count() > 0:
                    await btn.click()
                    await asyncio.sleep(1.5)

                    # URL 입력란 찾기
                    url_input_selectors = [
                        "input[placeholder*='URL']",
                        "input[placeholder*='url']",
                        "input[placeholder*='링크']",
                        "input[placeholder*='주소']",
                        ".se-popup-link input[type='text']",
                        "input.se-popup-link-input",
                    ]
                    for inp_sel in url_input_selectors:
                        try:
                            url_input = frame_locator.locator(inp_sel).first
                            if await url_input.count() > 0:
                                await url_input.click()
                                await url_input.fill("")
                                await asyncio.sleep(0.3)
                                await url_input.type(url, delay=5)
                                await asyncio.sleep(0.5)

                                # 확인 버튼
                                for cfm_sel in ["button:has-text('확인')", "button:has-text('적용')", "button.se-popup-button-confirm"]:
                                    try:
                                        cfm = frame_locator.locator(cfm_sel).first
                                        if await cfm.count() > 0:
                                            await cfm.click()
                                            await asyncio.sleep(1)
                                            logger.info(f"링크 삽입 완료 (선택 텍스트): {url}")
                                            if log:
                                                log(f"   🔗 링크 삽입 완료: {url}")
                                            return True
                                    except Exception:
                                        continue
                        except Exception:
                            continue

                    # 팝업 닫기
                    for close_sel in ["button:has-text('취소')", "button[class*='cancel']", ".se-popup-close"]:
                        try:
                            cb = frame_locator.locator(close_sel).first
                            if await cb.count() > 0:
                                await cb.click()
                                break
                        except Exception:
                            continue
                    break
            except Exception:
                continue
    except Exception as e:
        logger.warning(f"선택 텍스트 링크 삽입 실패: {e}")

    return False


async def _insert_link_via_editor(page, frame_locator, editor_el, url: str, text: str, log=None) -> bool:
    """
    Smart Editor 3에 클릭 가능한 URL 삽입
    방법 1: 에디터 툴바 링크 버튼
    방법 2: JavaScript로 <a> 태그 직접 삽입
    방법 3: execCommand insertHTML
    """

    # ── 방법 1: 에디터 툴바 링크 버튼 ──
    try:
        link_btn_selectors = [
            "button[data-name='link']",
            "button[data-type='link']",
            "button[aria-label*='링크']",
            "button[title*='링크']",
            "button[data-command='link']",
            ".se-toolbar button.se-link-toolbar-button",
            "button.se-text-paragraph-toolbar-button-link",
        ]
        for sel in link_btn_selectors:
            try:
                btn = frame_locator.locator(sel).first
                if await btn.count() > 0:
                    await btn.click()
                    await asyncio.sleep(1.5)
                    logger.info(f"링크 버튼 클릭 성공: {sel}")

                    # URL 입력란 찾기 (팝업/패널)
                    url_input_selectors = [
                        "input[placeholder*='URL']",
                        "input[placeholder*='url']",
                        "input[placeholder*='링크']",
                        "input[placeholder*='주소']",
                        ".se-popup-link input[type='text']",
                        ".se-link-popup input",
                        "input.se-popup-link-input",
                        "input[class*='link']",
                    ]
                    for inp_sel in url_input_selectors:
                        try:
                            url_input = frame_locator.locator(inp_sel).first
                            if await url_input.count() > 0:
                                await url_input.click()
                                await url_input.fill("")
                                await asyncio.sleep(0.3)
                                await url_input.type(url, delay=5)
                                await asyncio.sleep(0.5)
                                logger.info(f"URL 입력 완료: {inp_sel}")

                                # 확인 버튼 클릭
                                confirm_selectors = [
                                    "button:has-text('확인')",
                                    "button:has-text('적용')",
                                    "button.se-popup-button-confirm",
                                    ".se-popup-link button.se-popup-button-confirm",
                                    "button[class*='confirm']",
                                    "button[class*='apply']",
                                ]
                                for cfm_sel in confirm_selectors:
                                    try:
                                        cfm = frame_locator.locator(cfm_sel).first
                                        if await cfm.count() > 0:
                                            await cfm.click()
                                            await asyncio.sleep(1)
                                            logger.info(f"링크 삽입 완료 (툴바): {url}")
                                            if log:
                                                log(f"   🔗 링크 삽입 완료: {url}")
                                            return True
                                    except Exception:
                                        continue
                        except Exception:
                            continue

                    # 팝업 닫기
                    for close_sel in ["button:has-text('취소')", "button[class*='cancel']", ".se-popup-close"]:
                        try:
                            cb = frame_locator.locator(close_sel).first
                            if await cb.count() > 0:
                                await cb.click()
                                await asyncio.sleep(0.3)
                                break
                        except Exception:
                            continue
                    break
            except Exception:
                continue
    except Exception as e:
        logger.warning(f"링크 버튼 방식 실패: {e}")

    # ── 방법 2: JavaScript로 <a> 태그 직접 삽입 ──
    try:
        # iframe 내부 frame 객체 가져오기
        frames = page.frames
        for frame in frames:
            if 'cafe_main' in (frame.name or ''):
                result = await frame.evaluate("""(url) => {
                    const sel = window.getSelection();
                    if (sel && sel.rangeCount > 0) {
                        const range = sel.getRangeAt(0);
                        const a = document.createElement('a');
                        a.href = url;
                        a.textContent = url;
                        a.target = '_blank';
                        a.rel = 'noopener';
                        range.insertNode(a);
                        range.setStartAfter(a);
                        range.collapse(true);
                        sel.removeAllRanges();
                        sel.addRange(range);
                        return true;
                    }
                    return false;
                }""", url)
                if result:
                    logger.info(f"링크 삽입 완료 (JS): {url}")
                    if log:
                        log(f"   🔗 링크 삽입 완료 (JS): {url}")
                    return True
    except Exception as e:
        logger.warning(f"JS 링크 삽입 실패: {e}")

    # ── 방법 3: execCommand insertHTML ──
    try:
        frames = page.frames
        for frame in frames:
            if 'cafe_main' in (frame.name or ''):
                html = f'<a href="{url}" target="_blank" rel="noopener">{url}</a>'
                result = await frame.evaluate(
                    """(html) => document.execCommand('insertHTML', false, html)""",
                    html
                )
                if result:
                    logger.info(f"링크 삽입 완료 (execCommand): {url}")
                    if log:
                        log(f"   🔗 링크 삽입 완료: {url}")
                    return True
    except Exception as e:
        logger.warning(f"execCommand 링크 삽입 실패: {e}")

    logger.warning(f"모든 링크 삽입 방법 실패: {url}")
    return False


async def upload_image_from_url_iframe(page, frame_locator, img_url: str, log=None):
    """네이버 카페 에디터(iframe)에 이미지 업로드

    Smart Editor 3 구조:
    - 에디터가 iframe#cafe_main 내부에 있음
    - 이미지 버튼 클릭 → file_chooser 이벤트 발생
    - file input이 iframe 안 또는 바깥에 존재할 수 있음
    """
    if not img_url:
        return

    # ── 이미지 다운로드 ──────────────────────
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        res = requests.get(img_url, headers=headers, timeout=15)
        if res.status_code != 200:
            logger.warning(f"이미지 다운로드 실패: {res.status_code}")
            if log:
                log(f"   ⚠️ 이미지 다운로드 실패 ({res.status_code})")
            return
    except Exception as e:
        logger.warning(f"이미지 다운로드 오류: {e}")
        if log:
            log(f"   ⚠️ 이미지 다운로드 오류: {e}")
        return

    ext = "jpg"
    ct = res.headers.get("content-type", "")
    if "png" in ct:
        ext = "png"
    elif "webp" in ct:
        ext = "webp"

    tmp_dir = tempfile.gettempdir()
    tmp_path = os.path.join(tmp_dir, f"naver_img_{datetime.now().strftime('%H%M%S%f')}.{ext}")
    with open(tmp_path, "wb") as f:
        f.write(res.content)

    # ── 얼굴 감지 → 얼굴 아래만 크롭 ──────────────
    try:
        import cv2
        import numpy as np
        img_cv = cv2.imdecode(np.frombuffer(open(tmp_path, "rb").read(), np.uint8), cv2.IMREAD_COLOR)
        if img_cv is not None:
            gray = cv2.cvtColor(img_cv, cv2.COLOR_BGR2GRAY)
            face_cascade = cv2.CascadeClassifier(cv2.data.haarcascades + "haarcascade_frontalface_default.xml")
            faces = face_cascade.detectMultiScale(gray, scaleFactor=1.1, minNeighbors=5, minSize=(30, 30))
            if len(faces) > 0:
                # 얼굴 영역 중 가장 아래쪽 하단 y 좌표 계산
                face_bottom = max(y + h for (x, y, w, h) in faces)
                # 얼굴 아래 약간 여유(20px)를 두고 크롭
                crop_y = min(face_bottom + 20, img_cv.shape[0])
                remaining_height = img_cv.shape[0] - crop_y
                # 남은 영역이 원본의 30% 이상일 때만 크롭 (너무 작으면 스킵)
                if remaining_height >= img_cv.shape[0] * 0.3:
                    cropped = img_cv[crop_y:, :]
                    cv2.imwrite(tmp_path, cropped, [cv2.IMWRITE_JPEG_QUALITY, 90])
                    logger.info(f"👤 얼굴 감지 → 크롭 완료: 상단 {crop_y}px 제거 (남은 높이 {remaining_height}px)")
                    if log:
                        log(f"   👤 얼굴 감지 → 얼굴 제외하고 크롭")
                else:
                    # 크롭 후 남는 영역이 너무 작으면 이미지 스킵
                    logger.warning(f"👤 얼굴 감지 — 크롭 후 남은 영역 부족, 이미지 스킵")
                    if log:
                        log(f"   ⚠️ 얼굴 이미지 — 크롭 불가, 스킵")
                    os.remove(tmp_path)
                    return
    except ImportError:
        logger.warning("OpenCV 미설치 — 얼굴 감지 건너뜀")
    except Exception as e:
        logger.warning(f"얼굴 감지 처리 오류 (원본 유지): {e}")

    # 이미지 리사이즈 (가로 최대 800px)
    try:
        from PIL import Image
        with Image.open(tmp_path) as img:
            if img.width > 800:
                ratio = 800 / img.width
                new_size = (800, int(img.height * ratio))
                img = img.resize(new_size, Image.LANCZOS)
                img.save(tmp_path, quality=85)
                logger.info(f"이미지 리사이즈: → {new_size[0]}x{new_size[1]}")
    except ImportError:
        logger.warning("Pillow 미설치 — 이미지 리사이즈 건너뜀")
    except Exception as e:
        logger.warning(f"이미지 리사이즈 실패: {e}")

    logger.info(f"이미지 저장: {tmp_path} ({os.path.getsize(tmp_path):,} bytes)")

    try:
        # ── 방법 1: iframe 내 이미지 버튼 클릭 → file_chooser ──
        # Smart Editor 3 툴바 사진 버튼 셀렉터
        img_btn_selectors = [
            "button[data-name='image']",
            "button[data-type='image']",
            "button.se-image-toolbar-button",
            "button[class*='image']",
            "button[aria-label*='사진']",
            "button[aria-label*='이미지']",
            "button[aria-label*='Photo']",
            "button[title*='사진']",
            "button[title*='이미지']",
            ".se-toolbar-item-image button",
            "li.se-toolbar-item-image button",
            "button.tool_photo",
            "a.se-oglink-toolbar-button",
        ]

        for sel in img_btn_selectors:
            try:
                el = frame_locator.locator(sel).first
                if await el.count() > 0:
                    logger.info(f"이미지 버튼 발견 (iframe): {sel}")
                    async with page.expect_file_chooser(timeout=5000) as fc_info:
                        await el.click()
                    fc = await fc_info.value
                    await fc.set_files(tmp_path)
                    await asyncio.sleep(3)
                    logger.info(f"이미지 업로드 완료 (iframe 버튼): {sel}")
                    if log:
                        log(f"   ✅ 이미지 업로드 완료")
                    return
            except Exception as e:
                logger.debug(f"iframe 이미지 버튼 시도 실패 ({sel}): {e}")
                continue

        # ── 방법 2: 메인 페이지에서 이미지 버튼 찾기 ──
        # iframe 바깥(상위 페이지)에 에디터 툴바가 있는 경우
        for sel in img_btn_selectors:
            try:
                el = page.locator(sel).first
                if await el.count() > 0:
                    logger.info(f"이미지 버튼 발견 (메인): {sel}")
                    async with page.expect_file_chooser(timeout=5000) as fc_info:
                        await el.click()
                    fc = await fc_info.value
                    await fc.set_files(tmp_path)
                    await asyncio.sleep(3)
                    logger.info(f"이미지 업로드 완료 (메인 버튼): {sel}")
                    if log:
                        log(f"   ✅ 이미지 업로드 완료")
                    return
            except Exception as e:
                logger.debug(f"메인 이미지 버튼 시도 실패 ({sel}): {e}")
                continue

        # ── 방법 3: iframe 내 file input 직접 ──
        for sel in ["input[type='file'][accept*='image']", "input[type='file']"]:
            try:
                el = frame_locator.locator(sel).first
                if await el.count() > 0:
                    await el.set_input_files(tmp_path)
                    await asyncio.sleep(3)
                    logger.info(f"이미지 업로드 완료 (iframe file input): {sel}")
                    if log:
                        log(f"   ✅ 이미지 업로드 완료")
                    return
            except Exception:
                continue

        # ── 방법 4: 메인 페이지 file input 직접 ──
        for sel in ["input[type='file'][accept*='image']", "input[type='file']"]:
            try:
                el = page.locator(sel).first
                if await el.count() > 0:
                    await el.set_input_files(tmp_path)
                    await asyncio.sleep(3)
                    logger.info(f"이미지 업로드 완료 (메인 file input): {sel}")
                    if log:
                        log(f"   ✅ 이미지 업로드 완료")
                    return
            except Exception:
                continue

        # ── 방법 5: JavaScript로 file input 생성 후 트리거 ──
        try:
            # iframe 내부 프레임 직접 접근
            frames = page.frames
            editor_frame = None
            for f in frames:
                if "cafe_main" in (f.name or "") or "cafe" in (f.url or ""):
                    editor_frame = f
                    break

            if editor_frame:
                # 숨겨진 file input 찾기
                file_inputs = await editor_frame.query_selector_all("input[type='file']")
                if file_inputs:
                    await file_inputs[0].set_input_files(tmp_path)
                    await asyncio.sleep(3)
                    logger.info("이미지 업로드 완료 (frame query)")
                    if log:
                        log(f"   ✅ 이미지 업로드 완료")
                    return
        except Exception as e:
            logger.debug(f"frame query 시도 실패: {e}")

        logger.warning("이미지 업로드: 모든 방법 실패 — file input을 찾지 못했습니다")
        if log:
            log(f"   ⚠️ 이미지 업로드 실패 — 에디터에서 이미지 버튼을 찾지 못했습니다")

        # 디버그: 현재 페이지 구조 출력
        try:
            btn_count = await frame_locator.locator("button").count()
            input_count = await frame_locator.locator("input[type='file']").count()
            logger.info(f"디버그: iframe 내 button={btn_count}개, file input={input_count}개")
            # 툴바 버튼 목록 출력
            for i in range(min(btn_count, 20)):
                btn = frame_locator.locator("button").nth(i)
                attrs = await btn.evaluate(
                    "el => ({tag: el.tagName, cls: el.className, name: el.getAttribute('data-name'), aria: el.getAttribute('aria-label'), title: el.title, text: el.textContent?.trim()?.slice(0,30)})"
                )
                logger.info(f"  button[{i}]: {attrs}")
        except Exception:
            pass

    finally:
        try:
            os.remove(tmp_path)
        except Exception:
            pass


async def input_tags_iframe(frame_locator, tags: list, log=None):
    """iframe 내 태그 입력 (#일본구매대행 #브랜드명 #모델명)"""
    if not tags:
        return

    tag_selectors = [
        "input[placeholder*='태그']",
        "input[placeholder*='Tag']",
        "input[class*='tag']",
        "input[id*='tag']",
    ]

    tag_el = None
    for sel in tag_selectors:
        try:
            el = frame_locator.locator(sel).first
            if await el.count() > 0:
                tag_el = el
                break
        except Exception:
            continue

    if not tag_el:
        if log:
            log("   ⚠️ 태그 입력란을 찾지 못했습니다")
        return

    try:
        entered = 0
        for tag in tags:
            # 매번 입력란을 다시 찾기 (Enter 후 DOM이 변경될 수 있음)
            current_el = None
            for sel in tag_selectors:
                try:
                    el = frame_locator.locator(sel).first
                    if await el.count() > 0:
                        current_el = el
                        break
                except Exception:
                    continue
            if not current_el:
                current_el = tag_el

            await current_el.click()
            await asyncio.sleep(0.3)
            # 기존 내용 지우기
            await current_el.press("Control+a")
            await asyncio.sleep(0.1)
            await current_el.type(tag, delay=30)
            await asyncio.sleep(0.5)
            await current_el.press("Enter")
            await asyncio.sleep(1)
            entered += 1
            if log:
                log(f"   🏷️ 태그 {entered}/{len(tags)}: #{tag}")
        if log:
            log(f"   ✅ 태그 입력 완료: {entered}개")
    except Exception as e:
        if log:
            log(f"   ⚠️ 태그 입력 실패 ({entered}/{len(tags)}): {e}")


async def type_content_to_editor(page, content: str):
    """
    Smart Editor 3 (새 네이버 카페 UI) 본문 입력

    구조: .se-content > .se-canvas > .se-section-text (클릭 가능한 영역)
    방법: 클릭 후 클립보드 붙여넣기 (긴 텍스트 타이핑보다 빠름)
    """
    # HTML 태그 제거한 순수 텍스트 (에디터에 직접 입력)
    # <img src="..."> 태그는 에디터 내 이미지로 처리 불가 → URL 텍스트로 변환
    import re
    plain = re.sub(r'<img\s+src="([^"]+)"[^>]*>', r'\n[이미지: \1]', content)
    plain = re.sub(r'<[^>]+>', '', plain)  # 나머지 HTML 태그 제거
    plain = plain.strip()

    editor_selectors = [
        ".se-content",
        ".se-section-text",
        "[class*='se-module-text']",
        "[contenteditable=true]",
    ]

    clicked = False
    for sel in editor_selectors:
        try:
            el = page.locator(sel).first
            if await el.count() > 0:
                await el.click()
                clicked = True
                logger.info(f"에디터 클릭: {sel}")
                break
        except Exception:
            continue

    if not clicked:
        logger.warning("에디터 영역을 찾지 못했습니다")
        return

    await asyncio.sleep(0.5)

    # 방법 1: DataTransfer paste 이벤트 시뮬레이션 (클립보드 API 불필요)
    try:
        pasted = await page.evaluate("""(text) => {
            const dt = new DataTransfer();
            dt.setData('text/plain', text);
            const target = document.activeElement || document.body;
            target.dispatchEvent(new ClipboardEvent('paste', {
                clipboardData: dt, bubbles: true, cancelable: true
            }));
            return true;
        }""", plain)
        await asyncio.sleep(1)
        logger.info("본문 붙여넣기 완료 (DataTransfer paste)")
        return
    except Exception as e:
        logger.warning(f"DataTransfer paste 실패: {e} → keyboard.type으로 전환")

    # 방법 2: 직접 키보드 타이핑 (느리지만 확실)
    logger.info(f"keyboard.type 시작 (글자 수: {len(plain)})")
    await page.keyboard.type(plain, delay=5)
    logger.info("본문 키보드 입력 완료")


async def upload_image_from_url(page, img_url: str):
    """
    이미지 URL → 파일 다운로드 → 에디터에 파일 업로드

    전략:
    1) 에디터 이미지 버튼 클릭 → file input 활성화 → set_input_files
    2) 숨겨진 input[type=file] 직접 set_input_files (fallback)
    """
    if not img_url:
        return

    # ── 이미지 다운로드 ──────────────────────
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        res = requests.get(img_url, headers=headers, timeout=15)
        if res.status_code != 200:
            logger.warning(f"이미지 다운로드 실패: {res.status_code}")
            return
    except Exception as e:
        logger.warning(f"이미지 다운로드 오류: {e}")
        return

    # 확장자 추출 (png/jpg/webp 등)
    ext = "jpg"
    ct = res.headers.get("content-type", "")
    if "png" in ct:
        ext = "png"
    elif "webp" in ct:
        ext = "webp"
    elif "gif" in ct:
        ext = "gif"

    tmp_dir = tempfile.gettempdir()
    tmp_path = os.path.join(tmp_dir, f"naver_img_{datetime.now().strftime('%H%M%S%f')}.{ext}")
    with open(tmp_path, "wb") as f:
        f.write(res.content)

    logger.info(f"이미지 저장: {tmp_path} ({len(res.content):,} bytes)")

    # ── 얼굴 감지 → 얼굴 아래만 크롭 ──────────────
    try:
        import cv2
        import numpy as np
        img_cv = cv2.imdecode(np.frombuffer(open(tmp_path, "rb").read(), np.uint8), cv2.IMREAD_COLOR)
        if img_cv is not None:
            gray = cv2.cvtColor(img_cv, cv2.COLOR_BGR2GRAY)
            face_cascade = cv2.CascadeClassifier(cv2.data.haarcascades + "haarcascade_frontalface_default.xml")
            faces = face_cascade.detectMultiScale(gray, scaleFactor=1.1, minNeighbors=5, minSize=(30, 30))
            if len(faces) > 0:
                face_bottom = max(y + h for (x, y, w, h) in faces)
                crop_y = min(face_bottom + 20, img_cv.shape[0])
                remaining_height = img_cv.shape[0] - crop_y
                if remaining_height >= img_cv.shape[0] * 0.3:
                    cropped = img_cv[crop_y:, :]
                    cv2.imwrite(tmp_path, cropped, [cv2.IMWRITE_JPEG_QUALITY, 90])
                    logger.info(f"👤 얼굴 감지 → 크롭 완료: 상단 {crop_y}px 제거")
                else:
                    logger.warning(f"👤 얼굴 감지 — 크롭 후 남은 영역 부족, 이미지 스킵")
                    os.remove(tmp_path)
                    return
    except ImportError:
        logger.warning("OpenCV 미설치 — 얼굴 감지 건너뜀")
    except Exception as e:
        logger.warning(f"얼굴 감지 처리 오류 (원본 유지): {e}")

    try:
        # ── 방법 1: 에디터 이미지 버튼 클릭 후 file_chooser 이용 ──
        img_btn_selectors = [
            "button[data-name='image']",
            "button[aria-label*='사진']",
            "button[aria-label*='이미지']",
            "button[title*='사진']",
            "button[title*='이미지']",
            ".se-toolbar button[class*='image']",
            "[class*='image'] button",
        ]
        for sel in img_btn_selectors:
            try:
                el = page.locator(sel).first
                if await el.count() > 0:
                    async with page.expect_file_chooser(timeout=4000) as fc_info:
                        await el.click()
                    fc = await fc_info.value
                    await fc.set_files(tmp_path)
                    await asyncio.sleep(2)
                    logger.info(f"이미지 업로드 완료 (버튼 클릭): {sel}")
                    return
            except Exception:
                continue

        # ── 방법 2: hidden input[type=file] 직접 set_input_files ──
        file_input_selectors = [
            "input[type='file'][accept*='image']",
            "input[type='file']",
        ]
        for sel in file_input_selectors:
            try:
                el = page.locator(sel).first
                if await el.count() > 0:
                    await el.set_input_files(tmp_path)
                    await asyncio.sleep(2)
                    logger.info(f"이미지 업로드 완료 (file input): {sel}")
                    return
            except Exception:
                continue

        logger.warning("이미지 업로드: file input을 찾지 못했습니다")

    finally:
        try:
            os.remove(tmp_path)
        except Exception:
            pass


# =============================================
# 게시글 템플릿
# =============================================

def make_post_title(product: dict, price_info: dict) -> str:
    """게시글 제목 생성"""
    source_type = product.get("source_type", "sports")
    name = product.get("name_ko") or product.get("name", "상품명 없음")
    brand = product.get("brand_ko") or product.get("brand", "")
    price_krw = format_price(price_info["price_final"])

    if source_type == "vintage":
        grade = product.get("condition_grade", "")
        grade_labels = {"NS":"신품","S":"S급","A":"A급","B":"B급","C":"C급","D":"D급"}
        grade_text = grade_labels.get(grade, "")
        title = f"[{brand}] {name}"
        if grade_text:
            title += f" [{grade_text}]"
        if len(title) > 75:
            title = title[:72] + "..."
        return f"{title} / {price_krw}"
    else:
        title = f"[{brand}] {name}"
        if len(title) > 80:
            title = title[:77] + "..."
        return f"{title} / {price_krw}"


def make_post_content(product: dict, price_info: dict) -> str:
    """게시글 본문 생성"""
    source_type = product.get("source_type", "sports")
    if source_type == "vintage":
        return _make_vintage_content(product, price_info)
    return _make_sports_content(product, price_info)


def _make_vintage_content(product: dict, price_info: dict) -> str:
    """빈티지 상품 카페 게시글 본문"""
    name = product.get("name_ko") or product.get("name", "")
    brand = product.get("brand", "")
    code = product.get("product_code", "")
    grade = product.get("condition_grade", "")
    grade_labels = {"NS":"신품/미사용","S":"중고S (최상)","A":"중고A (양호)","B":"중고B (사용감 있음)","C":"중고C (사용감 많음)","D":"중고D (난있음)"}
    grade_text = grade_labels.get(grade, grade)
    material = product.get("material", "")
    color = product.get("color", "")
    desc = product.get("description_ko") or product.get("description", "")
    price_krw = format_price(price_info["price_final"])

    # B2B 가격 (5% 할인)
    import math
    b2b_price = int(math.ceil(price_info["price_final"] * 0.95 / 100) * 100)
    b2b_text = format_price(b2b_price)

    shop_url = f"https://vintage.theone-biz.com/shop?code={code}"

    content = f"""★ 사업자 회원 5% 할인! B2B 파트너 모집 중 ★
TheOne Vintage에서 사업자 등록 시 모든 상품 5% 자동 할인!
▶ 가입: vintage.theone-biz.com

━━━━━━━━━━━━━━━━━━

[{brand}] {name}

상품번호: {code}
상태: {grade_text}

━━━━━━━━━━━━━━━━━━

✅ 구매대행가: {price_krw}
💼 사업자 회원가: {b2b_text} (5% 할인)

(관부가세/해외배송비 별도)

━━━━━━━━━━━━━━━━━━"""

    if material:
        content += f"\n소재: {material}"
    if color:
        content += f"\n사이즈: {color}"

    if desc and len(desc) > 10:
        content += f"\n\n📋 상품 설명:\n{desc[:300]}"

    content += f"""

━━━━━━━━━━━━━━━━━━

🛒 온라인 구매: {shop_url}
💬 카카오톡 상담: TheOne Vintage 채널

━━━━━━━━━━━━━━━━━━

★ TheOne Vintage ★
일본 현지 프리미엄 빈티지 구매대행 전문
✔ 정품 보증 ✔ 안전 배송 ✔ 실시간 환율 적용

💼 사업자 회원 혜택
✔ 전 상품 B2C 대비 5% 할인
✔ AI 상품 분석 기능 제공
✔ 대량 구매 추가 상담 가능

▶ 회원가입: vintage.theone-biz.com"""

    return content.strip()


def _make_sports_content(product: dict, price_info: dict) -> str:
    """스포츠 상품 카페 게시글 본문 (기존)"""
    name = product.get("name_ko") or product.get("name", "상품명 없음")
    name_ja = product.get("name", "")
    brand = product.get("brand_ko") or product.get("brand", "")
    link = product.get("link", "")
    code = product.get("product_code", "")

    # 사이즈 정보
    sizes = product.get("sizes", [])
    available_sizes = [s["size"] for s in sizes if s.get("in_stock")]
    size_text = ", ".join(available_sizes) if available_sizes else "문의 바랍니다"

    content = f"""[{brand}] {name}

상품번호: {code}
상품명(일본어): {name_ja}

━━━━━━━━━━━━━━━━━━

💴 일본 현지가: ¥{price_info['price_jpy']:,}
💱 적용 환율: 1엔 = {price_info['rate']}원
📦 국제배송비 포함

✅ 구매대행가: {format_price(price_info['price_final'])}

━━━━━━━━━━━━━━━━━━

📏 재고 사이즈: {size_text}

🔗 일본 상품 링크:
{link}

━━━━━━━━━━━━━━━━━━
※ 환율 변동에 따라 가격이 달라질 수 있습니다.
※ 구매 문의는 댓글 또는 쪽지로 연락주세요!
※ 주문 후 배송까지 약 7~14일 소요됩니다."""

    return content.strip()
