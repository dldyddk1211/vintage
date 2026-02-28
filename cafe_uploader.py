"""
cafe_uploader.py
네이버 카페 자동 로그인 + 상품 게시글 업로드
"""

import asyncio
import os
import logging
import requests
from datetime import datetime
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

from config import NAVER_ID, NAVER_PW, CAFE_URL, CAFE_MENU_NAME
from exchange import calc_buying_price, format_price

logger = logging.getLogger(__name__)


# =============================================
# 메인 업로드 함수
# =============================================

async def upload_products(products: list, status_callback=None, max_upload=None):
    """
    상품 리스트를 네이버 카페에 업로드

    Args:
        products       : 스크래퍼에서 받은 상품 딕셔너리 리스트
        status_callback: 진행상황 콜백
        max_upload     : 최대 업로드 개수 (None = 전체)

    Returns:
        int: 업로드 성공 개수
    """
    def log(msg):
        logger.info(msg)
        if status_callback:
            status_callback(msg)

    success_count = 0
    upload_list = products[:max_upload] if max_upload else products

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,  # 네이버 로그인은 창을 보이게 하는 편이 안전
            args=["--no-sandbox"]
        )
        context = await browser.new_context()
        page = await context.new_page()

        try:
            # ── 로그인 ─────────────────────────────────
            log("🔐 네이버 로그인 중...")
            login_ok = await naver_login(page)
            if not login_ok:
                log("❌ 네이버 로그인 실패")
                return 0
            log("✅ 로그인 성공")

            # ── 카페 이동 ──────────────────────────────
            log(f"🏠 카페 이동 중: {CAFE_URL}")
            await page.goto(CAFE_URL, wait_until="domcontentloaded", timeout=20000)
            await asyncio.sleep(2)

            # ── 상품별 업로드 ──────────────────────────
            for i, product in enumerate(upload_list, 1):
                try:
                    log(f"📤 [{i}/{len(upload_list)}] 업로드 중: {product.get('name', '이름없음')[:30]}")
                    ok = await upload_single_product(page, product)
                    if ok:
                        success_count += 1
                        log(f"   ✅ 업로드 성공 ({success_count}개 완료)")
                    else:
                        log(f"   ⚠️ 업로드 실패")
                    await asyncio.sleep(3)  # 게시글 간 딜레이
                except Exception as e:
                    log(f"   ❌ 오류: {e}")
                    continue

        except Exception as e:
            log(f"❌ 전체 오류: {e}")
            logger.exception(e)
        finally:
            await browser.close()

    log(f"🎉 업로드 완료: 총 {success_count}/{len(upload_list)}개 성공")
    return success_count


# =============================================
# 네이버 로그인
# =============================================

async def naver_login(page):
    """네이버 로그인 처리"""
    try:
        await page.goto("https://nid.naver.com/nidlogin.login", timeout=20000)
        await asyncio.sleep(1)

        # 아이디 입력
        await page.fill("#id", NAVER_ID)
        await asyncio.sleep(0.5)

        # 비밀번호 입력
        await page.fill("#pw", NAVER_PW)
        await asyncio.sleep(0.5)

        # 로그인 버튼 클릭
        await page.click(".btn_login")
        await page.wait_for_load_state("networkidle", timeout=15000)
        await asyncio.sleep(2)

        # 로그인 성공 확인 (네이버 메인으로 이동됐는지)
        current_url = page.url
        if "nidlogin" not in current_url:
            return True

        # 2단계 인증이나 캡차 처리 필요한 경우
        logger.warning("로그인 후 URL 확인 필요: " + current_url)
        return False

    except Exception as e:
        logger.error(f"로그인 오류: {e}")
        return False


# =============================================
# 단일 상품 업로드
# =============================================

async def upload_single_product(page, product: dict) -> bool:
    """
    상품 하나를 카페 게시글로 작성
    """
    try:
        # 가격 계산
        price_info = calc_buying_price(product.get("price_jpy", 0))

        # 게시글 제목 & 내용 생성
        title = make_post_title(product)
        content = make_post_content(product, price_info)

        # 글쓰기 페이지 이동
        write_url = f"{CAFE_URL}?iframe_url=/ArticleWrite.nhn"
        await page.goto(write_url, wait_until="domcontentloaded", timeout=20000)
        await asyncio.sleep(2)

        # iframe 진입 (네이버 카페는 iframe 사용)
        frame = page.frame_locator("iframe#cafe_main").first
        if not frame:
            logger.warning("카페 iframe을 찾지 못했습니다")
            return False

        # 게시판 선택
        menu_ok = await select_cafe_menu(frame)
        if not menu_ok:
            logger.warning("게시판 선택 실패")

        # 제목 입력
        await frame.locator("#subject").fill(title)
        await asyncio.sleep(0.5)

        # 에디터에 내용 입력 (스마트에디터 4)
        await type_content_to_editor(frame, content)

        # 이미지 업로드 (이미지 URL이 있는 경우)
        if product.get("img_url"):
            await upload_image_from_url(frame, product["img_url"])

        # 등록 버튼 클릭
        await frame.locator("button.BaseBtn--submit, .btn_upload").click()
        await page.wait_for_load_state("networkidle", timeout=15000)
        await asyncio.sleep(2)

        return True

    except Exception as e:
        logger.error(f"단일 업로드 오류: {e}")
        return False


async def select_cafe_menu(frame):
    """카페 게시판 선택"""
    try:
        menu_btn = frame.locator(f"text={CAFE_MENU_NAME}").first
        if await menu_btn.count() > 0:
            await menu_btn.click()
            await asyncio.sleep(1)
            return True
    except Exception:
        pass
    return False


async def type_content_to_editor(frame, content: str):
    """스마트에디터에 내용 입력"""
    editor_selectors = [
        ".se-content",
        ".se2_input",
        "iframe.se_iframe",
        "[contenteditable=true]",
    ]
    for sel in editor_selectors:
        try:
            el = frame.locator(sel).first
            if await el.count() > 0:
                await el.click()
                await el.type(content, delay=10)
                return
        except Exception:
            continue

    # iframe 안의 body에 직접 입력
    try:
        editor_frame = frame.frame_locator("iframe.se_iframe").content_frame()
        await editor_frame.locator("body").click()
        await editor_frame.locator("body").type(content, delay=10)
    except Exception as e:
        logger.warning(f"에디터 입력 오류: {e}")


async def upload_image_from_url(frame, img_url: str):
    """이미지 URL에서 파일 다운로드 후 업로드"""
    try:
        # 이미지 다운로드
        res = requests.get(img_url, timeout=10)
        if res.status_code != 200:
            return

        tmp_path = f"/tmp/upload_img_{datetime.now().strftime('%H%M%S')}.jpg"
        with open(tmp_path, "wb") as f:
            f.write(res.content)

        # 파일 업로드 입력
        file_input = frame.locator("input[type=file]").first
        if await file_input.count() > 0:
            await file_input.set_input_files(tmp_path)
            await asyncio.sleep(2)

        os.remove(tmp_path)
    except Exception as e:
        logger.warning(f"이미지 업로드 오류: {e}")


# =============================================
# 게시글 템플릿
# =============================================

def make_post_title(product: dict) -> str:
    name = product.get("name", "상품명 없음")
    price_jpy = product.get("price_jpy", 0)
    return f"[일본직구] {name} ¥{price_jpy:,}"


def make_post_content(product: dict, price_info: dict) -> str:
    name = product.get("name", "상품명 없음")
    link = product.get("link", "")

    content = f"""
🛒 상품명: {name}

💴 일본 현지가: ¥{price_info['price_jpy']:,}
💱 적용 환율: 1엔 = {price_info['rate']}원
💰 구매대행가: {format_price(price_info['price_krw_margin'])}
📦 배송비: {format_price(price_info['shipping_fee'])}
✅ 최종 가격: {format_price(price_info['price_final'])}

🔗 일본 상품 링크:
{link}

※ 환율 변동에 따라 가격이 달라질 수 있습니다.
※ 구매 문의는 댓글 또는 쪽지로 연락주세요!
""".strip()

    return content