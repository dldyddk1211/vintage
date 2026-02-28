"""
scraper.py
Xebio 사이트에서 セール → NIKE 브랜드 필터 후 상품 수집
"""

import asyncio
import json
import os
import re
import logging
from datetime import datetime
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

from config import (
    XEBIO_BASE_URL, SCRAPE_DELAY, OUTPUT_DIR, IMAGE_DIR
)

logger = logging.getLogger(__name__)

# app.py의 status 딕셔너리를 참조하기 위한 전역 참조
_app_status = None

def set_app_status(status_dict):
    """app.py에서 status 딕셔너리를 주입받아 일시정지/리셋 신호 감지"""
    global _app_status
    _app_status = status_dict

def _check_flag(flag: str) -> bool:
    """일시정지(pause) 또는 중단(stop) 플래그 확인"""
    if _app_status is None:
        return False
    if flag == "pause":
        return _app_status.get("paused", False)
    if flag == "stop":
        return _app_status.get("stop_requested", False)
    return False


# =============================================
# 메인 스크래핑 함수
# =============================================

async def scrape_nike_sale(status_callback=None, max_pages=None):
    """
    Xebio 메인 → セール → NIKE 브랜드 필터 → 전체 상품 수집

    Args:
        status_callback : 진행상황 문자열을 실시간으로 전달할 콜백 함수
        max_pages       : 테스트용 최대 페이지 수 (None = 전체 수집)

    Returns:
        list: 수집된 상품 딕셔너리 리스트
    """
    os.makedirs(IMAGE_DIR, exist_ok=True)
    products = []

    def log(msg):
        logger.info(msg)
        if status_callback:
            status_callback(msg)

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,          # 브라우저 창이 보이게
            slow_mo=300,             # 동작 사이 딜레이(ms)
            args=["--no-sandbox", "--disable-dev-shm-usage",
                  "--window-size=1280,900"]
        )
        context = await browser.new_context(
            locale="ja-JP",
            viewport={"width": 1280, "height": 900},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            )
        )
        page = await context.new_page()

        try:
            # ── STEP 1: Xebio 메인 접속 ──────────────────
            log("━" * 45)
            log("🚀 [STEP 1/4] Xebio 메인 페이지 접속 중...")
            log(f"   🌐 접속 URL: {XEBIO_BASE_URL}")
            await page.goto(XEBIO_BASE_URL, wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(2)
            log("   ✅ 메인 페이지 접속 완료!")

            # ── STEP 2: セール 카테고리 클릭 ─────────────
            log("━" * 45)
            log("🏷️  [STEP 2/4] セール(세일) 카테고리 탐색 중...")
            sale_ok = await click_sale_category(page)
            if not sale_ok:
                log("   ❌ セール 카테고리를 찾지 못했습니다")
                log("   💡 사이트 구조가 변경됐을 수 있어요")
                return []
            await page.wait_for_load_state("networkidle", timeout=20000)
            await asyncio.sleep(2)
            log("   ✅ セール 페이지 이동 완료!")
            log(f"   🔗 현재 URL: {page.url}")

            # ── STEP 3: 세일 전체 상품 수집 준비 ────────
            log("━" * 45)
            log("🛍️  [STEP 3/4] 세일 전체 상품 수집 준비 중...")
            log("   📋 브랜드 필터 없이 전체 세일 상품을 수집합니다")
            log(f"   ✅ 준비 완료! 현재 URL: {page.url}")

            # ── STEP 4: 페이지 순회하며 상품 수집 ──────────
            log("━" * 45)
            log("📦 [STEP 4/4] 상품 전체 수집 시작!")
            total = await get_total_count(page)
            if total:
                log(f"   📊 총 수집 대상: 약 {total:,}개 상품")

            current_page = 1
            while True:
                # ── 리셋 요청 체크 ──────────────────────
                if status_callback and _check_flag("stop"):
                    log("🔄 리셋 요청 — 수집을 중단합니다")
                    products = []
                    return []

                # ── 일시정지 체크 ───────────────────────
                if status_callback and _check_flag("pause"):
                    log("⏸️ 일시정지 중... (재개 버튼을 누르면 계속됩니다)")
                    while _check_flag("pause"):
                        await asyncio.sleep(1)
                        if _check_flag("stop"):
                            log("🔄 리셋 요청 — 수집을 중단합니다")
                            return []
                    log("▶️ 수집 재개!")

                log(f"   📄 [{current_page}페이지] 상품 파싱 중...")
                page_products = await parse_product_list(page)
                products.extend(page_products)
                pct = int(len(products) / total * 100) if total else 0
                bar = "█" * (pct // 5) + "░" * (20 - pct // 5)
                log(f"   [{bar}] {pct}% — {len(page_products)}개 수집 / 누적: {len(products):,}개")

                if max_pages and current_page >= max_pages:
                    log(f"   🛑 테스트 모드: {max_pages}페이지에서 중단")
                    break

                has_next = await go_next_page(page)
                if not has_next:
                    log("   ✅ 마지막 페이지 도달!")
                    break

                current_page += 1
                await asyncio.sleep(SCRAPE_DELAY)

        except PlaywrightTimeout as e:
            log(f"⏰ 타임아웃: {e}")
        except Exception as e:
            log(f"❌ 오류 발생: {e}")
            logger.exception(e)
        finally:
            await browser.close()

    if products:
        save_products(products)
        log("━" * 45)
        log(f"🎉 스크래핑 완료!")
        log(f"   📦 총 수집: {len(products):,}개 상품")
        log(f"   💾 결과 저장: output/latest.json")
        log("━" * 45)

    return products


# =============================================
# 사이트 조작 함수
# =============================================

async def click_sale_category(page):
    """메인에서 セール 메뉴 클릭"""
    candidates = [
        "a:has-text('セール')",
        "a[href*='sale']",
        "a[href*='xb_sale']",
        "text=セール",
    ]
    for sel in candidates:
        try:
            el = page.locator(sel).first
            if await el.count() > 0:
                await el.click()
                return True
        except Exception:
            continue

    # 모든 <a> 태그에서 텍스트 검색
    for link in await page.locator("a").all():
        try:
            text = (await link.inner_text()).strip()
            href = await link.get_attribute("href") or ""
            if "セール" in text or "sale" in href.lower():
                await link.click()
                return True
        except Exception:
            continue

    return False


async def select_nike_brand(page):
    """ブランドで絞り込む 섹션에서 NIKE 선택"""
    candidates = [
        "img[alt='NIKE']",
        "img[alt*='Nike']",
        "a:has-text('NIKE')",
        "label:has-text('NIKE')",
        "text=NIKE",
        "[data-brand='NIKE']",
    ]
    for sel in candidates:
        try:
            el = page.locator(sel).first
            if await el.count() > 0:
                await el.click()
                return True
        except Exception:
            continue

    # ブランドで絞り込む 섹션의 첫 번째 아이템 클릭
    brand_sections = [
        "text=ブランドで絞り込む",
        "[class*='brand-filter']",
        "[class*='brand_filter']",
    ]
    for bsel in brand_sections:
        try:
            section = page.locator(bsel).first
            if await section.count() > 0:
                first = section.locator("a, label, button, li").first
                if await first.count() > 0:
                    await first.click()
                    return True
        except Exception:
            continue

    return False


async def get_total_count(page):
    """페이지에서 총 상품 수 파싱"""
    for sel in [".product-count", "[class*='count']", "[class*='total']", ".search-results"]:
        try:
            el = page.locator(sel).first
            if await el.count() > 0:
                text = await el.inner_text()
                numbers = re.findall(r'[\d,]+', text)
                if numbers:
                    return int(numbers[0].replace(",", ""))
        except Exception:
            continue
    return 0


async def parse_product_list(page):
    """현재 페이지 상품 목록 파싱"""
    products = []

    card_selectors = [
        ".product-tile",
        ".product-item",
        ".product-list__item",
        "[class*='product-card']",
        "li.product",
        ".item-box",
    ]

    items = None
    for sel in card_selectors:
        cnt = await page.locator(sel).count()
        if cnt > 0:
            items = page.locator(sel)
            logger.info(f"선택자 매칭: {sel} ({cnt}개)")
            break

    if items is None:
        logger.warning("상품 카드 선택자 미매칭")
        return []

    for i in range(await items.count()):
        try:
            product = await extract_product_info(items.nth(i))
            if product:
                products.append(product)
        except Exception as e:
            logger.debug(f"상품 {i} 파싱 오류: {e}")

    return products


async def extract_product_info(item):
    """상품 카드 하나에서 상품명 / 가격 / 링크 / 이미지 추출"""
    try:
        # 상품명
        name = ""
        for sel in [".product-name", ".product-title", "h2", "h3", "[class*='name']"]:
            el = item.locator(sel).first
            if await el.count() > 0:
                name = (await el.inner_text()).strip()
                if name:
                    break

        # 가격 (엔화)
        price_jpy = 0
        for sel in [".price", ".product-price", "[class*='price']", ".sale-price", ".new-price"]:
            el = item.locator(sel).first
            if await el.count() > 0:
                txt = await el.inner_text()
                numbers = re.findall(r'[\d,]+', txt)
                if numbers:
                    price_jpy = int(numbers[0].replace(",", ""))
                    break

        # 상품 링크
        link = ""
        a_tag = item.locator("a").first
        if await a_tag.count() > 0:
            href = await a_tag.get_attribute("href") or ""
            link = href if href.startswith("http") else (XEBIO_BASE_URL + href if href else "")

        # 이미지 URL
        img_url = ""
        for sel in ["img", "[class*='image'] img", ".product-image img"]:
            img = item.locator(sel).first
            if await img.count() > 0:
                src = (await img.get_attribute("src") or
                       await img.get_attribute("data-src") or "")
                if src:
                    img_url = src if src.startswith("http") else (
                        "https:" + src if src.startswith("//") else XEBIO_BASE_URL + src
                    )
                break

        if not name and not link:
            return None

        # 품번 추출 (URL 마지막 경로 또는 상품 코드 영역)
        product_code = ""
        if link:
            import re as _re
            m = _re.search(r'/([A-Z0-9\-]+)(?:\?|$)', link)
            if m:
                product_code = m.group(1)

        # 브랜드 추출
        brand = ""
        for sel in ["[class*='brand']", ".product-brand", ".brand-name"]:
            el = item.locator(sel).first
            if await el.count() > 0:
                brand = (await el.inner_text()).strip()
                break

        return {
            "name": name,
            "brand": brand,
            "product_code": product_code,
            "price_jpy": price_jpy,
            "link": link,
            "img_url": img_url,
            "scraped_at": datetime.now().isoformat(),
            "selected": True,   # 기본값: 업로드 대상
        }

    except Exception as e:
        logger.debug(f"extract 오류: {e}")
        return None


async def go_next_page(page):
    """다음 페이지 버튼 클릭, 없으면 False"""
    for sel in [
        "a[aria-label='次へ']",
        "a.pagination__next",
        ".pagination__item--next a",
        "a:has-text('次へ')",
        "[class*='next']:not([class*='disabled']) a",
    ]:
        try:
            btn = page.locator(sel).first
            if await btn.count() > 0:
                cls = await btn.get_attribute("class") or ""
                disabled = await btn.get_attribute("disabled")
                if "disabled" not in cls and disabled is None:
                    await btn.click()
                    await page.wait_for_load_state("networkidle", timeout=15000)
                    return True
        except Exception:
            continue
    return False


# =============================================
# 저장 / 불러오기
# =============================================

def save_products(products: list) -> str:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(OUTPUT_DIR, f"products_{ts}.json")

    for p in [path, os.path.join(OUTPUT_DIR, "latest.json")]:
        with open(p, "w", encoding="utf-8") as f:
            json.dump(products, f, ensure_ascii=False, indent=2)
    return path


def load_latest_products() -> list:
    path = os.path.join(OUTPUT_DIR, "latest.json")
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# 단독 테스트
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    results = asyncio.run(scrape_nike_sale(max_pages=2))
    print(f"\n총 {len(results)}개 수집")
    if results:
        print(json.dumps(results[0], ensure_ascii=False, indent=2))