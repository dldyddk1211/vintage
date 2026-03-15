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
    XEBIO_BASE_URL, XEBIO_DOMAIN, SCRAPE_DELAY, OUTPUT_DIR, IMAGE_DIR
)
from translator import translate_ja_ko, translate_brand
from site_config import get_site, get_category, build_url

logger = logging.getLogger(__name__)

# app.py의 status 딕셔너리를 참조하기 위한 전역 참조
_app_status = None

# 실행 중인 브라우저 인스턴스 (리셋 시 강제 종료용)
_browser = None
_playwright = None

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


async def force_close_browser():
    """리셋 시 실행 중인 브라우저 강제 종료"""
    global _browser, _playwright
    try:
        if _browser:
            await _browser.close()
            _browser = None
            logger.info("🔄 브라우저 강제 종료 완료")
        if _playwright:
            await _playwright.stop()
            _playwright = None
    except Exception as e:
        logger.debug(f"브라우저 종료 오류 (무시): {e}")


# =============================================
# 메인 스크래핑 함수
# =============================================

def _parse_pages(pages_str: str) -> list:
    """페이지 지정 문자열 파싱 → 정렬된 페이지 번호 리스트 반환
    '2-10' → [2,3,...,10], '2,3,5' → [2,3,5], '2' → [2], '' → []
    """
    if not pages_str or not pages_str.strip():
        return []
    pages_str = pages_str.strip()
    result = set()
    for part in pages_str.split(","):
        part = part.strip()
        if "-" in part:
            bounds = part.split("-", 1)
            try:
                start, end = int(bounds[0].strip()), int(bounds[1].strip())
                for p in range(start, end + 1):
                    if p >= 1:
                        result.add(p)
            except ValueError:
                continue
        else:
            try:
                p = int(part)
                if p >= 1:
                    result.add(p)
            except ValueError:
                continue
    return sorted(result)


async def scrape_nike_sale(status_callback=None,
                           site_id="xebio", category_id="sale",
                           keyword="", pages="", brand_code=""):
    """
    지정 사이트/카테고리에서 상품 수집

    Args:
        status_callback : 진행상황 문자열을 실시간으로 전달할 콜백 함수
        site_id         : 사이트 ID (예: "xebio")
        category_id     : 카테고리 ID (예: "sale", "running")
        keyword         : 검색 키워드 (비어있으면 전체)
        pages           : 페이지 지정 (예: "2-10", "2,3,5", "2", 비우면 전체)
        brand_code      : 브랜드 코드 (예: "004278" = 나이키, 비우면 전체)

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
        global _browser, _playwright
        _playwright = p
        browser = await p.chromium.launch(
            headless=False,          # 브라우저 창이 보이게
            slow_mo=300,             # 동작 사이 딜레이(ms)
            args=["--no-sandbox", "--disable-dev-shm-usage",
                  "--window-size=1280,900"]
        )
        _browser = browser  # 전역에 저장 (리셋 시 강제 종료용)
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
            # ── 사이트/카테고리 URL 결정 ─────────────────
            site_info = get_site(site_id)
            cat_info = get_category(site_id, category_id)
            CATEGORY_URL = build_url(site_id, category_id, brand_code)
            site_name = site_info["name"] if site_info else "Xebio"
            cat_name = cat_info["name"] if cat_info else "세일"
            if brand_code:
                from site_config import get_brands
                brand_names = get_brands(site_id)
                brand_label = brand_names.get(brand_code, brand_code)
                cat_name = f"{cat_name} + {brand_label}"
            base_url = site_info["base_url"] if site_info else XEBIO_BASE_URL

            # 페이지 지정 파싱
            target_pages = _parse_pages(pages)

            # fallback: site_config에 없으면 기존 세일 URL 사용
            if not CATEGORY_URL:
                CATEGORY_URL = "https://www.supersports.com/ja-jp/xebio/products/?discount=sale"
                base_url = XEBIO_BASE_URL

            kw_label = f" [{keyword}]" if keyword else ""

            # ── STEP 1: 메인 접속 ──────────────────
            log("━" * 45)
            log(f"🚀 [STEP 1/5] {site_name} 메인 페이지 접속 중...")
            log(f"   🌐 접속 URL: {base_url}")
            await page.goto(base_url, wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(2)
            log("   ✅ 메인 페이지 접속 완료!")

            # ── STEP 2: 카테고리 이동 ────────
            log("━" * 45)
            log(f"🏷️  [STEP 2/5] {cat_name} 페이지로 이동 중...")
            await page.goto(CATEGORY_URL, wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(3)
            log(f"   ✅ {cat_name} 페이지 이동 완료!")
            log(f"   🔗 현재 URL: {page.url}")

            # ── STEP 3: 키워드 검색 (입력값이 있을 때만) ────────
            if keyword:
                log("━" * 45)
                log(f"🔍 [STEP 3/5] 키워드 검색: {keyword}")

                # 사이트 내 검색창 찾기 (さらに絞り込む)
                search_selectors = [
                    'input[placeholder*="絞り込む"]',
                    'input[placeholder*="さらに"]',
                    'input.middle[type="search"]',
                    'input[type="search"]',
                ]
                search_input = None
                for sel in search_selectors:
                    try:
                        el = page.locator(sel).first
                        if await el.is_visible(timeout=3000):
                            search_input = el
                            log(f"   ✅ 검색창 발견: {sel}")
                            break
                    except Exception:
                        continue

                if search_input:
                    # 검색창에 키워드 입력 후 Enter
                    await search_input.click()
                    await asyncio.sleep(0.5)
                    await search_input.fill(keyword)
                    await asyncio.sleep(0.5)
                    await search_input.press("Enter")
                    log(f"   ⌨️ '{keyword}' 입력 후 검색 실행")
                    await asyncio.sleep(3)
                    # 검색 결과 로딩 대기
                    for sel in [".product-tile", ".product-item", "[class*='product-card']"]:
                        try:
                            await page.wait_for_selector(sel, timeout=8000)
                            break
                        except Exception:
                            continue
                    log(f"   ✅ 검색 완료! URL: {page.url}")
                else:
                    log("   ⚠️ 검색창을 찾지 못함 — 키워드 없이 진행합니다")
            else:
                log("━" * 45)
                log(f"🔍 [STEP 3/5] 키워드 없음 — 전체 상품 수집")

            # ── STEP 4: 수집 준비 ────────
            log("━" * 45)
            log(f"🛍️  [STEP 4/5] {site_name} › {cat_name}{kw_label} 상품 수집 준비 중...")
            if target_pages:
                log(f"   📄 지정 페이지: {pages}")
            else:
                log("   📋 전체 상품을 수집합니다")
            log(f"   ✅ 준비 완료! 현재 URL: {page.url}")

            # ── STEP 5: 페이지 순회하며 상품 수집 ──────────
            log("━" * 45)
            log(f"📦 [STEP 5/5] 상품 수집 시작!{kw_label}")
            total = await get_total_count(page)
            if total:
                log(f"   📊 총 수집 대상: 약 {total:,}개 상품")

            current_page = 1
            prev_product_links = set()  # 중복 감지용

            # 페이지 지정 모드: 지정된 페이지만 수집
            # target_pages가 있으면 해당 페이지까지 순회하되 지정 페이지만 수집
            max_target_page = max(target_pages) if target_pages else None

            while True:
                # ── 리셋 체크 ───────────────────────────
                if status_callback and _check_flag("stop"):
                    log("🔄 리셋 요청 — 수집을 중단합니다")
                    return []

                # ── 일시정지 체크 ───────────────────────
                if status_callback and _check_flag("pause"):
                    log("⏸️ 일시정지 중... (재개 버튼을 누르면 계속됩니다)")
                    while _check_flag("pause"):
                        await asyncio.sleep(1)
                        if _check_flag("stop"):
                            return []
                    log("▶️ 수집 재개!")

                # ── 1페이지는 현재 페이지 그대로 (카테고리/검색 결과), 이후는 다음 버튼 클릭 ──
                if current_page == 1:
                    log(f"   📄 [{current_page}페이지] 상품 파싱 중...")
                    log(f"   🔗 {page.url}")
                else:
                    log(f"   📄 [{current_page}페이지] 다음 페이지로 이동 중...")
                    moved = await go_next_page(page)
                    if not moved:
                        log("   ✅ 마지막 페이지 도달! (다음 버튼 없음)")
                        break
                    # SPA 렌더링 대기 — 상품 카드가 나타날 때까지
                    for sel in [".product-tile", ".product-item", "[class*='product-card']"]:
                        try:
                            await page.wait_for_selector(sel, timeout=5000)
                            break
                        except Exception:
                            continue
                    await asyncio.sleep(3)

                # ── 페이지 지정 모드: 해당 페이지가 아니면 건너뛰기 ──
                if target_pages and current_page not in target_pages:
                    log(f"   ⏭️ [{current_page}페이지] 건너뜀 (지정 페이지 아님)")
                    if max_target_page and current_page >= max_target_page:
                        log(f"   🛑 지정 페이지 수집 완료!")
                        break
                    current_page += 1
                    await asyncio.sleep(0.5)
                    continue

                actual_url = page.url
                log(f"   ✅ 실제 URL: {actual_url}")

                page_products = await parse_product_list(page)
                log(f"   📦 이 페이지에서 수집: {len(page_products)}개")

                # 상품이 없으면 마지막 페이지
                if not page_products:
                    log("   ✅ 마지막 페이지 도달! (상품 없음)")
                    break

                # 이전 페이지와 완전 동일한 상품이면 중단
                curr_links = set(p.get("link", "") for p in page_products)
                if curr_links and curr_links == prev_product_links:
                    log(f"   ⚠️ 이전 페이지와 동일한 상품 — 마지막 페이지로 판단")
                    break
                prev_product_links = curr_links

                products.extend(page_products)
                pct = int(len(products) / total * 100) if total else 0
                bar = "█" * (pct // 5) + "░" * (20 - pct // 5)
                log(f"   [{bar}] {pct}% — {len(page_products)}개 수집 / 누적: {len(products):,}개")

                # 지정 페이지 모드: 마지막 지정 페이지 도달 시 중단
                if max_target_page and current_page >= max_target_page:
                    log(f"   🛑 지정 페이지 수집 완료!")
                    break

                current_page += 1
                await asyncio.sleep(SCRAPE_DELAY)

            # ── STEP 5: 상세 페이지 수집 ──────────────────
            log("━" * 45)
            log(f"🔎 [STEP 6] 상품 상세 페이지 수집 시작!")
            log(f"   📋 총 {len(products):,}개 상품 상세 페이지 방문 예정")
            log(f"   ⏱️  예상 소요 시간: 약 {len(products) * 2 // 60}분")

            for i, product in enumerate(products, 1):
                # 리셋/일시정지 체크
                if status_callback and _check_flag("stop"):
                    log("🔄 리셋 요청 — 상세 수집 중단")
                    return []
                while status_callback and _check_flag("pause"):
                    log("⏸️ 일시정지 중...")
                    await asyncio.sleep(1)
                    if _check_flag("stop"):
                        return []
                    log("▶️ 재개!")

                link = product.get("link", "")
                if not link:
                    continue

                pct = int(i / len(products) * 100)
                bar = "█" * (pct // 5) + "░" * (20 - pct // 5)
                if i % 10 == 1 or i == len(products):
                    log(f"   [{bar}] {pct}% — {i:,}/{len(products):,} 상세 수집 중...")

                try:
                    # 링크 정규화 - 잘못된 URL 보정
                    if not link.startswith("http"):
                        link = "https://www.supersports.com" + link
                    # 첫 3개 상품은 링크 로그 출력 (디버깅용)
                    if i <= 3:
                        log(f"   🔗 상세 URL 확인: {link}")
                    detail = await scrape_detail_page(page, link)
                    product.update(detail)
                except Exception as e:
                    log(f"   ⚠️ 상세 오류 ({link[:60]}): {e}")

                # 매 상품마다 리셋 체크
                if status_callback and _check_flag("stop"):
                    log("🔄 리셋 — 상세 수집 즉시 중단")
                    return []

                await asyncio.sleep(SCRAPE_DELAY)

            log("   ✅ 상세 페이지 수집 완료!")

        except PlaywrightTimeout as e:
            log(f"⏰ 타임아웃: {e}")
        except Exception as e:
            log(f"❌ 오류 발생: {e}")
            logger.exception(e)
        finally:
            await browser.close()
            _browser = None
            _playwright = None

    if products:
        # 상품에 사이트/카테고리 정보 태깅
        for p in products:
            p["site_id"] = site_id
            p["category_id"] = category_id

        # ── 빅데이터 DB 중복 체크 & 필터링 ──────────
        try:
            from product_db import bulk_exists, insert_products
            existing = bulk_exists(site_id, products)
            if existing:
                before = len(products)
                products = [
                    p for p in products
                    if (p.get("product_code", ""), p.get("price_jpy", 0)) not in existing
                ]
                skipped = before - len(products)
                log(f"   🔍 빅데이터 중복 체크: {skipped}개 중복 제외 → {len(products)}개 신규")
            # DB에 누적 저장
            new_count = insert_products(products)
            log(f"   💾 빅데이터 DB: {new_count}개 신규 저장")
        except Exception as e:
            logger.warning(f"빅데이터 DB 처리 실패: {e}")

        # 기존 latest.json에서 완료/중복 상품 보존 후 새 상품 병합
        try:
            old_products = load_latest_products()
            preserved = [p for p in old_products if p.get("cafe_status") in ("완료", "중복")]
            preserved_codes = {p.get("product_code") for p in preserved if p.get("product_code")}
            # 새 상품 중 보존 품번과 겹치지 않는 것만 추가
            new_only = [p for p in products if p.get("product_code") not in preserved_codes]
            merged = preserved + new_only
            if preserved:
                log(f"   📌 기존 완료/중복 {len(preserved)}개 보존 + 신규 {len(new_only)}개 병합")
            save_products(merged)
        except Exception as e:
            logger.warning(f"상품 병합 저장 실패, 신규만 저장: {e}")
            save_products(products)

        # 수집 이력 기록
        try:
            from scrape_history import add_history
            brand_label_for_history = ""
            if brand_code:
                from site_config import get_brands
                brand_names = get_brands(site_id)
                brand_label_for_history = brand_names.get(brand_code, brand_code)
            add_history(site_id, category_id, len(products), keyword=keyword, brand=brand_label_for_history)
        except Exception as e:
            logger.warning(f"수집 이력 저장 실패: {e}")

        log("━" * 45)
        log(f"🎉 전체 수집 완료!")
        log(f"   📦 총 수집: {len(products):,}개 상품 (목록 + 상세)")
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
                # click() 대신 goto()로 직접 이동 시도
                href = await el.get_attribute("href") or ""
                if href:
                    url = href if href.startswith("http") else (XEBIO_BASE_URL + href)
                    await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                else:
                    await el.click()
                return True
        except Exception:
            continue

    # 모든 <a> 태그에서 텍스트/href 검색 → 직접 URL 이동
    for link in await page.locator("a").all():
        try:
            text = (await link.inner_text()).strip()
            href = await link.get_attribute("href") or ""
            if "セール" in text or "sale" in href.lower():
                url = href if href.startswith("http") else (XEBIO_BASE_URL + href)
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)
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
    """
    상품 카드에서 정보 추출
    실제 선택자 기준:
      브랜드 : b.caption
      상품명 : b.title
      가격   : strong.sale
    """
    try:
        # ── 브랜드 ────────────────────────────────
        # <b class="jsx-XXXX caption">ナイキ</b>
        brand = ""
        for sel in [
            "b[class*='caption']",
            "b.caption",
            "[class*='caption']",
        ]:
            el = item.locator(sel).first
            if await el.count() > 0:
                txt = (await el.inner_text()).strip()
                if txt:
                    brand = txt
                    break

        # ── 상품명 ────────────────────────────────
        # <b class="jsx-XXXX title">商品名...</b>
        name = ""
        for sel in [
            "b[class*='title']",
            "b.title",
            "[class*='title']",
            "h2", "h3",
        ]:
            el = item.locator(sel).first
            if await el.count() > 0:
                txt = (await el.inner_text()).strip()
                if txt:
                    name = txt
                    break

        # ── 가격 (엔화) ───────────────────────────
        # <strong class="jsx-XXXX sale">￥9,980<span>（税込）</span></strong>
        price_jpy = 0
        for sel in [
            "strong[class*='sale']",
            "strong.sale",
            "[class*='sale'] strong",
            ".price", "strong",
        ]:
            el = item.locator(sel).first
            if await el.count() > 0:
                txt = await el.inner_text()
                # ￥9,980 → 9980
                numbers = re.findall(r'[\d,]+', txt)
                if numbers:
                    val = int(numbers[0].replace(",", ""))
                    if val > 100:
                        price_jpy = val
                        break

        # ── 상품 링크 ─────────────────────────────
        link = ""
        a_tag = item.locator("a").first
        if await a_tag.count() > 0:
            href = await a_tag.get_attribute("href") or ""
            if href.startswith("http"):
                link = href
            elif href.startswith("/"):
                link = XEBIO_DOMAIN + href
            elif href:
                link = XEBIO_DOMAIN + "/" + href

        # ── 이미지 ────────────────────────────────
        img_url = ""
        for sel in ["img", "[class*='image'] img"]:
            img = item.locator(sel).first
            if await img.count() > 0:
                src = (await img.get_attribute("src") or
                       await img.get_attribute("data-src") or "")
                if src and "placeholder" not in src:
                    img_url = src if src.startswith("http") else (
                        "https:" + src if src.startswith("//") else XEBIO_DOMAIN + src
                    )
                break

        if not name and not link:
            return None

        # 번역 (일본어 → 한국어)
        name_ko  = translate_ja_ko(name)
        brand_ko = translate_brand(brand)

        return {
            "name"        : name,          # 일본어 원문
            "name_ko"     : name_ko,       # 한국어 번역
            "brand"       : brand,         # 브랜드 원문
            "brand_ko"    : brand_ko,      # 브랜드 한국어
            "product_code": "",            # 상세 페이지에서 수집
            "price_jpy"   : price_jpy,
            "link"        : link,
            "img_url"     : img_url,
            "scraped_at"  : datetime.now().isoformat(),
            "selected"    : True,
        }

    except Exception as e:
        logger.debug(f"extract 오류: {e}")
        return None


async def scrape_detail_page(page, url: str) -> dict:
    """
    상품 상세 페이지에서 추가 정보 수집
    - 상세 설명
    - 사이즈 목록 + 재고 여부
    - 상세 이미지 여러 장
    - 정가 vs 세일가 (할인율)
    """
    detail = {
        "description"   : "",
        "sizes"         : [],   # [{"size": "25.0", "in_stock": True}, ...]
        "detail_images" : [],   # 추가 이미지 URL 리스트
        "original_price": 0,    # 정가 (엔화)
        "discount_rate" : 0,    # 할인율 (%)
        "in_stock"      : False,
    }

    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=20000)
        await asyncio.sleep(1)

        # ── 품번 (メーカー品番) ─────────────────
        # <span class="jsx-XXXX title">メーカー品番</span>
        # <span class="jsx-XXXX description">FN8454-403</span>
        # → 같은 부모 아래 title + description 쌍으로 존재
        try:
            spec_titles = page.locator("span[class*='title']")
            cnt = await spec_titles.count()
            for i in range(cnt):
                txt = (await spec_titles.nth(i).inner_text()).strip()
                if "品番" in txt or "メーカー" in txt:
                    # 같은 부모(li or div)에서 description span 찾기
                    parent = spec_titles.nth(i).locator("xpath=..")
                    desc = parent.locator("span[class*='description']").first
                    if await desc.count() > 0:
                        code = (await desc.inner_text()).strip()
                        if code:
                            detail["product_code"] = code
                            break
            # 못 찾았을 경우 — 모든 description span 순회
            if not detail.get("product_code"):
                descs = page.locator("span[class*='description']")
                dcnt = await descs.count()
                for i in range(dcnt):
                    val = (await descs.nth(i).inner_text()).strip()
                    # 품번 패턴: 영문+숫자 조합 (예: FN8454-403, AB1234-001)
                    if re.match(r'^[A-Z]{1,4}[\d]', val):
                        detail["product_code"] = val
                        break
        except Exception as e:
            logger.debug(f"품번 추출 오류: {e}")

        # ── 상세 설명 ───────────────────────────
        for sel in [
            "[class*='description']",
            ".product-description",
            ".item-description",
            "#description",
        ]:
            el = page.locator(sel).first
            if await el.count() > 0:
                txt = (await el.inner_text()).strip()
                if txt and len(txt) > 10:
                    detail["description"] = txt[:500]
                    break

        # ── 사이즈 목록 + 재고 ──────────────────
        # Xebio 사이즈 선택 버튼 구조 기반
        size_selectors = [
            "[class*='size'] button",
            "[class*='size'] li",
            "[class*='size-item']",
            "[class*='sizeList'] li",
            "button[class*='size']",
        ]
        for sel in size_selectors:
            items = page.locator(sel)
            cnt = await items.count()
            if cnt > 0:
                sizes = []
                for i in range(cnt):
                    item = items.nth(i)
                    size_text = (await item.inner_text()).strip()
                    size_text = re.sub(r'[^\d.]', '', size_text)  # 숫자/점만 남김
                    cls      = await item.get_attribute("class") or ""
                    disabled = await item.get_attribute("disabled")
                    in_stock = (
                        "sold" not in cls.lower() and
                        "disable" not in cls.lower() and
                        "unavailable" not in cls.lower() and
                        disabled is None
                    )
                    if size_text:
                        sizes.append({"size": size_text, "in_stock": in_stock})
                if sizes:
                    detail["sizes"]    = sizes
                    detail["in_stock"] = any(s["in_stock"] for s in sizes)
                    break

        # 재고 여부 (사이즈 없는 경우)
        if not detail["sizes"]:
            for sel in [".sold-out", "[class*='soldout']", "[class*='outOfStock']"]:
                el = page.locator(sel).first
                if await el.count() > 0:
                    detail["in_stock"] = False
                    break
            else:
                detail["in_stock"] = True

        # ── 정가 vs 세일가 (할인율) ─────────────
        for sel in ["[class*='original']", "[class*='regular']", "[class*='before']"]:
            el = page.locator(sel).first
            if await el.count() > 0:
                txt = await el.inner_text()
                nums = re.findall(r'[\d,]+', txt)
                if nums:
                    val = int(nums[0].replace(",", ""))
                    if val > 100:
                        detail["original_price"] = val
                        break

        # ── 상세 이미지 여러 장 ─────────────────
        img_selectors = [
            "[class*='thumbnail'] img",
            "[class*='gallery'] img",
            "[class*='swiper'] img",
            "[class*='images'] img",
        ]
        for sel in img_selectors:
            imgs = page.locator(sel)
            cnt = await imgs.count()
            if cnt > 1:
                urls = []
                for i in range(min(cnt, 8)):
                    src = (await imgs.nth(i).get_attribute("src") or
                           await imgs.nth(i).get_attribute("data-src") or "")
                    if src and "placeholder" not in src:
                        src = src if src.startswith("http") else ("https:" + src if src.startswith("//") else src)
                        urls.append(src)
                if urls:
                    detail["detail_images"] = urls
                    break

    except Exception as e:
        logger.debug(f"상세 수집 오류: {e}")

    # ── 스펙 항목 전체 수집 (브랜드·품번 백업용) ────
    try:
        specs = {}
        titles = page.locator("span[class*='title']")
        cnt = await titles.count()
        for i in range(cnt):
            title_txt = (await titles.nth(i).inner_text()).strip()
            if not title_txt:
                continue
            parent = titles.nth(i).locator("xpath=..")
            desc_el = parent.locator("span[class*='description']").first
            if await desc_el.count() > 0:
                desc_txt = (await desc_el.inner_text()).strip()
                if desc_txt:
                    specs[title_txt] = desc_txt
        if specs:
            detail["specs"] = specs
            # 브랜드 백업 (목록 카드에서 추출 실패 시 사용)
            if "ブランド" in specs and not detail.get("brand"):
                detail["brand"] = specs["ブランド"]
            # 품번 백업
            if not detail.get("product_code"):
                for key in ["メーカー品番", "品番", "商品コード"]:
                    if key in specs and specs[key]:
                        detail["product_code"] = specs[key]
                        break
        # 최종 fallback: 상품명에서 품번 패턴 추출 (예: HV8150-004, FN8454-403)
        if not detail.get("product_code") and detail.get("name"):
            m = re.search(r'[A-Z]{1,4}\d[\w]*-\d{2,4}', detail["name"])
            if m:
                detail["product_code"] = m.group(0)
    except Exception as e:
        logger.debug(f"스펙 수집 오류: {e}")

    # ── 상세 설명 번역 ──────────────────────────
    if detail.get("description"):
        try:
            detail["description_ko"] = translate_ja_ko(detail["description"])
        except Exception:
            detail["description_ko"] = detail["description"]
    else:
        detail["description_ko"] = ""

    return detail


async def go_next_page(page):
    """다음 페이지 버튼 클릭, 없으면 False"""
    old_url = page.url

    for sel in [
        "a[aria-label='次へ']",
        "a.pagination__next",
        ".pagination__item--next a",
        "a:has-text('次へ')",
        "a:has-text('次のページ')",
        "a:has-text('>')",
        "[class*='next']:not([class*='disabled']) a",
        "nav[class*='pagination'] a:last-child",
        "[class*='pager'] a:last-child",
    ]:
        try:
            btn = page.locator(sel).first
            if await btn.count() > 0:
                cls = await btn.get_attribute("class") or ""
                disabled = await btn.get_attribute("disabled")
                if "disabled" not in cls and disabled is None:
                    # 클릭 전 href 확인 — href가 있으면 직접 이동
                    href = await btn.get_attribute("href") or ""
                    logger.info(f"다음 페이지 버튼 발견: {sel} / href={href[:80]}")

                    if href and href.startswith("http"):
                        await page.goto(href, wait_until="domcontentloaded", timeout=30000)
                    elif href and href.startswith("/"):
                        await page.goto(f"https://www.supersports.com{href}",
                                        wait_until="domcontentloaded", timeout=30000)
                    else:
                        await btn.click()
                        # SPA: URL 변경 또는 콘텐츠 로딩 대기
                        try:
                            await page.wait_for_load_state("networkidle", timeout=15000)
                        except Exception:
                            pass

                    # URL이 바뀌었거나 충분히 대기
                    await asyncio.sleep(3)
                    new_url = page.url
                    logger.info(f"페이지 이동: {old_url[:60]} → {new_url[:60]}")
                    return True
        except Exception as e:
            logger.debug(f"다음 페이지 선택자 오류 ({sel}): {e}")
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
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        # 스크래핑 중 파일 쓰기와 동시 읽기 시 안전 처리
        return []


# 단독 테스트
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    results = asyncio.run(scrape_nike_sale(pages="1-2"))
    print(f"\n총 {len(results)}개 수집")
    if results:
        print(json.dumps(results[0], ensure_ascii=False, indent=2))