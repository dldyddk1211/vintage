"""
secondst_crawler.py
2ndstreet.jp 중고/빈티지 상품 크롤러 (Playwright)

사이트: https://www.2ndstreet.jp
검색 URL: /search?category=XXXXXX&page=N
"""

import asyncio
import json
import logging
import re
from datetime import datetime
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

logger = logging.getLogger(__name__)

# ── 상품명 기반 카테고리 자동 분류 ──
_CATEGORY_RULES = [
    # (카테고리ID, 카테고리명, 키워드 목록)
    ("951002", "가방", [
        "バッグ", "リュック", "ポーチ", "ボストン", "クラッチ", "ウエスト",
        "ショルダー", "トート", "ハンド", "バック", "カナパ", "スピーディ",
        "アルマ", "サック", "ボリード", "バーキン", "ケリー",
    ]),
    ("951003", "신발", [
        "シューズ", "スニーカー", "ブーツ", "サンダル", "パンプス",
        "ローファー", "スリッポン", "ミュール",
    ]),
    ("951004", "시계", [
        "腕時計", "ウォッチ", "時計",
    ]),
    ("951005", "악세서리", [
        "ネックレス", "ブレスレット", "リング", "ピアス", "イヤリング",
        "ベルト", "サングラス", "キーケース", "キーリング", "スカーフ",
        "マフラー", "帽子", "キャップ", "財布", "ウォレット", "コインケース",
        "カードケース", "手袋",
    ]),
    ("951001", "의류", [
        "ジャケット", "コート", "シャツ", "パンツ", "スカート", "ワンピース",
        "ブラウス", "ニット", "セーター", "カーディガン", "ベスト",
        "Tシャツ", "スウェット", "パーカー", "ダウン",
    ]),
]

_BREADCRUMB_CATEGORY_MAP = {
    "バッグ": ("951002", "가방"), "bag": ("951002", "가방"),
    "衣類": ("951001", "의류"), "clothing": ("951001", "의류"),
    "シューズ": ("951003", "신발"), "shoes": ("951003", "신발"),
    "時計": ("951004", "시계"), "watch": ("951004", "시계"),
    "アクセサリー": ("951005", "악세서리"), "accessory": ("951005", "악세서리"),
    "ジュエリー": ("951005", "악세서리"),
    "小物": ("951005", "악세서리"),
}


def _classify_category(name: str, breadcrumb: str = "") -> tuple:
    """상품 카테고리 자동 분류 → (category_id, subcategory)
    1차: breadcrumb에서 추출
    2차: 상품명 키워드 매칭
    """
    # 1차: breadcrumb
    if breadcrumb:
        for key, (cat_id, cat_name) in _BREADCRUMB_CATEGORY_MAP.items():
            if key in breadcrumb:
                return cat_id, cat_name

    # 2차: 상품명 키워드
    if name:
        for cat_id, cat_name, keywords in _CATEGORY_RULES:
            for kw in keywords:
                if kw in name:
                    return cat_id, cat_name

    return "", ""


_app_status = None
_browser = None
_playwright = None


def set_app_status(status_dict):
    global _app_status
    _app_status = status_dict


def _check_stop():
    """중지 요청 확인"""
    if _app_status and _app_status.get("stop_requested"):
        return True
    return False


async def force_close_browser():
    """브라우저 강제 종료"""
    global _browser, _playwright
    try:
        if _browser:
            await _browser.close()
    except Exception:
        pass
    try:
        if _playwright:
            await _playwright.stop()
    except Exception:
        pass
    _browser = None
    _playwright = None


async def scrape_2ndstreet(
    status_callback=None,
    category="951002",
    keyword="",
    pages="",
    max_pages=999,
    brand_code="",
    batch_size=10,
    batch_rest=120,
):
    """
    2ndstreet.jp에서 빈티지 상품 수집 (대량 안전 수집)

    pages 미입력 시 → 검색 결과 수 확인 후 전체 페이지 자동 수집
    batch_size 페이지마다 batch_rest초 휴식
    """
    global _browser, _playwright

    def log(msg):
        logger.info(msg)
        if status_callback:
            status_callback(msg)

    import random as _random
    log("🏪 2ndstreet.jp 크롤링 시작")

    # 페이지 범위: 직접 지정 or 자동 (첫 페이지에서 총 수량 확인 후 결정)
    auto_detect_pages = not pages or not pages.strip()
    page_list = _parse_pages(pages, max_pages) if not auto_detect_pages else [1]
    if auto_detect_pages:
        log("   📄 페이지 미지정 → 첫 페이지에서 총 수량 확인 후 자동 설정")

    products = []

    try:
        # ── Playwright 브라우저 시작 ──
        _playwright = await async_playwright().start()
        _browser = await _playwright.chromium.launch(
            headless=False,
            slow_mo=300,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-translate",
                "--disable-features=TranslateUI,Translate",
                "--lang=ja",
                "--accept-lang=ja",
            ],
        )
        context = await _browser.new_context(
            viewport={"width": 1280, "height": 900},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            locale="ja-JP",
            extra_http_headers={"Accept-Language": "ja"},
        )
        # 크롬 번역 완전 비활성화
        await context.add_init_script("""
            Object.defineProperty(navigator, 'language', {get: () => 'ja'});
            Object.defineProperty(navigator, 'languages', {get: () => ['ja', 'ja-JP']});
            // 번역 바 자동 닫기
            const _closeTranslate = () => {
                const bar = document.querySelector('html[class*="translated"]');
                if (bar) { document.documentElement.removeAttribute('class'); }
                const iframe = document.querySelector('.goog-te-banner-frame, #\\:1\\.container');
                if (iframe) iframe.remove();
                const body = document.querySelector('body[style*="top"]');
                if (body) body.style.top = '0';
            };
            setInterval(_closeTranslate, 500);
        """)
        page = await context.new_page()

        for page_num in page_list:
            if _check_stop():
                log("⛔ 중지 요청 — 크롤링 중단")
                break

            # ── URL 구성 ──
            params = []
            if category:
                params.append(f"category={category}")
            if brand_code:
                params.append(f"brand%5B%5D={brand_code}")
            # 컨디션: 전체 (제한 없음)
            params.append("sortBy=recommend")
            if keyword:
                params.append(f"keyword={keyword}")
            params.append(f"page={page_num}")
            url = "https://www.2ndstreet.jp/search?" + "&".join(params)

            log(f"📄 페이지 {page_num} 로딩: {url}")

            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                await asyncio.sleep(5)

                # 1) WorldShopping body-lock 해제 + 오버레이 제거 (클릭 차단 원인)
                try:
                    await page.evaluate("""() => {
                        // WorldShopping body-lock 해제 (이게 쿠키 버튼 클릭을 차단함)
                        document.body.classList.remove('zigzag-worldshopping-style-body-lock');
                        // WorldShopping 오버레이/배너 제거
                        document.querySelectorAll(
                            '[id*="zigzag-worldshopping"], [id*="ws-"], ' +
                            'iframe[src*="worldshopping"], ' +
                            '[class*="WorldShopping"]:not(body):not(script):not(style), ' +
                            '[class*="worldshopping"]:not(body):not(script):not(style)'
                        ).forEach(el => el.remove());
                        document.body.style.overflow = 'auto';
                    }""")
                    log("   🔓 WorldShopping body-lock 해제")
                except Exception:
                    pass
                await asyncio.sleep(1)

                # 2) OneTrust 쿠키 배너 — JS로 직접 클릭 (Playwright 클릭이 차단될 수 있으므로)
                try:
                    clicked = await page.evaluate("""() => {
                        const btn = document.querySelector('#onetrust-accept-btn-handler');
                        if (btn) { btn.click(); return true; }
                        return false;
                    }""")
                    if clicked:
                        log("   🍪 쿠키 배너 닫기 (OneTrust)")
                        await asyncio.sleep(2)
                except Exception:
                    pass

                # 3) 남은 팝업 처리
                for cookie_sel in [
                    "button:has-text('以上の内容を確認しました')",
                    "button:has-text('이상의 내용을 확인했습니다')",
                    "button:has-text('確認しました')",
                    "button:has-text('閉じる')",
                    "button:has-text('닫기')",
                ]:
                    try:
                        btn = page.locator(cookie_sel).first
                        if await btn.count() > 0 and await btn.is_visible():
                            await btn.click(timeout=3000)
                            log(f"   🍪 팝업 닫기: {cookie_sel[:40]}")
                            await asyncio.sleep(1)
                    except Exception:
                        continue

                # 4) 잔여 오버레이 JS 강제 제거
                try:
                    await page.evaluate("""() => {
                        // OneTrust 잔여 요소
                        const ot = document.querySelector('#onetrust-consent-sdk');
                        if (ot) ot.remove();
                        const df = document.querySelector('.onetrust-pc-dark-filter');
                        if (df) df.remove();
                        // 쿠폰/안내 팝업
                        document.querySelectorAll('[class*="coupon"], [class*="Coupon"], [class*="modal-overlay"], [class*="balloon"], [class*="tooltip"], [class*="guide"], [class*="announce"]').forEach(el => el.remove());
                        // 크롬 번역 바
                        document.querySelectorAll('.goog-te-banner-frame, .skiptranslate, #goog-gt-tt').forEach(el => el.remove());
                        if (document.documentElement) document.documentElement.style.top = '0';
                        if (document.body) {
                            document.body.style.top = '0';
                            document.body.style.overflow = 'auto';
                        }
                    }""")
                except Exception:
                    pass

                # 첫 페이지에서 총 상품 수 확인 → 전체 페이지 자동 설정
                if auto_detect_pages and page_num == 1:
                    try:
                        total_text = await page.evaluate("""() => {
                            const els = document.querySelectorAll('*');
                            for (const el of els) {
                                const t = el.innerText || '';
                                const m = t.match(/検索結果[：:]\s*([\d,]+)\s*点/);
                                if (m) return m[1];
                            }
                            return '';
                        }""")
                        if total_text:
                            total_items = int(total_text.replace(",", ""))
                            items_per_page = 30
                            total_pages = (total_items + items_per_page - 1) // items_per_page
                            page_list = list(range(1, total_pages + 1))
                            log(f"   🔢 검색결과: {total_items:,}개 → 총 {total_pages}페이지 수집 예정")
                            log(f"   ⏱️ {batch_size}페이지마다 {batch_rest}초 휴식")
                        else:
                            page_list = list(range(1, 6))
                            log("   ⚠️ 검색결과 수 확인 실패 → 기본 5페이지")
                    except Exception as e:
                        page_list = list(range(1, 6))
                        log(f"   ⚠️ 총 수량 확인 실패: {e} → 기본 5페이지")

            except PlaywrightTimeout:
                log(f"   ⚠️ 페이지 {page_num} 타임아웃")
                continue

            # ── 상품 카드 탐색 ──
            card_selectors = [
                "a[href*='/goods/']",
                "a[href*='/item/']",
                "[class*='itemCard']",
                "[class*='item-card']",
                "[class*='productCard']",
                "[class*='product-card']",
                ".js-click-item",
                "[data-item]",
            ]

            items = []
            used_selector = ""
            for sel in card_selectors:
                try:
                    els = page.locator(sel)
                    cnt = await els.count()
                    if cnt > 0:
                        items = els
                        used_selector = sel
                        log(f"   ✅ 상품 카드 발견: {sel} ({cnt}개)")
                        break
                except Exception:
                    continue

            if not items:
                # 팝업이 남아있을 수 있으므로 JS로 강제 제거 후 재시도
                log("   ⚠️ 상품 카드 미발견 — 팝업 재처리 후 재시도...")
                try:
                    await page.evaluate("""() => {
                        document.body.classList.remove('zigzag-worldshopping-style-body-lock');
                        document.querySelectorAll('[id*="worldshopping"], [class*="worldshopping"]:not(body):not(script):not(style), [class*="WorldShopping"]:not(body):not(script):not(style), [id*="ws-"], [id*="onetrust"], [class*="onetrust"], [class*="cookie"], [class*="modal"], [class*="overlay"], [class*="popup"], [class*="dialog"]').forEach(el => el.remove());
                        document.body.style.overflow = 'auto';
                        if (document.documentElement) document.documentElement.style.top = '0';
                        if (document.body) document.body.style.top = '0';
                    }""")
                except Exception:
                    pass
                await asyncio.sleep(3)

                # 재시도
                for sel in card_selectors:
                    try:
                        els = page.locator(sel)
                        cnt = await els.count()
                        if cnt > 0:
                            items = els
                            used_selector = sel
                            log(f"   ✅ 재시도 성공: {sel} ({cnt}개)")
                            break
                    except Exception:
                        continue

            if not items:
                log("   ⚠️ 상품 카드를 찾을 수 없습니다. 페이지 구조 분석 중...")
                try:
                    html_snippet = await page.evaluate("""() => {
                        const body = document.body;
                        const all = body.querySelectorAll('a[href]');
                        const items = [];
                        for (let i = 0; i < Math.min(all.length, 20); i++) {
                            const a = all[i];
                            if (a.href.includes('/item/') || a.href.includes('/product/')) {
                                items.push({
                                    tag: a.tagName,
                                    href: a.href,
                                    class: a.className.slice(0, 80),
                                    parent: a.parentElement ? a.parentElement.className.slice(0, 80) : ''
                                });
                            }
                        }
                        return JSON.stringify(items, null, 2);
                    }""")
                    log(f"   🔍 발견된 링크 구조:\n{html_snippet}")
                except Exception as e:
                    log(f"   ❌ 구조 분석 실패: {e}")
                continue

            # ── 상품 정보 추출 ──
            cnt = await items.count()
            page_products = []

            for i in range(cnt):
                if _check_stop():
                    break
                try:
                    el = items.nth(i)
                    product = await _extract_product_from_card(el, page)
                    if product and product.get("product_code"):
                        product["source_type"] = "vintage"
                        product["site_id"] = "2ndstreet"
                        product["category_id"] = category
                        product["scraped_at"] = datetime.now().isoformat()
                        page_products.append(product)
                        brand = product.get("brand", "")
                        name = (product.get("name") or "")[:40]
                        price = product.get("price_jpy", 0)
                        log(f"      📌 [{i+1}/{cnt}] {brand} {name} ¥{price:,}")
                except Exception as e:
                    logger.debug(f"상품 추출 오류 [{i}]: {e}")
                    continue

            log(f"   📦 페이지 {page_num}/{len(page_list)}: {len(page_products)}개 수집 (누적 {len(products) + len(page_products)}개)")
            products.extend(page_products)

            # 다음 페이지 전 대기 (3~5초 랜덤)
            delay = _random.uniform(3, 5)
            await asyncio.sleep(delay)

            # 배치 휴식 (batch_size 페이지마다)
            pages_done = page_list.index(page_num) + 1 if page_num in page_list else 0
            if pages_done > 0 and pages_done % batch_size == 0 and pages_done < len(page_list):
                log(f"   😴 {pages_done}페이지 완료 — {batch_rest}초 휴식 중... (서버 부하 방지)")
                await asyncio.sleep(batch_rest)

        # ── 상세 페이지 스크래핑 ──
        if products:
            log(f"🔍 상세 페이지 스크래핑 시작 ({len(products)}개)...")
            for idx, prod in enumerate(products):
                if _check_stop():
                    log("⛔ 중지 요청 — 상세 스크래핑 중단")
                    break
                link = prod.get("link", "")
                if not link:
                    continue
                try:
                    log(f"   📄 [{idx+1}/{len(products)}] 상세: {prod.get('brand','')} {(prod.get('name') or '')[:35]} ¥{prod.get('price_jpy',0):,}")
                    await page.goto(link, wait_until="domcontentloaded", timeout=20000)
                    await asyncio.sleep(2)

                    # WorldShopping body-lock 해제 + 쿠키 배너 JS 클릭
                    try:
                        await page.evaluate("""() => {
                            document.body.classList.remove('zigzag-worldshopping-style-body-lock');
                            document.querySelectorAll(
                                '[id*="zigzag-worldshopping"], [id*="ws-"], ' +
                                'iframe[src*="worldshopping"], ' +
                                '[class*="WorldShopping"]:not(body):not(script):not(style), ' +
                                '[class*="worldshopping"]:not(body):not(script):not(style)'
                            ).forEach(el => el.remove());
                            document.body.style.overflow = 'auto';
                            const btn = document.querySelector('#onetrust-accept-btn-handler');
                            if (btn) btn.click();
                            const ot = document.querySelector('#onetrust-consent-sdk');
                            if (ot) ot.remove();
                            const df = document.querySelector('.onetrust-pc-dark-filter');
                            if (df) df.remove();
                            document.querySelectorAll('.goog-te-banner-frame, .skiptranslate').forEach(el => el.remove());
                            if (document.documentElement) document.documentElement.style.top = '0';
                            if (document.body) document.body.style.top = '0';
                        }""")
                    except Exception:
                        pass
                    # 남은 팝업 닫기
                    for sel in ["button:has-text('以上の内容を確認しました')", "button:has-text('閉じる')"]:
                        try:
                            btn = page.locator(sel).first
                            if await btn.count() > 0 and await btn.is_visible():
                                await btn.click(timeout=3000)
                                await asyncio.sleep(0.5)
                        except:
                            pass

                    detail = await _extract_detail_page(page)
                    if detail.get("detail_images"):
                        prod["detail_images"] = detail["detail_images"]
                    if detail.get("description"):
                        prod["description"] = detail["description"]
                    if detail.get("condition_grade"):
                        prod["condition_grade"] = detail["condition_grade"]
                    if detail.get("color"):
                        prod["color"] = detail["color"]
                    if detail.get("material"):
                        prod["material"] = detail["material"]
                    if detail.get("size"):
                        prod["size"] = detail["size"]
                    if detail.get("measured_size"):
                        prod["color"] = detail["measured_size"]  # 실측 사이즈를 color 필드에 저장
                    if detail.get("description"):
                        prod["description"] = detail["description"]
                    if detail.get("price_jpy") and detail["price_jpy"] > 100:
                        prod["price_jpy"] = detail["price_jpy"]
                    # 자동 카테고리 분류
                    if not prod.get("category_id") or prod["category_id"] == category:
                        auto_cat_id, auto_subcat = _classify_category(
                            prod.get("name", ""),
                            detail.get("breadcrumb", "")
                        )
                        if auto_cat_id:
                            prod["category_id"] = auto_cat_id
                            prod["subcategory"] = auto_subcat
                    # 즉시 번역 + DB 저장
                    _translate_and_save(prod, log)
                    name_ko = (prod.get("name_ko") or "")[:35]
                    log(f"      ✅ {name_ko} | 이미지 {len(detail.get('detail_images',[]))}개 | {prod.get('subcategory','?')}")
                    await asyncio.sleep(_random.uniform(2, 3))
                except Exception as e:
                    log(f"      ⚠️ 상세 스크래핑 실패: {e}")
                    # 상세 실패해도 기본 정보로 저장
                    _translate_and_save(prod, log)
                    continue

    except Exception as e:
        log(f"❌ 크롤링 오류: {e}")
    finally:
        await force_close_browser()

    log(f"🏪 2ndstreet 수집 완료: 총 {len(products)}개")
    return products


# ── 상세 페이지 재수집 (설명 없는 상품) ──────────────────

_rescrape_running = False
_rescrape_stop = False

def is_rescrape_running():
    return _rescrape_running

def stop_rescrape():
    global _rescrape_stop
    _rescrape_stop = True

async def rescrape_details(log=None):
    """DB에서 설명이 없는 2ndstreet 상품의 상세 페이지를 재수집"""
    global _rescrape_running, _rescrape_stop
    if _rescrape_running:
        if log:
            log("⚠️ 이미 재수집 진행 중입니다")
        return

    _rescrape_running = True
    _rescrape_stop = False
    if not log:
        log = lambda msg: logger.info(msg)

    import random as _random

    try:
        from product_db import _conn as db_conn
        import sqlite3

        conn = db_conn()
        conn.row_factory = sqlite3.Row
        rows = conn.execute("""
            SELECT product_code, link, name, brand FROM products
            WHERE site_id='2ndstreet'
              AND (description IS NULL OR description='')
              AND link IS NOT NULL AND link != ''
        """).fetchall()
        conn.close()

        if not rows:
            log("✅ 설명이 없는 상품이 없습니다")
            return

        log(f"🔄 상세 페이지 재수집 시작: {len(rows)}개 상품")

        pw = await async_playwright().start()
        browser = await pw.chromium.launch(
            headless=False,
            slow_mo=200,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-translate",
                "--disable-features=TranslateUI,Translate",
                "--lang=ja",
                "--accept-lang=ja",
            ],
        )
        context = await browser.new_context(
            viewport={"width": 1280, "height": 900},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            locale="ja-JP",
            extra_http_headers={"Accept-Language": "ja"},
        )
        await context.add_init_script("""
            Object.defineProperty(navigator, 'language', {get: () => 'ja'});
            Object.defineProperty(navigator, 'languages', {get: () => ['ja', 'ja-JP']});
        """)
        page = await context.new_page()

        success = 0
        fail = 0

        for idx, row in enumerate(rows):
            if _rescrape_stop:
                log("⛔ 중지 요청 — 재수집 중단")
                break

            code = row["product_code"]
            link = row["link"]
            name = (row["name"] or "")[:35]
            brand = row["brand"] or ""

            log(f"   📄 [{idx+1}/{len(rows)}] {brand} {name}")

            try:
                await page.goto(link, wait_until="domcontentloaded", timeout=20000)
                await asyncio.sleep(3)

                # WorldShopping body-lock 해제 + 쿠키 처리
                await page.evaluate("""() => {
                    document.body.classList.remove('zigzag-worldshopping-style-body-lock');
                    document.querySelectorAll(
                        '[id*="zigzag-worldshopping"], [id*="ws-"], ' +
                        'iframe[src*="worldshopping"], ' +
                        '[class*="WorldShopping"]:not(body):not(script):not(style), ' +
                        '[class*="worldshopping"]:not(body):not(script):not(style)'
                    ).forEach(el => el.remove());
                    document.body.style.overflow = 'auto';
                    const btn = document.querySelector('#onetrust-accept-btn-handler');
                    if (btn) btn.click();
                    const ot = document.querySelector('#onetrust-consent-sdk');
                    if (ot) ot.remove();
                    document.querySelectorAll('.goog-te-banner-frame, .skiptranslate').forEach(el => el.remove());
                    if (document.documentElement) document.documentElement.style.top = '0';
                    if (document.body) document.body.style.top = '0';
                }""")
                await asyncio.sleep(1)

                # 스크롤 다운 (lazy-loading)
                for _ in range(6):
                    await page.evaluate('() => window.scrollBy(0, 400)')
                    await asyncio.sleep(0.3)
                await asyncio.sleep(1)

                # 상세 정보 추출
                detail = await _extract_detail_page(page)

                # DB 업데이트
                updated_fields = []
                conn = db_conn()
                if detail.get("description"):
                    conn.execute(
                        "UPDATE products SET description=? WHERE product_code=? AND site_id='2ndstreet'",
                        (detail["description"], code)
                    )
                    updated_fields.append(f"설명({len(detail['description'])}자)")
                if detail.get("detail_images"):
                    import json as _json
                    conn.execute(
                        "UPDATE products SET detail_images=? WHERE product_code=? AND site_id='2ndstreet'",
                        (_json.dumps(detail["detail_images"]), code)
                    )
                    updated_fields.append(f"이미지({len(detail['detail_images'])}개)")
                if detail.get("condition_grade"):
                    conn.execute(
                        "UPDATE products SET condition_grade=? WHERE product_code=? AND site_id='2ndstreet'",
                        (detail["condition_grade"], code)
                    )
                    updated_fields.append(f"등급({detail['condition_grade']})")
                if detail.get("color"):
                    conn.execute(
                        "UPDATE products SET color=? WHERE product_code=? AND site_id='2ndstreet'",
                        (detail["color"], code)
                    )
                    updated_fields.append("컬러")
                if detail.get("material"):
                    conn.execute(
                        "UPDATE products SET material=? WHERE product_code=? AND site_id='2ndstreet'",
                        (detail["material"], code)
                    )
                    updated_fields.append("소재")
                # 실측 사이즈 → description에 추가
                if detail.get("measured_size") and not detail.get("description"):
                    measured = detail["measured_size"]
                    conn.execute(
                        "UPDATE products SET description=? WHERE product_code=? AND site_id='2ndstreet'",
                        (f"실측: {measured}", code)
                    )
                    updated_fields.append(f"실측({measured[:30]})")
                conn.commit()
                conn.close()

                # AI 번역 (설명 + 상품명)
                if updated_fields:
                    try:
                        from translator import translate_ja_ko
                        conn2 = db_conn()
                        if detail.get("description"):
                            desc_ko = translate_ja_ko(detail["description"])
                            if desc_ko:
                                conn2.execute(
                                    "UPDATE products SET description_ko=? WHERE product_code=? AND site_id='2ndstreet'",
                                    (desc_ko, code)
                                )
                                updated_fields.append("번역")
                        # name_ko가 없거나 일본어가 남아있으면 재번역
                        import re as _re
                        cur_name_ko = conn2.execute(
                            "SELECT name_ko, name FROM products WHERE product_code=? AND site_id='2ndstreet'",
                            (code,)
                        ).fetchone()
                        if cur_name_ko:
                            nk = cur_name_ko[0] or ""
                            if _re.search(r'[\u3040-\u30FF\u4E00-\u9FFF]', nk) or not nk:
                                name_ko = translate_ja_ko(cur_name_ko[1] or "")
                                if name_ko:
                                    conn2.execute(
                                        "UPDATE products SET name_ko=? WHERE product_code=? AND site_id='2ndstreet'",
                                        (name_ko, code)
                                    )
                        conn2.commit()
                        conn2.close()
                    except Exception as te:
                        log(f"      ⚠️ 번역: {str(te)[:40]}")

                if updated_fields:
                    log(f"      ✅ {', '.join(updated_fields)}")
                    success += 1
                else:
                    log(f"      ⚠️ 추출 데이터 없음")
                    fail += 1

                await asyncio.sleep(_random.uniform(2, 3))

            except Exception as e:
                log(f"      ❌ 실패: {str(e)[:60]}")
                fail += 1
                continue

        await browser.close()
        await pw.stop()

        log(f"🏪 상세 재수집 완료: 성공 {success}개, 실패 {fail}개")

    except Exception as e:
        log(f"❌ 재수집 오류: {e}")
    finally:
        _rescrape_running = False
        _rescrape_stop = False


def _translate_and_save(product: dict, log_func=None):
    """상품 1건 즉시 번역 + DB 저장 (AI 번역 우선)"""
    try:
        from translator import translate_ja_ko, translate_brand
        if product.get("name") and not product.get("name_ko"):
            # AI가 최우선으로 번역 (translate_ja_ko 내부에서 처리)
            product["name_ko"] = translate_ja_ko(product["name"])
        if product.get("description") and not product.get("description_ko"):
            product["description_ko"] = translate_ja_ko(product["description"])
        if product.get("brand") and not product.get("brand_ko"):
            product["brand_ko"] = translate_brand(product["brand"])
    except Exception as e:
        if log_func:
            log_func(f"      ⚠️ 번역 오류: {e}")

    try:
        from product_db import insert_products
        insert_products([product])
    except Exception as e:
        if log_func:
            log_func(f"      ⚠️ DB 저장 실패: {e}")


async def _extract_product_from_card(el, page) -> dict:
    """상품 카드에서 정보 추출"""
    product = {
        "name": "",
        "brand": "",
        "price_jpy": 0,
        "link": "",
        "img_url": "",
        "product_code": "",
        "condition_grade": "",
        "color": "",
        "gender": "",
    }

    try:
        # 링크
        href = await el.get_attribute("href")
        if href:
            if not href.startswith("http"):
                href = "https://www.2ndstreet.jp" + href
            product["link"] = href
            # URL에서 상품코드 추출
            code_match = re.search(r'goodsId/(\d+)', href)
            if code_match:
                product["product_code"] = code_match.group(1)
            else:
                code_match = re.search(r'/id/(\d+)', href)
                if code_match:
                    product["product_code"] = code_match.group(1)
                else:
                    code_match = re.search(r'/code/(\w+)', href)
                    if code_match:
                        product["product_code"] = code_match.group(1)
    except Exception:
        pass

    try:
        # 이미지
        img = el.locator("img").first
        if await img.count() > 0:
            src = await img.get_attribute("src") or await img.get_attribute("data-src") or ""
            if src:
                product["img_url"] = src
    except Exception:
        pass

    try:
        # JavaScript로 구조화된 정보 직접 추출
        card_info = await el.evaluate("""(el) => {
            const info = {brand: '', name: '', price: 0, grade: ''};
            // 브랜드
            const brandEl = el.querySelector('[class*="brand"], [class*="Brand"]');
            if (brandEl) info.brand = brandEl.innerText.trim();
            // 상품명
            const nameEl = el.querySelector('[class*="name"], [class*="Name"], [class*="title"], [class*="Title"]');
            if (nameEl) info.name = nameEl.innerText.trim();
            // 가격 — 전체 텍스트에서 추출
            const text = el.innerText || '';
            const priceMatch = text.match(/[¥￥]\s?([\d,]+)/);
            if (priceMatch) info.price = parseInt(priceMatch[1].replace(/,/g, ''));
            // 등급
            const gradeEl = el.querySelector('[class*="condition"], [class*="Condition"], [class*="grade"], [class*="Grade"]');
            if (gradeEl) {
                const gt = gradeEl.innerText.trim();
                const gm = gt.match(/(新品|未使用|[SABCDN])/);
                if (gm) info.grade = gm[1] === '新品' || gm[1] === '未使用' ? 'NS' : gm[1];
            }
            return info;
        }""")
        if card_info.get("brand") and not product["brand"]:
            product["brand"] = card_info["brand"]
        if card_info.get("name") and not product["name"]:
            product["name"] = card_info["name"]
        if card_info.get("price") and not product["price_jpy"]:
            product["price_jpy"] = card_info["price"]
        if card_info.get("grade") and not product["condition_grade"]:
            product["condition_grade"] = card_info["grade"]
    except Exception:
        pass

    # 폴백: 텍스트 기반 추출
    if not product["price_jpy"] or not product["brand"]:
        try:
            text = await el.inner_text()
            full_text = text.replace("\n", " ")
            # 가격 (전체 텍스트에서)
            if not product["price_jpy"]:
                price_match = re.search(r'[¥￥]\s?([\d,]+)', full_text)
                if price_match:
                    price_str = price_match.group(1).replace(",", "")
                    if price_str.isdigit() and int(price_str) > 100:
                        product["price_jpy"] = int(price_str)

            lines = [l.strip() for l in text.split("\n") if l.strip()]
            # 등급
            if not product["condition_grade"]:
                for line in lines:
                    grade_match = re.match(r'^(?:中古)?([SABCDN])\s*$', line)
                    if grade_match:
                        product["condition_grade"] = grade_match.group(1)
                        break
            # 브랜드 (첫 줄)
            if lines and not product["brand"]:
                product["brand"] = lines[0]
            # 상품명 (두 번째 줄)
            if len(lines) > 1 and not product["name"]:
                product["name"] = lines[1]
        except Exception:
            pass

    return product


async def _extract_detail_page(page) -> dict:
    """상세 페이지에서 추가 정보 추출"""
    detail = {
        "detail_images": [],
        "description": "",
        "condition_grade": "",
        "color": "",
        "size": "",
    }

    try:
        # 상세 이미지: 메인 이미지 URL에서 번호(1~10)를 변경하여 생성
        # 패턴: .../goods/XXXXXX/XX/XXXXX/1_tn.jpg → 1.jpg, 2.jpg, ...
        import re as _re_img
        main_img_el = page.locator("img[src*='cdn2.2ndstreet.jp'][src*='/goods/']").first
        base_url = ""
        if await main_img_el.count() > 0:
            base_url = await main_img_el.get_attribute("src") or ""
        if not base_url:
            # og:image 또는 meta에서 찾기
            og = await page.query_selector("meta[property='og:image']")
            if og:
                base_url = await og.get_attribute("content") or ""
        if not base_url:
            # URL에서 goodsId 추출하여 직접 구성
            url_match = _re_img.search(r'goodsId/(\d+)', page.url)
            if url_match:
                gid = url_match.group(1)
                # 상품코드에서 경로 구성: 2329972265995 → 232997/22/65995
                if len(gid) >= 10:
                    base_url = f"https://cdn2.2ndstreet.jp/img/pc/goods/{gid[:6]}/{gid[6:8]}/{gid[8:]}/1_tn.jpg"

        if base_url and "/goods/" in base_url:
            # 메인 이미지 경로에서 1~9번 이미지 URL 생성
            base_dir = base_url.rsplit("/", 1)[0] + "/"
            detail["detail_images"] = [f"{base_dir}{n}.jpg" for n in range(1, 10)]

        # 상품 설명 텍스트 (검색 안내 팝업만 제외 — 실제 상품 설명은 허용)
        _desc_excludes = ['キーワード', '検索窓']
        desc_selectors = [
            "[class*='goodsComment']",
            "[class*='itemComment']",
            "[class*='detail_comment']",
        ]
        for sel in desc_selectors:
            try:
                el = page.locator(sel).first
                if await el.count() > 0:
                    text = (await el.inner_text()).strip()
                    if text and len(text) > 10 and not any(ex in text for ex in _desc_excludes):
                        if len(text) > len(detail["description"]):
                            detail["description"] = text
            except:
                continue

        # 상품 스펙 추출 (일본어/한국어 번역 둘 다 대응)
        spec_info = await page.evaluate(r"""() => {
            const result = {grade: '', color: '', size: '', price: 0, material: '', pattern: '', model: '', description: '', measured_size: ''};
            const bodyText = document.body.innerText || '';

            // 1) 컨디션 (일본어/한국어 둘 다)
            const gm = bodyText.match(/(?:商品の状態|상품의 상태)[：:\s]*(?:中古|중고)\s*([A-D])/);
            if (gm) result.grade = gm[1];
            else if (bodyText.includes('未使用品') || bodyText.includes('미사용품') || bodyText.match(/(?:新品|신품)/)) result.grade = 'NS';

            // 2) 가격 (税込/세금포함)
            const pm = bodyText.match(/[¥￥]\s?([\d,]+)\s*(?:税込|세금)/);
            if (pm) result.price = parseInt(pm[1].replace(/,/g, ''));
            if (!result.price) {
                const pm2 = bodyText.match(/[¥￥]\s?([\d,]+)/);
                if (pm2 && parseInt(pm2[1].replace(/,/g, '')) > 100) result.price = parseInt(pm2[1].replace(/,/g, ''));
            }

            // 3) 상세 스펙 (dt/dd 쌍 개별 매칭 — dl 안에 여러 쌍 대응)
            const allDts = document.querySelectorAll('dt');
            for (const dt of allDts) {
                const label = (dt.innerText || '').trim();
                const dd = dt.nextElementSibling;
                if (!dd || dd.tagName !== 'DD') continue;
                const value = (dd.innerText || '').trim();
                if (!value || value === '—' || value === '-' || value === 'ー') continue;

                if (label.match(/型番|형번/)) result.model = value;
                else if (label.match(/カラー|칼라|컬러|色/)) result.color = value;
                else if (label.match(/素材|소재|生地|천/)) result.material = value;
                else if (label.match(/柄|무늬|模様/)) result.pattern = value;
                else if (label.match(/^サイズ$|^사이즈$/) || (label.match(/サイズ|사이즈/) && !label.match(/実寸|실측/))) result.size = value;
                else if (label.match(/実寸|실측/)) result.measured_size = value;
            }
            // table tr 폴백
            const trs = document.querySelectorAll('table tr');
            for (const tr of trs) {
                const th = tr.querySelector('th');
                const td = tr.querySelector('td');
                if (!th || !td) continue;
                const label = th.innerText.trim();
                const value = td.innerText.trim();
                if (!value || value === '—' || value === '-' || value === 'ー') continue;
                if (!result.color && label.match(/カラー|칼라|컬러|色/)) result.color = value;
                else if (!result.material && label.match(/素材|소재|生地|천/)) result.material = value;
                else if (!result.size && (label.match(/^サイズ$|^사이즈$/) || (label.match(/サイズ|사이즈/) && !label.match(/実寸|실측/)))) result.size = value;
                else if (!result.measured_size && label.match(/実寸|실측/)) result.measured_size = value;
            }

            // 4) 상품 설명 (일본어/한국어)
            // 상품 설명 헤더(商品の説明) 바로 다음 요소에서 추출
            const allEls = document.querySelectorAll('h2, h3, div, section, p');
            for (const el of allEls) {
                const t = (el.innerText || '').trim();
                if (t.match(/^(商品\s*(の\s*)?説明|상품\s*(의\s*)?설명)$/)) {
                    const next = el.nextElementSibling;
                    if (next) {
                        const dt = next.innerText.trim();
                        if (dt && dt.length >= 10) { result.description = dt; break; }
                    }
                }
            }
            // 폴백: comment 클래스
            if (!result.description) {
                const descEl = document.querySelector('[class*="goodsComment"], [class*="itemComment"]');
                if (descEl) {
                    const dt = descEl.innerText.trim();
                    if (dt && dt.length >= 10) result.description = dt;
                }
            }

            return result;
        }""")
        if spec_info.get("grade"):
            detail["condition_grade"] = spec_info["grade"]
        if spec_info.get("color"):
            detail["color"] = spec_info["color"]
        if spec_info.get("material"):
            detail["material"] = spec_info["material"]
        if spec_info.get("size"):
            detail["size"] = spec_info["size"]
        if spec_info.get("measured_size"):
            detail["measured_size"] = spec_info["measured_size"]
        if spec_info.get("description") and not detail.get("description"):
            detail["description"] = spec_info["description"]
        if spec_info.get("price") and spec_info["price"] > 100:
            detail["price_jpy"] = spec_info["price"]

        # 1차: breadcrumb에서 카테고리 추출
        category_info = await page.evaluate("""() => {
            const bc = document.querySelectorAll('.breadcrumb a, .breadcrumb li, nav[aria-label*="bread"] a, .pankuzu a, [class*="breadcrumb"] a');
            const texts = [];
            for (const el of bc) texts.push(el.innerText.trim());
            return texts.join(' > ');
        }""")
        if category_info:
            detail["breadcrumb"] = category_info

    except Exception:
        pass

    return detail


def _parse_pages(pages_str: str, max_pages: int = 5) -> list:
    """페이지 범위 문자열 파싱"""
    if not pages_str:
        return list(range(1, max_pages + 1))
    result = []
    for part in pages_str.split(","):
        part = part.strip()
        if "-" in part:
            try:
                a, b = part.split("-")
                result.extend(range(int(a), int(b) + 1))
            except ValueError:
                pass
        else:
            try:
                result.append(int(part))
            except ValueError:
                pass
    return result or list(range(1, max_pages + 1))
