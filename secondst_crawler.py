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
    max_pages=5,
    brand_code="",
):
    """
    2ndstreet.jp에서 빈티지 상품 수집

    Args:
        status_callback: 실시간 로그 콜백
        category: 카테고리 코드 (951002=가방, 951001=의류 등)
        keyword: 검색 키워드
        pages: 페이지 범위 (예: "1-3")
        max_pages: 최대 페이지 수

    Returns:
        list of product dicts
    """
    global _browser, _playwright

    def log(msg):
        logger.info(msg)
        if status_callback:
            status_callback(msg)

    log("🏪 2ndstreet.jp 크롤링 시작")

    # 페이지 범위 파싱
    page_list = _parse_pages(pages, max_pages)
    log(f"   📄 수집 페이지: {page_list}")

    products = []

    try:
        # ── Playwright 브라우저 시작 ──
        _playwright = await async_playwright().start()
        _browser = await _playwright.chromium.launch(
            headless=False,
            slow_mo=300,
            args=["--disable-blink-features=AutomationControlled"],
        )
        context = await _browser.new_context(
            viewport={"width": 1280, "height": 900},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            locale="ja-JP",
        )
        page = await context.new_page()

        for page_num in page_list:
            if _check_stop():
                log("⛔ 중지 요청 — 크롤링 중단")
                break

            # ── URL 구성 ──
            url = f"https://www.2ndstreet.jp/search?category={category}"
            if brand_code:
                url += f"&brand%5B%5D={brand_code}"
            # 컨디션: 신품・미사용 / 중고A / 중고B 까지만
            url += "&conditions%5B%5D=NS&conditions%5B%5D=A&conditions%5B%5D=B"
            url += "&sortBy=recommend"
            if keyword:
                url += f"&keyword={keyword}"
            url += f"&page={page_num}"

            log(f"📄 페이지 {page_num} 로딩: {url}")

            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                await asyncio.sleep(2)

                # 쿠키 동의 팝업 자동 닫기 (여러 단계)
                for cookie_sel in [
                    "button:has-text('すべての Cookie を受け入れる')",
                    "button:has-text('すべて許可する')",
                    "button:has-text('保存して閉じる')",
                    "button:has-text('以上の内容を確認しました')",
                    "button:has-text('閉じる')",
                    "#onetrust-accept-btn-handler",
                    "[class*='WorldShopping'] [class*='close']",
                    "[class*='worldshopping'] button",
                ]:
                    try:
                        btn = page.locator(cookie_sel).first
                        if await btn.count() > 0 and await btn.is_visible():
                            await btn.click()
                            log(f"   🍪 쿠키 동의 닫기")
                            await asyncio.sleep(1)
                    except Exception:
                        continue

                # 하단 WorldShopping 배너 / 쿠폰 팝업 강제 제거
                try:
                    await page.evaluate("""() => {
                        document.querySelectorAll('[id*="worldshopping"], [class*="worldshopping"], [class*="WorldShopping"], [id*="ws-"]').forEach(el => el.remove());
                        document.querySelectorAll('[class*="coupon"], [class*="Coupon"]').forEach(el => { if(el.style) el.style.display = 'none'; });
                    }""")
                except Exception:
                    pass

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
                # 셀렉터 디스커버리 — 페이지 구조 덤프
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
                except Exception as e:
                    logger.debug(f"상품 추출 오류 [{i}]: {e}")
                    continue

            log(f"   📦 페이지 {page_num}: {len(page_products)}개 수집")
            products.extend(page_products)

            # 다음 페이지 전 대기
            await asyncio.sleep(1.5)

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
                    log(f"   📄 [{idx+1}/{len(products)}] 상세 로드: {prod.get('brand','')} {prod.get('name','')[:30]}")
                    await page.goto(link, wait_until="domcontentloaded", timeout=20000)
                    await asyncio.sleep(2)

                    # 팝업 닫기
                    for sel in ["button:has-text('以上の内容を確認しました')", "button:has-text('閉じる')", "button:has-text('すべての Cookie を受け入れる')"]:
                        try:
                            btn = page.locator(sel).first
                            if await btn.count() > 0 and await btn.is_visible():
                                await btn.click()
                                await asyncio.sleep(0.5)
                        except:
                            pass

                    detail = await _extract_detail_page(page)
                    if detail.get("detail_images"):
                        prod["detail_images"] = detail["detail_images"]
                    if detail.get("description"):
                        prod["description"] = detail["description"]
                    if detail.get("condition_grade") and not prod.get("condition_grade"):
                        prod["condition_grade"] = detail["condition_grade"]
                    if detail.get("color") and not prod.get("color"):
                        prod["color"] = detail["color"]
                    if detail.get("size"):
                        prod["size"] = detail["size"]
                    log(f"      ✅ 이미지 {len(detail.get('detail_images',[]))}개, 설명 {len(detail.get('description',''))}자")
                    await asyncio.sleep(1)
                except Exception as e:
                    log(f"      ⚠️ 상세 스크래핑 실패: {e}")
                    continue

    except Exception as e:
        log(f"❌ 크롤링 오류: {e}")
    finally:
        await force_close_browser()

    log(f"🏪 2ndstreet 수집 완료: 총 {len(products)}개")

    # ── 번역 ──
    if products:
        log("🌏 일본어 → 한국어 번역 중...")
        try:
            from translator import translate_ja_ko, translate_brand
            for p in products:
                if p.get("name") and not p.get("name_ko"):
                    p["name_ko"] = translate_ja_ko(p["name"])
                if p.get("brand") and not p.get("brand_ko"):
                    p["brand_ko"] = translate_brand(p["brand"])
        except Exception as e:
            log(f"   ⚠️ 번역 오류: {e}")

    # ── DB 저장 ──
    if products:
        try:
            from product_db import insert_products
            new_count = insert_products(products)
            log(f"💾 빅데이터 DB: {new_count}개 신규 저장")
        except Exception as e:
            log(f"⚠️ DB 저장 실패: {e}")

    return products


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
        # 텍스트 기반 정보 추출
        text = await el.inner_text()
        lines = [l.strip() for l in text.split("\n") if l.strip()]

        for line in lines:
            # 가격 (¥ 또는 円)
            price_match = re.search(r'[¥￥]?\s?([\d,]+)\s*(?:円|$)', line)
            if price_match and not product["price_jpy"]:
                price_str = price_match.group(1).replace(",", "")
                if price_str.isdigit():
                    product["price_jpy"] = int(price_str)
                continue

            # 상태 (A, B, C, D, S, N)
            grade_match = re.match(r'^[SABCDN]\s*$', line)
            if grade_match:
                product["condition_grade"] = line.strip()
                continue

        # 브랜드 (보통 첫 줄)
        if lines and not product["brand"]:
            product["brand"] = lines[0]

        # 상품명 (보통 두 번째 줄)
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
        # 상세 이미지 (큰 이미지)
        imgs = await page.query_selector_all("img[src*='cdn2.2ndstreet.jp']")
        seen = set()
        for img in imgs:
            src = await img.get_attribute("src") or ""
            if not src or src in seen:
                continue
            # 썸네일(_tn) 제외, 큰 이미지만
            if "_tn." in src:
                src = src.replace("_tn.", ".")
            if src in seen:
                continue
            seen.add(src)
            detail["detail_images"].append(src)
            if len(detail["detail_images"]) >= 20:
                break

        # 상품 설명 텍스트
        desc_selectors = [
            "[class*='goodsComment']",
            "[class*='itemComment']",
            "[class*='description']",
            "[class*='detail_comment']",
            "[class*='goodsDetail']",
        ]
        for sel in desc_selectors:
            try:
                el = page.locator(sel).first
                if await el.count() > 0:
                    text = (await el.inner_text()).strip()
                    if text and len(text) > len(detail["description"]):
                        detail["description"] = text
            except:
                continue

        # 상품 스펙 테이블에서 정보 추출
        spec_info = await page.evaluate("""() => {
            const result = {grade: '', color: '', size: ''};
            // th/td 테이블 또는 dt/dd 리스트
            const rows = document.querySelectorAll('tr, dl');
            for (const row of rows) {
                const text = row.innerText || '';
                if (text.includes('状態') || text.includes('コンディション')) {
                    const m = text.match(/中古([A-Z])|新品|未使用|([A-Z])ランク/);
                    if (m) result.grade = m[1] || m[2] || '新品';
                }
                if (text.includes('カラー') || text.includes('色')) {
                    const parts = text.split(/[：:]|\\t|\\n/).filter(s => s.trim());
                    if (parts.length >= 2) result.color = parts[parts.length-1].trim();
                }
                if (text.includes('サイズ')) {
                    const parts = text.split(/[：:]|\\t|\\n/).filter(s => s.trim());
                    if (parts.length >= 2) result.size = parts[parts.length-1].trim();
                }
            }
            return result;
        }""")
        if spec_info.get("grade"):
            detail["condition_grade"] = spec_info["grade"]
        if spec_info.get("color"):
            detail["color"] = spec_info["color"]
        if spec_info.get("size"):
            detail["size"] = spec_info["size"]

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
