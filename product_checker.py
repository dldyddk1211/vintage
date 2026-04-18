"""
product_checker.py
기존 상품 최신화 체커 — 2ndstreet 상품 판매 상태 확인

기능:
  1. 유휴 시간: 서버가 쉴 때 자동으로 오래된 상품부터 상태 체크
  2. 수집 중: 수집 300개 → 체크 300개 교대 실행
  3. Windows 수집PC에서만 실행

상태 체크 방법:
  - 상품 URL HTTP 요청 → 200(판매중) / 404(품절) / 리다이렉트(품절)
  - 판매중: checked_at 갱신 + 가격 변동 감지
  - 품절: product_status = 'sold_out'
"""

import asyncio
import logging
import time
from datetime import datetime

logger = logging.getLogger(__name__)

# 체크 상태 (app.py에서 참조)
checker_status = {
    "running": False,
    "stop_requested": False,
    "checked": 0,
    "sold_out": 0,
    "price_changed": 0,
    "errors": 0,
    "total_target": 0,
    "current_brand": "",
    "log": [],
}

CHUNK_SIZE = 300  # 1회 체크 단위


def _log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    checker_status["log"].append(f"[{ts}] {msg}")
    if len(checker_status["log"]) > 500:
        checker_status["log"] = checker_status["log"][-300:]
    logger.info(f"[체커] {msg}")


def get_unchecked_products(limit=300):
    """체크가 필요한 상품 조회 (오래된 순)"""
    from product_db import _conn
    conn = _conn()
    try:
        rows = conn.execute("""
            SELECT id, site_id, product_code, brand, name, link, price_jpy, checked_at
            FROM products
            WHERE source_type='vintage'
              AND (product_status IS NULL OR product_status = 'available' OR product_status = '')
              AND link IS NOT NULL AND link != ''
            ORDER BY
              CASE WHEN checked_at IS NULL OR checked_at = '' THEN 0 ELSE 1 END,
              checked_at ASC
            LIMIT ?
        """, (limit,)).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_check_stats():
    """체크 현황 통계"""
    from product_db import _conn
    conn = _conn()
    try:
        total = conn.execute("SELECT COUNT(*) FROM products WHERE source_type='vintage'").fetchone()[0]
        checked = conn.execute(
            "SELECT COUNT(*) FROM products WHERE source_type='vintage' AND checked_at IS NOT NULL AND checked_at != ''"
        ).fetchone()[0]
        sold_out = conn.execute(
            "SELECT COUNT(*) FROM products WHERE source_type='vintage' AND product_status='sold_out'"
        ).fetchone()[0]
        never_checked = conn.execute(
            "SELECT COUNT(*) FROM products WHERE source_type='vintage' AND (checked_at IS NULL OR checked_at = '')"
        ).fetchone()[0]
        old_7d = conn.execute("""
            SELECT COUNT(*) FROM products WHERE source_type='vintage'
            AND checked_at IS NOT NULL AND checked_at != ''
            AND date(checked_at) < date('now','-7 days','localtime')
        """).fetchone()[0]
        return {
            "total": total, "checked": checked, "sold_out": sold_out,
            "never_checked": never_checked, "old_7d": old_7d,
            "need_check": never_checked + old_7d,
        }
    finally:
        conn.close()


async def check_products_batch(products, status_callback=None):
    """상품 배치 상태 체크 (Playwright 사용)

    Returns: {"checked": N, "sold_out": N, "price_changed": N, "errors": N}
    """
    from playwright.async_api import async_playwright
    from product_db import _conn
    import random

    result = {"checked": 0, "sold_out": 0, "price_changed": 0, "errors": 0}
    if not products:
        return result

    def log(msg):
        if status_callback:
            status_callback(msg)
        _log(msg)

    playwright = None
    browser = None
    try:
        playwright = await async_playwright().start()
        browser = await playwright.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled", "--lang=ja"],
        )
        context = await browser.new_context(
            viewport={"width": 1280, "height": 900},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            locale="ja-JP",
        )
        page = await context.new_page()

        conn = _conn()
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        for i, p in enumerate(products):
            if checker_status["stop_requested"]:
                log("중지 요청 — 체크 중단")
                break

            link = p["link"]
            pid = p["id"]

            try:
                resp = await page.goto(link, wait_until="domcontentloaded", timeout=15000)
                await asyncio.sleep(random.uniform(1, 2))

                if resp is None or resp.status == 404:
                    # 품절/삭제
                    conn.execute(
                        "UPDATE products SET product_status='sold_out', checked_at=? WHERE id=?",
                        (now, pid)
                    )
                    result["sold_out"] += 1
                elif resp.status == 200:
                    # 페이지 존재 확인 — 실제 상품이 있는지 체크
                    is_sold = await page.evaluate("""() => {
                        // 2ndstreet: 판매 종료 시 특정 문구 표시
                        const body = document.body.innerText || '';
                        if (body.includes('この商品は売り切れました') ||
                            body.includes('売り切れ') ||
                            body.includes('この商品は現在販売しておりません') ||
                            body.includes('ページが見つかりません')) return true;
                        // 상품 가격이 없으면 품절
                        const price = document.querySelector('.itemPrice, .price, [class*="price"]');
                        if (!price) return true;
                        return false;
                    }""")

                    if is_sold:
                        conn.execute(
                            "UPDATE products SET product_status='sold_out', checked_at=? WHERE id=?",
                            (now, pid)
                        )
                        result["sold_out"] += 1
                    else:
                        # 가격 변동 체크
                        new_price = await page.evaluate("""() => {
                            const el = document.querySelector('.itemPrice, [class*="price"]');
                            if (!el) return 0;
                            const m = el.innerText.replace(/[^0-9]/g, '');
                            return parseInt(m) || 0;
                        }""")

                        update_sql = "UPDATE products SET product_status='available', checked_at=? WHERE id=?"
                        update_params = [now, pid]

                        if new_price > 0 and new_price != p["price_jpy"]:
                            update_sql = "UPDATE products SET product_status='available', checked_at=?, price_jpy=? WHERE id=?"
                            update_params = [now, new_price, pid]
                            result["price_changed"] += 1
                            log(f"   가격변동: {p['brand']} ¥{p['price_jpy']:,} → ¥{new_price:,}")

                        conn.execute(update_sql, update_params)

                    result["checked"] += 1
                else:
                    # 기타 상태 (502 등) — 다음에 재시도
                    result["errors"] += 1

            except Exception as e:
                result["errors"] += 1
                if i < 3:  # 첫 몇 개만 에러 로그
                    log(f"   체크 오류: {str(e)[:60]}")

            # 진행률
            if (i + 1) % 50 == 0:
                conn.commit()
                log(f"   진행: {i+1}/{len(products)} (품절:{result['sold_out']})")

            # 봇 감지 방지 대기
            await asyncio.sleep(random.uniform(0.5, 1.5))

        conn.commit()
        conn.close()

    except Exception as e:
        log(f"브라우저 오류: {e}")
    finally:
        if browser:
            await browser.close()
        if playwright:
            await playwright.stop()

    return result


def run_check_batch(chunk_size=300, status_callback=None):
    """동기 래퍼 — 1배치(300개) 상태 체크 실행"""
    products = get_unchecked_products(chunk_size)
    if not products:
        if status_callback:
            status_callback("체크할 상품 없음 — 전체 최신화 완료")
        return {"checked": 0, "sold_out": 0, "price_changed": 0, "errors": 0}

    brands = {}
    for p in products:
        brands[p["brand"]] = brands.get(p["brand"], 0) + 1
    brand_summary = ", ".join(f"{b}({c})" for b, c in sorted(brands.items(), key=lambda x: -x[1])[:5])

    if status_callback:
        status_callback(f"상태 체크 시작: {len(products)}개 [{brand_summary}]")

    return asyncio.run(check_products_batch(products, status_callback))
