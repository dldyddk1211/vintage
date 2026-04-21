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


# 배치 내 사용자별 할인 상품 누적 (유저당 1회 SMS 발송을 위함)
_batch_user_discounts = {}  # {username: {"phone": str, "items": [...]}}


def _queue_cart_price_drop(product_code, brand, name, old_price, new_price):
    """장바구니 고객별 할인 상품 누적 (배치 종료 시 1회 SMS 발송)"""
    if not product_code or old_price <= 0 or new_price <= 0:
        return
    discount_pct = (1 - new_price / old_price) * 100
    if discount_pct < 5:  # 최소 5% 이상 할인만
        return
    try:
        import sqlite3
        from data_manager import get_path
        user_db_path = f"{get_path('db')}/users.db"
        conn = sqlite3.connect(user_db_path)
        conn.row_factory = sqlite3.Row
        try:
            rows = conn.execute("""
                SELECT DISTINCT c.username, u.phone, u.name
                FROM cart c LEFT JOIN users u ON c.username = u.username
                WHERE c.product_code = ? AND u.phone IS NOT NULL AND u.phone != ''
            """, (product_code,)).fetchall()
        finally:
            conn.close()

        for row in rows:
            username = row["username"]
            phone = (row["phone"] or "").replace("-", "")
            if not phone or len(phone) < 10:
                continue
            if username not in _batch_user_discounts:
                _batch_user_discounts[username] = {"phone": phone, "items": []}
            _batch_user_discounts[username]["items"].append({
                "brand": brand,
                "name": name,
                "old_price": old_price,
                "new_price": new_price,
                "discount_pct": discount_pct,
            })
    except Exception as e:
        logger.warning(f"cart SMS 큐 오류: {e}")


def _send_batch_cart_sms():
    """배치 종료 시 누적된 고객별 할인 상품을 SMS로 1회 발송"""
    global _batch_user_discounts
    if not _batch_user_discounts:
        return
    try:
        from aligo_sms import send_sms
        for username, data in _batch_user_discounts.items():
            items = data["items"]
            if not items:
                continue
            phone = data["phone"]
            # 메시지 구성
            header = f"[TheOne Vintage] 장바구니 상품 할인!\n"
            lines = []
            for it in items[:5]:  # 최대 5개까지 표시
                short = (it["brand"] + " " + (it["name"] or "").split("/")[0])[:20]
                lines.append(f"• {short}\n  ¥{it['old_price']:,} → ¥{it['new_price']:,} ({it['discount_pct']:.0f}%↓)")
            if len(items) > 5:
                lines.append(f"• 외 {len(items)-5}건 더")
            footer = "\n지금 주문하세요!"
            msg = header + "\n".join(lines) + footer
            try:
                send_sms(phone, msg, title="할인알림", msg_type="LMS")
                _log(f"   📱 SMS 발송: {username} ({phone}) - {len(items)}개 할인 통합")
            except Exception as e:
                logger.warning(f"SMS 실패 ({username}): {e}")
    finally:
        _batch_user_discounts = {}


def get_unchecked_products(limit=300):
    """체크가 필요한 상품 조회 (오래된 순)"""
    from product_db import _conn
    conn = _conn()
    try:
        rows = conn.execute("""
            SELECT id, site_id, product_code, brand, brand_ko, name, link, price_jpy, checked_at,
                   internal_code, category_id
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
            headless=False,
            args=["--disable-blink-features=AutomationControlled", "--lang=ja", "--disable-translate"],
        )
        context = await browser.new_context(
            viewport={"width": 1280, "height": 900},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            locale="ja-JP",
        )
        page = await context.new_page()

        conn = _conn()
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        _batch_start = time.time()

        for i, p in enumerate(products):
            if checker_status["stop_requested"]:
                log("중지 요청 — 체크 중단")
                break

            link = p["link"]
            pid = p["id"]

            try:
                resp = await page.goto(link, wait_until="domcontentloaded", timeout=20000)
                await asyncio.sleep(random.uniform(1.5, 2.5))
                # 가격 요소 로딩 대기 (최대 6초)
                price_loaded = False
                try:
                    await page.wait_for_selector('[itemprop="price"], .priceMain, .priceNum', timeout=6000)
                    price_loaded = True
                except Exception:
                    pass

                if resp is None or resp.status == 404:
                    # 품절/삭제
                    conn.execute(
                        "UPDATE products SET product_status='sold_out', checked_at=? WHERE id=?",
                        (now, pid)
                    )
                    result["sold_out"] += 1
                    log(f"   [{i+1}] 품절: {p['brand']} {(p.get('name') or '')[:30]} (404)")
                elif resp.status == 200:
                    # 페이지 존재 확인 — 실제 상품이 있는지 체크
                    is_sold = await page.evaluate("""() => {
                        const body = document.body.innerText || '';
                        // 페이지 자체가 없는 경우
                        if (body.includes('ページが見つかりません')) return true;
                        // 확실한 품절 문구 (메인 상품에만 나타남)
                        if (body.includes('※申し訳ございません。この商品は売切れ') ||
                            body.includes('※申し訳ございません。この商品は売り切れ') ||
                            body.includes('この商品は現在販売しておりません')) return true;
                        // SOLD OUT 텍스트가 메인 가격 근처에 있는지 체크 (추천상품 영역 오탐 방지)
                        const price = document.querySelector('[itemprop="price"], .priceMain, .priceNum');
                        if (!price) return true;
                        // 가격 요소 주변(부모의 부모)에서 SOLD OUT 텍스트 검색
                        let parent = price.parentElement;
                        for (let i = 0; i < 4 && parent; i++) {
                            if ((parent.innerText || '').includes('SOLD OUT')) return true;
                            parent = parent.parentElement;
                        }
                        return false;
                    }""")
                    # 가격 로드 실패 시 한번 더 대기 후 재확인 (오탐 방지)
                    if is_sold and not price_loaded:
                        await asyncio.sleep(2)
                        is_sold = await page.evaluate("""() => {
                            const body = document.body.innerText || '';
                            if (body.includes('SOLD OUT') || body.includes('売り切れ') || body.includes('売切れ')) return true;
                            const price = document.querySelector('[itemprop="price"], .priceMain, .priceNum');
                            return !price;
                        }""")

                    if is_sold:
                        conn.execute(
                            "UPDATE products SET product_status='sold_out', checked_at=? WHERE id=?",
                            (now, pid)
                        )
                        result["sold_out"] += 1
                        log(f"   [{i+1}] 품절: {p['brand']} {(p.get('name') or '')[:30]} (売切)")
                    else:
                        # 가격 변동 체크 — 정확한 메인 가격 요소만 선택
                        new_price = await page.evaluate("""() => {
                            // 우선순위: itemprop=price (가장 정확)
                            const el = document.querySelector('[itemprop="price"], .priceNum, .priceMain');
                            if (!el) return 0;
                            // content 속성이 있으면 우선 사용 (숫자 원본)
                            const content = el.getAttribute('content');
                            if (content && /^\\d+$/.test(content)) return parseInt(content);
                            const m = el.innerText.replace(/[^0-9]/g, '');
                            return parseInt(m) || 0;
                        }""")

                        update_sql = "UPDATE products SET product_status='available', checked_at=? WHERE id=?"
                        update_params = [now, pid]

                        if new_price > 0 and new_price != p["price_jpy"]:
                            update_sql = "UPDATE products SET product_status='available', checked_at=?, price_jpy=? WHERE id=?"
                            update_params = [now, new_price, pid]
                            result["price_changed"] += 1
                            # 가격 이력 기록 (기존 price_changes 테이블 사용)
                            try:
                                old_price = p["price_jpy"] or 0
                                change_type = "가격인하" if new_price < old_price else "가격인상"
                                conn.execute("""INSERT INTO price_changes
                                    (product_id, site_id, product_code, internal_code, brand_ko, category_id,
                                     old_price, new_price, change_type, updated_at)
                                    VALUES (?,?,?,?,?,?,?,?,?,?)""",
                                    (pid, p.get("site_id", ""), p.get("product_code", ""),
                                     p.get("internal_code", ""),
                                     p.get("brand_ko", "") or p.get("brand", ""),
                                     p.get("category_id", ""),
                                     old_price, new_price, change_type, now))
                            except Exception as e:
                                logger.warning(f"price_changes 기록 실패: {e}")
                            # 장바구니 고객 할인 상품 누적 (배치 종료 시 1회 SMS 발송)
                            if new_price < p["price_jpy"]:
                                try:
                                    _queue_cart_price_drop(
                                        p.get("product_code", ""),
                                        p.get("brand", ""),
                                        p.get("name", ""),
                                        p["price_jpy"], new_price
                                    )
                                except Exception as e:
                                    logger.warning(f"SMS 큐 실패: {e}")
                            log(f"   [{i+1}] 가격변동: {p['brand']} {(p.get('name') or '')[:25]} ¥{p['price_jpy']:,} → ¥{new_price:,}")
                        else:
                            log(f"   [{i+1}] 판매중: {p['brand']} {(p.get('name') or '')[:30]} ¥{new_price or p['price_jpy']:,}")

                        conn.execute(update_sql, update_params)

                    result["checked"] += 1
                else:
                    # 기타 상태 (403 봇차단, 502 서버오류 등)
                    result["errors"] += 1
                    err_detail = "봇 차단" if resp.status == 403 else f"서버 오류"
                    log(f"   [{i+1}] HTTP {resp.status} ({err_detail}): {p['brand']} {(p.get('name') or '')[:25]}")

            except Exception as e:
                result["errors"] += 1
                err_msg = str(e)
                # 에러 타입별 분류
                if "Timeout" in err_msg or "timeout" in err_msg:
                    err_type = "타임아웃(15초 초과)"
                elif "net::ERR_" in err_msg:
                    err_type = "네트워크 오류"
                elif "Target closed" in err_msg or "Browser closed" in err_msg:
                    err_type = "브라우저 종료됨"
                elif "Navigation" in err_msg:
                    err_type = "페이지 이동 실패"
                else:
                    err_type = "기타"
                log(f"   [{i+1}] 오류({err_type}): {p['brand']} {(p.get('name') or '')[:25]} | {err_msg[:80]}")

            # 진행률 요약 (30개마다)
            if (i + 1) % 30 == 0 or i == len(products) - 1:
                elapsed = time.time() - _batch_start if '_batch_start' in dir() else 0
                avg = elapsed / (i + 1) if i > 0 else 0
                remain = avg * (len(products) - i - 1)
                remain_min = int(remain // 60)
                remain_sec = int(remain % 60)
                log(f"   ━━━ 진행: {i+1}/{len(products)} | 판매중:{result['checked']-result['sold_out']} 품절:{result['sold_out']} 가격변동:{result['price_changed']} 오류:{result['errors']} | 남은시간: ~{remain_min}분{remain_sec}초 ━━━")
            if (i + 1) % 50 == 0:
                conn.commit()

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
        # 배치 종료 → 장바구니 고객별 할인 SMS 1회 통합 발송
        try:
            _send_batch_cart_sms()
        except Exception as e:
            logger.warning(f"배치 SMS 발송 실패: {e}")

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

    start = time.time()
    result = asyncio.run(check_products_batch(products, status_callback))
    elapsed = time.time() - start
    mins = int(elapsed // 60)
    secs = int(elapsed % 60)
    if status_callback:
        status_callback(f"배치 완료: {result['checked']}개 체크 | 품절 {result['sold_out']} | 가격변동 {result['price_changed']} | 오류 {result['errors']} | 소요 {mins}분{secs}초")
    return result
