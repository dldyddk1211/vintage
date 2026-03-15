"""
app.py
Flask 웹 대시보드 서버
접속: http://yaglobal.iptime.org:3000/jp_sourcing
"""

import asyncio
import json
import logging
import os
import threading
from datetime import datetime

from apscheduler.schedulers.background import BackgroundScheduler
from functools import wraps
from flask import Flask, jsonify, render_template, request, Response, session, redirect, url_for
import queue

from config import (
    SERVER_HOST, SERVER_PORT, URL_PREFIX,
    AUTO_SCHEDULE_HOUR, AUTO_SCHEDULE_MINUTE,
    LOGIN_USERS, SECRET_KEY, APP_ENV,
    OUTPUT_DIR, DB_DIR,
)
from data_manager import get_status as get_data_status, set_data_root, get_data_root, ensure_dirs, is_connected
from xebio_search import scrape_nike_sale, load_latest_products, set_app_status, force_close_browser
from cafe_uploader import upload_products, naver_manual_login, has_saved_cookies, delete_cookies, request_upload_stop, is_upload_stop_requested
from exchange import get_jpy_to_krw_rate, get_cached_rate, calc_buying_price, set_margin_rate, get_margin_rate, set_price_config, get_price_config
from post_generator import get_ai_config, set_ai_config, verify_ai_key
from site_config import get_sites_for_ui
from scrape_history import get_history as get_scrape_history
from cafe_schedule import load_schedule, save_schedule, load_check_schedule, save_check_schedule
from product_db import init_db as init_product_db, get_stats as bigdata_get_stats, search_products as bigdata_search, get_brands as bigdata_get_brands, delete_all as bigdata_delete_all, delete_by_site as bigdata_delete_site, get_total_count as bigdata_total, export_all as bigdata_export
from cafe_monitor import start_monitor, stop_monitor, is_monitoring, batch_check_cafe_duplicates
from telegram_bot import start_bot, stop_bot, is_bot_running

# =============================================
# 앱 초기화
# =============================================

APP_VERSION = "빅데이터 상태필터"
app = Flask(__name__, template_folder="templates", static_folder="static")
app.secret_key = SECRET_KEY
app.config["TEMPLATES_AUTO_RELOAD"] = True   # 템플릿 변경 즉시 반영
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


@app.after_request
def add_no_cache(response):
    """브라우저 캐시 방지 — 항상 최신 HTML/JS 제공"""
    if "text/html" in response.content_type:
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    return response


# =============================================
# 로그인 인증
# =============================================

def login_required(f):
    """로그인 필수 데코레이터"""
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("logged_in"):
            return redirect(f"{URL_PREFIX}/login")
        return f(*args, **kwargs)
    return decorated


@app.route(f"{URL_PREFIX}/login", methods=["GET", "POST"])
def login():
    """로그인 페이지"""
    if session.get("logged_in"):
        return redirect(f"{URL_PREFIX}/")

    error = None
    username = ""
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        if username in LOGIN_USERS and LOGIN_USERS[username] == password:
            session["logged_in"] = True
            session["username"] = username
            logger.info(f"로그인 성공: {username}")
            return redirect(f"{URL_PREFIX}/")
        else:
            error = "아이디 또는 비밀번호가 올바르지 않습니다"
            logger.warning(f"로그인 실패: {username}")

    return render_template("login.html",
                           error=error, username=username,
                           url_prefix=URL_PREFIX, env=APP_ENV)


@app.route(f"{URL_PREFIX}/logout")
def logout():
    """로그아웃"""
    session.clear()
    return redirect(f"{URL_PREFIX}/login")

# 진행상황 브로드캐스트 (멀티 클라이언트 SSE 지원)
_log_subscribers = []          # 각 클라이언트별 queue 리스트
_log_subscribers_lock = threading.Lock()
_log_history = []              # 최근 로그 100개 보관 (새 접속 시 전송)
_LOG_HISTORY_MAX = 100

# 현재 실행 상태
status = {
    "scraping": False,
    "uploading": False,
    "last_scrape": None,
    "last_upload": None,
    "product_count": 0,
    "uploaded_count": 0,
    "paused": False,      # 일시정지 플래그
    "stop_requested": False,  # 중단 요청 플래그
}


def push_log(msg: str):
    """실시간 로그를 모든 접속 클라이언트에게 브로드캐스트"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    full_msg = f"[{timestamp}] {msg}"

    # 히스토리에 저장
    _log_history.append(full_msg)
    if len(_log_history) > _LOG_HISTORY_MAX:
        _log_history.pop(0)

    # 모든 구독자에게 전송
    with _log_subscribers_lock:
        dead = []
        for q in _log_subscribers:
            try:
                q.put_nowait(full_msg)
            except Exception:
                dead.append(q)
        for q in dead:
            _log_subscribers.remove(q)

    logger.info(msg)


def _subscribe_logs() -> queue.Queue:
    """새 SSE 클라이언트 등록 — 개별 큐 반환"""
    q = queue.Queue()
    # 최근 히스토리 전송 (접속 즉시 이전 로그 확인 가능)
    for msg in _log_history:
        q.put_nowait(msg)
    with _log_subscribers_lock:
        _log_subscribers.append(q)
    return q


def _unsubscribe_logs(q: queue.Queue):
    """SSE 클라이언트 해제"""
    with _log_subscribers_lock:
        if q in _log_subscribers:
            _log_subscribers.remove(q)


# =============================================
# 스크래핑 / 업로드 실행 함수 (백그라운드)
# =============================================

def run_scrape(site_id="xebio", category_id="sale", keyword="", pages="", brand_code=""):
    """백그라운드 스레드에서 스크래핑 실행"""
    if status["scraping"]:
        push_log("⚠️ 이미 스크래핑이 진행 중입니다")
        return

    status["scraping"] = True
    try:
        products = asyncio.run(scrape_nike_sale(
            status_callback=push_log,
            site_id=site_id,
            category_id=category_id,
            keyword=keyword,
            pages=pages,
            brand_code=brand_code,
        ))
        status["product_count"] = len(products)
        status["last_scrape"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        push_log(f"🎉 스크래핑 완료: {len(products)}개 상품 수집")
    except Exception as e:
        push_log(f"❌ 스크래핑 오류: {e}")
    finally:
        status["scraping"] = False


def _shuffle_by_brand(products: list) -> list:
    """브랜드가 연속되지 않도록 섞기 — 라운드로빈 방식 (브랜드 내 수집 순서 유지)"""
    import random
    from collections import defaultdict

    brand_buckets = defaultdict(list)
    for p in products:
        brand = (p.get("brand_ko") or p.get("brand") or "기타").strip()
        brand_buckets[brand].append(p)

    # 브랜드 내부는 수집 순서 그대로 유지 (shuffle 안 함)
    # 브랜드 순서만 랜덤
    brand_keys = list(brand_buckets.keys())
    random.shuffle(brand_keys)

    # 라운드로빈으로 섞기
    result = []
    while brand_keys:
        empty_brands = []
        for brand in brand_keys:
            if brand_buckets[brand]:
                result.append(brand_buckets[brand].pop(0))
            else:
                empty_brands.append(brand)
        for b in empty_brands:
            brand_keys.remove(b)

    return result


def run_upload(max_upload=None, shuffle_brands=False, checked_codes=None, delay_min=8, delay_max=13):
    """백그라운드 스레드에서 업로드 실행

    우선순위:
    1. checked_codes에 있는 상품 우선
    2. 나머지는 대기(待機) 상품에서 랜덤으로 채움
    3. max_upload 수량만큼만 업로드
    """
    if status["uploading"]:
        push_log("⚠️ 이미 업로드가 진행 중입니다")
        return

    products = load_latest_products()
    if not products:
        push_log("⚠️ 업로드할 상품이 없습니다. 먼저 스크래핑을 실행하세요")
        return

    import random as _random

    # 체크된 상품과 대기 상품 분리
    checked_set = set(checked_codes) if checked_codes else set()
    checked_products = []
    waiting_products = []

    for p in products:
        code = p.get("product_code", "")
        is_waiting = (p.get("cafe_status") or "대기") == "대기"
        if code and code in checked_set:
            checked_products.append(p)
        elif is_waiting:
            waiting_products.append(p)

    # 업로드 대상 결정
    if checked_set:
        # 체크된 상품이 있는 경우
        if max_upload and len(checked_products) >= max_upload:
            # 체크 수 >= 업로드 수량 → 체크된 것만 수량만큼
            selected = checked_products[:max_upload]
            push_log(f"📋 체크 상품 {len(checked_products)}개 중 {max_upload}개만 업로드")
        elif max_upload and len(checked_products) < max_upload:
            # 체크 수 < 업로드 수량 → 체크 우선 + 나머지 대기에서 랜덤
            remaining = max_upload - len(checked_products)
            _random.shuffle(waiting_products)
            filler = waiting_products[:remaining]
            selected = checked_products + filler
            push_log(f"📋 체크 {len(checked_products)}개 우선 + 대기 {len(filler)}개 랜덤 = 총 {len(selected)}개 업로드")
        else:
            # 수량 지정 없음 → 체크된 것만
            selected = checked_products
            push_log(f"📋 체크된 상품 {len(selected)}개 업로드")
    else:
        # 체크 없음
        if max_upload:
            # 수량만 지정 → 대기 상품에서 랜덤
            _random.shuffle(waiting_products)
            selected = waiting_products[:max_upload]
            push_log(f"📋 대기 상품에서 랜덤 {len(selected)}개 업로드")
        else:
            # 체크도 수량도 없음 → 기존 방식 (선택된 상품)
            selected = [p for p in products if p.get("selected", True)]
            push_log(f"📋 선택된 상품 {len(selected)}개 업로드")

    if not selected:
        push_log("⚠️ 업로드할 상품이 없습니다")
        return

    # 브랜드 랜덤 섞기
    if shuffle_brands:
        selected = _shuffle_by_brand(selected)
        brands_order = [p.get("brand_ko") or p.get("brand", "") for p in selected[:10]]
        push_log(f"🔀 브랜드 랜덤 적용: {' → '.join(brands_order[:5])}...")

    push_log(f"📤 총 {len(selected)}개 업로드 시작")
    status["uploading"] = True
    try:
        count = asyncio.run(upload_products(
            products=selected,
            status_callback=push_log,
            max_upload=max_upload,
            delay_min=delay_min,
            delay_max=delay_max,
        ))
        status["uploaded_count"] = count
        status["last_upload"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # 업로드 히스토리 저장
        _save_upload_history(selected[:count])

        # 업로드 완료 상품에 cafe_uploaded 표시
        _mark_uploaded_products(selected[:count])

        push_log(f"🎉 업로드 완료: {count}개 성공")
    except Exception as e:
        push_log(f"❌ 업로드 오류: {e}")
    finally:
        status["uploading"] = False


def _save_upload_history(uploaded_products: list):
    """업로드된 상품을 히스토리에 저장 (중복 체크용)"""
    history_path = os.path.join(DB_DIR, "uploaded_history.json")
    os.makedirs(DB_DIR, exist_ok=True)
    if os.path.exists(history_path):
        with open(history_path, "r", encoding="utf-8") as f:
            history = json.load(f)
    else:
        history = []

    for p in uploaded_products:
        history.append({
            "product_code": p.get("product_code", ""),
            "name": p.get("name", ""),
            "price_jpy": p.get("price_jpy", 0),
            "brand": p.get("brand", ""),
            "uploaded_at": datetime.now().isoformat(),
        })

    with open(history_path, "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False, indent=2)


def _mark_uploaded_products(uploaded_products: list):
    """업로드 완료된 상품에 cafe_status='완료' 표시 후 latest.json + DB 저장"""
    try:
        uploaded_codes = {p.get("product_code") for p in uploaded_products if p.get("product_code")}
        if not uploaded_codes:
            return

        now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
        products = load_latest_products()
        changed = False
        for p in products:
            if p.get("product_code") in uploaded_codes:
                p["cafe_status"] = "완료"
                p["cafe_uploaded"] = True
                p["cafe_uploaded_at"] = now_str
                changed = True

        if changed:
            from xebio_search import save_products
            save_products(products)
            logger.info(f"✅ {len(uploaded_codes)}개 상품 업로드 완료 표시")

        # 빅데이터 DB에도 반영
        try:
            from product_db import update_cafe_status
            for code in uploaded_codes:
                update_cafe_status(code, "완료", now_str)
        except Exception as e:
            logger.warning(f"DB 상태 업데이트 실패: {e}")
    except Exception as e:
        logger.warning(f"업로드 완료 표시 실패: {e}")


def run_auto_pipeline():
    """자동 모드: 스크래핑 → 업로드 순서로 실행"""
    push_log("⏰ 자동 실행 시작 (스크래핑 → 업로드)")
    run_scrape()
    if status["product_count"] > 0:
        run_upload()


def run_scheduled_upload(slot_id: str, brand: str, quantity: int):
    """스케줄 슬롯에 의한 자동 카페 업로드"""
    if status["uploading"]:
        push_log(f"⚠️ [{slot_id}] 이미 업로드가 진행 중이라 스킵합니다")
        return

    products = load_latest_products()
    if not products:
        push_log(f"⏰ [{slot_id}] 업로드할 상품이 없습니다")
        return

    # 대기 상태만 필터
    waiting = [p for p in products if (p.get("cafe_status") or "대기") == "대기"]

    # 브랜드 필터
    if brand and brand != "ALL":
        waiting = [p for p in waiting
                   if (p.get("brand_ko") or "").strip() == brand
                   or (p.get("brand") or "").strip() == brand]

    if not waiting:
        push_log(f"⏰ [{slot_id}] 조건에 맞는 대기 상품이 없습니다 (브랜드: {brand})")
        return

    # 브랜드 ALL이면 랜덤 섞기
    if brand == "ALL":
        waiting = _shuffle_by_brand(waiting)

    # 수량 제한
    to_upload = waiting[:quantity]
    push_log(f"⏰ [{slot_id}] 자동 업로드 시작 — {brand} {len(to_upload)}개")

    status["uploading"] = True
    try:
        count = asyncio.run(upload_products(
            products=to_upload,
            status_callback=push_log,
            max_upload=quantity
        ))
        status["uploaded_count"] = count
        status["last_upload"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        _save_upload_history(to_upload[:count])
        _mark_uploaded_products(to_upload[:count])
        push_log(f"⏰ [{slot_id}] 자동 업로드 완료: {count}개 성공")
    except Exception as e:
        push_log(f"❌ [{slot_id}] 자동 업로드 오류: {e}")
    finally:
        status["uploading"] = False


# =============================================
# 자동 스케줄러 설정
# =============================================

scheduler = BackgroundScheduler()


def _register_schedule_jobs():
    """스케줄 설정 파일을 읽어 APScheduler 잡 등록/갱신"""
    slots = load_schedule()
    for slot in slots:
        job_id = f"cafe_schedule_{slot['id']}"
        # 기존 잡 제거
        if scheduler.get_job(job_id):
            scheduler.remove_job(job_id)
        if slot.get("enabled"):
            scheduler.add_job(
                func=run_scheduled_upload,
                trigger="cron",
                hour=slot["hour"],
                minute=slot["minute"],
                id=job_id,
                name=f"카페 자동업로드 [{slot['label']}] {slot['hour']:02d}:{slot['minute']:02d}",
                args=[slot["id"], slot.get("brand", "ALL"), slot.get("quantity", 5)],
                replace_existing=True,
            )
            logger.info(f"📅 스케줄 등록: {slot['label']} {slot['hour']:02d}:{slot['minute']:02d} (브랜드={slot.get('brand','ALL')}, 수량={slot.get('quantity',5)})")


def _register_check_schedule_job():
    """업로드 체크 자동 확인 스케줄 잡 등록/갱신"""
    job_id = "upload_check_auto"
    if scheduler.get_job(job_id):
        scheduler.remove_job(job_id)
    sched = load_check_schedule()
    if sched.get("enabled"):
        scheduler.add_job(
            func=lambda: threading.Thread(target=_run_upload_check, daemon=True).start(),
            trigger="cron",
            hour=sched["hour"],
            minute=sched["minute"],
            id=job_id,
            name=f"업로드 체크 자동확인 {sched['hour']:02d}:{sched['minute']:02d}",
            replace_existing=True,
        )
        logger.info(f"📅 체크 스케줄 등록: {sched['hour']:02d}:{sched['minute']:02d}")


_register_schedule_jobs()
_register_check_schedule_job()
scheduler.start()
set_app_status(status)  # xebio_search에 status 딕셔너리 주입

# 카페 모니터 + 텔레그램 봇 자동 시작
try:
    _monitor_started = start_monitor(log_callback=push_log, interval=180)
    _bot_started = start_bot(log_callback=push_log)
    if _monitor_started:
        logger.info("📡 카페 모니터 자동 시작됨")
    if _bot_started:
        logger.info("🤖 텔레그램 봇 자동 시작됨")
except Exception as e:
    logger.warning(f"⚠️ 모니터/봇 자동 시작 실패: {e}")


# =============================================
# 라우트 (URL)
# =============================================

@app.route(f"{URL_PREFIX}/")
@login_required
def dashboard():
    """메인 대시보드 페이지"""
    products = load_latest_products()
    rate = get_jpy_to_krw_rate()
    return render_template(
        "dashboard.html",
        status=status,
        rate=rate,
        product_count=len(products),
        schedule_time=f"{AUTO_SCHEDULE_HOUR:02d}:{AUTO_SCHEDULE_MINUTE:02d}",
        url_prefix=URL_PREFIX,
        env=APP_ENV,
        version=APP_VERSION,
    )


@app.route(f"{URL_PREFIX}/products")
@login_required
def get_products():
    """수집된 상품 목록 JSON 반환 (브랜드 필터, 페이지네이션)
    latest.json + 빅데이터 DB 미업로드 상품 병합
    """
    products = load_latest_products()

    # 빅데이터 DB에서 미업로드 상품 병합 (중복 제거)
    include_db = request.args.get("include_db", "true").lower()
    if include_db == "true":
        from product_db import get_unuploaded_products
        db_products = get_unuploaded_products()
        # latest.json에 있는 품번 수집
        existing_codes = set()
        for p in products:
            code = p.get("product_code", "")
            if code:
                existing_codes.add(code)
        # DB 상품 중 latest.json에 없는 것만 추가
        for dp in db_products:
            if dp.get("product_code") and dp["product_code"] not in existing_codes:
                existing_codes.add(dp["product_code"])
                products.append(dp)

    # 브랜드별 수량 집계 (필터 적용 전 전체 기준)
    brand_counts = {}
    for p in products:
        b = (p.get("brand_ko") or p.get("brand") or "").strip()
        if b:
            brand_counts[b] = brand_counts.get(b, 0) + 1
    total_all = len(products)

    # 브랜드 필터 (한국어/원문 모두 비교)
    brand_filter = request.args.get("brand", "").strip()
    search_filter = request.args.get("search", "").strip().lower()
    status_filter = request.args.get("status", "").strip()
    if brand_filter and brand_filter != "ALL":
        products = [p for p in products if
                    (p.get("brand_ko") or "").strip() == brand_filter or
                    (p.get("brand")    or "").strip() == brand_filter]
    if search_filter:
        products = [p for p in products if search_filter in p.get("name", "").lower()
                    or search_filter in p.get("brand", "").lower()
                    or search_filter in p.get("product_code", "").lower()]
    if status_filter and status_filter != "ALL":
        products = [p for p in products if (p.get("cafe_status") or "대기") == status_filter]
        # 완료/중복 필터 시 DB에서도 해당 상태 상품 병합
        if status_filter in ("완료", "중복"):
            try:
                from product_db import get_products_by_status
                db_status_products = get_products_by_status(status_filter)
                existing_codes = {p.get("product_code") for p in products if p.get("product_code")}
                for dp in db_status_products:
                    if dp.get("product_code") and dp["product_code"] not in existing_codes:
                        existing_codes.add(dp["product_code"])
                        products.append(dp)
            except Exception as e:
                logger.warning(f"DB 상태 조회 실패: {e}")

    page = request.args.get("page", 1, type=int)
    per_page = request.args.get("per_page", 50, type=int)
    start = (page - 1) * per_page
    end = start + per_page
    page_products = products[start:end]

    # 구매대행 가격 추가 - 환율 한 번만 조회 후 재사용
    rate = get_jpy_to_krw_rate()
    for p in page_products:
        if p.get("price_jpy"):
            p["price_info"] = calc_buying_price(p["price_jpy"], rate=rate)

    return jsonify({
        "total": len(products),
        "total_all": total_all,
        "page": page,
        "per_page": per_page,
        "products": page_products,
        "brand_counts": brand_counts,
    })


@app.route(f"{URL_PREFIX}/products/download")
@login_required
def download_excel():
    """수집된 상품 엑셀 다운로드"""
    import io
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.utils import get_column_letter
    from flask import send_file

    products = load_latest_products()
    rate = get_cached_rate()

    wb = Workbook()
    ws = wb.active
    ws.title = "상품목록"

    # ── 헤더 스타일 ─────────────────────────
    header_font    = Font(bold=True, color="FFFFFF", name="Arial", size=10)
    header_fill    = PatternFill("solid", start_color="2D3A8C")
    header_align   = Alignment(horizontal="center", vertical="center")
    thin_border    = Border(
        left=Side(style="thin"), right=Side(style="thin"),
        top=Side(style="thin"), bottom=Side(style="thin")
    )
    alt_fill       = PatternFill("solid", start_color="F0F4FF")

    # ── 헤더 정의 ───────────────────────────
    headers = [
        ("상품번호",   15),
        ("브랜드",     14),
        ("제품명(한국어)", 38),
        ("제품명(일본어)", 38),
        ("엔화",       12),
        ("구매대행원가",  16),
    ]

    for col, (title, width) in enumerate(headers, 1):
        cell = ws.cell(row=1, column=col, value=title)
        cell.font      = header_font
        cell.fill      = header_fill
        cell.alignment = header_align
        cell.border    = thin_border
        ws.column_dimensions[get_column_letter(col)].width = width

    ws.row_dimensions[1].height = 22

    # ── 데이터 행 ───────────────────────────
    price_font   = Font(name="Arial", size=9)
    normal_font  = Font(name="Arial", size=9)
    center_align = Alignment(horizontal="center", vertical="center")
    left_align   = Alignment(horizontal="left",   vertical="center", wrap_text=True)

    for row_idx, p in enumerate(products, 2):
        # 구매대행원가 계산
        cost_krw = 0
        if p.get("price_jpy"):
            from exchange import calc_buying_price
            info     = calc_buying_price(p["price_jpy"], rate=rate)
            cost_krw = info["cost_krw"]

        row_data = [
            p.get("product_code", ""),
            p.get("brand_ko") or p.get("brand", ""),
            p.get("name_ko")  or p.get("name",  ""),
            p.get("name", ""),
            p.get("price_jpy", 0),
            cost_krw,
        ]

        fill = alt_fill if row_idx % 2 == 0 else None

        for col, value in enumerate(row_data, 1):
            cell = ws.cell(row=row_idx, column=col, value=value)
            cell.border = thin_border
            cell.font   = normal_font

            if col in (1, 2):           # 상품번호, 브랜드 - 중앙정렬
                cell.alignment = center_align
            elif col in (3, 4):         # 제품명 - 좌측정렬 + 줄바꿈
                cell.alignment = left_align
            elif col in (5, 6):         # 가격 - 숫자 형식
                cell.alignment = center_align
                if col == 5:
                    cell.number_format = '#,##0"엔"'
                else:
                    cell.number_format = '#,##0"원"'

            if fill:
                cell.fill = fill

        ws.row_dimensions[row_idx].height = 18

    # ── 요약 행 ─────────────────────────────
    sum_row = len(products) + 2
    ws.cell(row=sum_row, column=1, value="합계").font = Font(bold=True, name="Arial", size=9)
    ws.cell(row=sum_row, column=5, value=f'=SUM(E2:E{sum_row-1})').number_format = '#,##0"엔"'
    ws.cell(row=sum_row, column=6, value=f'=SUM(F2:F{sum_row-1})').number_format = '#,##0"원"'
    for col in range(1, 7):
        ws.cell(row=sum_row, column=col).fill   = PatternFill("solid", start_color="E8EDFF")
        ws.cell(row=sum_row, column=col).border = thin_border

    # ── 파일 저장 후 전송 ────────────────────
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)

    from datetime import datetime
    filename = f"xebio_sale_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
    return send_file(
        buf,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        as_attachment=True,
        download_name=filename
    )


@app.route(f"{URL_PREFIX}/products/brands")
@login_required
def get_brands():
    """수집된 상품의 브랜드 목록 반환 (한국어 번역 우선, DB 미업로드 상품 포함)"""
    products = load_latest_products()

    # DB 미업로드 상품 병합
    from product_db import get_unuploaded_products
    db_products = get_unuploaded_products()
    existing_codes = {p.get("product_code", "") for p in products if p.get("product_code")}
    for dp in db_products:
        if dp.get("product_code") and dp["product_code"] not in existing_codes:
            existing_codes.add(dp["product_code"])
            products.append(dp)

    brand_map = {}  # 원문 → 한국어 매핑
    brand_counts = {}

    for p in products:
        b_raw = (p.get("brand") or "").strip()
        b_ko  = (p.get("brand_ko") or b_raw).strip()
        if not b_raw:
            continue
        brand_map[b_raw] = b_ko
        key = b_ko or b_raw
        brand_counts[key] = brand_counts.get(key, 0) + 1

    # 한국어 기준으로 정렬
    brands_ko = sorted(set(brand_map.values()))
    return jsonify({
        "brands"   : brands_ko,          # 콤보박스에 표시할 한국어 목록
        "brand_map": brand_map,          # 원문→한국어 (필터 시 역매핑용)
        "counts"   : brand_counts,
    })


@app.route(f"{URL_PREFIX}/products/update", methods=["POST"])
@login_required
def update_products():
    """상품 선택 상태 업데이트 (체크박스)"""
    data = request.json or {}
    # product_code 기반 선택 (우선) 또는 인덱스 기반 (하위호환)
    selected_codes = set(data.get("selected_codes", []))
    selected_ids = set(data.get("selected", []))

    products = load_latest_products()
    if selected_codes:
        for p in products:
            p["selected"] = p.get("product_code", "") in selected_codes
    else:
        for i, p in enumerate(products):
            p["selected"] = i in selected_ids

    from xebio_search import save_products
    save_products(products)
    count = sum(1 for p in products if p.get("selected"))
    return jsonify({"ok": True, "selected_count": count})


@app.route(f"{URL_PREFIX}/products/delete", methods=["POST"])
@login_required
def delete_products():
    """상품 삭제 (인덱스 기준, 복수 가능)"""
    data = request.json or {}
    indices = data.get("indices", [])
    if not indices:
        return jsonify({"ok": False, "message": "삭제할 인덱스가 없습니다"})

    products = load_latest_products()
    # 내림차순 정렬 후 삭제 (인덱스 밀림 방지)
    valid = sorted(set(int(i) for i in indices if 0 <= int(i) < len(products)), reverse=True)
    for i in valid:
        products.pop(i)

    from xebio_search import save_products
    save_products(products)
    msg = f"상품 {len(valid)}개 삭제 완료 (남은 상품: {len(products)}개)"
    push_log(f"🗑️ " + msg)
    return jsonify({"ok": True, "deleted": len(valid), "remaining": len(products), "message": msg})


@app.route(f"{URL_PREFIX}/products/check-duplicate", methods=["POST"])
@login_required
def check_duplicate():
    """업로드 전 중복 체크 (품번 + 가격 기준)"""
    data = request.json or {}
    selected_indices = data.get("indices", [])
    products = load_latest_products()

    uploaded_path = os.path.join(DB_DIR, "uploaded_history.json")
    if os.path.exists(uploaded_path):
        with open(uploaded_path, "r", encoding="utf-8") as f:
            history = json.load(f)
    else:
        history = []

    results = {"block": [], "price_changed": [], "ok": []}
    for idx in selected_indices:
        if idx >= len(products):
            continue
        p = products[idx]
        code = p.get("product_code", "")
        price = p.get("price_jpy", 0)

        matched = [h for h in history if h.get("product_code") == code and code]
        if not matched:
            results["ok"].append(idx)
        else:
            old_price = matched[-1].get("price_jpy", 0)
            if old_price == price:
                results["block"].append({"idx": idx, "name": p.get("name"), "reason": "동일 가격 중복"})
            else:
                results["price_changed"].append({
                    "idx": idx, "name": p.get("name"),
                    "old_price": old_price, "new_price": price,
                    "diff": price - old_price
                })

    return jsonify(results)


@app.route(f"{URL_PREFIX}/products/status", methods=["POST"])
@login_required
def update_product_status():
    """개별 상품의 cafe_status 변경 (대기/완료/중복)"""
    data = request.json or {}
    product_code = data.get("product_code", "").strip()
    new_status = data.get("status", "").strip()

    if not product_code or new_status not in ("대기", "완료", "중복"):
        return jsonify({"ok": False, "message": "잘못된 요청입니다"})

    products = load_latest_products()
    found = False
    for p in products:
        if p.get("product_code") == product_code:
            p["cafe_status"] = new_status
            if new_status == "완료":
                p["cafe_uploaded"] = True
                p["cafe_uploaded_at"] = datetime.now().strftime("%Y-%m-%d %H:%M")
            elif new_status == "대기":
                p["cafe_uploaded"] = False
                p.pop("cafe_uploaded_at", None)
            found = True
            break

    if not found:
        return jsonify({"ok": False, "message": "상품을 찾을 수 없습니다"})

    from xebio_search import save_products
    save_products(products)
    # DB에도 반영
    try:
        from product_db import update_cafe_status
        uploaded_at = datetime.now().strftime("%Y-%m-%d %H:%M") if new_status == "완료" else ""
        update_cafe_status(product_code, new_status, uploaded_at)
    except Exception as e:
        logger.warning(f"DB 상태 업데이트 실패: {e}")
    return jsonify({"ok": True, "product_code": product_code, "status": new_status})


@app.route(f"{URL_PREFIX}/products/bulk-status", methods=["POST"])
@login_required
def bulk_update_product_status():
    """체크된 상품의 cafe_status 일괄 변경"""
    data = request.json or {}
    codes = data.get("codes", [])
    new_status = data.get("status", "").strip()

    if not codes or new_status not in ("대기", "완료", "중복"):
        return jsonify({"ok": False, "message": "잘못된 요청입니다"})

    products = load_latest_products()
    code_set = set(codes)
    count = 0
    for p in products:
        if p.get("product_code") in code_set:
            p["cafe_status"] = new_status
            if new_status == "완료":
                p["cafe_uploaded"] = True
                p["cafe_uploaded_at"] = datetime.now().strftime("%Y-%m-%d %H:%M")
            elif new_status == "대기":
                p["cafe_uploaded"] = False
                p.pop("cafe_uploaded_at", None)
            count += 1

    from xebio_search import save_products
    save_products(products)
    # DB에도 반영
    try:
        from product_db import update_cafe_status
        uploaded_at = datetime.now().strftime("%Y-%m-%d %H:%M") if new_status == "완료" else ""
        for code in codes:
            update_cafe_status(code, new_status, uploaded_at)
    except Exception as e:
        logger.warning(f"DB 일괄 상태 업데이트 실패: {e}")
    return jsonify({"ok": True, "count": count, "status": new_status})


@app.route(f"{URL_PREFIX}/status")
@login_required
def get_status():
    """현재 실행 상태 반환"""
    products = load_latest_products()
    return jsonify({
        **status,
        "product_count": len(products),
        "rate": get_cached_rate(),  # 캐시 우선 사용
        "margin": get_margin_rate(),
        "schedule_time": f"{AUTO_SCHEDULE_HOUR:02d}:{AUTO_SCHEDULE_MINUTE:02d}",
    })


# ── 수동 실행 API ──────────────────────────

@app.route(f"{URL_PREFIX}/run/scrape", methods=["POST"])
@login_required
def manual_scrape():
    """수동 스크래핑 실행"""
    data = request.json or {}
    site_id = data.get("site_id", "xebio")
    category_id = data.get("category_id", "sale")
    keyword = data.get("keyword", "")
    pages = data.get("pages", "")
    brand_code = data.get("brand_code", "")
    thread = threading.Thread(
        target=run_scrape,
        args=(site_id, category_id, keyword, pages, brand_code),
        daemon=True,
    )
    thread.start()
    desc = f"{site_id} › {category_id}"
    if brand_code:
        desc += f" [{brand_code}]"
    if keyword:
        desc += f" [{keyword}]"
    if pages:
        desc += f" (p.{pages})"
    return jsonify({"ok": True, "message": f"스크래핑 시작됨 ({desc})"})


# ── 사이트/카테고리 API ────────────────────────

@app.route(f"{URL_PREFIX}/sites", methods=["GET"])
@login_required
def api_sites():
    """사이트/카테고리 트리 반환"""
    return jsonify(get_sites_for_ui())


@app.route(f"{URL_PREFIX}/scrape-history", methods=["GET"])
@login_required
def api_scrape_history():
    """수집 이력 반환"""
    limit = request.args.get("limit", 50, type=int)
    return jsonify(get_scrape_history(limit))


# ── 빅데이터 관리 API ──────────────────────────

@app.route(f"{URL_PREFIX}/bigdata/stats", methods=["GET"])
@login_required
def api_bigdata_stats():
    """빅데이터 통계"""
    return jsonify(bigdata_get_stats())


@app.route(f"{URL_PREFIX}/bigdata/products", methods=["GET"])
@login_required
def api_bigdata_products():
    """빅데이터 상품 검색"""
    return jsonify(bigdata_search(
        query=request.args.get("q", ""),
        site_id=request.args.get("site_id", ""),
        category_id=request.args.get("category_id", ""),
        brand=request.args.get("brand", ""),
        cafe_status=request.args.get("cafe_status", ""),
        page=request.args.get("page", 1, type=int),
        per_page=request.args.get("per_page", 50, type=int),
    ))


@app.route(f"{URL_PREFIX}/bigdata/brands", methods=["GET"])
@login_required
def api_bigdata_brands():
    """빅데이터 브랜드 목록"""
    return jsonify(bigdata_get_brands())


@app.route(f"{URL_PREFIX}/bigdata/delete", methods=["POST"])
@login_required
def api_bigdata_delete():
    """빅데이터 삭제"""
    data = request.json or {}
    scope = data.get("scope", "")
    if scope == "all":
        count = bigdata_delete_all()
        return jsonify({"ok": True, "deleted": count, "message": f"전체 {count}개 삭제"})
    elif scope == "site":
        site_id = data.get("site_id", "")
        if not site_id:
            return jsonify({"ok": False, "message": "site_id 필요"})
        count = bigdata_delete_site(site_id)
        return jsonify({"ok": True, "deleted": count, "message": f"{site_id} {count}개 삭제"})
    return jsonify({"ok": False, "message": "scope 지정 필요 (all 또는 site)"})


@app.route(f"{URL_PREFIX}/bigdata/download")
@login_required
def api_bigdata_download():
    """빅데이터 엑셀 다운로드"""
    import io
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.utils import get_column_letter
    from flask import send_file

    q = request.args.get("q", "")
    site_id = request.args.get("site_id", "")
    brand = request.args.get("brand", "")

    products = bigdata_export(query=q, site_id=site_id, brand=brand)

    # 환율 정보
    rate = get_cached_rate()

    wb = Workbook()
    ws = wb.active
    ws.title = "수집상품"

    # ── 스타일 ─────────────────────────
    header_font = Font(bold=True, color="FFFFFF", name="Arial", size=10)
    header_fill = PatternFill("solid", start_color="1a1710")
    header_align = Alignment(horizontal="center", vertical="center")
    thin_border = Border(
        left=Side(style="thin"), right=Side(style="thin"),
        top=Side(style="thin"), bottom=Side(style="thin")
    )
    alt_fill = PatternFill("solid", start_color="F5F0E0")
    gold_fill = PatternFill("solid", start_color="D4A54A")

    # ── 헤더 ───────────────────────────
    headers = [
        ("사이트", 12),
        ("카테고리", 12),
        ("품번", 16),
        ("브랜드", 14),
        ("상품명(한국어)", 40),
        ("상품명(일본어)", 40),
        ("일본가(엔)", 14),
        ("할인전가", 14),
        ("할인율(%)", 10),
        ("구매대행원가", 16),
        ("재고", 8),
        ("링크", 40),
        ("수집일", 18),
    ]

    for col, (title, width) in enumerate(headers, 1):
        cell = ws.cell(row=1, column=col, value=title)
        cell.font = header_font
        cell.fill = gold_fill
        cell.alignment = header_align
        cell.border = thin_border
        ws.column_dimensions[get_column_letter(col)].width = width
    ws.row_dimensions[1].height = 24

    # ── 데이터 ─────────────────────────
    normal_font = Font(name="Arial", size=9)
    center_align = Alignment(horizontal="center", vertical="center")
    left_align = Alignment(horizontal="left", vertical="center", wrap_text=True)

    for row_idx, p in enumerate(products, 2):
        cost_krw = 0
        if p.get("price_jpy") and rate:
            try:
                from exchange import calc_buying_price
                info = calc_buying_price(p["price_jpy"], rate=rate)
                cost_krw = info["cost_krw"]
            except Exception:
                pass

        row_data = [
            p.get("site_id", ""),
            p.get("category_id", ""),
            p.get("product_code", ""),
            p.get("brand_ko", ""),
            p.get("name_ko", ""),
            p.get("name", ""),
            p.get("price_jpy", 0),
            p.get("original_price", 0),
            p.get("discount_rate", 0),
            cost_krw,
            "O" if p.get("in_stock") else "X",
            p.get("link", ""),
            p.get("created_at", ""),
        ]

        fill = alt_fill if row_idx % 2 == 0 else None
        for col, value in enumerate(row_data, 1):
            cell = ws.cell(row=row_idx, column=col, value=value)
            cell.border = thin_border
            cell.font = normal_font
            if col in (1, 2, 3, 4, 9, 11):
                cell.alignment = center_align
            elif col in (5, 6, 12):
                cell.alignment = left_align
            elif col in (7, 8, 10):
                cell.alignment = center_align
                if col == 7:
                    cell.number_format = '#,##0'
                elif col == 8:
                    cell.number_format = '#,##0'
                elif col == 10:
                    cell.number_format = '#,##0'
            if fill:
                cell.fill = fill
        ws.row_dimensions[row_idx].height = 18

    # ── 요약 ───────────────────────────
    sum_row = len(products) + 2
    ws.cell(row=sum_row, column=1, value=f"합계 {len(products)}건").font = Font(bold=True, name="Arial", size=9)
    ws.cell(row=sum_row, column=7, value=f'=SUM(G2:G{sum_row-1})').number_format = '#,##0'
    ws.cell(row=sum_row, column=10, value=f'=SUM(J2:J{sum_row-1})').number_format = '#,##0'
    for col in range(1, 14):
        ws.cell(row=sum_row, column=col).fill = PatternFill("solid", start_color="E8E0D0")
        ws.cell(row=sum_row, column=col).border = thin_border

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)

    suffix = f"_{site_id}" if site_id else ""
    filename = f"bigdata{suffix}_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
    return send_file(
        buf,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        as_attachment=True,
        download_name=filename,
    )


# ── 카페 모니터 & 텔레그램 봇 API ─────────────

@app.route(f"{URL_PREFIX}/monitor/status", methods=["GET"])
@login_required
def api_monitor_status():
    """모니터/봇 상태"""
    return jsonify({
        "monitor_running": is_monitoring(),
        "bot_running": is_bot_running(),
    })


@app.route(f"{URL_PREFIX}/monitor/start", methods=["POST"])
@login_required
def api_monitor_start():
    """카페 모니터 + 텔레그램 봇 시작"""
    data = request.json or {}
    interval = data.get("interval", 180)

    monitor_ok = start_monitor(log_callback=push_log, interval=interval)
    bot_ok = start_bot(log_callback=push_log)

    return jsonify({
        "ok": True,
        "monitor": "시작됨" if monitor_ok else "이미 실행중",
        "bot": "시작됨" if bot_ok else "이미 실행중",
    })


@app.route(f"{URL_PREFIX}/monitor/stop", methods=["POST"])
@login_required
def api_monitor_stop():
    """카페 모니터 + 텔레그램 봇 종료"""
    stop_monitor()
    stop_bot()
    return jsonify({"ok": True, "message": "모니터 & 봇 종료"})


# ── 카페 업로드 스케줄 API ────────────────────

@app.route(f"{URL_PREFIX}/cafe-schedule", methods=["GET"])
@login_required
def api_get_schedule():
    """스케줄 설정 조회"""
    slots = load_schedule()
    # 현재 등록된 잡 상태도 포함
    for slot in slots:
        job_id = f"cafe_schedule_{slot['id']}"
        job = scheduler.get_job(job_id)
        slot["registered"] = job is not None
        if job and job.next_run_time:
            slot["next_run"] = job.next_run_time.strftime("%Y-%m-%d %H:%M")
        else:
            slot["next_run"] = None
    return jsonify({"ok": True, "slots": slots})


@app.route(f"{URL_PREFIX}/cafe-schedule", methods=["POST"])
@login_required
def api_save_schedule():
    """스케줄 설정 저장 + 잡 재등록"""
    data = request.json or {}
    slots = data.get("slots", [])
    if not isinstance(slots, list) or len(slots) != 4:
        return jsonify({"ok": False, "error": "4개 슬롯 필요"}), 400

    save_schedule(slots)
    _register_schedule_jobs()
    push_log("📅 카페 업로드 스케줄 설정이 저장되었습니다")
    return jsonify({"ok": True})


# ── 업로드 체크 자동 확인 스케줄 API ──────────────

@app.route(f"{URL_PREFIX}/check-schedule", methods=["GET"])
@login_required
def api_get_check_schedule():
    """업로드 체크 스케줄 설정 조회"""
    sched = load_check_schedule()
    job = scheduler.get_job("upload_check_auto")
    sched["registered"] = job is not None
    if job and job.next_run_time:
        sched["next_run"] = job.next_run_time.strftime("%Y-%m-%d %H:%M")
    else:
        sched["next_run"] = None
    return jsonify({"ok": True, "schedule": sched})


@app.route(f"{URL_PREFIX}/check-schedule", methods=["POST"])
@login_required
def api_save_check_schedule():
    """업로드 체크 스케줄 설정 저장 + 잡 재등록"""
    data = request.json or {}
    sched = {
        "enabled": bool(data.get("enabled", False)),
        "hour": int(data.get("hour", 9)),
        "minute": int(data.get("minute", 0)),
    }
    save_check_schedule(sched)
    _register_check_schedule_job()
    push_log(f"📅 업로드 체크 자동확인 설정 저장됨: {'활성' if sched['enabled'] else '비활성'} {sched['hour']:02d}:{sched['minute']:02d}")
    return jsonify({"ok": True})


@app.route(f"{URL_PREFIX}/upload-status-summary", methods=["GET"])
@login_required
def api_upload_status_summary():
    """업로드 상태 요약 — 대기/완료/중복/전체 수량 + 예상 시간"""
    products = load_latest_products()
    waiting = 0
    uploaded = 0
    duplicate = 0
    total = len(products)
    for p in products:
        st = (p.get("cafe_status") or "대기")
        if st == "대기":
            waiting += 1
        elif st == "완료":
            uploaded += 1
        elif st == "중복":
            duplicate += 1
    return jsonify({
        "ok": True,
        "waiting_count": waiting,
        "uploaded_count": uploaded,
        "duplicate_count": duplicate,
        "total_count": total,
        "avg_minutes_per_post": 10,
    })


@app.route(f"{URL_PREFIX}/run/upload", methods=["POST"])
@login_required
def manual_upload():
    """수동 업로드 실행"""
    data = request.json or {}
    max_upload = data.get("max_upload")
    shuffle_brands = data.get("shuffle_brands", False)
    checked_codes = data.get("checked_codes")  # 체크된 상품 코드 배열
    delay_min = data.get("delay_min", 8)
    delay_max = data.get("delay_max", 13)
    thread = threading.Thread(
        target=run_upload,
        args=(max_upload, shuffle_brands, checked_codes, delay_min, delay_max),
        daemon=True,
    )
    thread.start()
    return jsonify({"ok": True, "message": "업로드 시작됨"})


@app.route(f"{URL_PREFIX}/run/upload-stop", methods=["POST"])
@login_required
def upload_stop():
    """업로드 중지 요청"""
    request_upload_stop()
    push_log("⏹ 업로드 중지 요청됨 — 현재 작업 완료 후 중지됩니다")
    return jsonify({"ok": True, "message": "업로드 중지 요청됨"})


_upload_check_stop = False

def _run_upload_check():
    """백그라운드에서 카페 중복 체크 실행 (Playwright 브라우저 사용)"""
    global _upload_check_stop
    _upload_check_stop = False

    from config import CAFE_MY_NICKNAME

    products = load_latest_products()
    waiting = [p for p in products if (p.get("cafe_status") or "대기") == "대기"]

    if not waiting:
        push_log("⚠️ 대기 상품이 없습니다")
        return

    push_log(f"🔍 업로드 체크 시작: {len(waiting)}개 상품 — 브라우저로 카페 검색 중...")

    from xebio_search import save_products

    def stop_check():
        return _upload_check_stop

    try:
        checked, duplicates = asyncio.run(
            batch_check_cafe_duplicates(
                products=waiting,
                nickname=CAFE_MY_NICKNAME,
                days=30,
                log=push_log,
                save_callback=lambda: save_products(products),
                stop_check=stop_check,
            )
        )

        # 최종 저장
        save_products(products)

        if _upload_check_stop:
            push_log(f"⏹ 체크 중지됨: {checked}개 확인, {duplicates}개 중복 발견")
        else:
            push_log(f"✅ 체크 완료: {checked}개 확인, {duplicates}개 중복 발견")

    except Exception as e:
        push_log(f"❌ 체크 오류: {e}")
    finally:
        _upload_check_stop = False


@app.route(f"{URL_PREFIX}/ai/verify", methods=["POST"])
@login_required
def ai_verify():
    """AI API 키 정상 작동 여부 확인"""
    result = verify_ai_key()
    return jsonify(result)


@app.route(f"{URL_PREFIX}/run/upload-check", methods=["POST"])
@login_required
def upload_check():
    """대기 상품을 카페에서 검색하여 중복 여부 체크 (백그라운드)"""
    thread = threading.Thread(target=_run_upload_check, daemon=True)
    thread.start()
    return jsonify({"ok": True, "message": "카페 중복 체크 시작됨 — 로그를 확인하세요"})


@app.route(f"{URL_PREFIX}/run/upload-check-stop", methods=["POST"])
@login_required
def upload_check_stop():
    """업로드 체크 중지"""
    global _upload_check_stop
    _upload_check_stop = True
    push_log("⏹ 업로드 체크 중지 요청됨")
    return jsonify({"ok": True})


@app.route(f"{URL_PREFIX}/run/auto", methods=["POST"])
@login_required
def manual_auto():
    """수동으로 자동 파이프라인(스크래핑+업로드) 실행"""
    thread = threading.Thread(target=run_auto_pipeline, daemon=True)
    thread.start()
    return jsonify({"ok": True, "message": "자동 파이프라인 시작됨"})


@app.route(f"{URL_PREFIX}/products/translate", methods=["POST"])
@login_required
def translate_products():
    """기존 수집 데이터 일괄 번역"""
    try:
        from translator import translate_ja_ko, translate_brand, TRANSLATE_AVAILABLE
        if not TRANSLATE_AVAILABLE:
            return jsonify({"ok": False, "message": "googletrans 미설치 — pip install googletrans==4.0.0-rc1"})

        products = load_latest_products()
        if not products:
            return jsonify({"ok": False, "message": "수집된 상품이 없습니다"})

        push_log(f"🌐 번역 시작: 총 {len(products)}개 상품")
        count = 0
        for p in products:
            changed = False
            # 상품명 번역
            if p.get("name") and not p.get("name_ko"):
                p["name_ko"] = translate_ja_ko(p["name"])
                changed = True
            # 브랜드 번역
            if p.get("brand") and not p.get("brand_ko"):
                p["brand_ko"] = translate_brand(p["brand"])
                changed = True
            # 상세 설명 번역
            if p.get("description") and not p.get("description_ko"):
                p["description_ko"] = translate_ja_ko(p["description"])
                changed = True
            if changed:
                count += 1

        from xebio_search import save_products
        save_products(products)
        msg = f"번역 완료: {count}개 상품"
        push_log(f"✅ " + msg)
        return jsonify({"ok": True, "message": msg, "count": count})

    except Exception as e:
        push_log(f"❌ 번역 오류: {e}")
        return jsonify({"ok": False, "message": str(e)})


@app.route(f"{URL_PREFIX}/settings/dict", methods=["GET"])
@login_required
def get_dict():
    """커스텀 단어장 조회"""
    from translator import CUSTOM_DICT
    return jsonify({"dict": CUSTOM_DICT})


@app.route(f"{URL_PREFIX}/settings/dict", methods=["POST"])
@login_required
def update_dict():
    """커스텀 단어장 단어 추가/수정"""
    from translator import CUSTOM_DICT
    data = request.json or {}
    ja = data.get("ja", "").strip()
    ko = data.get("ko", "").strip()
    if not ja or not ko:
        return jsonify({"ok": False, "message": "일본어와 한국어를 모두 입력해주세요"})
    CUSTOM_DICT[ja] = ko
    push_log(f"📖 단어 추가: {ja} → {ko}")
    return jsonify({"ok": True, "message": f"{ja} → {ko} 추가 완료"})


@app.route(f"{URL_PREFIX}/settings/dict/<path:ja>", methods=["DELETE"])
@login_required
def delete_dict(ja):
    """커스텀 단어장 단어 삭제"""
    from translator import CUSTOM_DICT
    ja = ja.strip()
    if ja in CUSTOM_DICT:
        del CUSTOM_DICT[ja]
        push_log(f"🗑️ 단어 삭제: {ja}")
        return jsonify({"ok": True})
    return jsonify({"ok": False, "message": "단어 없음"})


@app.route(f"{URL_PREFIX}/settings/margin", methods=["POST"])
@login_required
def update_margin():
    """마진율 변경 (하위 호환)"""
    data = request.json or {}
    pct = data.get("margin_pct", 20)   # 퍼센트로 받기 (예: 20 → 1.2)
    rate = 1 + (pct / 100)
    rate = max(1.0, min(rate, 3.0))    # 0~200% 범위 제한
    set_margin_rate(rate)
    msg = f"마진율 변경: {pct}% (x{round(rate,2)})"
    push_log("💰 " + msg)
    return jsonify({"ok": True, "margin_pct": pct, "margin_rate": round(rate, 2), "message": msg})


@app.route(f"{URL_PREFIX}/settings/price", methods=["GET"])
@login_required
def get_price_settings():
    """현재 가격 설정 조회"""
    return jsonify({"ok": True, **get_price_config()})


@app.route(f"{URL_PREFIX}/settings/price", methods=["POST"])
@login_required
def update_price_settings():
    """가격 계산 변수 일괄 변경"""
    data = request.json or {}
    jp_fee   = data.get("jp_fee_pct")       # % 단위 (예: 3 → 0.03)
    markup   = data.get("buy_markup_pct")   # % 단위 (예: 2 → 0.02)
    margin   = data.get("margin_pct")       # % 단위 (예: 10 → 0.10)
    shipping = data.get("intl_shipping_krw")# 원화 (예: 15000)

    set_price_config(
        jp_fee   = jp_fee   / 100 if jp_fee   is not None else None,
        buy_markup = markup / 100 if markup   is not None else None,
        margin   = margin   / 100 if margin   is not None else None,
        shipping = shipping if shipping is not None else None,
    )
    cfg = get_price_config()
    msg = (f"가격설정 변경: 수수료={cfg['jp_fee_pct']}% "
           f"환율추가={cfg['buy_markup_pct']}% "
           f"마진={cfg['margin_pct']}% "
           f"배송={cfg['intl_shipping_krw']:,}원")
    push_log("💰 " + msg)
    return jsonify({"ok": True, **cfg, "message": msg})


# ── 데이터 경로 설정 ─────────────────────

@app.route(f"{URL_PREFIX}/settings/data-path", methods=["GET"])
@login_required
def get_data_path():
    """데이터 저장 경로 상태 조회"""
    return jsonify({"ok": True, **get_data_status()})


@app.route(f"{URL_PREFIX}/settings/data-path", methods=["POST"])
@login_required
def update_data_path():
    """데이터 저장 경로 변경"""
    data = request.json or {}
    new_path = data.get("path", "").strip()
    if not new_path:
        return jsonify({"ok": False, "message": "경로를 입력해주세요"})

    ok = set_data_root(new_path)
    if ok:
        push_log(f"📁 데이터 경로 변경: {new_path}")
        return jsonify({"ok": True, "message": f"경로 변경 완료: {new_path}", **get_data_status()})
    else:
        return jsonify({"ok": False, "message": "경로 생성 실패 — 경로를 확인해주세요"})


@app.route(f"{URL_PREFIX}/settings/data-path/reset", methods=["POST"])
@login_required
def reset_data_path():
    """데이터 저장 경로 초기화 (OS 기본값)"""
    from data_manager import _default_path
    default = _default_path()
    ok = set_data_root(default)
    if ok:
        push_log(f"📁 데이터 경로 초기화: {default}")
        return jsonify({"ok": True, "message": f"기본 경로로 초기화: {default}", **get_data_status()})
    return jsonify({"ok": False, "message": "초기화 실패"})


# ── AI 설정 ─────────────────────────────

@app.route(f"{URL_PREFIX}/settings/ai", methods=["GET"])
@login_required
def get_ai_settings():
    """AI 설정 조회"""
    return jsonify(get_ai_config())


@app.route(f"{URL_PREFIX}/settings/ai", methods=["POST"])
@login_required
def update_ai_settings():
    """AI 설정 변경 (provider, gemini_key, claude_key, openai_key)"""
    data = request.json or {}
    set_ai_config(
        provider=data.get("provider"),
        gemini_key=data.get("gemini_key"),
        claude_key=data.get("claude_key"),
        openai_key=data.get("openai_key"),
    )
    push_log(f"🤖 AI 설정 변경: {data.get('provider', '변경없음')}")
    return jsonify({"ok": True, **get_ai_config()})


@app.route(f"{URL_PREFIX}/settings/ai/test", methods=["POST"])
@login_required
def test_ai():
    """AI 연결 테스트"""
    try:
        from post_generator import verify_ai_key, _ai_config
        provider = _ai_config["provider"]
        has_key = bool(_ai_config.get("openai_key")) if provider == "openai" else \
                  bool(_ai_config.get("gemini_key")) if provider == "gemini" else \
                  bool(_ai_config.get("claude_key"))
        logger.info(f"🧪 AI 테스트 — provider: {provider}, key_set: {has_key}")
        result = verify_ai_key()
        logger.info(f"🧪 AI 테스트 결과 — ok: {result['ok']}, msg: {result['message']}")
        if result["ok"]:
            return jsonify({"ok": True, "provider": result["provider"], "response": result["message"]})
        else:
            return jsonify({"ok": False, "message": f"[{result['provider']}] {result['message']}"})
    except Exception as e:
        import traceback
        logger.error(f"🧪 AI 테스트 예외: {traceback.format_exc()}")
        return jsonify({"ok": False, "message": str(e)})


# ── 텔레그램 알림 설정 ────────────────────

@app.route(f"{URL_PREFIX}/settings/telegram", methods=["GET"])
@login_required
def get_telegram_settings():
    """텔레그램 설정 조회"""
    from notifier import get_telegram_config
    return jsonify({"ok": True, **get_telegram_config()})


@app.route(f"{URL_PREFIX}/settings/telegram", methods=["POST"])
@login_required
def update_telegram_settings():
    """텔레그램 설정 변경"""
    from notifier import set_telegram_config, get_telegram_config
    data = request.json or {}
    set_telegram_config(
        bot_token=data.get("bot_token"),
        chat_id=data.get("chat_id"),
    )
    push_log("📬 텔레그램 설정 변경")
    return jsonify({"ok": True, **get_telegram_config()})


@app.route(f"{URL_PREFIX}/settings/telegram/test", methods=["POST"])
@login_required
def test_telegram():
    """텔레그램 연결 테스트"""
    from notifier import send_telegram, is_configured
    if not is_configured():
        return jsonify({"ok": False, "message": "텔레그램 설정이 필요합니다 (Bot Token + Chat ID)"})
    ok = send_telegram("🔔 JP Sourcing 텔레그램 알림 테스트!")
    if ok:
        return jsonify({"ok": True, "message": "테스트 메시지 전송 성공!"})
    return jsonify({"ok": False, "message": "전송 실패 — Token/Chat ID를 확인해주세요"})


# ── 네이버 로그인 (쿠키 저장) ────────────

@app.route(f"{URL_PREFIX}/naver/status")
@login_required
def naver_status():
    """네이버 로그인 상태 확인"""
    return jsonify({"logged_in": has_saved_cookies()})


@app.route(f"{URL_PREFIX}/naver/login", methods=["POST"])
@login_required
def naver_login():
    """네이버 수동 로그인 시작 (브라우저 열림)"""
    def run_login():
        result = asyncio.run(naver_manual_login(status_callback=push_log))
        if result:
            push_log("✅ 네이버 로그인 & 쿠키 저장 완료!")
        else:
            push_log("❌ 네이버 로그인 실패 또는 시간 초과")

    thread = threading.Thread(target=run_login, daemon=True)
    thread.start()
    return jsonify({"ok": True, "message": "로그인 브라우저가 열립니다. 직접 로그인해주세요."})


@app.route(f"{URL_PREFIX}/naver/logout", methods=["POST"])
@login_required
def naver_logout():
    """네이버 쿠키 삭제"""
    delete_cookies()
    push_log("🗑️ 네이버 쿠키 삭제 완료")
    return jsonify({"ok": True, "message": "네이버 로그인 정보가 삭제되었습니다"})


@app.route(f"{URL_PREFIX}/run/pause", methods=["POST"])
@login_required
def pause_scrape():
    """일시정지: 현재 상품 완료 후 멈춤"""
    if not status["scraping"]:
        return jsonify({"ok": False, "message": "실행 중인 작업이 없습니다"})
    status["paused"] = True
    push_log("⏸️ 일시정지 요청 — 현재 상품 수집 완료 후 멈춥니다...")
    return jsonify({"ok": True, "message": "일시정지 요청됨"})


@app.route(f"{URL_PREFIX}/run/resume", methods=["POST"])
@login_required
def resume_scrape():
    """일시정지 해제"""
    status["paused"] = False
    push_log("▶️ 재개 — 수집을 계속합니다!")
    return jsonify({"ok": True, "message": "재개됨"})


@app.route(f"{URL_PREFIX}/run/reset", methods=["POST"])
@login_required
def reset_all():
    """리셋: 수집 중단 + 브라우저 강제 종료 + 데이터 삭제 + 상태 초기화"""
    import glob, shutil

    # 중단 요청
    status["stop_requested"] = True
    status["paused"] = False

    # 브라우저 강제 종료 (백그라운드 스레드에서 실행)
    def close_browser():
        try:
            asyncio.run(force_close_browser())
            push_log("🔄 브라우저 종료 완료")
        except Exception as e:
            logger.debug(f"브라우저 종료 오류: {e}")
    threading.Thread(target=close_browser, daemon=True).start()

    # output 폴더 데이터 삭제
    for f in glob.glob("output/*.json"):
        try: os.remove(f)
        except: pass
    img_dir = "output/images"
    if os.path.exists(img_dir):
        shutil.rmtree(img_dir)
        os.makedirs(img_dir, exist_ok=True)

    # scraping/uploading 즉시 False로 → 백그라운드 스레드가 루프 탈출
    status["scraping"]  = False
    status["uploading"] = False

    # 잠시 후 전체 초기화 (브라우저 종료 완료 대기)
    import time
    def delayed_reset():
        time.sleep(1.5)
        status.update({
            "scraping"      : False,
            "uploading"     : False,
            "last_scrape"   : None,
            "last_upload"   : None,
            "product_count" : 0,
            "uploaded_count": 0,
            "paused"        : False,
            "stop_requested": False,
        })
        push_log("✅ 리셋 완료 — 초기 상태로 돌아갔습니다")
    threading.Thread(target=delayed_reset, daemon=True).start()

    push_log("🔄 리셋 완료 — 모든 데이터가 삭제되고 초기화되었습니다")
    return jsonify({"ok": True, "message": "리셋 완료"})


# ── 실시간 로그 스트리밍 (SSE) ─────────────

@app.route(f"{URL_PREFIX}/logs/stream")
@login_required
def log_stream():
    """
    Server-Sent Events로 실시간 로그 전송
    멀티 클라이언트 지원 — 데스크탑/태블릿/모바일 모두 동시 수신
    """
    client_queue = _subscribe_logs()

    def generate():
        try:
            while True:
                try:
                    msg = client_queue.get(timeout=30)
                    yield f"data: {json.dumps({'msg': msg})}\n\n"
                except queue.Empty:
                    yield f"data: {json.dumps({'msg': '.'})}\n\n"  # heartbeat
        finally:
            _unsubscribe_logs(client_queue)

    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


# =============================================
# 서버 실행
# =============================================

if __name__ == "__main__":
    # 데이터 폴더 생성 (외부 저장소)
    try:
        ensure_dirs()
    except Exception:
        # 외부 저장소 미연결 시 로컬 fallback
        os.makedirs("output", exist_ok=True)
        os.makedirs("logs", exist_ok=True)

    # 빅데이터 DB 초기화
    try:
        init_product_db()
    except Exception as e:
        print(f"⚠️ 빅데이터 DB 초기화 실패: {e}")

    print(f"\n  Xebio Dashboard: http://{SERVER_HOST}:{SERVER_PORT}{URL_PREFIX}\n")

    app.run(
        host=SERVER_HOST,
        port=SERVER_PORT,
        debug=False,
        threaded=True,
        use_reloader=True,       # 파일 수정 시 자동 재기동
    )