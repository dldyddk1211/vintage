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
from flask import Flask, jsonify, render_template, request, Response
import queue

from config import (
    SERVER_HOST, SERVER_PORT, URL_PREFIX,
    AUTO_SCHEDULE_HOUR, AUTO_SCHEDULE_MINUTE
)
from xebio_search import scrape_nike_sale, load_latest_products, set_app_status, force_close_browser
from cafe_uploader import upload_products
from exchange import get_jpy_to_krw_rate, get_cached_rate, calc_buying_price, set_margin_rate, get_margin_rate

# =============================================
# 앱 초기화
# =============================================

app = Flask(__name__, template_folder="templates", static_folder="static")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# 진행상황 메시지 큐 (SSE 실시간 전송용)
log_queue = queue.Queue()

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
    """실시간 로그 큐에 메시지 추가"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    full_msg = f"[{timestamp}] {msg}"
    log_queue.put(full_msg)
    logger.info(msg)


# =============================================
# 스크래핑 / 업로드 실행 함수 (백그라운드)
# =============================================

def run_scrape(max_pages=None):
    """백그라운드 스레드에서 스크래핑 실행"""
    if status["scraping"]:
        push_log("⚠️ 이미 스크래핑이 진행 중입니다")
        return

    status["scraping"] = True
    try:
        products = asyncio.run(scrape_nike_sale(
            status_callback=push_log,
            max_pages=max_pages
        ))
        status["product_count"] = len(products)
        status["last_scrape"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        push_log(f"🎉 스크래핑 완료: {len(products)}개 상품 수집")
    except Exception as e:
        push_log(f"❌ 스크래핑 오류: {e}")
    finally:
        status["scraping"] = False


def run_upload(max_upload=None):
    """백그라운드 스레드에서 업로드 실행 - 선택된 상품만"""
    if status["uploading"]:
        push_log("⚠️ 이미 업로드가 진행 중입니다")
        return

    products = load_latest_products()
    if not products:
        push_log("⚠️ 업로드할 상품이 없습니다. 먼저 스크래핑을 실행하세요")
        return

    # 선택된 상품만 필터
    selected = [p for p in products if p.get("selected", True)]
    if not selected:
        push_log("⚠️ 선택된 상품이 없습니다. 체크박스로 상품을 선택해주세요")
        return

    push_log(f"📋 선택된 상품 {len(selected)}개 업로드 시작")
    status["uploading"] = True
    try:
        count = asyncio.run(upload_products(
            products=selected,
            status_callback=push_log,
            max_upload=max_upload
        ))
        status["uploaded_count"] = count
        status["last_upload"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # 업로드 히스토리 저장
        _save_upload_history(selected[:count])
        push_log(f"🎉 업로드 완료: {count}개 성공")
    except Exception as e:
        push_log(f"❌ 업로드 오류: {e}")
    finally:
        status["uploading"] = False


def _save_upload_history(uploaded_products: list):
    """업로드된 상품을 히스토리에 저장 (중복 체크용)"""
    history_path = os.path.join(OUTPUT_DIR, "uploaded_history.json")
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

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(history_path, "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False, indent=2)


def run_auto_pipeline():
    """자동 모드: 스크래핑 → 업로드 순서로 실행"""
    push_log("⏰ 자동 실행 시작 (스크래핑 → 업로드)")
    run_scrape()
    if status["product_count"] > 0:
        run_upload()


# =============================================
# 자동 스케줄러 설정
# =============================================

scheduler = BackgroundScheduler()
scheduler.add_job(
    func=run_auto_pipeline,
    trigger="cron",
    hour=AUTO_SCHEDULE_HOUR,
    minute=AUTO_SCHEDULE_MINUTE,
    id="auto_pipeline",
    name=f"매일 {AUTO_SCHEDULE_HOUR:02d}:{AUTO_SCHEDULE_MINUTE:02d} 자동 실행"
)
scheduler.start()
set_app_status(status)  # xebio_search에 status 딕셔너리 주입


# =============================================
# 라우트 (URL)
# =============================================

@app.route(f"{URL_PREFIX}/")
@app.route(f"{URL_PREFIX}")
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
        url_prefix=URL_PREFIX
    )


@app.route(f"{URL_PREFIX}/products")
def get_products():
    """수집된 상품 목록 JSON 반환 (브랜드 필터, 페이지네이션)"""
    products = load_latest_products()

    # 브랜드 필터 (한국어/원문 모두 비교)
    brand_filter = request.args.get("brand", "").strip()
    search_filter = request.args.get("search", "").strip().lower()
    if brand_filter and brand_filter != "ALL":
        products = [p for p in products if
                    (p.get("brand_ko") or "").strip() == brand_filter or
                    (p.get("brand")    or "").strip() == brand_filter]
    if search_filter:
        products = [p for p in products if search_filter in p.get("name", "").lower()
                    or search_filter in p.get("brand", "").lower()
                    or search_filter in p.get("product_code", "").lower()]

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
        "page": page,
        "per_page": per_page,
        "products": page_products
    })


@app.route(f"{URL_PREFIX}/products/download")
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
def get_brands():
    """수집된 상품의 브랜드 목록 반환 (한국어 번역 우선)"""
    products = load_latest_products()
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
def update_products():
    """상품 선택 상태 업데이트 (체크박스)"""
    data = request.json or {}
    selected_ids = set(data.get("selected", []))  # 선택된 인덱스 목록

    products = load_latest_products()
    for i, p in enumerate(products):
        p["selected"] = i in selected_ids

    from xebio_search import save_products
    save_products(products)
    return jsonify({"ok": True, "selected_count": len(selected_ids)})


@app.route(f"{URL_PREFIX}/products/check-duplicate", methods=["POST"])
def check_duplicate():
    """업로드 전 중복 체크 (품번 + 가격 기준)"""
    data = request.json or {}
    selected_indices = data.get("indices", [])
    products = load_latest_products()

    uploaded_path = os.path.join(OUTPUT_DIR, "uploaded_history.json")
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


@app.route(f"{URL_PREFIX}/status")
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
def manual_scrape():
    """수동 스크래핑 실행"""
    max_pages = request.json.get("max_pages") if request.json else None
    thread = threading.Thread(target=run_scrape, args=(max_pages,), daemon=True)
    thread.start()
    return jsonify({"ok": True, "message": "스크래핑 시작됨"})


@app.route(f"{URL_PREFIX}/run/upload", methods=["POST"])
def manual_upload():
    """수동 업로드 실행"""
    max_upload = request.json.get("max_upload") if request.json else None
    thread = threading.Thread(target=run_upload, args=(max_upload,), daemon=True)
    thread.start()
    return jsonify({"ok": True, "message": "업로드 시작됨"})


@app.route(f"{URL_PREFIX}/run/auto", methods=["POST"])
def manual_auto():
    """수동으로 자동 파이프라인(스크래핑+업로드) 실행"""
    thread = threading.Thread(target=run_auto_pipeline, daemon=True)
    thread.start()
    return jsonify({"ok": True, "message": "자동 파이프라인 시작됨"})


@app.route(f"{URL_PREFIX}/products/translate", methods=["POST"])
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
def get_dict():
    """커스텀 단어장 조회"""
    from translator import CUSTOM_DICT
    return jsonify({"dict": CUSTOM_DICT})


@app.route(f"{URL_PREFIX}/settings/dict", methods=["POST"])
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
def update_margin():
    """마진율 변경"""
    data = request.json or {}
    pct = data.get("margin_pct", 20)   # 퍼센트로 받기 (예: 20 → 1.2)
    rate = 1 + (pct / 100)
    rate = max(1.0, min(rate, 3.0))    # 0~200% 범위 제한
    set_margin_rate(rate)
    msg = f"마진율 변경: {pct}% (x{round(rate,2)})"
    push_log("💰 " + msg)
    return jsonify({"ok": True, "margin_pct": pct, "margin_rate": round(rate, 2), "message": msg})


@app.route(f"{URL_PREFIX}/run/pause", methods=["POST"])
def pause_scrape():
    """일시정지: 현재 상품 완료 후 멈춤"""
    if not status["scraping"]:
        return jsonify({"ok": False, "message": "실행 중인 작업이 없습니다"})
    status["paused"] = True
    push_log("⏸️ 일시정지 요청 — 현재 상품 수집 완료 후 멈춥니다...")
    return jsonify({"ok": True, "message": "일시정지 요청됨"})


@app.route(f"{URL_PREFIX}/run/resume", methods=["POST"])
def resume_scrape():
    """일시정지 해제"""
    status["paused"] = False
    push_log("▶️ 재개 — 수집을 계속합니다!")
    return jsonify({"ok": True, "message": "재개됨"})


@app.route(f"{URL_PREFIX}/run/reset", methods=["POST"])
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
def log_stream():
    """
    Server-Sent Events로 실시간 로그 전송
    브라우저에서 EventSource로 수신
    """
    def generate():
        while True:
            try:
                msg = log_queue.get(timeout=30)
                yield f"data: {json.dumps({'msg': msg})}\n\n"
            except queue.Empty:
                yield f"data: {json.dumps({'msg': '.'})}\n\n"  # heartbeat

    return Response(generate(), mimetype="text/event-stream",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


# =============================================
# 서버 실행
# =============================================

if __name__ == "__main__":
    # output 폴더 생성
    os.makedirs("output", exist_ok=True)
    os.makedirs("logs", exist_ok=True)

    print(f"""
╔══════════════════════════════════════════════════╗
║   Xebio 소싱 대시보드                            ║
║   http://{SERVER_HOST}:{SERVER_PORT}{URL_PREFIX}
╚══════════════════════════════════════════════════╝
    """)

    app.run(
        host=SERVER_HOST,
        port=SERVER_PORT,
        debug=False,
        threaded=True
    )