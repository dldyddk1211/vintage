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
from xebio_search import scrape_nike_sale, load_latest_products
from cafe_uploader import upload_products
from exchange import get_jpy_to_krw_rate, calc_buying_price

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
    """백그라운드 스레드에서 업로드 실행"""
    if status["uploading"]:
        push_log("⚠️ 이미 업로드가 진행 중입니다")
        return

    products = load_latest_products()
    if not products:
        push_log("⚠️ 업로드할 상품이 없습니다. 먼저 스크래핑을 실행하세요")
        return

    status["uploading"] = True
    try:
        count = asyncio.run(upload_products(
            products=products,
            status_callback=push_log,
            max_upload=max_upload
        ))
        status["uploaded_count"] = count
        status["last_upload"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        push_log(f"🎉 업로드 완료: {count}개 성공")
    except Exception as e:
        push_log(f"❌ 업로드 오류: {e}")
    finally:
        status["uploading"] = False


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
    """수집된 상품 목록 JSON 반환"""
    products = load_latest_products()
    page = request.args.get("page", 1, type=int)
    per_page = request.args.get("per_page", 50, type=int)

    start = (page - 1) * per_page
    end = start + per_page
    page_products = products[start:end]

    # 각 상품에 구매대행 가격 추가
    for p in page_products:
        if p.get("price_jpy"):
            p["price_info"] = calc_buying_price(p["price_jpy"])

    return jsonify({
        "total": len(products),
        "page": page,
        "per_page": per_page,
        "products": page_products
    })


@app.route(f"{URL_PREFIX}/status")
def get_status():
    """현재 실행 상태 반환"""
    products = load_latest_products()
    return jsonify({
        **status,
        "product_count": len(products),
        "rate": get_jpy_to_krw_rate(),
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