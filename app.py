"""
app.py
Flask 웹 대시보드 서버
접속: http://yaglobal.iptime.org:3000/jp_sourcing
"""

import asyncio
import json
import logging
import math
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
from data_manager import get_status as get_data_status, set_data_root, get_data_root, get_path, ensure_dirs, is_connected
from xebio_search import scrape_nike_sale, load_latest_products, set_app_status, force_close_browser
from cafe_uploader import upload_products, naver_manual_login, has_saved_cookies, delete_cookies, request_upload_stop, is_upload_stop_requested
from exchange import get_jpy_to_krw_rate, get_cached_rate, calc_buying_price, set_margin_rate, get_margin_rate, set_price_config, get_price_config
from user_db import init_db as init_user_db, get_user as get_customer, create_user, check_password as check_customer_pw, username_exists
from post_generator import get_ai_config, set_ai_config, verify_ai_key
from site_config import get_sites_for_ui
from scrape_history import get_history as get_scrape_history
from cafe_schedule import load_schedule, save_schedule, load_check_schedule, save_check_schedule, load_task_schedule, save_task_schedule
from product_db import init_db as init_product_db, get_stats as bigdata_get_stats, search_products as bigdata_search, get_brands as bigdata_get_brands, delete_all as bigdata_delete_all, delete_by_site as bigdata_delete_site, delete_by_ids as bigdata_delete_ids, get_total_count as bigdata_total, export_all as bigdata_export, export_csv as bigdata_export_csv, merge_products as bigdata_merge
from cafe_monitor import start_monitor, stop_monitor, is_monitoring, batch_check_cafe_duplicates
from telegram_bot import start_bot, stop_bot, is_bot_running

# =============================================
# 앱 초기화
# =============================================

APP_VERSION = "자동작업 스케줄"
app = Flask(__name__, template_folder="templates", static_folder="static")
app.secret_key = SECRET_KEY
app.config["TEMPLATES_AUTO_RELOAD"] = True   # 템플릿 변경 즉시 반영
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


@app.after_request
def add_no_cache(response):
    """브라우저/프록시 캐시 방지 — HTML + JSON 모두 적용"""
    if "text/html" in response.content_type or "application/json" in response.content_type:
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    return response


# =============================================
# 로그인 인증
# =============================================

def login_required(f):
    """로그인 필수 데코레이터 (모든 인증된 사용자)"""
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("logged_in"):
            # API 요청이면 JSON 에러 반환 (프론트 SyntaxError 방지)
            if "/api/" in request.path or request.is_json:
                return jsonify({"ok": False, "error": "로그인이 필요합니다"}), 401
            return redirect(f"{URL_PREFIX}/login")
        return f(*args, **kwargs)
    return decorated


def admin_required(f):
    """관리자 전용 데코레이터"""
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("logged_in"):
            if "/api/" in request.path or request.is_json:
                return jsonify({"ok": False, "error": "로그인이 필요합니다"}), 401
            return redirect(f"{URL_PREFIX}/login")
        # 기존 세션(role 없음)은 admin으로 간주
        if session.get("role", "admin") != "admin":
            return redirect(f"{URL_PREFIX}/shop")
        return f(*args, **kwargs)
    return decorated


@app.route(f"{URL_PREFIX}/login", methods=["GET", "POST"])
def login():
    """로그인 페이지"""
    if session.get("logged_in"):
        if session.get("role", "admin") == "admin":
            return redirect(f"{URL_PREFIX}/")
        return redirect(f"{URL_PREFIX}/shop")

    error = None
    username = ""
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        # 1) 관리자 확인
        if username in LOGIN_USERS and LOGIN_USERS[username] == password:
            session["logged_in"] = True
            session["username"] = username
            session["role"] = "admin"
            session["level"] = "b2b"
            logger.info(f"관리자 로그인: {username}")
            return redirect(f"{URL_PREFIX}/")
        # 2) 고객 확인
        customer = get_customer(username)
        if customer and check_customer_pw(customer, password):
            cust_status = customer["status"] if "status" in customer.keys() else "approved"
            expires_at = customer["expires_at"] if "expires_at" in customer.keys() else ""
            if cust_status == "pending":
                error = "가입 승인 대기 중입니다. 관리자 승인 후 이용 가능합니다."
                logger.info(f"승인 대기 로그인 시도: {username}")
            elif cust_status == "rejected":
                error = "가입이 거절되었습니다. 관리자에게 문의해주세요."
            elif expires_at and expires_at < datetime.now().strftime("%Y-%m-%d"):
                error = f"사용 기간이 만료되었습니다. (만료일: {expires_at}) 관리자에게 문의해주세요."
                logger.info(f"기간 만료 로그인 시도: {username} (만료: {expires_at})")
            else:
                session["logged_in"] = True
                session["username"] = username
                session["role"] = "customer"
                session["level"] = customer["level"] if "level" in customer.keys() else "b2c"
                logger.info(f"고객 로그인: {username} (level={session['level']})")
                return redirect(f"{URL_PREFIX}/shop")
        elif customer:
            error = "비밀번호가 올바르지 않습니다"
        else:
            error = "아이디 또는 비밀번호가 올바르지 않습니다"
        logger.warning(f"로그인 실패: {username}")

    return render_template("login.html",
                           error=error, username=username,
                           url_prefix=URL_PREFIX, env=APP_ENV)


@app.route(f"{URL_PREFIX}/signup", methods=["GET", "POST"])
def signup():
    """회원가입 페이지"""
    if session.get("logged_in"):
        if session.get("role", "admin") == "admin":
            return redirect(f"{URL_PREFIX}/")
        return redirect(f"{URL_PREFIX}/shop")

    error = None
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        name = request.form.get("name", "").strip()
        phone = request.form.get("phone", "").strip()
        if not username or not password:
            error = "아이디와 비밀번호는 필수입니다"
        elif len(password) < 4:
            error = "비밀번호는 4자 이상이어야 합니다"
        elif username in LOGIN_USERS:
            error = "사용할 수 없는 아이디입니다"
        elif username_exists(username):
            error = "이미 존재하는 아이디입니다"
        else:
            if create_user(username, password, name, phone):
                logger.info(f"회원가입 (승인대기): {username}")
                return render_template("signup.html", error=None, success=True,
                                       url_prefix=URL_PREFIX, env=APP_ENV)
            else:
                error = "회원가입 실패. 다시 시도해주세요."

    return render_template("signup.html", error=error, success=False,
                           url_prefix=URL_PREFIX, env=APP_ENV)


@app.route(f"{URL_PREFIX}/shop")
@login_required
def shop():
    """고객용 빈티지 상품 카탈로그"""
    user_level = session.get("level", "b2c")
    return render_template("shop.html",
                           url_prefix=URL_PREFIX, env=APP_ENV,
                           username=session.get("username"),
                           is_admin=session.get("role", "admin") == "admin",
                           user_level=user_level)


def _calc_vintage_price(jpy: int, margin_type="b2c") -> int:
    """빈티지 상품 한국 판매가 계산 (b2c 또는 b2b)"""
    if not jpy or jpy <= 0:
        return 0
    cfg = _vintage_price_config
    fee = cfg["jp_fee_pct"] / 100
    markup = cfg["buy_markup_pct"] / 100
    margin = cfg.get(f"margin_{margin_type}_pct", 15.0) / 100
    jp_ship = cfg.get("jp_domestic_shipping", 800)
    intl_ship = cfg["intl_shipping_krw"]
    rate = get_cached_rate() or 9.23
    jpy_total = (jpy + jp_ship) * (1 + fee)
    raw = jpy_total * rate * (1 + markup) * (1 + margin) + intl_ship
    return int(math.ceil(raw / 100) * 100)


@app.route(f"{URL_PREFIX}/shop/api/products")
@login_required
def shop_api_products():
    """고객용 빈티지 상품 API"""
    brand = request.args.get("brand", "").strip()
    condition = request.args.get("condition", "").strip()
    bag_type = request.args.get("bag_type", "").strip()
    keyword = request.args.get("keyword", "").strip()
    site = request.args.get("site", "").strip()
    price_min = request.args.get("price_min", 0, type=int)
    price_max = request.args.get("price_max", 0, type=int)
    user_level = session.get("level", "b2c")
    page = request.args.get("page", 1, type=int)
    per_page = request.args.get("per_page", 24, type=int)
    sort = request.args.get("sort", "newest")

    from product_db import _conn
    conn = _conn()
    try:
        base_where = "source_type='vintage' AND brand NOT LIKE '%OFF%'"
        base_params = []
        if site:
            base_where += " AND site_id = ?"
            base_params.append(site)

        # 사이트 목록
        site_rows = conn.execute(
            f"SELECT site_id, COUNT(*) c FROM products WHERE {base_where} GROUP BY site_id ORDER BY c DESC", base_params
        ).fetchall()
        site_names = {"2ndstreet": "세컨드스트리트", "kindal": "킨달", "brandoff": "브랜드오프", "komehyo": "코메효"}
        sites = [{"id": r["site_id"], "name": site_names.get(r["site_id"], r["site_id"]), "count": r["c"]} for r in site_rows]

        # 브랜드 목록
        brand_rows = conn.execute(
            f"SELECT brand, COUNT(*) c FROM products WHERE {base_where} GROUP BY brand ORDER BY c DESC", base_params
        ).fetchall()
        brands = [{"name": r["brand"], "count": r["c"]} for r in brand_rows]

        # 가방 종류 목록 (상품명 첫 번째 / 앞부분)
        bag_type_map = {
            "ショルダーバッグ": "숄더백", "トートバッグ": "토트백", "リュック": "백팩",
            "ハンドバッグ": "핸드백", "ポーチ": "파우치", "ボストンバッグ": "보스턴백",
            "クラッチバッグ": "클러치", "ウエストバッグ": "웨이스트백",
            "ショルダー": "숄더", "バッグ": "가방",
        }
        bag_rows = conn.execute(f"SELECT name FROM products WHERE {base_where}", base_params).fetchall()
        bag_counts = {}
        for r in bag_rows:
            n = r["name"] or ""
            for ja, ko in bag_type_map.items():
                if ja in n:
                    bag_counts[ko] = bag_counts.get(ko, 0) + 1
                    break
        bag_types = [{"name": k, "count": v} for k, v in sorted(bag_counts.items(), key=lambda x: -x[1])]

        # 상품 조회
        sql = f"SELECT * FROM products WHERE {base_where}"
        params = list(base_params)
        if brand:
            brands_list = [b.strip() for b in brand.split(",") if b.strip()]
            if len(brands_list) == 1:
                sql += " AND brand = ?"
                params.append(brands_list[0])
            elif len(brands_list) > 1:
                placeholders = ",".join(["?"] * len(brands_list))
                sql += f" AND brand IN ({placeholders})"
                params.extend(brands_list)
        if condition:
            cond_list = [c.strip() for c in condition.split(",") if c.strip()]
            if len(cond_list) == 1:
                sql += " AND condition_grade = ?"
                params.append(cond_list[0])
            elif len(cond_list) > 1:
                placeholders = ",".join(["?"] * len(cond_list))
                sql += f" AND condition_grade IN ({placeholders})"
                params.extend(cond_list)
        if bag_type:
            bag_list = [b.strip() for b in bag_type.split(",") if b.strip()]
            ja_keys = []
            for bt in bag_list:
                for ja, ko in bag_type_map.items():
                    if ko == bt:
                        ja_keys.append(ja)
                        break
            if len(ja_keys) == 1:
                sql += " AND name LIKE ?"
                params.append(f"%{ja_keys[0]}%")
            elif len(ja_keys) > 1:
                sql += " AND (" + " OR ".join(["name LIKE ?"] * len(ja_keys)) + ")"
                params.extend([f"%{k}%" for k in ja_keys])
        if keyword:
            sql += " AND (name LIKE ? OR brand LIKE ?)"
            params.extend([f"%{keyword}%", f"%{keyword}%"])

        # 가격대 필터 (한국 원화 → 엔화 역산, 레벨별 마진 적용)
        if price_min > 0 or price_max > 0:
            cfg = _vintage_price_config
            rate = get_cached_rate() or 9.23
            fee = cfg["jp_fee_pct"] / 100
            markup = cfg["buy_markup_pct"] / 100
            margin_key = "margin_b2b_pct" if user_level == "b2b" else "margin_b2c_pct"
            margin = cfg.get(margin_key, 15.0) / 100
            jp_ship = cfg.get("jp_domestic_shipping", 800)
            intl_ship = cfg["intl_shipping_krw"]
            # 역산: krw = (jpy + jp_ship) * (1+fee) * rate * (1+markup) * (1+margin) + intl_ship
            # jpy = (krw - intl_ship) / ((1+fee) * rate * (1+markup) * (1+margin)) - jp_ship
            multiplier = (1 + fee) * rate * (1 + markup) * (1 + margin)
            if price_min > 0 and multiplier > 0:
                jpy_min = max(0, (price_min - intl_ship) / multiplier - jp_ship)
                sql += " AND price_jpy >= ?"
                params.append(int(jpy_min))
            if price_max > 0 and multiplier > 0:
                jpy_max = (price_max - intl_ship) / multiplier - jp_ship
                sql += " AND price_jpy <= ?"
                params.append(int(jpy_max))

        if sort == "price_asc":
            sql += " ORDER BY price_jpy ASC"
        elif sort == "price_desc":
            sql += " ORDER BY price_jpy DESC"
        else:
            sql += " ORDER BY created_at DESC"

        # 총 개수
        count_sql = sql.replace("SELECT *", "SELECT COUNT(*) c", 1)
        total = conn.execute(count_sql, params).fetchone()["c"]

        sql += " LIMIT ? OFFSET ?"
        params.extend([per_page, (page - 1) * per_page])
        rows = conn.execute(sql, params).fetchall()

        products = []
        for r in rows:
            # 상세 이미지에서 상품 이미지만 필터링
            detail_imgs = []
            try:
                import json as _json
                imgs = _json.loads(r["detail_images"]) if r["detail_images"] else []
                detail_imgs = [img for img in imgs if any(ext in img.lower() for ext in ['.jpg', '.jpeg', '.png', '.webp'])]
            except Exception:
                pass

            # 설명: description_ko 우선, 없으면 description
            desc = ""
            desc_ko = r["description_ko"] if "description_ko" in r.keys() else ""
            desc_ja = r["description"] if "description" in r.keys() else ""
            if desc_ko and desc_ko.strip():
                desc = desc_ko
            elif desc_ja and desc_ja.strip() and desc_ja != "商品のお問い合わせ":
                desc = desc_ja

            products.append({
                "id": r["id"],
                "site_id": r["site_id"],
                "name": r["name_ko"] if r["name_ko"] and r["name_ko"] != r["name"] else r["name"],
                "name_ja": r["name"],
                "brand": r["brand"],
                "price_jpy": r["price_jpy"],
                "price_krw": _calc_vintage_price(r["price_jpy"], user_level),
                "price_b2c": _calc_vintage_price(r["price_jpy"], "b2c") if user_level == "b2b" else 0,
                "price_b2b": _calc_vintage_price(r["price_jpy"], "b2b") if user_level == "b2b" else 0,
                "user_level": user_level,
                "img_url": r["img_url"],
                "link": r["link"],
                "condition_grade": r["condition_grade"] if "condition_grade" in r.keys() else "",
                "product_code": r["internal_code"] if "internal_code" in r.keys() and r["internal_code"] else r["product_code"] or "",
                "size_info": r["color"] if "color" in r.keys() else "",
                "material": r["material"] if "material" in r.keys() else "",
                "color_raw": r["color"] if "color" in r.keys() else "",
                "description": desc,
                "detail_images": detail_imgs[:12],
            })

        return jsonify({
            "products": products,
            "sites": sites,
            "brands": brands,
            "bag_types": bag_types,
            "conditions": ["NS", "S", "A", "B", "C", "D"],
            "total": total,
            "page": page,
            "per_page": per_page,
            "total_pages": (total + per_page - 1) // per_page,
        })
    finally:
        conn.close()


@app.route(f"{URL_PREFIX}/logout")
def logout():
    """로그아웃"""
    session.clear()
    return redirect(f"{URL_PREFIX}/login")


# ── 회원관리 API ─────────────────────────────
@app.route(f"{URL_PREFIX}/members")
@admin_required
def get_members():
    """회원 목록 조회"""
    from user_db import _conn
    q = request.args.get("q", "").strip()
    conn = _conn()
    try:
        if q:
            rows = conn.execute(
                "SELECT * FROM users WHERE username LIKE ? OR name LIKE ? OR phone LIKE ? ORDER BY created_at DESC",
                (f"%{q}%", f"%{q}%", f"%{q}%")
            ).fetchall()
        else:
            rows = conn.execute("SELECT * FROM users ORDER BY created_at DESC").fetchall()
        members = [{"username": r["username"], "name": r["name"], "phone": r["phone"],
                     "role": r["role"], "level": r["level"] if "level" in r.keys() else "b2c",
                     "status": r["status"] if "status" in r.keys() else "approved",
                     "expires_at": r["expires_at"] if "expires_at" in r.keys() else "",
                     "created_at": r["created_at"]} for r in rows]
        return jsonify({"members": members, "total": len(members)})
    finally:
        conn.close()


@app.route(f"{URL_PREFIX}/members/<path:username>", methods=["DELETE"])
@admin_required
def delete_member(username):
    """회원 삭제"""
    from user_db import _conn
    conn = _conn()
    try:
        result = conn.execute("DELETE FROM users WHERE username = ?", (username,))
        conn.commit()
        if result.rowcount > 0:
            logger.info(f"회원 삭제: {username}")
            return jsonify({"ok": True, "message": f"{username} 삭제 완료"})
        return jsonify({"ok": False, "message": "해당 회원을 찾을 수 없습니다"})
    finally:
        conn.close()


@app.route(f"{URL_PREFIX}/members/<path:username>/status", methods=["POST"])
@admin_required
def change_member_status(username):
    """회원 승인/거절 + 기간 설정"""
    from datetime import timedelta
    data = request.json or {}
    new_status = data.get("status", "")
    period = data.get("period", "")  # free, 1m, 3m, 6m
    if new_status not in ("approved", "rejected", "pending"):
        return jsonify({"ok": False, "message": "잘못된 상태"})

    expires_at = ""
    if new_status == "approved" and period:
        if period == "free":
            expires_at = ""  # 무제한
        elif period == "1m":
            expires_at = (datetime.now() + timedelta(days=30)).strftime("%Y-%m-%d")
        elif period == "3m":
            expires_at = (datetime.now() + timedelta(days=90)).strftime("%Y-%m-%d")
        elif period == "6m":
            expires_at = (datetime.now() + timedelta(days=180)).strftime("%Y-%m-%d")
        elif period == "12m":
            expires_at = (datetime.now() + timedelta(days=365)).strftime("%Y-%m-%d")

    from user_db import _conn
    conn = _conn()
    try:
        conn.execute("UPDATE users SET status = ?, expires_at = ? WHERE username = ?",
                     (new_status, expires_at, username))
        conn.commit()
        label = {"approved": "승인", "rejected": "거절", "pending": "대기"}[new_status]
        exp_msg = f" (만료: {expires_at})" if expires_at else " (무제한)"
        logger.info(f"회원 {label}: {username}{exp_msg}")
        return jsonify({"ok": True, "message": f"{username} → {label}{exp_msg}"})
    finally:
        conn.close()


@app.route(f"{URL_PREFIX}/members/<path:username>/extend", methods=["POST"])
@admin_required
def extend_member(username):
    """회원 기간 연장"""
    from datetime import timedelta
    data = request.json or {}
    period = data.get("period", "3m")
    days_map = {"1m": 30, "3m": 90, "6m": 180, "12m": 365}
    days = days_map.get(period, 90)

    from user_db import _conn
    conn = _conn()
    try:
        row = conn.execute("SELECT expires_at FROM users WHERE username = ?", (username,)).fetchone()
        if not row:
            return jsonify({"ok": False, "message": "해당 회원을 찾을 수 없습니다"})
        # 기존 만료일 기준으로 연장 (이미 만료된 경우 오늘부터)
        current = row["expires_at"] if row["expires_at"] else ""
        if current and current >= datetime.now().strftime("%Y-%m-%d"):
            base = datetime.strptime(current, "%Y-%m-%d")
        else:
            base = datetime.now()
        new_expires = (base + timedelta(days=days)).strftime("%Y-%m-%d")
        conn.execute("UPDATE users SET expires_at = ?, status = 'approved' WHERE username = ?", (new_expires, username))
        conn.commit()
        logger.info(f"회원 기간 연장: {username} → {new_expires}")
        return jsonify({"ok": True, "message": f"{username} → {new_expires}까지 연장"})
    finally:
        conn.close()


@app.route(f"{URL_PREFIX}/members/<path:username>/level", methods=["POST"])
@admin_required
def change_member_level(username):
    """회원 레벨 변경 (b2c/b2b)"""
    data = request.json or {}
    level = data.get("level", "b2c")
    if level not in ("b2c", "b2b"):
        return jsonify({"ok": False, "message": "잘못된 레벨"})
    from user_db import _conn
    conn = _conn()
    try:
        result = conn.execute("UPDATE users SET level = ? WHERE username = ?", (level, username))
        conn.commit()
        if result.rowcount > 0:
            logger.info(f"회원 레벨 변경: {username} → {level}")
            return jsonify({"ok": True, "message": f"{username} → {level.upper()}"})
        return jsonify({"ok": False, "message": "해당 회원을 찾을 수 없습니다"})
    finally:
        conn.close()


@app.route(f"{URL_PREFIX}/vintage/translate", methods=["POST"])
@admin_required
def translate_vintage_products():
    """빈티지 상품 이름 일괄 한국어 번역"""
    from product_db import _conn
    from translator import translate_vintage_name
    conn = _conn()
    try:
        rows = conn.execute(
            "SELECT id, name, name_ko FROM products WHERE source_type='vintage' AND brand NOT LIKE '%OFF%'"
        ).fetchall()
        updated = 0
        for r in rows:
            name_ja = r["name"] or ""
            old_ko = r["name_ko"] or ""
            # 이미 번역된 것과 원본이 다르면 건너뜀 (수동 수정된 경우)
            new_ko = translate_vintage_name(name_ja)
            if new_ko != old_ko:
                conn.execute("UPDATE products SET name_ko = ? WHERE id = ?", (new_ko, r["id"]))
                updated += 1
        conn.commit()
        push_log(f"🌐 빈티지 상품 번역 완료: {updated}/{len(rows)}개 업데이트")
        return jsonify({"ok": True, "message": f"{updated}개 상품 번역 완료", "total": len(rows)})
    except Exception as e:
        return jsonify({"ok": False, "message": str(e)})
    finally:
        conn.close()


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
_upload_lock = threading.Lock()  # 업로드 동시 실행 방지 락


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
    """백그라운드 스레드에서 스크래핑 실행 (사이트별 크롤러 디스패치)"""
    if status["scraping"]:
        push_log("⚠️ 이미 스크래핑이 진행 중입니다")
        push_log("   💡 이전 작업이 비정상 종료된 경우 '리셋' 버튼을 눌러주세요")
        return
    # 중단 요청 초기화
    status["stop_requested"] = False

    push_log(f"🔧 run_scrape 시작: site={site_id}, cat={category_id}, brand={brand_code}, pages={pages}")
    status["scraping"] = True
    try:
        from site_config import get_site
        site_info = get_site(site_id)
        source_type = site_info.get("source_type", "sports") if site_info else "sports"
        push_log(f"   📡 source_type={source_type}, site_info={'있음' if site_info else '없음'}")

        if source_type == "vintage":
            from secondst_crawler import scrape_2ndstreet, set_app_status as set_2nd_status
            set_2nd_status(status)
            push_log("   🚀 2ndstreet 크롤러 시작...")
            products = asyncio.run(scrape_2ndstreet(
                status_callback=push_log,
                category=category_id,
                keyword=keyword,
                pages=pages,
                brand_code=brand_code,
            ))
        else:
            push_log("   🚀 Xebio 크롤러 시작 (Playwright 브라우저 열기)...")
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
        # 수집 이력 저장
        try:
            from scrape_history import add_history
            from site_config import get_brands as get_site_brands
            brand_name = ""
            if brand_code:
                brands_map = get_site_brands(site_id)
                brand_name = brands_map.get(brand_code, brand_code)
            add_history(
                site_id=site_id,
                category_id=category_id or "전체",
                product_count=len(products),
                keyword=keyword or "",
                brand=brand_name or "",
            )
        except Exception as e:
            logger.warning(f"수집 이력 저장 실패: {e}")
    except Exception as e:
        import traceback
        push_log(f"❌ 스크래핑 오류: {e}")
        push_log(f"   📋 상세: {traceback.format_exc()[-500:]}")
        logger.error(f"스크래핑 오류 상세:\n{traceback.format_exc()}")
    finally:
        status["scraping"] = False
        push_log("🔧 run_scrape 종료 (scraping=False)")


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


def run_upload(max_upload=None, shuffle_brands=False, checked_codes=None, delay_min=13, delay_max=15):
    """백그라운드 스레드에서 업로드 실행

    우선순위:
    1. checked_codes에 있는 상품 우선
    2. 나머지는 대기(待機) 상품에서 랜덤으로 채움
    3. max_upload 수량만큼만 업로드
    """
    if not _upload_lock.acquire(blocking=False):
        push_log("⚠️ 이미 업로드가 진행 중입니다 (락)")
        return
    if status["uploading"]:
        _upload_lock.release()
        push_log("⚠️ 이미 업로드가 진행 중입니다")
        return

    products = load_latest_products()
    # latest.json 상품은 모두 sports
    for p in products:
        if "source_type" not in p:
            p["source_type"] = "sports"

    # 빅데이터 DB 미업로드 상품 병합 (스포츠만)
    try:
        from product_db import get_unuploaded_products
        db_products = get_unuploaded_products(source_type="sports")
        existing_codes = {p.get("product_code", "") for p in products if p.get("product_code")}
        for dp in db_products:
            if dp.get("product_code") and dp["product_code"] not in existing_codes:
                existing_codes.add(dp["product_code"])
                products.append(dp)
    except Exception as e:
        logger.warning(f"DB 상품 병합 실패: {e}")

    # 빈티지 상품 제외
    products = [p for p in products if (p.get("source_type") or "sports") == "sports"]

    if not products:
        _upload_lock.release()
        push_log("⚠️ 업로드할 상품이 없습니다. 먼저 스크래핑을 실행하세요")
        return

    import random as _random

    # 체크된 상품과 대기 상품 분리 (product_code 중복 제거)
    checked_set = set(checked_codes) if checked_codes else set()
    # 빈 문자열 제거
    checked_set.discard("")
    checked_products = []
    waiting_products = []
    seen_codes = set()

    push_log(f"📋 업로드 요청: max_upload={max_upload}, checked_codes={len(checked_set)}개, shuffle={shuffle_brands}")
    if checked_set:
        push_log(f"   ✅ 체크된 품번: {', '.join(list(checked_set)[:5])}{'...' if len(checked_set) > 5 else ''}")

    for p in products:
        code = p.get("product_code", "")
        if code and code in seen_codes:
            continue  # 중복 product_code 건너뜀
        if code:
            seen_codes.add(code)
        is_waiting = (p.get("cafe_status") or "대기") == "대기"
        if code and code in checked_set:
            checked_products.append(p)
        elif is_waiting:
            waiting_products.append(p)

    push_log(f"   📊 체크 매칭: {len(checked_products)}개, 대기 상품: {len(waiting_products)}개")

    # 업로드 대상 결정
    if checked_set:
        # 체크된 상품이 있는 경우 → 체크된 것만 업로드 (랜덤 추가 안 함)
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
        _upload_lock.release()
        push_log("⚠️ 업로드할 상품이 없습니다")
        return

    # 브랜드 랜덤 섞기
    if shuffle_brands:
        selected = _shuffle_by_brand(selected)
        brands_order = [p.get("brand_ko") or p.get("brand", "") for p in selected[:10]]
        push_log(f"🔀 브랜드 랜덤 적용: {' → '.join(brands_order[:5])}...")

    # 활성 네이버 계정의 쿠키 경로 결정
    naver_data = _load_naver_accounts()
    active_slot = naver_data.get("active", 1)
    active_cookie = _get_cookie_path(active_slot)
    active_id = naver_data.get("accounts", {}).get(str(active_slot), {}).get("naver_id", "")
    if active_id:
        push_log(f"👤 네이버 계정: {active_id} (슬롯 {active_slot})")
    else:
        push_log(f"👤 네이버 계정: 기본 쿠키 사용 (슬롯 {active_slot})")

    push_log(f"📤 총 {len(selected)}개 업로드 시작")
    status["uploading"] = True
    try:
        count = asyncio.run(upload_products(
            products=selected,
            status_callback=push_log,
            max_upload=max_upload,
            delay_min=delay_min,
            delay_max=delay_max,
            on_single_success=_on_single_upload_success,
            cookie_path=active_cookie,
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
        _upload_lock.release()


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


def _on_single_upload_success(product: dict):
    """상품 1개 업로드 성공 시 즉시 latest.json + DB에 완료 표시 (중복 업로드 방지)"""
    try:
        code = product.get("product_code", "")
        if not code:
            return
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M")

        # latest.json에 즉시 반영
        products = load_latest_products()
        changed = False
        for p in products:
            if p.get("product_code") == code:
                p["cafe_status"] = "완료"
                p["cafe_uploaded"] = True
                p["cafe_uploaded_at"] = now_str
                changed = True
        if changed:
            from xebio_search import save_products
            save_products(products)

        # DB에도 반영
        try:
            from product_db import update_cafe_status
            update_cafe_status(code, "완료", now_str)
        except Exception:
            pass

        logger.info(f"✅ 즉시 완료 표시: {code}")
    except Exception as e:
        logger.warning(f"즉시 완료 표시 실패: {e}")


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
    if not _upload_lock.acquire(blocking=False):
        push_log(f"⚠️ [{slot_id}] 이미 업로드가 진행 중이라 스킵합니다 (락)")
        return
    if status["uploading"]:
        _upload_lock.release()
        push_log(f"⚠️ [{slot_id}] 이미 업로드가 진행 중이라 스킵합니다")
        return

    products = load_latest_products()
    for p in products:
        if "source_type" not in p:
            p["source_type"] = "sports"

    # 빅데이터 DB 미업로드 상품 병합 (스포츠만)
    try:
        from product_db import get_unuploaded_products
        db_products = get_unuploaded_products(source_type="sports")
        existing_codes = {p.get("product_code", "") for p in products if p.get("product_code")}
        for dp in db_products:
            if dp.get("product_code") and dp["product_code"] not in existing_codes:
                existing_codes.add(dp["product_code"])
                products.append(dp)
    except Exception as e:
        logger.warning(f"DB 상품 병합 실패: {e}")

    # 빈티지 상품 제외
    products = [p for p in products if (p.get("source_type") or "sports") == "sports"]

    if not products:
        _upload_lock.release()
        push_log(f"⏰ [{slot_id}] 업로드할 상품이 없습니다")
        return

    # 대기 상태만 필터 (product_code 중복 제거)
    seen_codes = set()
    waiting = []
    for p in products:
        code = p.get("product_code", "")
        if code and code in seen_codes:
            continue
        if code:
            seen_codes.add(code)
        if (p.get("cafe_status") or "대기") == "대기":
            waiting.append(p)

    # 브랜드 필터
    if brand and brand != "ALL":
        waiting = [p for p in waiting
                   if (p.get("brand_ko") or "").strip() == brand
                   or (p.get("brand") or "").strip() == brand]

    if not waiting:
        _upload_lock.release()
        push_log(f"⏰ [{slot_id}] 조건에 맞는 대기 상품이 없습니다 (브랜드: {brand})")
        return

    # 브랜드 ALL이면 랜덤 섞기
    if brand == "ALL":
        waiting = _shuffle_by_brand(waiting)

    # 수량 제한
    to_upload = waiting[:quantity]

    # 활성 네이버 계정의 쿠키 경로
    naver_data = _load_naver_accounts()
    active_slot = naver_data.get("active", 1)
    active_cookie = _get_cookie_path(active_slot)
    active_id = naver_data.get("accounts", {}).get(str(active_slot), {}).get("naver_id", "")
    push_log(f"⏰ [{slot_id}] 자동 업로드 시작 — {brand} {len(to_upload)}개 (계정: {active_id or '기본'})")

    status["uploading"] = True
    try:
        count = asyncio.run(upload_products(
            products=to_upload,
            status_callback=push_log,
            max_upload=quantity,
            on_single_success=_on_single_upload_success,
            cookie_path=active_cookie,
        ))
        status["uploaded_count"] = count
        status["last_upload"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        _save_upload_history(to_upload[:count])
        push_log(f"⏰ [{slot_id}] 자동 업로드 완료: {count}개 성공")
    except Exception as e:
        push_log(f"❌ [{slot_id}] 자동 업로드 오류: {e}")
    finally:
        status["uploading"] = False
        _upload_lock.release()


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


def run_scheduled_scrape(task_id, site_id, category_id, brand_code, keyword, pages):
    """스케줄에 의한 자동 수집"""
    from site_config import get_site, get_brands as get_site_brands
    brand_name = ""
    if brand_code:
        brands = get_site_brands(site_id)
        brand_name = brands.get(brand_code, brand_code)
    brand_msg = f" [{brand_name}]" if brand_name else ""
    push_log(f"⏰ [{task_id}] 자동 수집 시작{brand_msg}")
    try:
        run_scrape(
            site_id=site_id,
            category_id=category_id,
            keyword=keyword,
            pages=pages,
            brand_code=brand_code,
        )
    except Exception as e:
        push_log(f"❌ [{task_id}] 자동 수집 오류: {e}")


def run_scheduled_check(task_id, brand_name):
    """스케줄에 의한 자동 업로드 체크"""
    brand_filter = brand_name if brand_name and brand_name != "ALL" else ""
    push_log(f"⏰ [{task_id}] 자동 업로드 체크 시작 (브랜드: {brand_name or 'ALL'})")
    try:
        _run_upload_check(brand_filter=brand_filter)
    except Exception as e:
        push_log(f"❌ [{task_id}] 자동 체크 오류: {e}")


def run_scheduled_combo(task_id, site_id, category_id, brand_code, brand_name, keyword, pages):
    """스케줄에 의한 콤보 (수집 → 체크)"""
    push_log(f"⏰ [{task_id}] 콤보 시작: 수집 → 체크 (브랜드: {brand_name or 'ALL'})")
    try:
        # 1단계: 수집
        run_scrape(
            site_id=site_id,
            category_id=category_id,
            keyword=keyword,
            pages=pages,
            brand_code=brand_code,
        )
        push_log(f"⏰ [{task_id}] 수집 완료, 업로드 체크 시작...")
        # 2단계: 체크
        brand_filter = brand_name if brand_name and brand_name != "ALL" else ""
        _run_upload_check(brand_filter=brand_filter)
        push_log(f"⏰ [{task_id}] 콤보 완료!")
    except Exception as e:
        push_log(f"❌ [{task_id}] 콤보 오류: {e}")


def _register_task_schedule_jobs():
    """자동 작업 스케줄 (수집/체크/콤보) 잡 등록/갱신"""
    slots = load_task_schedule()
    for slot in slots:
        job_id = f"task_schedule_{slot['id']}"
        if scheduler.get_job(job_id):
            scheduler.remove_job(job_id)
        if slot.get("enabled"):
            task_type = slot.get("type", "scrape")
            if task_type == "scrape":
                func = lambda s=slot: threading.Thread(
                    target=run_scheduled_scrape,
                    args=(s["id"], s["site_id"], s["category_id"], s.get("brand_code", ""), s.get("keyword", ""), s.get("pages", "")),
                    daemon=True
                ).start()
            elif task_type == "check":
                func = lambda s=slot: threading.Thread(
                    target=run_scheduled_check,
                    args=(s["id"], s.get("brand_name", "ALL")),
                    daemon=True
                ).start()
            elif task_type == "combo":
                func = lambda s=slot: threading.Thread(
                    target=run_scheduled_combo,
                    args=(s["id"], s["site_id"], s["category_id"], s.get("brand_code", ""), s.get("brand_name", "ALL"), s.get("keyword", ""), s.get("pages", "")),
                    daemon=True
                ).start()
            else:
                continue

            scheduler.add_job(
                func=func,
                trigger="cron",
                hour=slot["hour"],
                minute=slot["minute"],
                id=job_id,
                name=f"자동 작업 [{slot['label']}] {slot['hour']:02d}:{slot['minute']:02d}",
                replace_existing=True,
            )
            type_label = {"scrape": "수집", "check": "체크", "combo": "콤보"}.get(task_type, task_type)
            logger.info(f"📅 작업 스케줄 등록: {slot['label']} ({type_label}) {slot['hour']:02d}:{slot['minute']:02d} 브랜드={slot.get('brand_name','ALL')}")


# 스케줄러 초기화 함수 (중복 시작 방지)
_scheduler_started = False


def _check_ai_api_job():
    """AI API 상태 확인 → 문제 시 텔레그램 알림"""
    try:
        from notifier import check_ai_api_and_notify
        check_ai_api_and_notify()
    except Exception as e:
        logger.warning(f"AI API 모니터링 오류: {e}")


def _start_scheduler_once():
    """스케줄러를 한 번만 시작 (중복 방지)"""
    global _scheduler_started
    if _scheduler_started:
        return
    _register_schedule_jobs()
    _register_check_schedule_job()
    _register_task_schedule_jobs()
    # AI API 상태 모니터링 (5분 간격)
    scheduler.add_job(
        func=_check_ai_api_job,
        trigger="interval",
        minutes=5,
        id="ai_api_monitor",
        name="AI API 상태 모니터링 (5분)",
        replace_existing=True,
    )
    logger.info("📡 AI API 모니터링 등록 (5분 간격)")
    scheduler.start()
    _scheduler_started = True
    logger.info("📅 스케줄러 시작됨 (PID: %d)", os.getpid())


# use_reloader=True 시 부모(리로더) + 자식(워커) 2개 프로세스가 생성됨
# 자식(워커)에만 WERKZEUG_RUN_MAIN="true" 설정됨
# 부모에서도 스케줄러가 시작되면 같은 잡이 2번 실행 → 브라우저 2개 열림!
# → 워커 프로세스에서만 스케줄러 시작
if os.environ.get("WERKZEUG_RUN_MAIN") == "true":
    _start_scheduler_once()

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
@admin_required
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
@admin_required
def get_products():
    """수집된 상품 목록 JSON 반환 (브랜드 필터, 페이지네이션)
    latest.json + 빅데이터 DB 미업로드 상품 병합
    """
    products = load_latest_products()
    # latest.json 상품은 모두 sports로 간주
    for p in products:
        if "source_type" not in p:
            p["source_type"] = "sports"

    # 빅데이터 DB에서 미업로드 상품 병합 (중복 제거)
    include_db = request.args.get("include_db", "true").lower()
    source_type_filter = request.args.get("source_type", "").strip()
    if include_db == "true":
        from product_db import get_unuploaded_products
        db_products = get_unuploaded_products(source_type=source_type_filter)
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

    # source_type 필터 (sports / vintage)
    if source_type_filter:
        products = [p for p in products if (p.get("source_type") or "sports") == source_type_filter]

    # 브랜드별 수량 집계 (source_type 필터 적용 후)
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
        # DB도 없고 latest에도 없으면 업로드 히스토리에서 완료 상품 복원
        if status_filter == "완료" and len(products) == 0:
            try:
                hist_path = os.path.join(get_path("db"), "uploaded_history.json")
                if os.path.exists(hist_path):
                    import json as _json
                    with open(hist_path, "r", encoding="utf-8") as _f:
                        hist = _json.load(_f)
                    for h in hist:
                        if h.get("product_code"):
                            products.append({
                                "product_code": h["product_code"],
                                "name": h.get("name", ""),
                                "name_ko": h.get("name", ""),
                                "brand": h.get("brand", ""),
                                "brand_ko": h.get("brand", ""),
                                "price_jpy": h.get("price_jpy", 0),
                                "cafe_status": "완료",
                                "cafe_uploaded_at": h.get("uploaded_at", ""),
                            })
            except Exception as e:
                logger.warning(f"업로드 히스토리 조회 실패: {e}")

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
@admin_required
def download_csv_products():
    """수집된 상품 CSV 다운로드"""
    import io
    import csv as csv_mod
    from flask import send_file

    products = load_latest_products()
    rate = get_cached_rate()

    # CSV 헤더
    fields = ["product_code", "brand", "brand_ko", "name_ko", "name", "price_jpy", "cost_krw"]
    buf = io.StringIO()
    writer = csv_mod.writer(buf)
    writer.writerow(fields)

    for p in products:
        cost_krw = 0
        if p.get("price_jpy"):
            from exchange import calc_buying_price
            info = calc_buying_price(p["price_jpy"], rate=rate)
            cost_krw = info["cost_krw"]

        writer.writerow([
            p.get("product_code", ""),
            p.get("brand", ""),
            p.get("brand_ko") or p.get("brand", ""),
            p.get("name_ko") or p.get("name", ""),
            p.get("name", ""),
            p.get("price_jpy", 0),
            cost_krw,
        ])

    output = io.BytesIO()
    output.write(b'\xef\xbb\xbf')  # UTF-8 BOM for Excel compatibility
    output.write(buf.getvalue().encode("utf-8"))
    output.seek(0)

    filename = f"products_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
    return send_file(
        output,
        mimetype="text/csv; charset=utf-8",
        as_attachment=True,
        download_name=filename
    )


@app.route(f"{URL_PREFIX}/products/brands")
@admin_required
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
@admin_required
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
@admin_required
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
@admin_required
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
@admin_required
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
@admin_required
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
@admin_required
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
@admin_required
def manual_scrape():
    """수동 스크래핑 실행"""
    data = request.json or {}
    site_id = data.get("site_id", "xebio")
    category_id = data.get("category_id", "sale")
    keyword = data.get("keyword", "")
    pages = data.get("pages", "")
    brand_code = data.get("brand_code", "")

    # 이미 진행 중이면 즉시 알림
    if status["scraping"]:
        return jsonify({"ok": False, "message": "⚠️ 이미 스크래핑이 진행 중입니다. 리셋 후 다시 시도해주세요."})

    push_log(f"🚀 수동 스크래핑 요청: site={site_id}, cat={category_id}, brand={brand_code}")
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
@admin_required
def api_sites():
    """사이트/카테고리 트리 반환"""
    return jsonify(get_sites_for_ui())


@app.route(f"{URL_PREFIX}/scrape-history", methods=["GET"])
@admin_required
def api_scrape_history():
    """수집 이력 반환"""
    limit = request.args.get("limit", 50, type=int)
    return jsonify(get_scrape_history(limit))


@app.route(f"{URL_PREFIX}/scrape-history", methods=["DELETE"])
@admin_required
def api_scrape_history_clear():
    """수집 이력 전체 삭제"""
    from scrape_history import _save
    _save([])
    return jsonify({"ok": True})


# ── 빅데이터 관리 API ──────────────────────────

@app.route(f"{URL_PREFIX}/bigdata/stats", methods=["GET"])
@admin_required
def api_bigdata_stats():
    """빅데이터 통계"""
    return jsonify(bigdata_get_stats())


@app.route(f"{URL_PREFIX}/bigdata/products", methods=["GET"])
@admin_required
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


@app.route(f"{URL_PREFIX}/bigdata/delete-selected", methods=["POST"])
@admin_required
def api_bigdata_delete_selected():
    """선택된 상품 삭제 (ID 리스트)"""
    data = request.json or {}
    ids = data.get("ids", [])
    if not ids:
        return jsonify({"ok": False, "message": "삭제할 상품을 선택하세요"})
    count = bigdata_delete_ids(ids)
    return jsonify({"ok": True, "deleted": count, "message": f"{count}개 삭제 완료"})


@app.route(f"{URL_PREFIX}/bigdata/brands", methods=["GET"])
@admin_required
def api_bigdata_brands():
    """빅데이터 브랜드 목록"""
    return jsonify(bigdata_get_brands())


@app.route(f"{URL_PREFIX}/bigdata/delete", methods=["POST"])
@admin_required
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
@admin_required
def api_bigdata_download():
    """빅데이터 CSV 다운로드"""
    import io
    import csv as csv_mod
    from flask import send_file

    q = request.args.get("q", "")
    site_id = request.args.get("site_id", "")
    brand = request.args.get("brand", "")

    products = bigdata_export_csv(query=q, site_id=site_id, brand=brand)

    # CSV 헤더 (병합 시 이 컬럼명 그대로 사용)
    fields = [
        "site_id", "category_id", "product_code", "brand", "brand_ko",
        "name", "name_ko", "price_jpy", "original_price", "discount_rate",
        "in_stock", "link", "img_url", "cafe_status", "cafe_uploaded_at", "created_at",
    ]

    buf = io.StringIO()
    writer = csv_mod.DictWriter(buf, fieldnames=fields, extrasaction="ignore")
    writer.writeheader()
    for p in products:
        writer.writerow(p)

    output = io.BytesIO()
    output.write(b'\xef\xbb\xbf')  # UTF-8 BOM
    output.write(buf.getvalue().encode("utf-8"))
    output.seek(0)

    suffix = f"_{site_id}" if site_id else ""
    filename = f"bigdata{suffix}_{datetime.now().strftime('%Y%m%d_%H%M')}.csv"
    return send_file(
        output,
        mimetype="text/csv; charset=utf-8",
        as_attachment=True,
        download_name=filename,
    )


@app.route(f"{URL_PREFIX}/bigdata/merge", methods=["POST"])
@admin_required
def api_bigdata_merge():
    """CSV 파일 업로드 + 병합 (created_at 기준 최신 데이터 우선)"""
    import csv as csv_mod
    import io

    if "file" not in request.files:
        return jsonify({"ok": False, "message": "파일이 없습니다"})

    file = request.files["file"]
    if not file.filename or not file.filename.lower().endswith(".csv"):
        return jsonify({"ok": False, "message": "CSV 파일만 업로드 가능합니다"})

    try:
        content = file.read().decode("utf-8-sig")  # BOM 처리
        reader = csv_mod.DictReader(io.StringIO(content))
        rows = list(reader)

        if not rows:
            return jsonify({"ok": False, "message": "CSV 파일이 비어있습니다"})

        # 필수 컬럼 확인
        required = {"site_id", "product_code", "price_jpy"}
        header_set = set(reader.fieldnames or [])
        missing = required - header_set
        if missing:
            return jsonify({"ok": False, "message": f"필수 컬럼 누락: {', '.join(missing)}"})

        result = bigdata_merge(rows)
        msg = f"병합 완료: 신규 {result['inserted']}개, 업데이트 {result['updated']}개, 스킵 {result['skipped']}개"
        push_log(f"📥 CSV {msg} (파일: {file.filename})")
        return jsonify({"ok": True, "message": msg, **result})
    except Exception as e:
        logger.error(f"CSV 병합 오류: {e}")
        return jsonify({"ok": False, "message": f"병합 실패: {str(e)}"})


# ── 카페 모니터 & 텔레그램 봇 API ─────────────

@app.route(f"{URL_PREFIX}/monitor/status", methods=["GET"])
@admin_required
def api_monitor_status():
    """모니터/봇 상태"""
    return jsonify({
        "monitor_running": is_monitoring(),
        "bot_running": is_bot_running(),
    })


@app.route(f"{URL_PREFIX}/monitor/start", methods=["POST"])
@admin_required
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
@admin_required
def api_monitor_stop():
    """카페 모니터 + 텔레그램 봇 종료"""
    stop_monitor()
    stop_bot()
    return jsonify({"ok": True, "message": "모니터 & 봇 종료"})


# ── 카페 업로드 스케줄 API ────────────────────

@app.route(f"{URL_PREFIX}/cafe-schedule", methods=["GET"])
@admin_required
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
@admin_required
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
@admin_required
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
@admin_required
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


# ── 자동 작업 스케줄 API (수집/체크/콤보) ──────────

@app.route(f"{URL_PREFIX}/task-schedule", methods=["GET"])
@admin_required
def api_get_task_schedule():
    """자동 작업 스케줄 설정 조회"""
    slots = load_task_schedule()
    for slot in slots:
        job_id = f"task_schedule_{slot['id']}"
        job = scheduler.get_job(job_id)
        slot["registered"] = job is not None
        if job and job.next_run_time:
            slot["next_run"] = job.next_run_time.strftime("%Y-%m-%d %H:%M")
        else:
            slot["next_run"] = None
    return jsonify({"ok": True, "slots": slots})


@app.route(f"{URL_PREFIX}/task-schedule", methods=["POST"])
@admin_required
def api_save_task_schedule():
    """자동 작업 스케줄 설정 저장 + 잡 재등록"""
    data = request.json or {}
    slots = data.get("slots", [])
    if not isinstance(slots, list) or len(slots) != 3:
        return jsonify({"ok": False, "error": "3개 슬롯 필요"}), 400

    save_task_schedule(slots)
    _register_task_schedule_jobs()
    push_log("📅 자동 작업 스케줄 설정이 저장되었습니다")
    return jsonify({"ok": True})


@app.route(f"{URL_PREFIX}/upload-status-summary", methods=["GET"])
@admin_required
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
@admin_required
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


@app.route(f"{URL_PREFIX}/run/test", methods=["POST"])
@admin_required
def run_test():
    """테스트 버튼 핸들러"""
    push_log("🧪 테스트 버튼 클릭됨 — 정상 작동 확인")
    return jsonify({"ok": True, "message": "테스트 성공"})


@app.route(f"{URL_PREFIX}/run/upload-preview", methods=["POST"])
@admin_required
def upload_preview():
    """업로드 전 미리보기 — 번역 결과 포함 리스트 반환"""
    data = request.json or {}
    checked_codes = data.get("checked_codes", [])
    if not checked_codes:
        return jsonify({"ok": False, "items": []})

    from post_generator import make_title, _has_japanese

    products = load_latest_products()
    # DB 상품 병합
    try:
        from product_db import get_unuploaded_products
        db_products = get_unuploaded_products()
        existing_codes = {p.get("product_code", "") for p in products if p.get("product_code")}
        for dp in db_products:
            if dp.get("product_code") and dp["product_code"] not in existing_codes:
                existing_codes.add(dp["product_code"])
                products.append(dp)
    except Exception:
        pass

    checked_set = set(checked_codes)
    items = []
    for p in products:
        code = p.get("product_code", "")
        if code not in checked_set:
            continue
        name = p.get("name_ko") or p.get("name", "")
        brand = p.get("brand_ko") or p.get("brand", "")
        title = make_title(p)
        has_jp = _has_japanese(title)
        items.append({
            "product_code": code,
            "brand": brand,
            "name": name[:50],
            "title": title[:60],
            "has_japanese": has_jp,
        })
    return jsonify({"ok": True, "items": items})


# ── 블로그 업로드 실행/중지 ────────────────
def run_blog_upload(checked_codes=None):
    """블로그 업로드 백그라운드 실행"""
    from blog_uploader import blog_upload_products, request_blog_upload_stop

    products = load_latest_products()
    # DB 상품 병합
    try:
        from product_db import get_unuploaded_products
        db_products = get_unuploaded_products()
        existing_codes = {p.get("product_code", "") for p in products if p.get("product_code")}
        for dp in db_products:
            if dp.get("product_code") and dp["product_code"] not in existing_codes:
                existing_codes.add(dp["product_code"])
                products.append(dp)
    except Exception:
        pass

    checked_set = set(checked_codes) if checked_codes else set()
    checked_set.discard("")
    selected = [p for p in products if p.get("product_code", "") in checked_set]

    if not selected:
        push_log("⚠️ 블로그 업로드할 상품이 없습니다")
        return

    # 블로그 계정 쿠키 경로
    blog_data = _load_blog_accounts()
    active_slot = blog_data.get("active", 1)
    blog_cookie = _get_blog_cookie_path(active_slot)
    active_id = blog_data.get("accounts", {}).get(str(active_slot), {}).get("naver_id", "")
    push_log(f"👤 블로그 계정: {active_id or '미설정'} (슬롯 {active_slot})")
    push_log(f"📝 블로그 업로드 {len(selected)}개 시작")

    try:
        count = asyncio.run(blog_upload_products(
            products=selected,
            status_callback=push_log,
            cookie_path=blog_cookie,
        ))
        push_log(f"🎉 블로그 업로드 완료: {count}개 성공")
    except Exception as e:
        push_log(f"❌ 블로그 업로드 오류: {e}")


@app.route(f"{URL_PREFIX}/run/blog-upload", methods=["POST"])
@admin_required
def manual_blog_upload():
    data = request.json or {}
    checked_codes = data.get("checked_codes")
    thread = threading.Thread(
        target=run_blog_upload,
        args=(checked_codes,),
        daemon=True,
    )
    thread.start()
    return jsonify({"ok": True, "message": "블로그 업로드 시작됨"})


@app.route(f"{URL_PREFIX}/run/blog-upload-stop", methods=["POST"])
@admin_required
def blog_upload_stop():
    from blog_uploader import request_blog_upload_stop
    request_blog_upload_stop()
    push_log("⏹ 블로그 업로드 중지 요청됨")
    return jsonify({"ok": True})


@app.route(f"{URL_PREFIX}/run/upload-stop", methods=["POST"])
@admin_required
def upload_stop():
    """업로드 중지 요청"""
    request_upload_stop()
    push_log("⏹ 업로드 중지 요청됨 — 현재 작업 완료 후 중지됩니다")
    return jsonify({"ok": True, "message": "업로드 중지 요청됨"})


@app.route(f"{URL_PREFIX}/run/upload-reset", methods=["POST"])
@admin_required
def upload_reset():
    """업로드 중지 + 상태 초기화"""
    request_upload_stop()
    push_log("🔄 업로드 리셋 — 작업 중지 및 초기화")
    return jsonify({"ok": True, "message": "업로드 리셋 완료"})


_upload_check_stop = False

def _run_upload_check(brand_filter=""):
    """백그라운드에서 카페 중복 체크 실행 (Playwright 브라우저 사용)"""
    global _upload_check_stop
    _upload_check_stop = False

    from config import CAFE_MY_NICKNAME

    products = load_latest_products()
    waiting = [p for p in products if (p.get("cafe_status") or "대기") == "대기"]

    # 브랜드 필터 적용
    if brand_filter and brand_filter != "ALL":
        waiting = [p for p in waiting if
                   (p.get("brand_ko") or "").strip() == brand_filter or
                   (p.get("brand") or "").strip() == brand_filter]

    if not waiting:
        push_log("⚠️ 대기 상품이 없습니다" + (f" (브랜드: {brand_filter})" if brand_filter else ""))
        return

    brand_msg = f" [브랜드: {brand_filter}]" if brand_filter and brand_filter != "ALL" else ""
    push_log(f"🔍 업로드 체크 시작: {len(waiting)}개 상품{brand_msg} — 브라우저로 카페 검색 중...")

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
@admin_required
def ai_verify():
    """AI API 키 정상 작동 여부 확인"""
    result = verify_ai_key()
    return jsonify(result)


@app.route(f"{URL_PREFIX}/run/upload-check", methods=["POST"])
@admin_required
def upload_check():
    """대기 상품을 카페에서 검색하여 중복 여부 체크 (백그라운드)"""
    data = request.json or {}
    brand_filter = data.get("brand", "")
    thread = threading.Thread(target=_run_upload_check, args=(brand_filter,), daemon=True)
    thread.start()
    msg = "카페 중복 체크 시작됨"
    if brand_filter and brand_filter != "ALL":
        msg += f" (브랜드: {brand_filter})"
    return jsonify({"ok": True, "message": msg + " — 로그를 확인하세요"})


@app.route(f"{URL_PREFIX}/run/upload-check-stop", methods=["POST"])
@admin_required
def upload_check_stop():
    """업로드 체크 중지"""
    global _upload_check_stop
    _upload_check_stop = True
    push_log("⏹ 업로드 체크 중지 요청됨")
    return jsonify({"ok": True})


@app.route(f"{URL_PREFIX}/run/auto", methods=["POST"])
@admin_required
def manual_auto():
    """수동으로 자동 파이프라인(스크래핑+업로드) 실행"""
    thread = threading.Thread(target=run_auto_pipeline, daemon=True)
    thread.start()
    return jsonify({"ok": True, "message": "자동 파이프라인 시작됨"})


@app.route(f"{URL_PREFIX}/products/translate", methods=["POST"])
@admin_required
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


@app.route(f"{URL_PREFIX}/products/rescrape-details", methods=["POST"])
@admin_required
def rescrape_details_api():
    """2ndstreet 설명 없는 상품의 상세 페이지 재수집"""
    from secondst_crawler import is_rescrape_running, rescrape_details
    if is_rescrape_running():
        return jsonify({"ok": False, "message": "이미 재수집 진행 중입니다"})

    def _run():
        import asyncio
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(rescrape_details(log=push_log))
        loop.close()

    import threading
    t = threading.Thread(target=_run, daemon=True)
    t.start()
    push_log("🔄 2ndstreet 상세 페이지 재수집 시작...")
    return jsonify({"ok": True, "message": "상세 페이지 재수집을 시작합니다"})


@app.route(f"{URL_PREFIX}/products/rescrape-details/stop", methods=["POST"])
@admin_required
def rescrape_details_stop():
    """재수집 중지"""
    from secondst_crawler import stop_rescrape, is_rescrape_running
    if not is_rescrape_running():
        return jsonify({"ok": False, "message": "진행 중인 재수집이 없습니다"})
    stop_rescrape()
    push_log("⛔ 재수집 중지 요청")
    return jsonify({"ok": True, "message": "재수집 중지 요청됨"})


@app.route(f"{URL_PREFIX}/settings/dict", methods=["GET"])
@admin_required
def get_dict():
    """커스텀 단어장 조회"""
    from translator import CUSTOM_DICT
    return jsonify({"dict": CUSTOM_DICT})


@app.route(f"{URL_PREFIX}/settings/dict", methods=["POST"])
@admin_required
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
@admin_required
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
@admin_required
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
@admin_required
def get_price_settings():
    """현재 가격 설정 조회"""
    return jsonify({"ok": True, **get_price_config()})


@app.route(f"{URL_PREFIX}/settings/price", methods=["POST"])
@admin_required
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


# ── 빈티지 가격 설정 ─────────────────────
_vintage_price_config = {
    "jp_fee_pct": 3.0,
    "buy_markup_pct": 2.0,
    "margin_b2c_pct": 15.0,
    "margin_b2b_pct": 8.0,
    "jp_domestic_shipping": 800,
    "intl_shipping_krw": 15000,
}

# 파일에서 로드
def _load_vintage_price():
    import json as _json
    path = os.path.join(get_path("db"), "vintage_price.json")
    if os.path.exists(path):
        try:
            with open(path, "r") as f:
                _vintage_price_config.update(_json.load(f))
        except Exception:
            pass

def _save_vintage_price():
    import json as _json
    path = os.path.join(get_path("db"), "vintage_price.json")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        _json.dump(_vintage_price_config, f)

_load_vintage_price()


@app.route(f"{URL_PREFIX}/settings/vintage-price", methods=["GET"])
@admin_required
def get_vintage_price():
    return jsonify({"ok": True, **_vintage_price_config})


@app.route(f"{URL_PREFIX}/settings/vintage-price", methods=["POST"])
@admin_required
def update_vintage_price():
    data = request.json or {}
    if data.get("jp_fee_pct") is not None:
        _vintage_price_config["jp_fee_pct"] = float(data["jp_fee_pct"])
    if data.get("buy_markup_pct") is not None:
        _vintage_price_config["buy_markup_pct"] = float(data["buy_markup_pct"])
    if data.get("margin_b2c_pct") is not None:
        _vintage_price_config["margin_b2c_pct"] = float(data["margin_b2c_pct"])
    if data.get("margin_b2b_pct") is not None:
        _vintage_price_config["margin_b2b_pct"] = float(data["margin_b2b_pct"])
    if data.get("jp_domestic_shipping") is not None:
        _vintage_price_config["jp_domestic_shipping"] = int(data["jp_domestic_shipping"])
    if data.get("intl_shipping_krw") is not None:
        _vintage_price_config["intl_shipping_krw"] = int(data["intl_shipping_krw"])
    _save_vintage_price()
    msg = (f"빈티지 가격설정: 수수료={_vintage_price_config['jp_fee_pct']}% "
           f"환율추가={_vintage_price_config['buy_markup_pct']}% "
           f"B2C={_vintage_price_config['margin_b2c_pct']}% "
           f"B2B={_vintage_price_config['margin_b2b_pct']}% "
           f"일본택배=¥{_vintage_price_config.get('jp_domestic_shipping',800):,} "
           f"국제배송={_vintage_price_config['intl_shipping_krw']:,}원")
    push_log("🎺 " + msg)
    return jsonify({"ok": True, **_vintage_price_config, "message": msg})


# ── 데이터 경로 설정 ─────────────────────

@app.route(f"{URL_PREFIX}/settings/data-path", methods=["GET"])
@admin_required
def get_data_path():
    """데이터 저장 경로 상태 조회"""
    return jsonify({"ok": True, **get_data_status()})


@app.route(f"{URL_PREFIX}/settings/data-path", methods=["POST"])
@admin_required
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
@admin_required
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
@admin_required
def get_ai_settings():
    """AI 설정 조회"""
    return jsonify(get_ai_config())


@app.route(f"{URL_PREFIX}/settings/ai", methods=["POST"])
@admin_required
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
@admin_required
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


# ── AI 채팅 위젯 API ───────────────────────

@app.route(f"{URL_PREFIX}/chat", methods=["POST"])
@admin_required
def api_chat():
    """AI 채팅 위젯 — 선택된 AI 모델과 대화"""
    data = request.json or {}
    message = data.get("message", "").strip()
    history = data.get("history", [])
    if not message:
        return jsonify({"ok": False, "reply": "메시지를 입력해주세요."})
    from post_generator import chat_with_ai
    result = chat_with_ai(message, history)
    return jsonify(result)


# ── 텔레그램 알림 설정 ────────────────────

@app.route(f"{URL_PREFIX}/settings/telegram", methods=["GET"])
@admin_required
def get_telegram_settings():
    """텔레그램 설정 조회"""
    from notifier import get_telegram_config
    return jsonify({"ok": True, **get_telegram_config()})


@app.route(f"{URL_PREFIX}/settings/telegram", methods=["POST"])
@admin_required
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
@admin_required
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
@admin_required
def naver_status():
    """네이버 로그인 상태 확인"""
    return jsonify({"logged_in": has_saved_cookies()})


# ── 네이버 계정 관리 (최대 3개) ─────────────────

_NAVER_ACCOUNTS_DB = os.path.join(get_path("db"), "naver_accounts.json")


def _load_naver_accounts() -> dict:
    """네이버 계정 목록 로드"""
    if os.path.exists(_NAVER_ACCOUNTS_DB):
        try:
            with open(_NAVER_ACCOUNTS_DB, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {"active": 1, "accounts": {}}


def _save_naver_accounts(data: dict):
    """네이버 계정 목록 저장"""
    os.makedirs(os.path.dirname(_NAVER_ACCOUNTS_DB), exist_ok=True)
    with open(_NAVER_ACCOUNTS_DB, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _get_cookie_path(slot: int) -> str:
    """슬롯별 쿠키 파일 경로"""
    if slot == 1:
        return "naver_cookies.json"  # 기존 호환
    return f"naver_cookies_{slot}.json"


@app.route(f"{URL_PREFIX}/naver/accounts", methods=["GET"])
@admin_required
def get_naver_accounts():
    """네이버 계정 목록 조회 (비밀번호 마스킹)"""
    data = _load_naver_accounts()
    result = {"active": data.get("active", 1), "accounts": {}}
    for slot, acc in data.get("accounts", {}).items():
        has_cookie = os.path.exists(_get_cookie_path(int(slot)))
        result["accounts"][slot] = {
            "naver_id": acc.get("naver_id", ""),
            "has_password": bool(acc.get("password")),
            "has_cookie": has_cookie,
        }
    return jsonify(result)


@app.route(f"{URL_PREFIX}/naver/accounts", methods=["POST"])
@admin_required
def save_naver_account():
    """네이버 계정 저장"""
    d = request.json or {}
    slot = str(d.get("slot", 1))
    naver_id = d.get("naver_id", "").strip()
    password = d.get("password", "").strip()
    if not naver_id:
        return jsonify({"ok": False, "message": "아이디를 입력해주세요"})

    data = _load_naver_accounts()
    if "accounts" not in data:
        data["accounts"] = {}
    existing = data["accounts"].get(slot, {})
    # 비밀번호가 비어있으면 기존 비밀번호 유지
    if not password and existing.get("password"):
        password = existing["password"]
    data["accounts"][slot] = {"naver_id": naver_id, "password": password}
    _save_naver_accounts(data)
    pw_msg = "비밀번호 저장됨" if password else "비밀번호 미설정"
    push_log(f"💾 네이버 계정 {slot} 저장: {naver_id} ({pw_msg})")
    return jsonify({"ok": True, "message": f"저장 완료 ({pw_msg})"})


@app.route(f"{URL_PREFIX}/naver/accounts/delete", methods=["POST"])
@admin_required
def delete_naver_account():
    """네이버 계정 삭제"""
    d = request.json or {}
    slot = str(d.get("slot", 1))
    data = _load_naver_accounts()
    if slot in data.get("accounts", {}):
        del data["accounts"][slot]
        _save_naver_accounts(data)
    # 쿠키 파일도 삭제
    cookie_path = _get_cookie_path(int(slot))
    if os.path.exists(cookie_path):
        os.remove(cookie_path)
    push_log(f"🗑️ 네이버 계정 {slot} 삭제")
    return jsonify({"ok": True})


@app.route(f"{URL_PREFIX}/naver/accounts/active", methods=["POST"])
@admin_required
def set_active_naver_account():
    """활성 계정 변경"""
    d = request.json or {}
    slot = int(d.get("slot", 1))
    data = _load_naver_accounts()
    data["active"] = slot
    _save_naver_accounts(data)
    # 활성 계정의 쿠키를 기본 쿠키 경로에 복사
    src = _get_cookie_path(slot)
    if os.path.exists(src) and slot != 1:
        import shutil
        shutil.copy2(src, "naver_cookies.json")
    push_log(f"✅ 활성 네이버 계정 변경: 계정 {slot}")
    return jsonify({"ok": True})


@app.route(f"{URL_PREFIX}/naver/login", methods=["POST"])
@admin_required
def naver_login():
    """네이버 로그인 시작 (저장된 계정 자동 입력)"""
    d = request.json or {}
    slot = int(d.get("slot", 1))
    cookie_path = _get_cookie_path(slot)

    # 저장된 계정 정보 가져오기
    acc_data = _load_naver_accounts()
    acc = acc_data.get("accounts", {}).get(str(slot), {})
    naver_id = acc.get("naver_id", "")
    password = acc.get("password", "")

    def run_login():
        from cafe_uploader import naver_manual_login_with_cookie_path
        result = asyncio.run(naver_manual_login_with_cookie_path(
            cookie_path=cookie_path, status_callback=push_log,
            naver_id=naver_id, password=password,
        ))
        if result:
            push_log(f"✅ 네이버 계정 {slot} 로그인 & 쿠키 저장 완료!")
            # 활성 계정이면 기본 쿠키에도 복사
            data = _load_naver_accounts()
            if data.get("active") == slot and slot != 1:
                import shutil
                shutil.copy2(cookie_path, "naver_cookies.json")
        else:
            push_log(f"❌ 네이버 계정 {slot} 로그인 실패 또는 시간 초과")

    thread = threading.Thread(target=run_login, daemon=True)
    thread.start()
    return jsonify({"ok": True, "message": f"계정 {slot} 로그인 브라우저가 열립니다."})


@app.route(f"{URL_PREFIX}/naver/logout", methods=["POST"])
@admin_required
def naver_logout():
    """네이버 쿠키 삭제"""
    delete_cookies()
    push_log("🗑️ 네이버 쿠키 삭제 완료")
    return jsonify({"ok": True, "message": "네이버 로그인 정보가 삭제되었습니다"})


# ── 블로그 계정 관리 ────────────────────────
_BLOG_ACCOUNTS_DB = os.path.join(get_path("db"), "blog_accounts.json")


def _load_blog_accounts():
    if os.path.exists(_BLOG_ACCOUNTS_DB):
        try:
            with open(_BLOG_ACCOUNTS_DB, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {"active": 1, "accounts": {}}


def _save_blog_accounts(data: dict):
    os.makedirs(os.path.dirname(_BLOG_ACCOUNTS_DB), exist_ok=True)
    with open(_BLOG_ACCOUNTS_DB, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _get_blog_cookie_path(slot: int) -> str:
    return f"blog_cookies_{slot}.json"


@app.route(f"{URL_PREFIX}/blog/accounts", methods=["GET"])
@admin_required
def get_blog_accounts():
    data = _load_blog_accounts()
    result = {"active": data.get("active", 1), "accounts": {}}
    for slot, acc in data.get("accounts", {}).items():
        has_cookie = os.path.exists(_get_blog_cookie_path(int(slot)))
        result["accounts"][slot] = {
            "naver_id": acc.get("naver_id", ""),
            "blog_id": acc.get("blog_id", ""),
            "has_password": bool(acc.get("password")),
            "has_cookie": has_cookie,
        }
    return jsonify(result)


@app.route(f"{URL_PREFIX}/blog/accounts", methods=["POST"])
@admin_required
def save_blog_account():
    d = request.json or {}
    slot = str(d.get("slot", 1))
    naver_id = d.get("naver_id", "").strip()
    blog_id = d.get("blog_id", "").strip()
    password = d.get("password", "").strip()
    if not naver_id:
        return jsonify({"ok": False, "message": "아이디를 입력해주세요"})
    data = _load_blog_accounts()
    if "accounts" not in data:
        data["accounts"] = {}
    data["accounts"][slot] = {"naver_id": naver_id, "blog_id": blog_id, "password": password}
    _save_blog_accounts(data)
    push_log(f"💾 블로그 계정 {slot} 저장: {naver_id} (블로그: {blog_id})")
    return jsonify({"ok": True})


@app.route(f"{URL_PREFIX}/blog/accounts/delete", methods=["POST"])
@admin_required
def delete_blog_account():
    d = request.json or {}
    slot = str(d.get("slot", 1))
    data = _load_blog_accounts()
    if slot in data.get("accounts", {}):
        del data["accounts"][slot]
        _save_blog_accounts(data)
    cookie_path = _get_blog_cookie_path(int(slot))
    if os.path.exists(cookie_path):
        os.remove(cookie_path)
    push_log(f"🗑️ 블로그 계정 {slot} 삭제")
    return jsonify({"ok": True})


@app.route(f"{URL_PREFIX}/blog/accounts/active", methods=["POST"])
@admin_required
def set_active_blog_account():
    d = request.json or {}
    slot = int(d.get("slot", 1))
    data = _load_blog_accounts()
    data["active"] = slot
    _save_blog_accounts(data)
    push_log(f"✅ 활성 블로그 계정 변경: 계정 {slot}")
    return jsonify({"ok": True})


@app.route(f"{URL_PREFIX}/blog/login", methods=["POST"])
@admin_required
def blog_login():
    d = request.json or {}
    slot = int(d.get("slot", 1))
    cookie_path = _get_blog_cookie_path(slot)

    # 저장된 계정 정보 가져오기 (카페 계정과 동일)
    acc_data = _load_naver_accounts()
    acc = acc_data.get("accounts", {}).get(str(slot), {})
    naver_id = acc.get("naver_id", "")
    password = acc.get("password", "")

    def run_login():
        from cafe_uploader import naver_manual_login_with_cookie_path
        result = asyncio.run(naver_manual_login_with_cookie_path(
            cookie_path=cookie_path, status_callback=push_log,
            naver_id=naver_id, password=password,
        ))
        if result:
            push_log(f"✅ 블로그 계정 {slot} 로그인 & 쿠키 저장 완료!")
        else:
            push_log(f"❌ 블로그 계정 {slot} 로그인 실패 또는 시간 초과")

    thread = threading.Thread(target=run_login, daemon=True)
    thread.start()
    return jsonify({"ok": True, "message": f"블로그 계정 {slot} 로그인 브라우저가 열립니다."})


@app.route(f"{URL_PREFIX}/blog/fetch-url", methods=["POST"])
@admin_required
def blog_fetch_url():
    """URL에서 제목, 본문, 이미지 추출 (JS 렌더링 사이트는 Playwright 사용)"""
    d = request.json or {}
    url = d.get("url", "").strip()
    if not url:
        return jsonify({"error": "URL이 비어있습니다"})

    # JS 렌더링이 필요한 사이트 목록
    js_sites = ["smartstore.naver.com", "shopping.naver.com", "brand.naver.com"]
    needs_playwright = any(site in url for site in js_sites)

    if needs_playwright:
        try:
            result = asyncio.run(_fetch_url_playwright(url))
            push_log(f"🌐 URL 추출 완료 (Playwright): {result.get('title', '')[:40]}... (이미지 {len(result.get('images', []))}개)")
            return jsonify(result)
        except Exception as e:
            return jsonify({"error": f"Playwright 추출 실패: {e}"})

    try:
        import requests as _req
        from bs4 import BeautifulSoup
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        resp = _req.get(url, headers=headers, timeout=15)
        resp.encoding = resp.apparent_encoding or "utf-8"
        soup = BeautifulSoup(resp.text, "html.parser")

        # 제목 추출
        title = ""
        for sel in [soup.find("meta", property="og:title"),
                    soup.find("meta", attrs={"name": "title"}),
                    soup.find("title")]:
            if sel:
                title = sel.get("content", "") if sel.name == "meta" else sel.get_text()
                if title:
                    break

        # 본문 추출
        body = ""
        for tag in ["article", "main", "[class*='content']", "[class*='detail']", "[class*='post']", "body"]:
            el = soup.select_one(tag)
            if el:
                for s in el.find_all(["script", "style", "nav", "header", "footer"]):
                    s.decompose()
                text = el.get_text(separator="\n", strip=True)
                if len(text) > 100:
                    body = text
                    break

        # 이미지 추출
        images = _extract_images_from_soup(soup, url)

        # 본문이 너무 짧으면 Playwright로 재시도
        if len(body) < 100:
            try:
                result = asyncio.run(_fetch_url_playwright(url))
                push_log(f"🌐 URL 추출 완료 (Playwright 폴백): {result.get('title', '')[:40]}...")
                return jsonify(result)
            except Exception:
                pass

        push_log(f"🌐 URL 추출 완료: {title[:40]}... (이미지 {len(images)}개)")
        return jsonify({"title": title.strip(), "body": body[:5000], "images": images})
    except Exception as e:
        return jsonify({"error": str(e)})


def _extract_images_from_soup(soup, base_url):
    """BeautifulSoup에서 이미지 URL 추출"""
    from urllib.parse import urlparse
    images = []
    seen = set()
    for img in soup.find_all("img"):
        src = img.get("src") or img.get("data-src") or ""
        if not src or src in seen:
            continue
        if src.startswith("//"):
            src = "https:" + src
        elif src.startswith("/"):
            parsed = urlparse(base_url)
            src = f"{parsed.scheme}://{parsed.netloc}{src}"
        w = img.get("width", "")
        h = img.get("height", "")
        if w and w.isdigit() and int(w) < 50:
            continue
        if h and h.isdigit() and int(h) < 50:
            continue
        if any(x in src.lower() for x in ["logo", "icon", "banner", "ad", "pixel", "tracking", "1x1"]):
            continue
        seen.add(src)
        images.append(src)
        if len(images) >= 30:
            break
    return images


async def _fetch_url_playwright(url: str) -> dict:
    """Playwright로 JS 렌더링 후 콘텐츠 추출 (스마트스토어 등)"""
    from playwright.async_api import async_playwright
    from cafe_uploader import load_cookies

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False, args=["--no-sandbox"])
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )

        # 네이버 계정 쿠키 로드 (로그인 상태로 접근)
        try:
            naver_data = _load_naver_accounts()
            active_slot = naver_data.get("active", 1)
            cookie_path = _get_cookie_path(active_slot)
            cookies = load_cookies(cookie_path)
            if cookies:
                await context.add_cookies(cookies)
                logger.info(f"🍪 네이버 쿠키 로드 (계정 {active_slot})")
        except Exception:
            pass

        page = await context.new_page()
        try:
            await page.goto(url, wait_until="networkidle", timeout=30000)
            await asyncio.sleep(3)

            # 스마트스토어 상세 영역 스크롤 (이미지 lazy load 대응)
            for _ in range(5):
                await page.keyboard.press("PageDown")
                await asyncio.sleep(0.5)

            # 더보기 버튼 클릭 (상세 정보 펼치기)
            try:
                more_btn = page.locator("a:has-text('상품정보 더보기'), button:has-text('더보기'), [class*='more']").first
                if await more_btn.count() > 0:
                    await more_btn.click()
                    await asyncio.sleep(2)
            except Exception:
                pass

            # 상세 페이지 끝까지 스크롤 (lazy load 이미지 전부 로드)
            for _ in range(20):
                await page.keyboard.press("PageDown")
                await asyncio.sleep(0.3)
            await asyncio.sleep(2)

            # 제목 추출
            title = ""
            for sel in ["h3._22kNQuEXmb", "h3[class*='title']", "._3oDjSvLDjZ", "h2", "h3", "title"]:
                try:
                    el = page.locator(sel).first
                    if await el.count() > 0:
                        title = (await el.inner_text()).strip()
                        if title:
                            break
                except Exception:
                    continue
            if not title:
                title = await page.title()

            # 본문 텍스트 추출 — se-main-container 내 se-text, se-sectionTitle만
            # se-quotation(인삿말/공지) 완전 제외
            body = ""
            try:
                body = await page.evaluate("""() => {
                    const container = document.querySelector('div.se-main-container');
                    if (!container) return '';
                    const lines = [];
                    // se-main-container 안의 모든 텍스트 paragraph 추출 (이미지/구분선 제외, 나머지 전부)
                    const paragraphs = container.querySelectorAll('p.se-text-paragraph');
                    for (const p of paragraphs) {
                        const text = p.innerText.replace(/\\u200B/g, '').trim();
                        if (text) lines.push(text);
                    }
                    return lines.join('\\n');
                }""")
            except Exception:
                pass

            # se-main-container 못 찾으면 폴백
            if not body:
                for sel in ["div._1Hj-MkenCi", "div._3e8dOKsKKM", "div[class*='detail']", "div[class*='content']"]:
                    try:
                        el = page.locator(sel).first
                        if await el.count() > 0:
                            text = await el.inner_text()
                            if len(text) > len(body):
                                body = text
                    except Exception:
                        continue

            # 이미지 추출 — se-main-container 내 shop-phinf 이미지만
            images = []
            seen = set()

            # 스마트스토어 상세 본문: se-main-container > se-image-resource
            detail_img_selectors = [
                "div.se-main-container img.se-image-resource",
                "div.se-viewer img.se-image-resource",
                "div._1Hj-MkenCi img",
                "div._3e8dOKsKKM img",
            ]
            detail_imgs = []
            for sel in detail_img_selectors:
                try:
                    imgs = await page.query_selector_all(sel)
                    if imgs:
                        detail_imgs = imgs
                        break
                except Exception:
                    continue

            if not detail_imgs:
                detail_imgs = await page.query_selector_all("img")

            for img in detail_imgs:
                for attr in ["src", "data-src", "data-lazy-src", "data-original"]:
                    src = await img.get_attribute(attr) or ""
                    if src and "shop-phinf.pstatic.net" in src:
                        if src.startswith("//"):
                            src = "https:" + src
                        if src not in seen:
                            seen.add(src)
                            images.append(src)
                        break
                if len(images) >= 30:
                    break

            body = body.strip()[:8000]
            return {"title": title.strip(), "body": body, "images": images}

        finally:
            await browser.close()


@app.route(f"{URL_PREFIX}/blog/post-url-content", methods=["POST"])
@admin_required
def blog_post_url_content():
    """URL에서 추출한 콘텐츠를 블로그에 발행"""
    d = request.json or {}
    title = d.get("title", "").strip()
    body = d.get("body", "").strip()
    images = d.get("images", [])
    category = d.get("category", "").strip()
    if not title or not body:
        return jsonify({"ok": False, "error": "제목과 본문이 필요합니다"})

    def run_post():
        try:
            from blog_uploader import blog_post_custom_content
            result = asyncio.run(blog_post_custom_content(
                title=title, body=body, images=images, log=push_log,
                category=category
            ))
            if result:
                push_log(f"✅ 블로그 URL 콘텐츠 발행 성공!")
            else:
                push_log(f"❌ 블로그 URL 콘텐츠 발행 실패")
        except Exception as e:
            push_log(f"❌ 블로그 발행 오류: {e}")

    thread = threading.Thread(target=run_post, daemon=True)
    thread.start()
    return jsonify({"ok": True, "message": "블로그 발행 시작"})


@app.route(f"{URL_PREFIX}/run/stop", methods=["POST"])
def stop_all():
    """가벼운 중단: 크롤링/업로드 중지 + 브라우저 정리 (데이터 삭제 없음)"""
    status["stop_requested"] = True
    status["paused"] = False

    def _cleanup():
        try:
            asyncio.run(force_close_browser())
        except Exception:
            pass
        # 2ndstreet 브라우저도 정리
        try:
            from secondst_crawler import force_close_browser as close_2nd
            asyncio.run(close_2nd())
        except Exception:
            pass
    threading.Thread(target=_cleanup, daemon=True).start()

    status["scraping"] = False
    status["uploading"] = False
    push_log("⛔ 페이지 이탈 감지 — 작업 중단 + 브라우저 정리")
    return jsonify({"ok": True})


@app.route(f"{URL_PREFIX}/run/pause", methods=["POST"])
@admin_required
def pause_scrape():
    """일시정지: 현재 상품 완료 후 멈춤"""
    if not status["scraping"]:
        return jsonify({"ok": False, "message": "실행 중인 작업이 없습니다"})
    status["paused"] = True
    push_log("⏸️ 일시정지 요청 — 현재 상품 수집 완료 후 멈춥니다...")
    return jsonify({"ok": True, "message": "일시정지 요청됨"})


@app.route(f"{URL_PREFIX}/run/resume", methods=["POST"])
@admin_required
def resume_scrape():
    """일시정지 해제"""
    status["paused"] = False
    push_log("▶️ 재개 — 수집을 계속합니다!")
    return jsonify({"ok": True, "message": "재개됨"})


@app.route(f"{URL_PREFIX}/run/unlock", methods=["POST"])
@admin_required
def unlock_status():
    """상태 잠금 해제 (데이터 삭제 없이 stuck 상태만 리셋)"""
    was_scraping = status["scraping"]
    was_uploading = status["uploading"]
    status["scraping"] = False
    status["uploading"] = False
    status["paused"] = False
    status["stop_requested"] = False
    msg = "🔓 상태 잠금 해제 완료"
    if was_scraping or was_uploading:
        msg += f" (scraping={was_scraping}, uploading={was_uploading} → False)"
    push_log(msg)
    return jsonify({"ok": True, "message": msg})


@app.route(f"{URL_PREFIX}/run/reset", methods=["POST"])
@admin_required
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
@admin_required
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
        except GeneratorExit:
            # 클라이언트 연결 끊김 (F5/탭닫기/강제종료)
            logger.info("🔌 SSE 클라이언트 연결 끊김 감지")
            # 다른 SSE 클라이언트가 남아있으면 크롤링 중단하지 않음
            remaining = len(_log_subscribers) - 1  # 현재 끊기는 클라이언트 제외
            if status.get("scraping") and remaining <= 0:
                logger.info("⛔ 마지막 SSE 클라이언트 끊김 — 작업 중단 + 브라우저 정리")
                status["stop_requested"] = True
                status["paused"] = False
                try:
                    asyncio.run(force_close_browser())
                except Exception:
                    pass
                try:
                    from secondst_crawler import force_close_browser as close_2nd
                    asyncio.run(close_2nd())
                except Exception:
                    pass
                status["scraping"] = False
            elif status.get("scraping"):
                logger.info(f"   ℹ️ SSE 클라이언트 끊김이지만 {remaining}개 클라이언트 남아있음 — 크롤링 계속")
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

    # 회원 DB 초기화
    try:
        init_user_db()
    except Exception as e:
        print(f"⚠️ 회원 DB 초기화 실패: {e}")

    # use_reloader=False일 때 스케줄러 시작 (reloader 사용 시에는 위에서 이미 시작됨)
    _start_scheduler_once()

    print(f"\n  Xebio Dashboard: http://{SERVER_HOST}:{SERVER_PORT}{URL_PREFIX}\n")

    app.run(
        host=SERVER_HOST,
        port=SERVER_PORT,
        debug=False,
        threaded=True,
        use_reloader=True,       # 파일 수정 시 자동 재기동
    )