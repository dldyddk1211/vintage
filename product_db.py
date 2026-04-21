"""
product_db.py
빅데이터 관리 — 수집 상품 누적 저장 (SQLite)

중복 기준: site_id + product_code + price_jpy 가 동일하면 중복
"""

import json
import os
import sqlite3
import logging
from datetime import datetime
from data_manager import get_path

logger = logging.getLogger(__name__)

# Mac: 로컬 DB (속도), Windows: NAS 공유 DB (get_path가 OS별 자동 분기)
_DB_PATH = os.path.join(get_path("db"), "products.db")


def _conn() -> sqlite3.Connection:
    """DB 연결 (스레드 안전)"""
    os.makedirs(os.path.dirname(_DB_PATH), exist_ok=True)
    conn = sqlite3.connect(_DB_PATH, check_same_thread=False, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    """테이블 & 인덱스 생성"""
    conn = _conn()
    try:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                site_id TEXT NOT NULL,
                category_id TEXT NOT NULL DEFAULT '',
                product_code TEXT NOT NULL DEFAULT '',
                name TEXT DEFAULT '',
                name_ko TEXT DEFAULT '',
                brand TEXT DEFAULT '',
                brand_ko TEXT DEFAULT '',
                price_jpy INTEGER NOT NULL DEFAULT 0,
                link TEXT DEFAULT '',
                img_url TEXT DEFAULT '',
                description TEXT DEFAULT '',
                description_ko TEXT DEFAULT '',
                sizes TEXT DEFAULT '[]',
                detail_images TEXT DEFAULT '[]',
                original_price INTEGER DEFAULT 0,
                discount_rate INTEGER DEFAULT 0,
                in_stock INTEGER DEFAULT 1,
                cafe_status TEXT DEFAULT '',
                cafe_uploaded_at TEXT DEFAULT '',
                scraped_at TEXT DEFAULT '',
                created_at TEXT DEFAULT (datetime('now','localtime')),
                UNIQUE(site_id, product_code, price_jpy)
            );
            CREATE INDEX IF NOT EXISTS idx_site ON products(site_id);
            CREATE INDEX IF NOT EXISTS idx_category ON products(site_id, category_id);
            CREATE INDEX IF NOT EXISTS idx_created ON products(created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_brand ON products(brand_ko);
            CREATE INDEX IF NOT EXISTS idx_code ON products(product_code);
        """)
        conn.commit()

        # 기존 DB에 새 컬럼 추가 (마이그레이션)
        # 마이그레이션: 새 컬럼 추가 (이미 존재하면 무시)
        for col, default in [
            ("cafe_status", "''"),
            ("cafe_uploaded_at", "''"),
            ("source_type", "'sports'"),
            ("condition_grade", "''"),
            ("color", "''"),
            ("material", "''"),
            ("gender", "''"),
            ("subcategory", "''"),
            ("internal_code", "''"),
            ("product_status", "'available'"),
            ("checked_at", "''"),
        ]:
            try:
                conn.execute(f"ALTER TABLE products ADD COLUMN {col} TEXT DEFAULT {default}")
                conn.commit()
            except sqlite3.OperationalError:
                pass
        conn.execute("CREATE INDEX IF NOT EXISTS idx_source_type ON products(source_type)")
        # site_id + product_code 유니크 인덱스 (중복 검열 강화)
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_site_code ON products(site_id, product_code)")

        # ── 가격 변경 이력 테이블 ──
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS price_changes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id INTEGER NOT NULL,
                site_id TEXT DEFAULT '',
                product_code TEXT DEFAULT '',
                internal_code TEXT DEFAULT '',
                brand_ko TEXT DEFAULT '',
                category_id TEXT DEFAULT '',
                old_price INTEGER DEFAULT 0,
                new_price INTEGER DEFAULT 0,
                change_type TEXT DEFAULT '',
                updated_at TEXT DEFAULT (datetime('now','localtime')),
                synced_at TEXT DEFAULT ''
            );
            CREATE INDEX IF NOT EXISTS idx_pc_updated ON price_changes(updated_at DESC);
            CREATE INDEX IF NOT EXISTS idx_pc_type ON price_changes(change_type);
        """)
        conn.commit()

        logger.info(f"빅데이터 DB 초기화 완료: {_DB_PATH}")
    finally:
        conn.close()


# ── 고유번호 생성 ──
_SITE_CODE = {
    "2ndstreet": "S",
    "kindal": "K",
    "brandoff": "B",
    "komehyo": "KM",
    "xebio": "X",
    "kabinet": "KV",
    "musinsa": "MS",
}

# 같은 배치 내에서 번호 중복 방지용 카운터
_internal_code_counter = {}

def _generate_internal_code(conn, site_id: str) -> str:
    """사이트별 고유번호 생성: S-260331-0001 (중복 방지 강화)"""
    prefix = _SITE_CODE.get(site_id, site_id[0].upper())
    today = datetime.now().strftime("%y%m%d")
    key = f"{prefix}-{today}"

    # 메모리 카운터에 값이 있으면 그걸 사용 (같은 배치 내 중복 방지)
    if key in _internal_code_counter:
        _internal_code_counter[key] += 1
        return f"No.{key}-{_internal_code_counter[key]:04d}"

    # DB에서 해당 날짜의 모든 번호를 가져와 최대값 찾기
    rows = conn.execute(
        "SELECT internal_code FROM products WHERE internal_code LIKE ?",
        (f"No.{prefix}-{today}-%",)
    ).fetchall()
    max_num = 0
    for row in rows:
        try:
            num = int(row["internal_code"].split("-")[-1])
            if num > max_num:
                max_num = num
        except (ValueError, IndexError):
            pass
    next_num = max_num + 1
    _internal_code_counter[key] = next_num
    return f"No.{key}-{next_num:04d}"


def exists(site_id: str, product_code: str, price_jpy: int = 0) -> bool:
    """중복 여부 확인"""
    if not product_code:
        return False
    conn = _conn()
    try:
        row = conn.execute(
            "SELECT 1 FROM products WHERE site_id=? AND product_code=? AND price_jpy=?",
            (site_id, product_code, price_jpy)
        ).fetchone()
        return row is not None
    finally:
        conn.close()


def bulk_exists(site_id: str, products: list, days=15) -> set:
    """상품 리스트에서 이미 DB에 있는 product_code 셋 반환"""
    conn = _conn()
    try:
        existing = set()
        codes = [p.get("product_code", "") for p in products if p.get("product_code")]
        for i in range(0, len(codes), 100):
            batch = codes[i:i+100]
            placeholders = ",".join(["?" for _ in batch])
            rows = conn.execute(
                f"SELECT product_code FROM products "
                f"WHERE site_id=? AND product_code IN ({placeholders})",
                [site_id] + batch
            ).fetchall()
            for row in rows:
                existing.add(row["product_code"])
        return existing
    finally:
        conn.close()


def bulk_check_price(site_id: str, products: list) -> dict:
    """상품 리스트의 DB 가격을 일괄 조회.
    Returns: {product_code: price_jpy} (DB에 있는 것만)
    """
    conn = _conn()
    try:
        result = {}
        codes = [p.get("product_code", "") for p in products if p.get("product_code")]
        for i in range(0, len(codes), 100):
            batch = codes[i:i+100]
            placeholders = ",".join(["?" for _ in batch])
            rows = conn.execute(
                f"SELECT product_code, price_jpy FROM products "
                f"WHERE site_id=? AND product_code IN ({placeholders})",
                [site_id] + batch
            ).fetchall()
            for row in rows:
                result[row["product_code"]] = row["price_jpy"]
        return result
    finally:
        conn.close()


def insert_products(products: list) -> int:
    """상품 리스트를 DB에 저장. 중복 시 가격/이미지/상태 업데이트 (cafe_status 유지). 저장+업데이트 수 반환"""
    if not products:
        return 0
    conn = _conn()
    try:
        inserted = 0
        updated = 0
        for p in products:
            site_id = p.get("site_id", "xebio")
            code = p.get("product_code", "")
            if not code:
                continue

            try:
                # 기존 상품 확인 (site_id + product_code)
                existing = conn.execute(
                    "SELECT id, price_jpy, cafe_status FROM products WHERE site_id = ? AND product_code = ?",
                    (site_id, code)
                ).fetchone()

                if existing:
                    # ── 가격 변경 감지 & 이력 저장 ──
                    old_price = existing["price_jpy"]
                    new_price = p.get("price_jpy", 0)
                    if old_price != new_price and new_price > 0 and old_price > 0:
                        change_type = "가격인하" if new_price < old_price else "가격인상"
                        try:
                            # internal_code 조회
                            ic_row = conn.execute(
                                "SELECT internal_code, brand_ko, category_id FROM products WHERE id=?",
                                (existing["id"],)
                            ).fetchone()
                            conn.execute("""
                                INSERT INTO price_changes
                                (product_id, site_id, product_code, internal_code, brand_ko, category_id,
                                 old_price, new_price, change_type, updated_at)
                                VALUES (?,?,?,?,?,?,?,?,?,?)
                            """, (
                                existing["id"], site_id, code,
                                ic_row["internal_code"] if ic_row else "",
                                ic_row["brand_ko"] if ic_row else "",
                                ic_row["category_id"] if ic_row else "",
                                old_price, new_price, change_type,
                                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            ))
                        except Exception as e:
                            logger.debug(f"가격 변경 이력 저장 오류: {e}")

                    # 중복 — 가격/이미지/상세 정보만 업데이트 (cafe_status 유지)
                    conn.execute("""
                        UPDATE products SET
                            price_jpy = ?, name = COALESCE(NULLIF(?, ''), name),
                            name_ko = COALESCE(NULLIF(?, ''), name_ko),
                            brand = COALESCE(NULLIF(?, ''), brand),
                            brand_ko = COALESCE(NULLIF(?, ''), brand_ko),
                            img_url = COALESCE(NULLIF(?, ''), img_url),
                            link = COALESCE(NULLIF(?, ''), link),
                            description = COALESCE(NULLIF(?, ''), description),
                            detail_images = CASE WHEN ? != '[]' THEN ? ELSE detail_images END,
                            condition_grade = COALESCE(NULLIF(?, ''), condition_grade),
                            color = COALESCE(NULLIF(?, ''), color),
                            material = COALESCE(NULLIF(?, ''), material),
                            category_id = COALESCE(NULLIF(?, ''), category_id),
                            subcategory = COALESCE(NULLIF(?, ''), subcategory),
                            in_stock = 1, scraped_at = ?
                        WHERE id = ?
                    """, (
                        p.get("price_jpy", 0),
                        p.get("name", ""), p.get("name_ko", ""),
                        p.get("brand", ""), p.get("brand_ko", ""),
                        p.get("img_url", ""), p.get("link", ""),
                        p.get("description", ""),
                        json.dumps(p.get("detail_images", []), ensure_ascii=False),
                        json.dumps(p.get("detail_images", []), ensure_ascii=False),
                        p.get("condition_grade", ""),
                        p.get("color", ""), p.get("material", ""),
                        p.get("category_id", ""), p.get("subcategory", ""),
                        p.get("scraped_at", datetime.now().isoformat()),
                        existing["id"],
                    ))
                    updated += 1
                else:
                    # 신규 저장 + 고유번호 생성
                    internal_code = _generate_internal_code(conn, site_id)
                    conn.execute("""
                        INSERT INTO products
                        (site_id, category_id, product_code, name, name_ko,
                         brand, brand_ko, price_jpy, link, img_url,
                         description, description_ko, sizes, detail_images,
                         original_price, discount_rate, in_stock, scraped_at,
                         source_type, condition_grade, color, material, gender, subcategory,
                         internal_code)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """, (
                        site_id, p.get("category_id", ""), code,
                        p.get("name", ""), p.get("name_ko", ""),
                        p.get("brand", ""), p.get("brand_ko", ""),
                        p.get("price_jpy", 0), p.get("link", ""), p.get("img_url", ""),
                        p.get("description", ""), p.get("description_ko", ""),
                        json.dumps(p.get("sizes", []), ensure_ascii=False),
                        json.dumps(p.get("detail_images", []), ensure_ascii=False),
                        p.get("original_price", 0), p.get("discount_rate", 0),
                        1 if p.get("in_stock", True) else 0,
                        p.get("scraped_at", datetime.now().isoformat()),
                        p.get("source_type", "sports"),
                        p.get("condition_grade", ""), p.get("color", ""),
                        p.get("material", ""), p.get("gender", ""), p.get("subcategory", ""),
                        internal_code,
                    ))
                    inserted += 1
            except Exception as e:
                logger.debug(f"상품 저장 오류: {e}")
        conn.commit()
        logger.info(f"빅데이터 DB: {inserted}개 신규, {updated}개 업데이트 (총 {len(products)}개)")
        return inserted + updated
    finally:
        conn.close()


def check_cafe_status(product_code: str) -> str:
    """빅데이터 DB에서 해당 품번의 카페 업로드 상태 확인
    Returns: '업로드완료', '중복', '' (미확인)
    """
    if not product_code:
        return ""
    conn = _conn()
    try:
        row = conn.execute(
            "SELECT cafe_status FROM products WHERE product_code=? AND cafe_status != '' "
            "ORDER BY created_at DESC LIMIT 1",
            (product_code,)
        ).fetchone()
        return row["cafe_status"] if row else ""
    finally:
        conn.close()


def bulk_check_cafe_status(product_codes: list) -> dict:
    """여러 품번의 카페 상태를 일괄 조회
    Returns: {product_code: cafe_status} 딕셔너리
    """
    if not product_codes:
        return {}
    conn = _conn()
    try:
        result = {}
        for i in range(0, len(product_codes), 100):
            batch = product_codes[i:i+100]
            placeholders = ",".join(["?" for _ in batch])
            rows = conn.execute(
                f"SELECT product_code, cafe_status FROM products "
                f"WHERE product_code IN ({placeholders}) AND cafe_status != '' "
                f"ORDER BY created_at DESC",
                batch
            ).fetchall()
            for row in rows:
                code = row["product_code"]
                if code not in result:  # 최신 것만
                    result[code] = row["cafe_status"]
        return result
    finally:
        conn.close()


def update_cafe_status(product_code: str, status: str, uploaded_at: str = ""):
    """빅데이터 DB에서 해당 품번의 카페 상태 업데이트"""
    if not product_code:
        return
    conn = _conn()
    try:
        if uploaded_at:
            conn.execute(
                "UPDATE products SET cafe_status=?, cafe_uploaded_at=? WHERE product_code=?",
                (status, uploaded_at, product_code)
            )
        else:
            conn.execute(
                "UPDATE products SET cafe_status=? WHERE product_code=?",
                (status, product_code)
            )
        conn.commit()
    finally:
        conn.close()


def get_product_status(product_code: str) -> str:
    """품번으로 카페 상태 조회 (대기/완료/업로드완료/중복 등)"""
    if not product_code:
        return ""
    conn = _conn()
    try:
        row = conn.execute(
            "SELECT cafe_status FROM products WHERE product_code=? ORDER BY created_at DESC LIMIT 1",
            (product_code,)
        ).fetchone()
        return row["cafe_status"] if row and row["cafe_status"] else ""
    except Exception:
        return ""
    finally:
        conn.close()


def get_stats(source_type=None) -> dict:
    """통계 반환 (source_type 필터 지원)"""
    conn = _conn()
    try:
        where = f" WHERE source_type='{source_type}'" if source_type else ""
        total = conn.execute(f"SELECT COUNT(*) c FROM products{where}").fetchone()["c"]

        # 사이트별 통계 (카테고리 + 브랜드)
        site_rows = conn.execute("""
            SELECT site_id, category_id, COUNT(*) c
            FROM products GROUP BY site_id, category_id
            ORDER BY site_id, category_id
        """).fetchall()
        by_site = {}
        for r in site_rows:
            sid = r["site_id"]
            if sid not in by_site:
                by_site[sid] = {"total": 0, "categories": {}}
            by_site[sid]["total"] += r["c"]
            by_site[sid]["categories"][r["category_id"]] = r["c"]

        # 사이트+카테고리별 브랜드 통계
        brand_rows = conn.execute("""
            SELECT site_id, category_id, brand_ko, COUNT(*) c
            FROM products WHERE brand_ko != ''
            GROUP BY site_id, category_id, brand_ko
            ORDER BY site_id, category_id, c DESC
        """).fetchall()
        by_site_brand = {}
        for r in brand_rows:
            key = f"{r['site_id']}|{r['category_id']}"
            if key not in by_site_brand:
                by_site_brand[key] = []
            by_site_brand[key].append({"brand": r["brand_ko"], "count": r["c"]})

        # 최근 통계
        today = conn.execute(
            "SELECT COUNT(*) c FROM products WHERE date(created_at) = date('now','localtime')"
        ).fetchone()["c"]
        week = conn.execute(
            "SELECT COUNT(*) c FROM products WHERE created_at >= datetime('now','localtime','-7 days')"
        ).fetchone()["c"]

        # 브랜드 Top 10
        brands = conn.execute("""
            SELECT brand_ko, COUNT(*) c FROM products
            WHERE brand_ko != '' GROUP BY brand_ko ORDER BY c DESC LIMIT 10
        """).fetchall()

        # 카페 업로드 통계
        uploaded_total = conn.execute(
            "SELECT COUNT(*) c FROM products WHERE cafe_status = '업로드완료'"
        ).fetchone()["c"]
        uploaded_today = conn.execute(
            "SELECT COUNT(*) c FROM products WHERE cafe_status = '업로드완료' "
            "AND date(cafe_uploaded_at) = date('now','localtime')"
        ).fetchone()["c"]
        uploaded_week = conn.execute(
            "SELECT COUNT(*) c FROM products WHERE cafe_status = '업로드완료' "
            "AND cafe_uploaded_at >= datetime('now','localtime','-7 days')"
        ).fetchone()["c"]
        uploaded_brands = conn.execute(
            "SELECT COUNT(DISTINCT brand_ko) c FROM products "
            "WHERE cafe_status = '업로드완료' AND brand_ko != ''"
        ).fetchone()["c"]

        return {
            "total": total,
            "today": today,
            "week": week,
            "by_site": by_site,
            "by_site_brand": by_site_brand,
            "top_brands": [{"brand": r["brand_ko"], "count": r["c"]} for r in brands],
            "uploaded_total": uploaded_total,
            "uploaded_today": uploaded_today,
            "uploaded_week": uploaded_week,
            "uploaded_brands": uploaded_brands,
        }
    finally:
        conn.close()


def search_products(query="", site_id="", category_id="", brand="",
                    cafe_status="", page=1, per_page=50, source_type="") -> dict:
    """상품 검색 (페이지네이션, source_type 필터)"""
    conn = _conn()
    try:
        conditions = []
        params = []

        if source_type:
            conditions.append("source_type = ?")
            params.append(source_type)
        if query:
            conditions.append("(name_ko LIKE ? OR product_code LIKE ? OR brand_ko LIKE ?)")
            q = f"%{query}%"
            params.extend([q, q, q])
        if site_id:
            conditions.append("site_id = ?")
            params.append(site_id)
        if category_id:
            conditions.append("category_id = ?")
            params.append(category_id)
        if brand:
            conditions.append("brand_ko = ?")
            params.append(brand)
        if cafe_status:
            if cafe_status == "대기":
                conditions.append("(cafe_status = '' OR cafe_status IS NULL)")
            else:
                conditions.append("cafe_status = ?")
                params.append(cafe_status)

        where = "WHERE " + " AND ".join(conditions) if conditions else ""
        count = conn.execute(f"SELECT COUNT(*) c FROM products {where}", params).fetchone()["c"]

        offset = (page - 1) * per_page
        rows = conn.execute(
            f"SELECT * FROM products {where} ORDER BY created_at DESC LIMIT ? OFFSET ?",
            params + [per_page, offset]
        ).fetchall()

        products = []
        for r in rows:
            products.append({
                "id": r["id"],
                "site_id": r["site_id"],
                "category_id": r["category_id"],
                "product_code": r["product_code"],
                "name_ko": r["name_ko"] or r["name"],
                "brand_ko": r["brand_ko"] or r["brand"],
                "price_jpy": r["price_jpy"],
                "img_url": r["img_url"],
                "link": r["link"],
                "cafe_status": r["cafe_status"] or "",
                "cafe_uploaded_at": r["cafe_uploaded_at"] or "",
                "created_at": r["created_at"],
            })

        return {
            "total": count,
            "page": page,
            "per_page": per_page,
            "pages": (count + per_page - 1) // per_page,
            "products": products,
        }
    finally:
        conn.close()


def get_brands() -> list:
    """브랜드 목록 (카운트 포함)"""
    conn = _conn()
    try:
        rows = conn.execute("""
            SELECT brand_ko, COUNT(*) c FROM products
            WHERE brand_ko != '' GROUP BY brand_ko ORDER BY c DESC
        """).fetchall()
        return [{"brand": r["brand_ko"], "count": r["c"]} for r in rows]
    finally:
        conn.close()


def delete_by_ids(ids: list) -> int:
    """ID 리스트로 상품 삭제"""
    if not ids:
        return 0
    conn = _conn()
    try:
        placeholders = ",".join("?" for _ in ids)
        cur = conn.execute(f"DELETE FROM products WHERE id IN ({placeholders})", ids)
        conn.commit()
        return cur.rowcount
    finally:
        conn.close()


def export_all(query="", site_id="", brand="", source_type="") -> list:
    """전체 상품 내보내기 (필터 적용 가능)"""
    conn = _conn()
    try:
        conditions = []
        params = []
        if source_type:
            conditions.append("source_type = ?")
            params.append(source_type)
        if query:
            conditions.append("(name_ko LIKE ? OR product_code LIKE ? OR brand_ko LIKE ?)")
            q = f"%{query}%"
            params.extend([q, q, q])
        if site_id:
            conditions.append("site_id = ?")
            params.append(site_id)
        if brand:
            conditions.append("brand_ko = ?")
            params.append(brand)

        where = "WHERE " + " AND ".join(conditions) if conditions else ""
        rows = conn.execute(
            f"SELECT * FROM products {where} ORDER BY created_at DESC", params
        ).fetchall()

        products = []
        for r in rows:
            products.append({
                "site_id": r["site_id"],
                "category_id": r["category_id"],
                "product_code": r["product_code"],
                "name": r["name"],
                "name_ko": r["name_ko"] or r["name"],
                "brand": r["brand"],
                "brand_ko": r["brand_ko"] or r["brand"],
                "price_jpy": r["price_jpy"],
                "original_price": r["original_price"],
                "discount_rate": r["discount_rate"],
                "link": r["link"],
                "img_url": r["img_url"],
                "in_stock": r["in_stock"],
                "created_at": r["created_at"],
            })
        return products
    finally:
        conn.close()


def delete_all() -> int:
    """전체 삭제"""
    conn = _conn()
    try:
        count = conn.execute("SELECT COUNT(*) c FROM products").fetchone()["c"]
        conn.execute("DELETE FROM products")
        conn.commit()
        logger.info(f"빅데이터 DB 전체 삭제: {count}개")
        return count
    finally:
        conn.close()


def delete_by_site(site_id: str) -> int:
    """사이트별 삭제"""
    conn = _conn()
    try:
        count = conn.execute("SELECT COUNT(*) c FROM products WHERE site_id=?", (site_id,)).fetchone()["c"]
        conn.execute("DELETE FROM products WHERE site_id=?", (site_id,))
        conn.commit()
        logger.info(f"빅데이터 DB 사이트 삭제 ({site_id}): {count}개")
        return count
    finally:
        conn.close()


def merge_products(csv_rows: list) -> dict:
    """CSV 데이터 병합 — created_at 비교하여 최신 데이터로 업데이트

    Args:
        csv_rows: list of dict (CSV 파싱 결과)
    Returns:
        {"inserted": N, "updated": N, "skipped": N}
    """
    conn = _conn()
    result = {"inserted": 0, "updated": 0, "skipped": 0}
    try:
        for row in csv_rows:
            site_id = row.get("site_id", "").strip()
            product_code = row.get("product_code", "").strip()
            price_jpy = int(row.get("price_jpy", 0) or 0)

            if not site_id or not product_code:
                result["skipped"] += 1
                continue

            csv_created = row.get("created_at", "").strip()

            # 기존 데이터 조회
            existing = conn.execute(
                "SELECT id, created_at FROM products WHERE site_id=? AND product_code=? AND price_jpy=?",
                (site_id, product_code, price_jpy)
            ).fetchone()

            if existing:
                db_created = existing["created_at"] or ""
                # CSV 데이터가 더 최신이면 업데이트
                if csv_created and csv_created > db_created:
                    conn.execute("""
                        UPDATE products SET
                            category_id=?, name=?, name_ko=?, brand=?, brand_ko=?,
                            link=?, img_url=?, original_price=?, discount_rate=?,
                            in_stock=?, cafe_status=?, cafe_uploaded_at=?, created_at=?
                        WHERE id=?
                    """, (
                        row.get("category_id", ""),
                        row.get("name", ""),
                        row.get("name_ko", ""),
                        row.get("brand", ""),
                        row.get("brand_ko", ""),
                        row.get("link", ""),
                        row.get("img_url", ""),
                        int(row.get("original_price", 0) or 0),
                        int(row.get("discount_rate", 0) or 0),
                        1 if str(row.get("in_stock", "1")).strip() in ("1", "O", "True", "true") else 0,
                        row.get("cafe_status", ""),
                        row.get("cafe_uploaded_at", ""),
                        csv_created,
                        existing["id"],
                    ))
                    result["updated"] += 1
                else:
                    result["skipped"] += 1
            else:
                # 신규 데이터 삽입 — internal_code 자동 발급
                csv_int_code = (row.get("internal_code") or "").strip()
                if not csv_int_code:
                    csv_int_code = _generate_internal_code(conn, site_id)
                conn.execute("""
                    INSERT INTO products
                    (site_id, category_id, product_code, name, name_ko,
                     brand, brand_ko, price_jpy, link, img_url,
                     original_price, discount_rate, in_stock,
                     cafe_status, cafe_uploaded_at, created_at,
                     internal_code, source_type)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    site_id,
                    row.get("category_id", ""),
                    product_code,
                    row.get("name", ""),
                    row.get("name_ko", ""),
                    row.get("brand", ""),
                    row.get("brand_ko", ""),
                    price_jpy,
                    row.get("link", ""),
                    row.get("img_url", ""),
                    int(row.get("original_price", 0) or 0),
                    int(row.get("discount_rate", 0) or 0),
                    1 if str(row.get("in_stock", "1")).strip() in ("1", "O", "True", "true") else 0,
                    row.get("cafe_status", ""),
                    row.get("cafe_uploaded_at", ""),
                    csv_created or datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    csv_int_code,
                    row.get("source_type", "vintage"),
                ))
                result["inserted"] += 1

        conn.commit()
        total = result["inserted"] + result["updated"]
        logger.info(f"CSV 병합 완료: 신규 {result['inserted']}개, 업데이트 {result['updated']}개, 스킵 {result['skipped']}개")
        return result
    except Exception as e:
        logger.error(f"CSV 병합 오류: {e}")
        raise
    finally:
        conn.close()


def export_csv(query="", site_id="", brand="", source_type="") -> list:
    """전체 상품 CSV 내보내기용 데이터 (필터 적용 가능)"""
    conn = _conn()
    try:
        conditions = []
        params = []
        if source_type:
            conditions.append("source_type = ?")
            params.append(source_type)
        if query:
            conditions.append("(name_ko LIKE ? OR product_code LIKE ? OR brand_ko LIKE ?)")
            q = f"%{query}%"
            params.extend([q, q, q])
        if site_id:
            conditions.append("site_id = ?")
            params.append(site_id)
        if brand:
            conditions.append("brand_ko = ?")
            params.append(brand)

        where = "WHERE " + " AND ".join(conditions) if conditions else ""
        rows = conn.execute(
            f"SELECT * FROM products {where} ORDER BY created_at DESC", params
        ).fetchall()

        products = []
        for r in rows:
            products.append({
                "site_id": r["site_id"],
                "category_id": r["category_id"],
                "product_code": r["product_code"],
                "name": r["name"],
                "name_ko": r["name_ko"] or r["name"],
                "brand": r["brand"],
                "brand_ko": r["brand_ko"] or r["brand"],
                "price_jpy": r["price_jpy"],
                "original_price": r["original_price"],
                "discount_rate": r["discount_rate"],
                "link": r["link"],
                "img_url": r["img_url"],
                "in_stock": r["in_stock"],
                "cafe_status": r["cafe_status"] or "",
                "cafe_uploaded_at": r["cafe_uploaded_at"] or "",
                "created_at": r["created_at"],
            })
        return products
    finally:
        conn.close()


def get_total_count() -> int:
    """전체 상품 수"""
    conn = _conn()
    try:
        return conn.execute("SELECT COUNT(*) c FROM products").fetchone()["c"]
    finally:
        conn.close()


def get_unuploaded_products(source_type="") -> list:
    """카페 업로드 안 된 상품 목록 반환 (빅데이터 DB에서)"""
    conn = _conn()
    try:
        sql = """
            SELECT * FROM products
            WHERE (cafe_status = '' OR cafe_status IS NULL)
        """
        params = []
        if source_type:
            sql += " AND (source_type = ?)"
            params.append(source_type)
        sql += " ORDER BY created_at DESC"
        rows = conn.execute(sql, params).fetchall()
        products = []
        seen_codes = set()
        for r in rows:
            # product_code 기준 중복 제거 (같은 코드 여러 행 방지)
            dup_key = r["product_code"] or f"{r['brand']}-{r['name']}"
            if dup_key in seen_codes:
                continue
            seen_codes.add(dup_key)
            products.append({
                "site_id": r["site_id"],
                "category_id": r["category_id"],
                "product_code": r["product_code"],
                "name": r["name"],
                "name_ko": r["name_ko"] or r["name"],
                "brand": r["brand"],
                "brand_ko": r["brand_ko"] or r["brand"],
                "price_jpy": r["price_jpy"],
                "original_price": r["original_price"],
                "discount_rate": r["discount_rate"],
                "link": r["link"],
                "img_url": r["img_url"],
                "description": r["description"],
                "description_ko": r["description_ko"],
                "sizes": json.loads(r["sizes"]) if r["sizes"] else [],
                "detail_images": json.loads(r["detail_images"]) if r["detail_images"] else [],
                "in_stock": bool(r["in_stock"]),
                "cafe_status": "",
                "scraped_at": r["scraped_at"],
                "created_at": r["created_at"],
                "source_type": r["source_type"] if "source_type" in r.keys() else "sports",
                "from_db": True,
            })
        return products
    finally:
        conn.close()


def get_products_by_status(status: str) -> list:
    """특정 cafe_status의 상품 목록 반환 (빅데이터 DB에서)"""
    conn = _conn()
    try:
        # cafe_uploaded_at 컬럼 존재 여부 확인
        cols = {row[1] for row in conn.execute("PRAGMA table_info(products)").fetchall()}
        has_uploaded_at = "cafe_uploaded_at" in cols

        order_clause = "ORDER BY cafe_uploaded_at DESC, created_at DESC" if has_uploaded_at else "ORDER BY created_at DESC"
        rows = conn.execute(f"""
            SELECT * FROM products
            WHERE cafe_status = ?
            {order_clause}
        """, (status,)).fetchall()
        products = []
        for r in rows:
            products.append({
                "site_id": r["site_id"],
                "category_id": r["category_id"],
                "product_code": r["product_code"],
                "name": r["name"],
                "name_ko": r["name_ko"] or r["name"],
                "brand": r["brand"],
                "brand_ko": r["brand_ko"] or r["brand"],
                "price_jpy": r["price_jpy"],
                "original_price": r["original_price"],
                "discount_rate": r["discount_rate"],
                "link": r["link"],
                "img_url": r["img_url"],
                "description": r["description"],
                "description_ko": r["description_ko"],
                "sizes": json.loads(r["sizes"]) if r["sizes"] else [],
                "detail_images": json.loads(r["detail_images"]) if r["detail_images"] else [],
                "in_stock": bool(r["in_stock"]),
                "cafe_status": r["cafe_status"] or "",
                "cafe_uploaded_at": (r["cafe_uploaded_at"] or "") if has_uploaded_at else "",
                "scraped_at": r["scraped_at"],
                "created_at": r["created_at"],
                "from_db": True,
            })
        return products
    finally:
        conn.close()


def get_price_changes(change_type="", limit=200) -> list:
    """가격 변경 이력 조회"""
    conn = _conn()
    try:
        tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        if "price_changes" not in tables:
            return []
        where = ""
        params = []
        if change_type:
            where = "WHERE pc.change_type = ?"
            params.append(change_type)
        rows = conn.execute(f"""
            SELECT pc.*, p.name_ko, p.name, p.img_url, p.link,
                   p.brand_ko as p_brand_ko, p.category_id as p_category,
                   p.internal_code as p_internal_code
            FROM price_changes pc
            LEFT JOIN products p ON pc.product_id = p.id
            {where}
            ORDER BY pc.updated_at DESC LIMIT ?
        """, params + [limit]).fetchall()
        result = []
        for r in rows:
            result.append({
                "id": r["id"], "product_id": r["product_id"],
                "internal_code": r["internal_code"] or (r["p_internal_code"] if "p_internal_code" in r.keys() else "") or "",
                "brand_ko": r["brand_ko"] or (r["p_brand_ko"] if "p_brand_ko" in r.keys() else "") or "",
                "category_id": r["category_id"] or (r["p_category"] if "p_category" in r.keys() else "") or "",
                "product_code": r["product_code"],
                "old_price": r["old_price"], "new_price": r["new_price"],
                "change_type": r["change_type"],
                "updated_at": r["updated_at"], "synced_at": r["synced_at"] or "",
                "name": (r["name_ko"] if "name_ko" in r.keys() else "") or (r["name"] if "name" in r.keys() else "") or "",
                "img_url": r["img_url"] if "img_url" in r.keys() else "",
                "link": r["link"] if "link" in r.keys() else "",
            })
        return result
    finally:
        conn.close()
