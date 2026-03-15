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
        try:
            conn.execute("ALTER TABLE products ADD COLUMN cafe_status TEXT DEFAULT ''")
            conn.commit()
        except sqlite3.OperationalError:
            pass  # 이미 존재
        try:
            conn.execute("ALTER TABLE products ADD COLUMN cafe_uploaded_at TEXT DEFAULT ''")
            conn.commit()
        except sqlite3.OperationalError:
            pass

        logger.info(f"빅데이터 DB 초기화 완료: {_DB_PATH}")
    finally:
        conn.close()


def exists(site_id: str, product_code: str, price_jpy: int) -> bool:
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
    """상품 리스트에서 이미 DB에 있고 days일 이내인 (product_code, price_jpy) 튜플 셋 반환
    days일 이상 지난 상품은 중복으로 취급하지 않음 (재수집 허용)
    """
    conn = _conn()
    try:
        existing = set()
        # 배치로 조회 (100개씩)
        codes = [(p.get("product_code", ""), p.get("price_jpy", 0)) for p in products if p.get("product_code")]
        for i in range(0, len(codes), 100):
            batch = codes[i:i+100]
            placeholders = ",".join(["(?,?)" for _ in batch])
            params = []
            for code, price in batch:
                params.extend([code, price])
            rows = conn.execute(
                f"SELECT product_code, price_jpy FROM products "
                f"WHERE site_id=? AND (product_code, price_jpy) IN ({placeholders}) "
                f"AND created_at >= datetime('now','localtime','-{days} days')",
                [site_id] + params
            ).fetchall()
            for row in rows:
                existing.add((row["product_code"], row["price_jpy"]))
        return existing
    finally:
        conn.close()


def insert_products(products: list) -> int:
    """상품 리스트를 DB에 저장 (15일 이상 된 중복은 갱신). 저장 수 반환"""
    if not products:
        return 0
    conn = _conn()
    try:
        inserted = 0
        for p in products:
            try:
                conn.execute("""
                    INSERT OR REPLACE INTO products
                    (site_id, category_id, product_code, name, name_ko,
                     brand, brand_ko, price_jpy, link, img_url,
                     description, description_ko, sizes, detail_images,
                     original_price, discount_rate, in_stock, scraped_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    p.get("site_id", "xebio"),
                    p.get("category_id", ""),
                    p.get("product_code", ""),
                    p.get("name", ""),
                    p.get("name_ko", ""),
                    p.get("brand", ""),
                    p.get("brand_ko", ""),
                    p.get("price_jpy", 0),
                    p.get("link", ""),
                    p.get("img_url", ""),
                    p.get("description", ""),
                    p.get("description_ko", ""),
                    json.dumps(p.get("sizes", []), ensure_ascii=False),
                    json.dumps(p.get("detail_images", []), ensure_ascii=False),
                    p.get("original_price", 0),
                    p.get("discount_rate", 0),
                    1 if p.get("in_stock", True) else 0,
                    p.get("scraped_at", datetime.now().isoformat()),
                ))
                if conn.total_changes:
                    inserted += 1
            except sqlite3.IntegrityError:
                pass  # 중복 — 스킵
            except Exception as e:
                logger.debug(f"상품 저장 오류: {e}")
        conn.commit()
        logger.info(f"빅데이터 DB: {inserted}개 신규 저장 (총 {len(products)}개 중)")
        return inserted
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


def get_stats() -> dict:
    """통계 반환"""
    conn = _conn()
    try:
        total = conn.execute("SELECT COUNT(*) c FROM products").fetchone()["c"]

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
                    cafe_status="", page=1, per_page=50) -> dict:
    """상품 검색 (페이지네이션)"""
    conn = _conn()
    try:
        conditions = []
        params = []

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


def export_all(query="", site_id="", brand="") -> list:
    """전체 상품 내보내기 (필터 적용 가능)"""
    conn = _conn()
    try:
        conditions = []
        params = []
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


def get_total_count() -> int:
    """전체 상품 수"""
    conn = _conn()
    try:
        return conn.execute("SELECT COUNT(*) c FROM products").fetchone()["c"]
    finally:
        conn.close()


def get_unuploaded_products() -> list:
    """카페 업로드 안 된 상품 목록 반환 (빅데이터 DB에서)"""
    conn = _conn()
    try:
        rows = conn.execute("""
            SELECT * FROM products
            WHERE cafe_status = '' OR cafe_status IS NULL
            ORDER BY created_at DESC
        """).fetchall()
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
                "cafe_status": "",
                "scraped_at": r["scraped_at"],
                "created_at": r["created_at"],
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
