"""
backfill_price_changes.py
NAS 백업 DB와 현재 DB를 비교하여 과거 가격 변동을 price_changes 테이블에 소급 기록
"""
import sqlite3
import os
from datetime import datetime

# 백업 DB 경로들 (시간 순)
BACKUP_DBS = [
    ("Z:/VOL1/파일공유/00 이용아/thone/srv/data/vintage/backups/products/products_20260418_0200.db", "2026-04-18 02:00:00"),
    ("Z:/VOL1/파일공유/00 이용아/thone/srv/data/vintage/backups/products/products_20260419_0200.db", "2026-04-19 02:00:00"),
]
CUR_DB = "C:/Users/pc/Documents/theone/srv/data/vintage/db/products.db"


def compare_and_backfill(old_db_path, old_ts, new_db_path):
    """두 DB 비교해서 가격 변경된 상품 찾아 price_changes에 기록"""
    if not os.path.exists(old_db_path):
        print(f"[SKIP] 백업 없음: {old_db_path}")
        return 0

    print(f"\n=== 비교: {os.path.basename(old_db_path)} → 현재 ===")
    conn_old = sqlite3.connect(old_db_path)
    conn_old.row_factory = sqlite3.Row
    conn_new = sqlite3.connect(new_db_path)
    conn_new.row_factory = sqlite3.Row

    try:
        # old DB에서 가격 읽기
        old_rows = conn_old.execute("""
            SELECT id, site_id, product_code, brand, name, price_jpy
            FROM products WHERE source_type='vintage' AND price_jpy > 0
        """).fetchall()
        old_prices = {r["id"]: dict(r) for r in old_rows}

        # new DB에서 비교
        inserted = 0
        skipped = 0
        for r in conn_new.execute("""
            SELECT id, site_id, product_code, brand, brand_ko, name, price_jpy,
                   internal_code, category_id
            FROM products WHERE source_type='vintage' AND price_jpy > 0
        """):
            pid = r["id"]
            if pid not in old_prices:
                continue
            old_price = old_prices[pid]["price_jpy"]
            new_price = r["price_jpy"]
            if old_price == new_price:
                continue

            # 이미 기록이 있는지 확인 (중복 방지)
            existing = conn_new.execute(
                "SELECT COUNT(*) FROM price_changes WHERE product_id=? AND old_price=? AND new_price=? AND updated_at=?",
                (pid, old_price, new_price, old_ts)
            ).fetchone()[0]
            if existing > 0:
                skipped += 1
                continue

            change_type = "가격인하" if new_price < old_price else "가격인상"
            try:
                conn_new.execute("""
                    INSERT INTO price_changes
                    (product_id, site_id, product_code, internal_code, brand_ko, category_id,
                     old_price, new_price, change_type, updated_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?)
                """, (
                    pid, r["site_id"] or "", r["product_code"] or "",
                    r["internal_code"] or "",
                    r["brand_ko"] or r["brand"] or "",
                    r["category_id"] or "",
                    old_price, new_price, change_type, old_ts,
                ))
                inserted += 1
            except Exception as e:
                print(f"  INSERT 실패 (id={pid}): {e}")

        conn_new.commit()
        print(f"  신규 기록: {inserted}건 | 중복 스킵: {skipped}건")
        return inserted
    finally:
        conn_old.close()
        conn_new.close()


def main():
    if not os.path.exists(CUR_DB):
        print(f"현재 DB 없음: {CUR_DB}")
        return

    total = 0
    for backup_path, ts in BACKUP_DBS:
        total += compare_and_backfill(backup_path, ts, CUR_DB)

    print(f"\n=== 완료: 총 {total}건 소급 기록 ===")

    # 최종 확인
    conn = sqlite3.connect(CUR_DB)
    cnt = conn.execute("SELECT COUNT(*) FROM price_changes").fetchone()[0]
    discount_cnt = conn.execute("SELECT COUNT(*) FROM price_changes WHERE change_type='가격인하'").fetchone()[0]
    increase_cnt = conn.execute("SELECT COUNT(*) FROM price_changes WHERE change_type='가격인상'").fetchone()[0]
    print(f"price_changes 총 {cnt}건 (인하 {discount_cnt}, 인상 {increase_cnt})")
    conn.close()


if __name__ == "__main__":
    main()
