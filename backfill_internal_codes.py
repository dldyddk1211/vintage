"""
backfill_internal_codes.py
빈티지 상품 중 internal_code 없는 상품에 일괄 발급
- 수집일(created_at 또는 scraped_at) 기준으로 날짜 부여
- 형식: No.S-YYMMDD-NNNN
"""
import sqlite3
from datetime import datetime
from collections import defaultdict

DB = "C:/Users/pc/Documents/theone/srv/data/vintage/db/products.db"


def main():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row

    rows = conn.execute("""
        SELECT id, created_at, scraped_at, site_id
        FROM products
        WHERE source_type='vintage'
          AND (internal_code IS NULL OR internal_code = '')
          AND site_id='2ndstreet'
        ORDER BY created_at, id
    """).fetchall()
    print(f"internal_code 없는 2ndstreet 빈티지 상품: {len(rows):,}개")

    if not rows:
        print("업데이트할 상품 없음")
        conn.close()
        return

    # 날짜별 기존 최대 번호 조회
    existing_nums = defaultdict(int)
    ex_rows = conn.execute(
        "SELECT internal_code FROM products WHERE internal_code LIKE 'No.S-%'"
    ).fetchall()
    for r in ex_rows:
        try:
            parts = r["internal_code"].split("-")
            date_key = parts[1]
            num = int(parts[-1])
            if num > existing_nums[date_key]:
                existing_nums[date_key] = num
        except Exception:
            pass

    assigned = 0
    for row in rows:
        date_src = row["created_at"] or row["scraped_at"] or ""
        if date_src:
            try:
                dt = datetime.strptime(date_src[:10], "%Y-%m-%d")
            except Exception:
                dt = datetime.now()
        else:
            dt = datetime.now()
        yymmdd = dt.strftime("%y%m%d")
        existing_nums[yymmdd] += 1
        new_code = f"No.S-{yymmdd}-{existing_nums[yymmdd]:04d}"
        try:
            conn.execute(
                "UPDATE products SET internal_code=? WHERE id=?",
                (new_code, row["id"])
            )
            assigned += 1
            if assigned % 2000 == 0:
                conn.commit()
                print(f"  진행: {assigned:,}/{len(rows):,}")
        except Exception as e:
            print(f"  실패 id={row['id']}: {e}")

    conn.commit()
    print(f"\n총 {assigned:,}개 internal_code 발급 완료")

    total = conn.execute("SELECT COUNT(*) FROM products WHERE source_type='vintage'").fetchone()[0]
    has_code = conn.execute("SELECT COUNT(*) FROM products WHERE source_type='vintage' AND internal_code != ''").fetchone()[0]
    print(f"전체 vintage: {total:,} / internal_code 보유: {has_code:,} ({has_code*100//total if total else 0}%)")
    conn.close()


if __name__ == "__main__":
    main()
