"""
ai_product_enrich.py
AI 상품명 + 대표 태그 생성 (이미지 Vision 기반)

기능:
 - 상품 이미지 1~2장 + 기존 상품명 → 쇼핑몰용 상품명 + 대표 태그 3개
 - Gemini Vision 우선 → OpenAI Vision 폴백
 - 결과 DB 저장 (shop_name, ai_tags, ai_analyzed_at)
"""

import base64
import json
import logging
import re
from datetime import datetime

import requests

logger = logging.getLogger(__name__)


def _img_to_base64(url: str, max_bytes: int = 2_000_000) -> tuple:
    """이미지 URL → base64 + mime_type (CDN hotlink 방지 우회)"""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Accept": "image/webp,image/apng,image/*,*/*;q=0.8",
        "Accept-Language": "ja,en;q=0.9",
        "Referer": "https://www.2ndstreet.jp/",
    }
    try:
        r = requests.get(url, timeout=10, stream=True, headers=headers)
        if r.status_code != 200:
            logger.warning(f"이미지 HTTP {r.status_code}: {url[:80]}")
            return None, None
        data = r.content
        if len(data) > max_bytes:
            data = data[:max_bytes]
        mime = r.headers.get("content-type", "image/jpeg").split(";")[0].strip()
        if "jpeg" not in mime and "png" not in mime and "webp" not in mime:
            mime = "image/jpeg"
        return base64.b64encode(data).decode(), mime
    except Exception as e:
        logger.warning(f"이미지 다운로드 실패 ({url[:60]}...): {e}")
        return None, None


def _build_prompt(name: str, brand: str, category: str) -> str:
    return f"""당신은 한국 명품 쇼핑몰의 상품명 전문가입니다.
아래 상품의 이미지와 기존 상품명을 바탕으로, **한국 쇼핑몰에서 바로 사용할 수 있는 세련된 상품명**과 **대표 검색 키워드 태그 3개**를 생성하세요.

[기존 정보]
- 브랜드: {brand or '미상'}
- 카테고리: {category or '미상'}
- 원본 상품명: {name or '미상'}

[중요 원칙]
- 원본 상품명에 **사이즈(S/M/L, 44/48/52 등), 색상(BLK/WHT/BRW/NVY 등), 소재(가죽/나일론/캔버스 등), 품번(0747060 같은 숫자)**이 있으면 **반드시 유지**하세요.
- 이미지를 분석해서 정확한 **모델명/라인명**을 파악해 추가하세요 (예: 루이비통 키폴55, 구찌 마몬트, 셀린느 클래식 등).
- 모델명은 이미지로 확신할 수 있을 때만 추가하세요. 확신 없으면 빼세요.

[출력 규칙]
1. shop_name 조합 순서: `브랜드 + 모델명(알면) + 원본 상세정보(사이즈/컬러/소재/품번)`
   예시: "루이비통 키폴55 보스턴백 모노그램 BRW" / "Salvatore Ferragamo 테일러드 재킷 50 울 NVY 스트라이프 0747060"
2. 태그 3개는 한국 검색 시 자주 쓰이는 표현으로 공백/특수문자 없이 (예: "루이비통키폴", "키폴55", "루이비통보스턴백")
3. 모델 확신 불가 시 카테고리 기반 일반 태그로.

[출력 형식 - JSON만, 다른 설명 금지]
{{
  "shop_name": "한국 쇼핑몰용 상품명 (최대 60자, 원본의 사이즈/컬러/소재/품번 유지)",
  "tags": ["태그1", "태그2", "태그3"]
}}"""


def _parse_json_response(text: str) -> dict:
    """AI 응답에서 JSON 추출"""
    if not text:
        return {}
    cleaned = text.strip()
    # ```json ... ``` 제거
    if "```" in cleaned:
        if "```json" in cleaned:
            cleaned = cleaned.split("```json")[-1].split("```")[0].strip()
        else:
            cleaned = cleaned.split("```")[1].split("```")[0].strip()
    # { } 구간 추출
    m = re.search(r'\{.*\}', cleaned, re.DOTALL)
    if m:
        cleaned = m.group(0)
    try:
        return json.loads(cleaned)
    except Exception as e:
        logger.warning(f"JSON 파싱 실패: {e} | raw={text[:200]}")
        return {}


def enrich_product(name: str, brand: str, category: str,
                   img_urls: list, max_images: int = 2) -> dict:
    """AI로 상품명 + 태그 생성

    Returns: {"shop_name": str, "tags": list[str]} or {} on failure
    """
    from post_generator import get_ai_config, _get_gemini, _get_openai

    config = get_ai_config()
    prompt = _build_prompt(name, brand, category)

    # 이미지 다운로드 (최대 2장)
    img_parts = []
    for url in (img_urls or [])[:max_images]:
        if not url:
            continue
        b64, mime = _img_to_base64(url)
        if b64:
            img_parts.append((b64, mime))
    if not img_parts:
        logger.warning("이미지 없음 — 건너뜀")
        return {}

    result_text = ""

    # 1) Gemini Vision 우선
    if config.get("gemini_key"):
        try:
            client = _get_gemini()
            from google.genai import types
            contents = [prompt]
            for b64, mime in img_parts:
                contents.append(types.Part.from_bytes(
                    data=base64.b64decode(b64), mime_type=mime))
            resp = client.models.generate_content(
                model="gemini-2.5-flash",
                contents=contents,
            )
            result_text = (resp.text or "").strip()
        except Exception as e:
            logger.warning(f"Gemini 실패: {e}")

    # 2) OpenAI Vision 폴백
    if not result_text and config.get("openai_key"):
        try:
            client = _get_openai()
            content = [{"type": "text", "text": prompt}]
            for b64, mime in img_parts:
                content.append({
                    "type": "image_url",
                    "image_url": {
                        "url": f"data:{mime};base64,{b64}",
                        "detail": "low"
                    }
                })
            resp = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[{"role": "user", "content": content}],
                max_tokens=400,
                timeout=30.0,
            )
            result_text = (resp.choices[0].message.content or "").strip()
        except Exception as e:
            logger.warning(f"OpenAI 실패: {e}")

    if not result_text:
        return {}

    parsed = _parse_json_response(result_text)
    shop_name = (parsed.get("shop_name") or "").strip()
    tags = parsed.get("tags") or []
    # 태그 정규화
    clean_tags = []
    for t in tags:
        if not isinstance(t, str):
            continue
        t = t.strip().replace(" ", "").replace("·", "").replace("/", "")
        if t and t not in clean_tags:
            clean_tags.append(t)
        if len(clean_tags) >= 3:
            break

    if not shop_name and not clean_tags:
        return {}

    return {"shop_name": shop_name, "tags": clean_tags}


def _db_connect_safe():
    """DB 연결 (락 대기 30초 + WAL 모드)"""
    import sqlite3 as _sq
    from product_db import _DB_PATH
    conn = _sq.connect(_DB_PATH, timeout=30, check_same_thread=False)
    conn.row_factory = _sq.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
    except Exception:
        pass
    return conn


def enrich_product_by_id(product_id: int) -> dict:
    """DB 상품 1건 AI 분석 + 저장"""
    import time as _t

    # 1) READ: 상품 정보 조회 (연결은 짧게 사용)
    conn = _db_connect_safe()
    try:
        row = conn.execute(
            "SELECT id, brand, brand_ko, name, name_ko, subcategory, img_url, detail_images FROM products WHERE id=?",
            (product_id,)
        ).fetchone()
        if not row:
            return {"ok": False, "message": "상품 없음"}
        # 필요한 데이터만 추출 후 연결 즉시 종료 (DB 락 해소)
        img_urls = []
        try:
            detail = json.loads(row["detail_images"] or "[]")
            if isinstance(detail, list):
                img_urls.extend([u for u in detail if u][:2])
        except Exception:
            pass
        if row["img_url"] and row["img_url"] not in img_urls:
            img_urls.insert(0, row["img_url"].replace("_tn.jpg", ".jpg"))
        img_urls = [u for u in img_urls if u][:2]
        name = row["name_ko"] or row["name"] or ""
        brand = row["brand_ko"] or row["brand"] or ""
        category = row["subcategory"] or ""
    finally:
        conn.close()

    # 2) AI 호출 (외부 API — DB 락과 무관)
    short_name = f"{brand} {name[:30]}" if brand else name[:40]
    result = enrich_product(name, brand, category, img_urls)
    if not result:
        return {"ok": False, "message": f"AI 분석 실패 | {short_name}"}

    # 3) WRITE: UPDATE (최대 5회 재시도, locked 시 대기)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    shop_name = result.get("shop_name", "")
    tags_json = json.dumps(result.get("tags", []), ensure_ascii=False)

    for attempt in range(5):
        try:
            conn = _db_connect_safe()
            try:
                conn.execute(
                    "UPDATE products SET shop_name=?, ai_tags=?, ai_analyzed_at=? WHERE id=?",
                    (shop_name, tags_json, now, product_id)
                )
                conn.commit()
                return {"ok": True, "id": product_id,
                        "original_name": short_name,
                        "shop_name": shop_name,
                        "tags": result.get("tags", [])}
            finally:
                conn.close()
        except Exception as e:
            msg = str(e)
            if "locked" in msg.lower() and attempt < 4:
                _t.sleep(1.5 * (attempt + 1))  # 1.5s, 3s, 4.5s, 6s
                continue
            return {"ok": False, "message": f"DB 쓰기 실패: {msg[:60]} | {short_name}"}
    return {"ok": False, "message": f"DB 락 5회 재시도 실패 | {short_name}"}


# 배치 중지 플래그
_STOP_FLAG = {"stop": False}


def request_stop():
    """배치 중지 요청"""
    _STOP_FLAG["stop"] = True


def reset_stop():
    """중지 플래그 리셋"""
    _STOP_FLAG["stop"] = False


def is_stop_requested() -> bool:
    return _STOP_FLAG["stop"]
