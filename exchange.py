"""
exchange.py
구글 검색에서 엔화 → 원화 환율 스크래핑 (완전 무료)
"""

import requests
import logging
from datetime import datetime

from config import MARGIN_RATE, SHIPPING_FEE

logger = logging.getLogger(__name__)

# 캐시 (1시간마다 갱신)
_cache = {"rate": None, "time": None}
CACHE_MINUTES = 60  # 캐시 유지 시간 (분)


def get_jpy_to_krw_rate() -> float:
    """
    구글 검색에서 실시간 엔→원 환율 가져오기
    검색어: JPY to KRW
    완전 무료, API 키 불필요

    Returns: 1엔 = ? 원  (예: 9.23)
    """
    now = datetime.now()

    # 캐시가 유효하면 바로 반환
    if _cache["rate"] and _cache["time"]:
        diff = (now - _cache["time"]).seconds / 60
        if diff < CACHE_MINUTES:
            logger.info(f"환율 캐시 사용: 1엔 = {_cache['rate']}원 ({int(diff)}분 전 조회)")
            return _cache["rate"]

    try:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "ko-KR,ko;q=0.9",
        }

        # 구글에 JPY to KRW 검색
        url = "https://www.google.com/search?q=JPY+to+KRW&hl=ko"
        res = requests.get(url, headers=headers, timeout=10)
        html = res.text

        # 구글 환율 파싱 (예: "9.23 대한민국 원")
        import re

        # 방법 1: 구글 환율 결과박스에서 숫자 추출
        patterns = [
            r'class="DFlfde[^"]*"[^>]*>([\d.]+)</span>',   # 구글 환율 박스
            r'"converted-amount"[^>]*>([\d.]+)',
            r'1\s*일본\s*엔\s*=\s*([\d.]+)',
            r'([\d]{1,2}\.[\d]{1,4})\s*대한민국\s*원',
            r'([\d]{1,2}\.[\d]{1,4})\s*KRW',
        ]

        rate = None
        for pattern in patterns:
            match = re.search(pattern, html)
            if match:
                val = float(match.group(1))
                # 환율 범위 체크 (1엔 = 6~15원 사이가 정상)
                if 6.0 < val < 15.0:
                    rate = val
                    break

        if rate:
            _cache["rate"] = rate
            _cache["time"] = now
            logger.info(f"✅ 구글 환율 조회 성공: 1엔 = {rate}원")
            return rate
        else:
            raise ValueError("환율 파싱 실패 — 구글 HTML 구조 변경됐을 수 있음")

    except Exception as e:
        logger.warning(f"⚠️ 구글 환율 조회 실패: {e}")
        logger.warning("   → 백업 API 시도 중...")

        # 백업: exchangerate-api (무료 플랜 1500건/월)
        try:
            r = requests.get("https://api.exchangerate-api.com/v4/latest/JPY", timeout=5)
            rate = r.json()["rates"]["KRW"]
            _cache["rate"] = rate
            _cache["time"] = now
            logger.info(f"✅ 백업 API 환율: 1엔 = {rate}원")
            return rate
        except Exception as e2:
            logger.warning(f"⚠️ 백업 API도 실패: {e2} → 기본값 9.0 사용")
            return _cache["rate"] or 9.0  # 캐시도 없으면 기본값


def calc_buying_price(price_jpy: int) -> dict:
    """
    구매대행 가격 계산

    Returns:
        price_jpy       : 일본 현지가 (엔)
        rate            : 적용 환율
        price_krw_raw   : 환율 적용 후 원화
        price_krw_margin: 마진 적용 후 원화
        price_final     : 배송비 포함 최종가
        shipping_fee    : 배송비
    """
    rate = get_jpy_to_krw_rate()
    raw_krw    = int(price_jpy * rate)
    margin_krw = int(raw_krw * MARGIN_RATE)
    final      = margin_krw + SHIPPING_FEE

    return {
        "price_jpy"       : price_jpy,
        "rate"            : round(rate, 2),
        "price_krw_raw"   : raw_krw,
        "price_krw_margin": margin_krw,
        "price_final"     : final,
        "shipping_fee"    : SHIPPING_FEE,
    }


def format_price(price: int) -> str:
    return f"{price:,}원"


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    r = calc_buying_price(12000)
    print(f"일본 현지가 : ¥{r['price_jpy']:,}")
    print(f"적용 환율   : 1엔 = {r['rate']}원")
    print(f"환율 적용   : {format_price(r['price_krw_raw'])}")
    print(f"마진 적용   : {format_price(r['price_krw_margin'])}")
    print(f"최종 가격   : {format_price(r['price_final'])} (배송비 포함)")