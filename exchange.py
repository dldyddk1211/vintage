"""
exchange.py
구글 검색에서 엔화 → 원화 환율 스크래핑 (완전 무료)
"""

import re
import requests
import logging
from datetime import datetime

from config import (
    MARGIN_RATE,
    JP_FREE_SHIPPING_THRESHOLD, JP_DOMESTIC_SHIPPING,
    JP_PLATFORM_FEE_RATE, JP_INTL_SHIPPING,
    EXCHANGE_RATE_MARKUP,
)

logger = logging.getLogger(__name__)

# 캐시 (2시간마다 갱신)
_cache = {"rate": None, "time": None}
CACHE_MINUTES = 120

# 대시보드에서 동적으로 변경 가능한 마진율
_current_margin = MARGIN_RATE


def set_margin_rate(rate: float):
    """대시보드에서 마진율 변경"""
    global _current_margin
    _current_margin = rate
    logger.info(f"마진율 변경: {rate} ({(rate-1)*100:.0f}%)")


def get_margin_rate() -> float:
    return _current_margin


def get_jpy_to_krw_rate() -> float:
    """
    구글 검색에서 실시간 엔→원 환율 가져오기 (2시간 캐시)
    """
    now = datetime.now()

    # 캐시 유효하면 바로 반환
    if _cache["rate"] and _cache["time"]:
        diff = (now - _cache["time"]).total_seconds() / 60
        if diff < CACHE_MINUTES:
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
        url = "https://www.google.com/search?q=JPY+to+KRW&hl=ko"
        res = requests.get(url, headers=headers, timeout=10)
        html = res.text

        patterns = [
            r'class="DFlfde[^"]*"[^>]*>([\d.]+)</span>',
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
                if 6.0 < val < 15.0:
                    rate = val
                    break

        if rate:
            _cache["rate"] = rate
            _cache["time"] = now
            logger.info(f"✅ 구글 환율 조회 성공: 1엔 = {rate}원")
            return rate
        else:
            raise ValueError("파싱 실패")

    except Exception as e:
        logger.warning(f"⚠️ 구글 환율 조회 실패: {e} → 백업 API 시도")
        try:
            r = requests.get("https://api.exchangerate-api.com/v4/latest/JPY", timeout=5)
            rate = r.json()["rates"]["KRW"]
            _cache["rate"] = rate
            _cache["time"] = now
            logger.info(f"✅ 백업 API 환율: 1엔 = {rate}원")
            return rate
        except Exception as e2:
            logger.warning(f"⚠️ 백업 API 실패: {e2} → 기본값 사용")
            return _cache["rate"] or 9.0


def get_cached_rate() -> float:
    """캐시된 환율 반환 (없으면 조회) - status API 전용"""
    if _cache["rate"]:
        return _cache["rate"]
    return get_jpy_to_krw_rate()


def calc_buying_price(price_jpy: int, rate: float = None, margin: float = None) -> dict:
    """
    구매대행 가격 계산 (정확한 원가 공식)

    [일본 원가 계산 - 엔화]
    1. 일본 내 배송료: 상품가 3980엔 이상 무료 / 이하 550엔
    2. 일본 업체 수수료: 3%
    3. 국제 배송비: 1500엔 고정
    → 일본 총 원가 (엔)

    [환율 적용]
    실시간 환율 × 1.5% (송금 시 환율 손실 반영)

    [판매가]
    원화 원가 × 마진율
    """
    if rate is None:
        rate = get_jpy_to_krw_rate()
    if margin is None:
        margin = _current_margin

    # ── 1. 일본 내 배송료 ──────────────────────
    domestic_shipping = 0 if price_jpy >= JP_FREE_SHIPPING_THRESHOLD else JP_DOMESTIC_SHIPPING

    # ── 2. 일본 업체 수수료 (3%) ───────────────
    platform_fee = int(price_jpy * JP_PLATFORM_FEE_RATE)

    # ── 3. 일본 총 원가 (엔화) ─────────────────
    total_jpy = price_jpy + domestic_shipping + platform_fee + JP_INTL_SHIPPING

    # ── 4. 송금 환율 적용 (실시간 + 1.5%) ──────
    remit_rate = rate * EXCHANGE_RATE_MARKUP

    # ── 5. 원화 원가 ───────────────────────────
    cost_krw = int(total_jpy * remit_rate)

    # ── 6. 판매가 (마진 적용) ──────────────────
    price_final = int(cost_krw * margin)

    return {
        "price_jpy"         : price_jpy,
        "domestic_shipping" : domestic_shipping,   # 일본 내 배송료 (엔)
        "platform_fee"      : platform_fee,         # 수수료 (엔)
        "intl_shipping"     : JP_INTL_SHIPPING,     # 국제 배송비 (엔)
        "total_jpy"         : total_jpy,            # 일본 총 원가 (엔)
        "rate"              : round(rate, 2),        # 실시간 환율
        "remit_rate"        : round(remit_rate, 2), # 송금 환율
        "cost_krw"          : cost_krw,             # 원화 원가
        "margin"            : margin,               # 마진율
        "price_final"       : price_final,          # 최종 판매가
    }


def format_price(price: int) -> str:
    return f"{price:,}원"


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    r = calc_buying_price(9980)
    print(f"일본 현지가     : ¥{r['price_jpy']:,}")
    print(f"일본 내 배송료  : ¥{r['domestic_shipping']:,}")
    print(f"수수료(3%)      : ¥{r['platform_fee']:,}")
    print(f"국제 배송비     : ¥{r['intl_shipping']:,}")
    print(f"일본 총 원가    : ¥{r['total_jpy']:,}")
    print(f"실시간 환율     : {r['rate']}원")
    print(f"송금 환율(+1.5%): {r['remit_rate']}원")
    print(f"원화 원가       : {format_price(r['cost_krw'])}")
    print(f"마진율          : {r['margin']} ({(r['margin']-1)*100:.0f}%)")
    print(f"최종 판매가     : {format_price(r['price_final'])}")