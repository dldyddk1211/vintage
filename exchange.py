"""
exchange.py
구글 검색에서 엔화 → 원화 환율 스크래핑 (완전 무료)
"""

import json
import math
import os
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

# ── 가격 설정 파일 경로 ──────────────
from data_manager import get_path
_PRICE_CONFIG_PATH = os.path.join(get_path("db"), "price_config.json")


def _load_saved_price_config() -> dict:
    """저장된 가격 설정 로드"""
    if os.path.exists(_PRICE_CONFIG_PATH):
        try:
            with open(_PRICE_CONFIG_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def _save_price_config():
    """가격 설정을 파일에 저장"""
    os.makedirs(os.path.dirname(_PRICE_CONFIG_PATH), exist_ok=True)
    data = {
        "jp_fee_rate": _jp_fee_rate,
        "exchange_buy_markup": _exchange_buy_markup,
        "margin_pct": _margin_pct,
        "intl_shipping_krw": _intl_shipping_krw,
    }
    with open(_PRICE_CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    logger.info(f"💾 가격 설정 저장됨: {_PRICE_CONFIG_PATH}")


# ── 가격 계산 변수 (저장된 값 우선, 없으면 기본값) ──────────────
_saved = _load_saved_price_config()
_jp_fee_rate       = _saved.get("jp_fee_rate", JP_PLATFORM_FEE_RATE)
_exchange_buy_markup = _saved.get("exchange_buy_markup", 0.02)
_margin_pct        = _saved.get("margin_pct", 0.10)
_intl_shipping_krw = _saved.get("intl_shipping_krw", 15000)


def set_price_config(jp_fee=None, buy_markup=None, margin=None, shipping=None):
    """대시보드에서 가격 계산 변수 일괄 변경 + 파일 저장"""
    global _jp_fee_rate, _exchange_buy_markup, _margin_pct, _intl_shipping_krw
    if jp_fee is not None:
        _jp_fee_rate = jp_fee
    if buy_markup is not None:
        _exchange_buy_markup = buy_markup
    if margin is not None:
        _margin_pct = margin
    if shipping is not None:
        _intl_shipping_krw = int(shipping)
    _save_price_config()
    logger.info(f"가격 설정 변경: 수수료={_jp_fee_rate*100:.1f}% 환율추가={_exchange_buy_markup*100:.1f}% 마진={_margin_pct*100:.1f}% 배송={_intl_shipping_krw}원")


def get_price_config() -> dict:
    return {
        "jp_fee_pct"      : round(_jp_fee_rate * 100, 1),
        "buy_markup_pct"  : round(_exchange_buy_markup * 100, 1),
        "margin_pct"      : round(_margin_pct * 100, 1),
        "intl_shipping_krw": _intl_shipping_krw,
    }


# 하위 호환성 유지
def set_margin_rate(rate: float):
    global _margin_pct
    _margin_pct = rate - 1  # 1.1 → 0.1
    _save_price_config()
    logger.info(f"마진율 변경: {_margin_pct*100:.0f}%")

def get_margin_rate() -> float:
    return 1 + _margin_pct


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
    구매대행 가격 계산 (무료배송 기준)

    공식: price_jpy × (1+수수료) × 환율 × (1+환율추가) × (1+마진율) + 국제배송비
    백원 단위 올림
    """
    if rate is None:
        rate = get_jpy_to_krw_rate()

    price_raw = (
        price_jpy
        * (1 + _jp_fee_rate)
        * rate
        * (1 + _exchange_buy_markup)
        * (1 + _margin_pct)
        + _intl_shipping_krw
    )
    price_final = math.ceil(price_raw / 100) * 100

    return {
        "price_jpy"  : price_jpy,
        "rate"       : round(rate, 2),
        "price_final": price_final,
    }


def format_price(price: int) -> str:
    return f"{price:,}원"


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    r = calc_buying_price(9880)
    print(f"일본 현지가  : ¥{r['price_jpy']:,}")
    print(f"적용 환율    : {r['rate']}원/엔")
    print(f"최종 판매가  : {format_price(r['price_final'])}")
    # 계산식 확인: 9880 * 1.03 * rate * 1.02 * 1.1 + 15000 (백원 올림)
    import math
    raw = r['price_jpy'] * 1.03 * r['rate'] * 1.02 * 1.1 + 15000
    print(f"계산 원값    : {raw:,.1f}원  →  올림: {math.ceil(raw/100)*100:,}원")
    print(f"최종 판매가     : {format_price(r['price_final'])}")