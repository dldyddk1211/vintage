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

# 캐시 (하루 1회 자정 기준 갱신)
_cache = {"rate": None, "date": None}  # date: "2026-04-04" 형식

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


def _fetch_rate() -> float:
    """엔화 → 원화 환율 가져오기 (구글 → 수출입은행 → 백업 API)"""
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "ko-KR,ko;q=0.9",
    }

    # 1) 구글 환율 (1순위 — 실시간)
    try:
        url = "https://www.google.com/search?q=1+JPY+to+KRW&hl=ko"
        res = requests.get(url, headers=headers, timeout=10)
        html = res.text
        patterns = [
            r'class="DFlfde[^"]*"[^>]*>([\d.]+)</span>',
            r'"converted-amount"[^>]*>([\d.]+)',
            r'data-value="([\d.]+)"',
            r'1\s*일본\s*엔\s*=?\s*([\d.]+)',
            r'([\d]{1,2}\.[\d]{1,4})\s*대한민국\s*원',
            r'([\d]{1,2}\.[\d]{1,4})\s*KRW',
            r'([\d]{1,2}\.[\d]{2,4})\s*원',
        ]
        for pattern in patterns:
            match = re.search(pattern, html)
            if match:
                val = float(match.group(1))
                if 6.0 < val < 15.0:
                    logger.info(f"✅ 구글 환율: 1엔 = {val}원")
                    return val
    except Exception as e:
        logger.warning(f"⚠️ 구글 환율 실패: {e}")

    # 2) 한국수출입은행 매매기준율 (폴백)
    try:
        url = "https://www.koreaexim.go.kr/site/program/financial/exchangeJSON"
        params = {"authkey": "SAMPLE", "searchdate": datetime.now().strftime("%Y%m%d"), "data": "AP01"}
        res = requests.get(url, params=params, headers=headers, timeout=10)
        if res.status_code == 200 and res.text.strip():
            data = res.json()
            for item in data:
                if item.get("cur_unit") == "JPY(100)":
                    deal_bas_r = float(item["deal_bas_r"].replace(",", ""))
                    rate = deal_bas_r / 100
                    if 6.0 < rate < 15.0:
                        logger.info(f"✅ 수출입은행 환율: 100엔 = {deal_bas_r}원 → 1엔 = {rate:.4f}원")
                        return rate
    except Exception as e:
        logger.warning(f"⚠️ 수출입은행 환율 실패: {e}")

    # 3) 백업 API
    try:
        r = requests.get("https://api.exchangerate-api.com/v4/latest/JPY", timeout=5)
        rate = r.json()["rates"]["KRW"]
        logger.info(f"✅ 백업 API 환율: 1엔 = {rate}원")
        return rate
    except Exception as e2:
        logger.warning(f"⚠️ 백업 API 실패: {e2}")
        return 0


# 환율 저장 파일
_RATE_FILE = os.path.join(get_path("db"), "daily_rate.json")


def _load_daily_rate():
    """저장된 일일 환율 로드"""
    if os.path.exists(_RATE_FILE):
        try:
            with open(_RATE_FILE, "r") as f:
                data = json.load(f)
                _cache["rate"] = data.get("rate")
                _cache["date"] = data.get("date")
        except Exception:
            pass


def _save_daily_rate(rate, date_str):
    """일일 환율 저장"""
    os.makedirs(os.path.dirname(_RATE_FILE), exist_ok=True)
    with open(_RATE_FILE, "w") as f:
        json.dump({"rate": rate, "date": date_str}, f)


# 서버 시작 시 저장된 환율 로드
_load_daily_rate()


def get_jpy_to_krw_rate() -> float:
    """
    하루 1회 자정 기준 환율 조회.
    같은 날이면 캐시 반환, 날짜 바뀌면 새로 조회.
    """
    today = datetime.now().strftime("%Y-%m-%d")

    # 오늘 이미 조회했으면 캐시 반환
    if _cache["rate"] and _cache["date"] == today:
        return _cache["rate"]

    # 새 날짜 → 환율 조회
    rate = _fetch_rate()
    if rate > 0:
        prev_rate = _cache.get("rate")
        _cache["rate"] = rate
        _cache["date"] = today
        _save_daily_rate(rate, today)
        logger.info(f"📅 일일 환율 갱신: 1엔 = {rate}원 ({today})")
        # 변동 알림 (이전 환율 대비 ±3% 이상)
        if prev_rate and abs(rate - prev_rate) / prev_rate > 0.03:
            try:
                from notifier import send_telegram
                change = ((rate - prev_rate) / prev_rate) * 100
                arrow = "📈" if change > 0 else "📉"
                send_telegram(
                    f"{arrow} <b>환율 변동 알림</b>\n"
                    f"💱 1엔 = {prev_rate:.2f}원 → {rate:.2f}원\n"
                    f"📊 변동: {change:+.2f}%\n"
                    f"📅 {today} 기준"
                )
            except Exception:
                pass
        return rate

    # 조회 실패 → 기존 캐시 유지
    return _cache["rate"] or 9.0


def get_cached_rate() -> float:
    """캐시된 환율 반환 (없으면 조회)"""
    if _cache["rate"]:
        # 날짜 확인
        today = datetime.now().strftime("%Y-%m-%d")
        if _cache["date"] != today:
            return get_jpy_to_krw_rate()
        return _cache["rate"]
    return get_jpy_to_krw_rate()


def refresh_daily_rate():
    """자정 스케줄러용 — 강제 환율 갱신"""
    _cache["date"] = None  # 캐시 무효화
    rate = get_jpy_to_krw_rate()
    logger.info(f"🔄 자정 환율 갱신 완료: 1엔 = {rate}원")
    return rate


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