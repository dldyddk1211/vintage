"""
translator.py
googletrans 비공식 라이브러리를 이용한 일본어 → 한국어 번역
"""

import logging
import time

logger = logging.getLogger(__name__)

# 번역기 초기화
try:
    from googletrans import Translator
    _translator = Translator()
    TRANSLATE_AVAILABLE = True
    logger.info("✅ googletrans 초기화 성공")
except ImportError:
    TRANSLATE_AVAILABLE = False
    logger.warning("⚠️ googletrans 미설치 — pip install googletrans==4.0.0-rc1")

# 번역 캐시 (같은 텍스트 반복 번역 방지)
_cache = {}

# ── 커스텀 단어장 (구글 번역 전에 먼저 치환) ──────────
# 잘못 번역되는 단어를 여기에 추가하세요
# 형식: "일본어 단어": "원하는 한국어"
CUSTOM_DICT = {
    # 스포츠 브랜드
    "ナイキ"          : "나이키",
    "アディダス"       : "아디다스",
    "アシックス"       : "아식스",
    "ニューバランス"    : "뉴발란스",
    "プーマ"          : "푸마",
    "ミズノ"          : "미즈노",
    "アンダーアーマー" : "언더아머",
    "コンバース"       : "컨버스",
    "ヴァンズ"         : "반스",
    "リーボック"       : "리복",
    "DUARIG"          : "듀아리그",

    # 신발 종류
    "スニーカー"        : "스니커즈",
    "ランニングシューズ" : "러닝화",
    "トレーニングシューズ": "트레이닝화",
    "ジョギングシューズ" : "조깅화",
    "サッカースパイク"   : "축구화",
    "フットサルシューズ" : "풋살화",
    "バスケットボールシューズ": "농구화",
    "ウォーキングシューズ": "워킹화",

    # 의류
    "シャツ"    : "셔츠",
    "パンツ"    : "팬츠",
    "ジャケット" : "자켓",
    "ウェア"    : "웨어",
    "ソックス"  : "양말",
    "キャップ"  : "캡",

    # 자주 쓰이는 단어
    "メンズ"   : "남성",
    "レディース": "여성",
    "キッズ"   : "키즈",
    "ユニセックス": "유니섹스",
    "部活"     : "클럽활동",
}


def apply_custom_dict(text: str) -> str:
    """커스텀 단어장 치환 (구글 번역 전에 먼저 적용)"""
    for ja, ko in CUSTOM_DICT.items():
        text = text.replace(ja, ko)
    return text


def translate_ja_ko(text: str, retries: int = 3) -> str:
    """
    일본어 → 한국어 번역
    1. 커스텀 단어장 먼저 치환
    2. 나머지는 구글 번역
    - 캐시 적용 (같은 텍스트 재번역 방지)
    - 실패 시 재시도 3회
    - 번역 불가 시 원문 반환
    """
    if not text or not text.strip():
        return text

    # 캐시 확인
    if text in _cache:
        return _cache[text]

    # 1단계: 커스텀 단어장 치환
    pre_translated = apply_custom_dict(text)

    if not TRANSLATE_AVAILABLE:
        _cache[text] = pre_translated
        return pre_translated

    for attempt in range(retries):
        try:
            # 커스텀 단어장 치환 후 번역 (더 정확한 결과)
            result = _translator.translate(pre_translated, src="ja", dest="ko")
            translated = result.text.strip()
            _cache[text] = translated  # 캐시 저장
            return translated
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(1)  # 재시도 전 1초 대기
            else:
                logger.debug(f"번역 실패 (원문 반환): {e}")
                return text  # 실패 시 원문 반환

    return text


def translate_brand(brand: str) -> str:
    """브랜드명 번역 (이미 영문이면 그대로 반환)"""
    if not brand:
        return brand
    # 영문/숫자만 있으면 번역 불필요
    if brand.isascii():
        return brand
    return translate_ja_ko(brand)


def translate_batch(texts: list) -> list:
    """여러 텍스트 일괄 번역 (캐시 활용)"""
    return [translate_ja_ko(t) for t in texts]