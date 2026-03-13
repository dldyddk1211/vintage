"""
post_generator.py
AI(Gemini/Claude)를 이용한 일본 구매대행 카페 게시글 자동 생성
"""

import re
import os
import json
import random
import logging
from config import ANTHROPIC_API_KEY, GEMINI_API_KEY, AI_PROVIDER
from exchange import format_price
from data_manager import get_path

logger = logging.getLogger(__name__)

# ── 사용자 번역 사전 (자동 저장) ─────────────
_USER_DICT_PATH = os.path.join(get_path("db"), "translation_dict.json")


def _load_user_dict() -> dict:
    """저장된 사용자 번역 사전 로드"""
    if os.path.exists(_USER_DICT_PATH):
        try:
            with open(_USER_DICT_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def _save_user_dict(d: dict):
    """사용자 번역 사전 파일 저장"""
    os.makedirs(os.path.dirname(_USER_DICT_PATH), exist_ok=True)
    with open(_USER_DICT_PATH, "w", encoding="utf-8") as f:
        json.dump(d, f, ensure_ascii=False, indent=2)


# 시작 시 사용자 사전 로드
_user_dict = _load_user_dict()

NAVER_FORM_URL = "https://naver.me/F2nuqgnV"

# ── 런타임 설정 (대시보드에서 변경 가능) ──────
_ai_config = {
    "provider": AI_PROVIDER,
    "gemini_key": GEMINI_API_KEY,
    "claude_key": ANTHROPIC_API_KEY,
}

# ── AI 클라이언트 (지연 초기화) ──────────────
_gemini_client = None
_claude_client = None


def set_ai_config(provider=None, gemini_key=None, claude_key=None):
    """대시보드에서 AI 설정 변경"""
    global _gemini_client, _claude_client
    if provider is not None:
        _ai_config["provider"] = provider.lower()
    if gemini_key is not None:
        _ai_config["gemini_key"] = gemini_key
        _gemini_client = None  # 클라이언트 재생성
    if claude_key is not None:
        _ai_config["claude_key"] = claude_key
        _claude_client = None
    logger.info(f"AI 설정 변경: provider={_ai_config['provider']}")


def get_ai_config() -> dict:
    """현재 AI 설정 반환 (키는 마스킹)"""
    def mask(key):
        if not key:
            return ""
        return key[:8] + "..." + key[-4:] if len(key) > 12 else "****"
    return {
        "provider": _ai_config["provider"],
        "gemini_key": mask(_ai_config["gemini_key"]),
        "claude_key": mask(_ai_config["claude_key"]),
        "gemini_set": bool(_ai_config["gemini_key"]),
        "claude_set": bool(_ai_config["claude_key"]),
    }


def _get_gemini():
    global _gemini_client
    if _gemini_client is None:
        from google import genai
        _gemini_client = genai.Client(api_key=_ai_config["gemini_key"])
    return _gemini_client


def _get_claude():
    global _claude_client
    if _claude_client is None:
        import anthropic
        _claude_client = anthropic.Anthropic(api_key=_ai_config["claude_key"])
    return _claude_client


# ── 카타카나 → 한국어 매핑 ──────────────────
_KATAKANA_MAP = {
    # ── 나이키 모델명 ──
    "ズーム": "줌", "フライ": "플라이", "ボメロ": "보메로",
    "ペガサス": "페가수스", "エア": "에어", "プラス": "플러스",
    "マックス": "맥스", "ヴェイパー": "베이퍼", "アルファ": "알파",
    "インフィニティ": "인피니티", "リアクト": "리액트", "フリー": "프리",
    "ワッフル": "와플", "コルテッツ": "코르테즈", "ダンク": "덩크",
    "フォース": "포스", "ジョーダン": "조던", "ブレーザー": "블레이저",
    # ── 아식스 모델명 ──
    "マジックスピード": "매직스피드", "メタスピード": "메타스피드",
    "ノヴァブラスト": "노바블래스트", "ノヴァ": "노바",
    "ゲルカヤノ": "겔카야노", "ゲルニンバス": "겔님버스",
    "ゲルキュムラス": "겔큐뮬러스", "ゲル": "겔",
    "カヤノ": "카야노", "ニンバス": "님버스", "キュムラス": "큐뮬러스",
    "グライドライド": "글라이드라이드", "エボライド": "에보라이드",
    "ライトレーサー": "라이트레이서", "ターサー": "타서",
    "スーパーブラスト": "슈퍼블래스트", "ブラスト": "블래스트",
    "スピード": "스피드", "マジック": "매직",
    # ── 색상 ──
    "ホワイト": "화이트", "ブラック": "블랙", "ブルー": "블루",
    "レッド": "레드", "グリーン": "그린", "ネイビー": "네이비",
    "グレー": "그레이", "ピンク": "핑크", "イエロー": "옐로",
    "オレンジ": "오렌지", "パープル": "퍼플", "ベージュ": "베이지",
    "シルバー": "실버", "ゴールド": "골드",
    # ── 일반 / 카테고리 ──
    "ランニングシューズ": "런닝화", "トレーニングシューズ": "트레이닝화",
    "スニーカー": "스니커즈", "ランニング": "런닝", "トレーニング": "트레이닝",
    "ウォーキング": "워킹", "バスケットボール": "농구", "サッカー": "축구",
    "テニス": "테니스", "ゴルフ": "골프", "シューズ": "슈즈",
    "クラブ活動": "클럽활동", "ジョギング": "조깅",
    "マラソン": "마라톤", "フィットネス": "피트니스",
}


def _translate_katakana(name: str) -> str:
    """카타카나 단어를 한국어로 변환 (내장 사전 + 사용자 사전)"""
    # 사용자 사전 먼저 적용 (우선순위 높음)
    for ja, ko in _user_dict.items():
        name = name.replace(ja, ko)
    # 내장 사전 적용
    for ja, ko in _KATAKANA_MAP.items():
        name = name.replace(ja, ko)
    return name


# 한국어 표기 통일 (스크래퍼 번역 결과 보정)
_KOREAN_FIX = {
    "러닝화": "런닝화",
    "러닝": "런닝",
}


# ── 인트로 템플릿 (랜덤 선택) ────────────────
_INTRO_TEMPLATES = [
    ("👟 오늘 소개 드릴 제품은 '{name}' 입니다.",
     "👉 일본 현지에서 직접 구매하여 발송되는 정품 구매대행 상품입니다."),
    ("🇯🇵 일본 직배송 구매대행 '{name}'",
     "👉 일본 공식 유통 정품을 직접 구매하여 배송해 드립니다."),
    ("✈️ 일본에서 직접 가져옵니다! '{name}'",
     "👉 일본 현지 정품을 직접 구매 후 배송해 드리는 구매대행 상품입니다."),
    ("🏃 러너들 주목! '{name}'",
     "👉 일본에서 직접 구매하는 100% 정품 구매대행 상품입니다."),
    ("🔥 한정 사이즈 안내 '{name}'",
     "👉 일본 현지 매장에서 직접 구매하여 발송하는 정품 상품입니다."),
    ("💙 오늘의 추천 상품 '{name}'",
     "👉 일본 현지에서 직접 구매 후 국내로 배송해 드리는 구매대행 상품입니다."),
    ("🎌 일본 직구 소식 '{name}'",
     "👉 일본 공식 판매처에서 직접 구매하여 배송해 드립니다."),
    ("⚡ 신상 입고 안내 '{name}'",
     "👉 일본 정식 유통 정품을 직접 구매하여 국내 배송해 드립니다."),
    ("🌟 이번 주 추천템 '{name}'",
     "👉 일본 현지 직구 방식으로 진행되는 100% 정품 구매대행 상품입니다."),
    ("🛒 구매대행 신상 안내 '{name}'",
     "👉 일본에서 직접 구매 후 발송하는 현지 정품 구매대행 상품입니다."),
    ("🏆 일본 베스트셀러 입고 '{name}'",
     "👉 일본 현지 매장에서 검증된 정품만을 직접 구매하여 보내드립니다."),
    ("📦 현지 직송 알림 '{name}'",
     "👉 주문 즉시 일본 공식 판매처에서 바잉을 시작하는 100% 정품 상품입니다."),
    ("🎁 나를 위한 러닝 선물 '{name}'",
     "👉 일본 직구로만 만날 수 있는 특별한 제품을 현지에서 직접 보내드립니다."),
    ("🥇 러닝 퀄리티 업그레이드 '{name}'",
     "👉 일본 현지 정식 유통 제품으로 진행하는 프리미엄 구매대행 상품입니다."),
    ("📣 사이즈 긴급 확보 '{name}'",
     "👉 일본 매장 내 극소량 재고를 직접 확인하여 배송해 드리는 정품입니다."),
    ("🌍 글로벌 트렌드 아이템 '{name}'",
     "👉 일본 내 인기 스포츠 아이템을 현지 바잉을 통해 가장 빠르게 전달합니다."),
    ("✅ 안심 구매대행 안내 '{name}'",
     "👉 일본 공식 매장 정품임을 보장하며, 현지에서 꼼꼼히 검수 후 발송됩니다."),
    ("👟 편안한 발걸음의 시작 '{name}'",
     "👉 일본 현지 정식 판매 제품을 직구로 안전하게 만나보세요."),
    ("✨ 오늘의 픽(Pick)! '{name}'",
     "👉 일본에서 직접 셀렉한 고퀄리티 스포츠 정품 구매대행 상품입니다."),
    ("🛫 일본 현지 직배송 뉴스 '{name}'",
     "👉 불필요한 유통 과정 없이 일본 현지에서 고객님께 바로 전달되는 정품입니다."),
]


def _pick_intro(product_name: str) -> tuple:
    """인트로 템플릿 랜덤 선택 후 상품명 삽입"""
    line1, line2 = random.choice(_INTRO_TEMPLATES)
    return line1.format(name=product_name), line2


# 상품명에서 제거할 카테고리 키워드
_CATEGORY_REMOVE = [
    "런닝화", "트레이닝화", "워킹화", "조깅화", "농구화", "축구화",
    "테니스화", "골프화", "스니커즈", "슈즈",
    "클럽활동", "부활동",
    "ランニングシューズ", "トレーニングシューズ", "ウォーキングシューズ",
    "スニーカー", "シューズ", "クラブ活動", "部活動",
]


def _clean_name(name: str, product_code: str = "") -> str:
    """상품명 정리: 성별 접두사·품번·카테고리 제거, 카타카나 번역, 한국어 표기 통일"""
    # 성별 접두사 제거: (남성), (남성、여성), (メンズ) 등
    name = re.sub(
        r'^\s*\((?:남성|여성|유니섹스|남녀공용|키즈|주니어|メンズ|ウィメンズ|レディース)'
        r'(?:[、,/\s]+(?:남성|여성|유니섹스|남녀공용|키즈|주니어|メンズ|ウィメンズ|レディース))*\)\s*',
        '', name
    )
    if product_code:
        name = name.replace(product_code, '').strip()
    name = _translate_katakana(name)
    for wrong, correct in _KOREAN_FIX.items():
        name = name.replace(wrong, correct)
    # 카테고리 키워드 제거 (런닝화, 트레이닝화, 클럽활동 등)
    for cat in _CATEGORY_REMOVE:
        name = name.replace(cat, '')
    name = re.sub(r'\s+', ' ', name).strip()
    return name


def _has_japanese(text: str) -> bool:
    """텍스트에 일본어(히라가나/카타카나/한자)가 포함되어 있는지 확인"""
    return bool(re.search(r'[\u3040-\u309F\u30A0-\u30FF\u4E00-\u9FFF]', text))


def _extract_japanese_words(text: str) -> list:
    """텍스트에서 일본어 단어(연속된 일본어 문자)를 추출"""
    return re.findall(r'[\u30A0-\u30FF\u3040-\u309F\u4E00-\u9FFF]+', text)


def _gemini_translate_name(text: str) -> str:
    """
    Gemini로 일본어가 포함된 상품명을 한국어로 번역
    번역 결과를 사용자 사전에 자동 저장
    """
    if not _has_japanese(text):
        return text
    if _ai_config["provider"] == "none" or not _ai_config["gemini_key"]:
        return text

    # 번역 전 일본어 단어 추출
    ja_words = _extract_japanese_words(text)

    try:
        prompt = f"""아래 상품명에 포함된 일본어 부분만 한국어로 번역해주세요.
이미 한국어/영어/숫자인 부분은 그대로 유지하세요.
번역 결과만 출력하세요. 설명이나 부연은 절대 쓰지 마세요.

상품명: {text}"""
        result = _call_gemini(prompt)
        result = result.strip().split("\n")[0].strip()  # 첫 줄만
        if result and not _has_japanese(result):
            logger.info(f"Gemini 번역: {text} → {result}")

            # 단어별 매핑 추출하여 사전 자동 저장
            _auto_save_translations(text, result, ja_words)

            return result
    except Exception as e:
        logger.warning(f"Gemini 번역 실패: {e}")

    return text


def _auto_save_translations(original: str, translated: str, ja_words: list):
    """Gemini 번역 결과에서 일본어→한국어 매핑을 추출하여 사전에 저장"""
    global _user_dict

    if not ja_words:
        return

    # 개별 단어 번역을 Gemini에 요청
    try:
        words_str = "\n".join(f"{w}" for w in ja_words if w not in _user_dict and w not in _KATAKANA_MAP)
        if not words_str:
            return

        prompt = f"""스포츠 신발/의류 상품명에 나오는 아래 일본어 단어들을 한국어 발음으로 음역해주세요.
모델명, 브랜드명, 고유명사는 뜻이 아닌 발음을 한국어로 적어주세요.
예: サブリナ=사브리나, バスケット=바스켓, ゲーマー=게이머, バッシュ=바슈
각 줄에 "일본어=한국어" 형식으로만 출력하세요.

{words_str}"""
        result = _call_gemini(prompt)

        new_count = 0
        for line in result.strip().split("\n"):
            line = line.strip()
            if "=" in line:
                parts = line.split("=", 1)
                ja = parts[0].strip()
                ko = parts[1].strip()
                if ja and ko and _has_japanese(ja) and not _has_japanese(ko):
                    if ja not in _user_dict and ja not in _KATAKANA_MAP:
                        _user_dict[ja] = ko
                        new_count += 1

        if new_count > 0:
            _save_user_dict(_user_dict)
            logger.info(f"✅ 번역 사전 자동 저장: {new_count}개 ({_USER_DICT_PATH})")

    except Exception as e:
        logger.warning(f"번역 사전 자동 저장 실패: {e}")


# ── 제목 / 태그 ────────────────────────────

def make_title(product: dict) -> str:
    """게시글 제목: 일본구매대행 브랜드명 상품명 품번"""
    brand = product.get("brand_ko", "") or product.get("brand", "")
    name = product.get("name_ko", "") or product.get("name", "")
    code = product.get("product_code", "")
    name = _clean_name(name, code)
    # 사전에 없는 일본어가 남아있으면 Gemini로 번역
    if _has_japanese(name):
        name = _gemini_translate_name(name)

    parts = ["일본구매대행"]
    if brand:
        parts.append(brand)
    if name:
        parts.append(name)
    if code:
        parts.append(code)

    title = " ".join(parts)
    if len(title) > 100:
        title = title[:97] + "..."
    return title


def make_tags(product: dict) -> list:
    """태그: #일본구매대행 #브랜드명 #품번"""
    tags = ["일본구매대행"]
    brand = product.get("brand_ko", "") or product.get("brand", "")
    if brand:
        tags.append(brand)
    code = product.get("product_code", "")
    if code:
        tags.append(code)
    return tags


# ── 프롬프트 생성 ──────────────────────────

def _build_prompt(product: dict, price_info: dict) -> str:
    """AI 공통 프롬프트"""
    name_ja = product.get("name", "")
    brand = product.get("brand_ko") or product.get("brand", "")
    link = product.get("link", "")
    code = product.get("product_code", "")
    price_krw = price_info.get("price_final", 0)
    rate = price_info.get("rate", 0)
    name_ko_clean = _clean_name(product.get("name_ko", "") or name_ja, code)
    # 사전에 없는 일본어가 남아있으면 Gemini로 번역
    if _has_japanese(name_ko_clean):
        name_ko_clean = _gemini_translate_name(name_ko_clean)

    sizes = product.get("sizes", [])
    available = [s["size"] for s in sizes if s.get("in_stock")]
    size_text = " / ".join(available) if available else "문의 바랍니다"

    description = product.get("description_ko") or product.get("description", "")

    # 상세 설명에 일본어가 남아있으면 Gemini로 미리 번역
    if description and _has_japanese(description):
        description = _translate_description(description)

    # 인트로 랜덤 선택
    full_name = f"{name_ko_clean} {code}".strip()
    intro_line1, intro_line2 = _pick_intro(full_name)

    return f"""당신은 일본 구매대행 전문 카페 운영자입니다.
아래 상품 정보로 네이버 카페 게시글 본문을 작성해주세요.

[상품 정보]
- 브랜드: {brand}
- 상품명: {name_ko_clean}
- 상품명(일본어): {name_ja}
- 품번: {code}
- 구매대행가: {format_price(price_krw)} (무료배송)
- 적용 환율: 1엔 = {rate}원
- 주문 가능 사이즈: {size_text}
- 상품 링크: {link}
- 상세 설명(반드시 한국어로 번역하여 사용할 것): {description[:800] if description else "없음"}

[작성 규칙]
1. 아래 형식을 반드시 그대로 따를 것 — 고정 문구는 절대 변경하지 말 것
2. (상품 특징) 부분: 일본어 설명을 한국어로 번역/요약하여 가독성 좋게 작성
3. 이모지 적절히 활용
4. 친근하고 신뢰감 있는 톤
5. 이미지 태그나 HTML은 절대 포함하지 말 것
6. 마크다운 문법(**, ##, - 등) 절대 사용하지 말 것 — 순수 텍스트만 출력
7. 절대로 일본어(히라가나, 카타카나, 한자)를 출력하지 말 것 — 모든 일본어는 반드시 한국어로 번역하여 출력
8. 소재명, 기술명 등 일본어로 된 전문 용어도 반드시 한국어로 번역 (예: 合成繊維→합성섬유, ゴム底→고무밑창, 合成樹脂→합성수지)

[상품 특징 작성 가이드]
- 상품에 맞는 이모지로 시작하는 핵심 포인트 제목을 먼저 쓸 것
- 번호 매기기(1. 2. 3.)로 핵심 포인트 3~5개 정리
- 각 포인트 아래에 구체적 설명 1~2줄 (기술명, 수치, 소재 등)
- 예시:
  🏃‍♂️ 나이키 에어 줌 페가수스 41 핵심 포인트
  1. 뛰어난 쿠셔닝
  ReactX 미드솔로 기존 대비 반발력 13% 향상
  에어 줌 유닛이 앞뒤 모두 탑재
  2. 쾌적한 착용감
  엔지니어드 메쉬 갑피로 통기성 확보
  플라이와이어 케이블로 발을 안정적으로 감싸줌

[출력 형식 - 이 형식 그대로 출력]
안녕하세요 서포트 센터장 입니다. ^^

{intro_line1}
{intro_line2}

가격 : {format_price(price_krw)} (무료배송)
배송일 : 대략 4-7일 소요

주문 가능 사이즈
{size_text}

(상품 특징 — 여기만 채우세요. 위 가이드 형식으로 작성)


👉 구매 문의 & 진행 방법
일본구매대행으로 구매 관심 있으신 분은 쪽지 또는 아래 네이버 폼 작성 부탁드려요!!

{NAVER_FORM_URL}

위 형식에서 (상품 특징) 부분만 채워서 전체를 출력하세요.
마크다운 문법은 절대 쓰지 마세요.
일본어(カタカナ, ひらがな, 漢字)는 절대 출력하지 마세요 — 모든 내용을 한국어로 번역하세요."""


# ── AI 호출 ────────────────────────────────

def _call_gemini(prompt: str) -> str:
    """Gemini API 호출"""
    client = _get_gemini()
    response = client.models.generate_content(
        model="gemini-2.5-flash-lite",
        contents=prompt,
    )
    return response.text.strip()


def _call_claude(prompt: str) -> str:
    """Claude API 호출"""
    client = _get_claude()
    with client.messages.stream(
        model="claude-sonnet-4-6",
        max_tokens=3000,
        messages=[{"role": "user", "content": prompt}]
    ) as stream:
        return stream.get_final_message().content[0].text.strip()


def _translate_description(desc: str) -> str:
    """상품 상세 설명(일본어)을 Gemini로 한국어 번역"""
    if not _has_japanese(desc):
        return desc
    if _ai_config["provider"] == "none" or not _ai_config["gemini_key"]:
        return desc

    try:
        prompt = f"""아래 일본어 상품 설명을 한국어로 번역해주세요.
소재명, 기술명 등 전문 용어도 모두 한국어로 번역하세요.
예: 合成繊維→합성섬유, ゴム底→고무밑창, 合成樹脂→합성수지, ベトナム製→베트남제
번역 결과만 출력하세요.

{desc[:800]}"""
        result = _call_gemini(prompt)
        result = result.strip()
        if result and not _has_japanese(result):
            logger.info(f"✅ 상세 설명 번역 완료 ({len(desc)}자 → {len(result)}자)")
            return result
        elif result:
            logger.warning("⚠️ 상세 설명 번역 후에도 일본어 잔존")
            return result  # 그래도 원문보다는 나음
    except Exception as e:
        logger.warning(f"상세 설명 번역 실패: {e}")

    return desc


def _retranslate_content(content: str) -> str:
    """본문에 남은 일본어를 Gemini로 재번역"""
    if not _has_japanese(content):
        return content
    if _ai_config["provider"] == "none" or not _ai_config["gemini_key"]:
        return content

    try:
        prompt = f"""아래 텍스트에서 일본어로 된 부분을 모두 한국어로 번역해주세요.
이미 한국어/영어/숫자인 부분은 그대로 유지하세요.
전체 문맥과 형식을 그대로 유지하면서 일본어만 한국어로 바꿔주세요.
번역 결과만 출력하세요.

{content}"""
        result = _call_gemini(prompt)
        result = _clean_ai_response(result)
        if result and len(result) > len(content) * 0.5:
            # 재번역 결과에서도 일본어 단어를 사전에 저장
            ja_words_before = _extract_japanese_words(content)
            if ja_words_before:
                _auto_save_translations(content, result, ja_words_before)
            logger.info("✅ 본문 재번역 완료")
            return result
    except Exception as e:
        logger.warning(f"본문 재번역 실패: {e}")

    # 재번역 실패 시 카타카나 사전으로 단어 단위 치환
    content = _translate_katakana(content)
    return content


def _clean_ai_response(content: str) -> str:
    """AI 응답에서 구분자 제거"""
    if content.startswith("---"):
        content = content[3:].strip()
    if content.endswith("---"):
        content = content[:-3].strip()
    # 마크다운 코드블록 제거
    if content.startswith("```"):
        content = re.sub(r'^```\w*\n?', '', content)
        content = re.sub(r'\n?```$', '', content)
        content = content.strip()
    return content


# ── 메인 함수 ──────────────────────────────

def generate_cafe_post(product: dict, price_info: dict) -> dict:
    """
    AI로 카페 게시글 제목 + 본문 생성

    Returns:
        {"title": str, "content": str, "tags": list[str]}
    """
    title = make_title(product)
    tags = make_tags(product)

    provider = _ai_config["provider"]

    # API 키 확인
    if provider == "gemini" and not _ai_config["gemini_key"]:
        logger.warning("⚠️ GEMINI_API_KEY 미설정 — 기본 템플릿 사용")
        return {"title": title, "content": _make_fallback_content(product, price_info), "tags": tags}
    elif provider == "claude" and not _ai_config["claude_key"]:
        logger.warning("⚠️ ANTHROPIC_API_KEY 미설정 — 기본 템플릿 사용")
        return {"title": title, "content": _make_fallback_content(product, price_info), "tags": tags}
    elif provider == "none":
        return {"title": title, "content": _make_fallback_content(product, price_info), "tags": tags}

    prompt = _build_prompt(product, price_info)

    try:
        if provider == "gemini":
            content = _call_gemini(prompt)
            logger.info("✅ Gemini 게시글 생성 완료")
        else:
            content = _call_claude(prompt)
            logger.info("✅ Claude 게시글 생성 완료")

        content = _clean_ai_response(content)

        # 본문에 일본어가 남아있으면 재번역 시도
        if _has_japanese(content):
            logger.warning("⚠️ 본문에 일본어 잔존 — 재번역 시도")
            content = _retranslate_content(content)

        return {"title": title, "content": content, "tags": tags}

    except Exception as e:
        logger.error(f"❌ {provider} API 오류: {e} — 기본 템플릿 사용")
        return {"title": title, "content": _make_fallback_content(product, price_info), "tags": tags}


# ── Fallback 템플릿 ────────────────────────

def _make_fallback_content(product: dict, price_info: dict) -> str:
    """AI 실패 시 기본 템플릿"""
    code = product.get("product_code", "")
    name_ko = _clean_name(product.get("name_ko", "") or product.get("name", ""), code)
    if _has_japanese(name_ko):
        name_ko = _gemini_translate_name(name_ko)
    price_krw = price_info.get("price_final", 0)

    sizes = product.get("sizes", [])
    available = [s["size"] for s in sizes if s.get("in_stock")]
    size_text = " / ".join(available) if available else "문의 바랍니다"

    description = product.get("description_ko") or product.get("description", "")
    desc_section = ""
    if description:
        desc_lines = description.strip().split("\n")
        desc_section = "\n".join(line.strip() for line in desc_lines if line.strip())

    full_name = f"{name_ko} {code}".strip()
    intro_line1, intro_line2 = _pick_intro(full_name)

    content = f"""안녕하세요 서포트 센터장 입니다. ^^

{intro_line1}
{intro_line2}

가격 : {format_price(price_krw)} (무료배송)
배송일 : 대략 4-7일 소요

주문 가능 사이즈
{size_text}"""

    if desc_section:
        content += f"\n\n{desc_section}"

    content += f"""


👉 구매 문의 & 진행 방법
일본구매대행으로 구매 관심 있으신 분은 쪽지 또는 아래 네이버 폼 작성 부탁드려요!!

{NAVER_FORM_URL}"""

    return content.strip()


# ── 이미지 URL ─────────────────────────────

def get_detail_image_urls(product: dict) -> list:
    """상세 이미지 URL 목록 (썸네일 제외, 첫 번째가 대표)"""
    images = []
    thumb = product.get("img_url", "")
    for url in product.get("detail_images", []):
        if url and url != thumb:
            images.append(url)
    return images[:8]
