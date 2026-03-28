"""
post_generator.py
AI(Gemini/Claude)를 이용한 일본 구매대행 카페 게시글 자동 생성
"""

import re
import os
import json
import random
import logging
from config import ANTHROPIC_API_KEY, GEMINI_API_KEY, OPENAI_API_KEY, AI_PROVIDER
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

# ── AI 설정 DB 저장/로드 (외부 경로 — GitHub 미포함) ──────
import sqlite3 as _sqlite3

_AI_SETTINGS_DB = os.path.join(get_path("db"), "ai_settings.db")


def _init_ai_settings_db():
    """AI 설정 DB 초기화"""
    os.makedirs(os.path.dirname(_AI_SETTINGS_DB), exist_ok=True)
    conn = _sqlite3.connect(_AI_SETTINGS_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ai_settings (
            key TEXT PRIMARY KEY,
            value TEXT DEFAULT ''
        )
    """)
    conn.commit()
    conn.close()


def _load_ai_settings_from_db() -> dict:
    """DB에서 AI 설정 로드 (없으면 config.py/.env 값 사용)"""
    _init_ai_settings_db()
    settings = {}
    try:
        conn = _sqlite3.connect(_AI_SETTINGS_DB)
        rows = conn.execute("SELECT key, value FROM ai_settings").fetchall()
        conn.close()
        for k, v in rows:
            settings[k] = v
    except Exception:
        pass

    return {
        "provider": settings.get("provider") or AI_PROVIDER,
        "gemini_key": settings.get("gemini_key") or GEMINI_API_KEY,
        "claude_key": settings.get("claude_key") or ANTHROPIC_API_KEY,
        "openai_key": settings.get("openai_key") or OPENAI_API_KEY,
    }


def _save_ai_settings_to_db(config: dict):
    """AI 설정을 DB에 저장"""
    _init_ai_settings_db()
    try:
        conn = _sqlite3.connect(_AI_SETTINGS_DB)
        for k, v in config.items():
            if v:  # 빈 값은 저장하지 않음
                conn.execute(
                    "INSERT OR REPLACE INTO ai_settings (key, value) VALUES (?, ?)",
                    (k, v)
                )
        conn.commit()
        conn.close()
        logger.info(f"💾 AI 설정 DB 저장 완료: {_AI_SETTINGS_DB}")
    except Exception as e:
        logger.warning(f"AI 설정 DB 저장 실패: {e}")


# ── 런타임 설정 (대시보드에서 변경 가능) ──────
_ai_config = _load_ai_settings_from_db()

# ── AI 클라이언트 (지연 초기화) ──────────────
_gemini_client = None
_claude_client = None
_openai_client = None


def set_ai_config(provider=None, gemini_key=None, claude_key=None, openai_key=None):
    """대시보드에서 AI 설정 변경 + .env 파일에 영구 저장"""
    global _gemini_client, _claude_client, _openai_client
    if provider is not None:
        _ai_config["provider"] = provider.lower()
    if gemini_key is not None:
        _ai_config["gemini_key"] = gemini_key
        _gemini_client = None  # 클라이언트 재생성
    if claude_key is not None:
        _ai_config["claude_key"] = claude_key
        _claude_client = None
    if openai_key is not None:
        _ai_config["openai_key"] = openai_key
        _openai_client = None
    logger.info(f"AI 설정 변경: provider={_ai_config['provider']}")

    # 외부 DB에 영구 저장 (GitHub 미포함)
    _save_ai_settings_to_db(_ai_config)
    # .env 파일에도 백업 저장
    _save_ai_config_to_env()


def _save_ai_config_to_env():
    """현재 AI 설정을 .env 파일에 저장 (서버 재시작 시에도 유지)"""
    env_path = os.path.join(os.path.dirname(__file__), ".env")
    env_vars = {}

    # 기존 .env 읽기
    if os.path.exists(env_path):
        with open(env_path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    env_vars[k.strip()] = v.strip()

    # AI 설정 업데이트
    env_vars["AI_PROVIDER"] = _ai_config["provider"]
    if _ai_config["gemini_key"]:
        env_vars["GEMINI_API_KEY"] = _ai_config["gemini_key"]
    if _ai_config["claude_key"]:
        env_vars["ANTHROPIC_API_KEY"] = _ai_config["claude_key"]
    if _ai_config.get("openai_key"):
        env_vars["OPENAI_API_KEY"] = _ai_config["openai_key"]

    # .env 파일 쓰기
    with open(env_path, "w", encoding="utf-8") as f:
        for k, v in env_vars.items():
            f.write(f"{k}={v}\n")

    logger.info("💾 AI 설정 .env 저장 완료")


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
        "openai_key": mask(_ai_config.get("openai_key", "")),
        "gemini_set": bool(_ai_config["gemini_key"]),
        "claude_set": bool(_ai_config["claude_key"]),
        "openai_set": bool(_ai_config.get("openai_key")),
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


def _get_openai():
    global _openai_client
    if _openai_client is None:
        from openai import OpenAI
        _openai_client = OpenAI(
            api_key=_ai_config.get("openai_key", ""),
            timeout=30.0,  # 30초 타임아웃
        )
    return _openai_client


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
    "テニス": "테니스", "ゴルフ": "골프", "ゴルフウェア": "골프웨어", "シューズ": "슈즈",
    "クラブ活動": "클럽활동", "ジョギング": "조깅",
    "マラソン": "마라톤", "フィットネス": "피트니스",
    # ── 추가 모델/카테고리 ──
    "バスケットシューズ": "농구화", "バッシュ": "농구화",
    "バスケット": "바스켓", "シリーズ": "시리즈",
    "エアー": "에어", "ブラッドライン": "블러드라인",
    "プロ": "프로", "ウルトラ": "울트라", "ライト": "라이트",
    "スーパー": "슈퍼", "ハイパー": "하이퍼", "ミッド": "미드",
    "ロー": "로우", "ハイ": "하이", "ウィメンズ": "여성",
    "メンズ": "남성", "レディース": "여성", "キッズ": "키즈",
    "ニュー": "뉴", "クラシック": "클래식", "モデル": "모델",
    "リミテッド": "리미티드", "エディション": "에디션",
    "カラー": "컬러", "タイプ": "타입", "サイズ": "사이즈",
    "コレクション": "컬렉션", "スタイル": "스타일",
    "レトロ": "레트로", "ヴィンテージ": "빈티지",
    "プレミアム": "프리미엄", "エリート": "엘리트",
    "フライニット": "플라이니트", "フォーム": "폼",
    "クッション": "쿠션", "ソール": "솔",
    # ── 스포츠/카테고리 추가 ──
    "スポーツシューズ": "스포츠화", "アウトドアシューズ": "아웃도어화",
    "カジュアルシューズ": "캐주얼화", "テニスシューズ": "테니스화",
    "スポーツ": "스포츠", "アウトドア": "아웃도어",
    "カジュアル": "캐주얼",
    # ── 아디다스 모델 ──
    "デュラモ": "듀라모", "ウルトラブースト": "울트라부스트",
    "アディゼロ": "아디제로", "ギャラクシー": "갤럭시",
    "スタンスミス": "스탠스미스", "スーパースター": "슈퍼스타",
    # ── 나이키 추가 ──
    "マノア": "마노아", "レザー": "레더", "レボリューション": "레볼루션",
    "ベルクロ": "벨크로",
    # ── 호카 모델 ──
    "アナカパ": "아나카파", "フリーダム": "프리덤",
    "クリフトン": "클리프턴", "ボンダイ": "본다이",
    "チャレンジャー": "챌린저", "スピードゴート": "스피드고트",
    "アラヒ": "아라히", "リンコン": "린콘",
    # ── 요넥스 ──
    "ヨネックス": "요넥스", "クレー": "클레이",
    # ── 노스페이스 ──
    "ザ・ノース・フェイス": "노스페이스", "ノースフェイス": "노스페이스",
    "ダウン": "다운", "アウター": "아우터",
    "マウンテン": "마운틴", "ジャケット": "재킷",
    # ── 오클리 ──
    "オークリー": "오클리",
    # ── 기타 ──
    "テイタム": "테이텀", "ゲルフープ": "겔후프",
    "オールコート": "올코트", "インドア": "인도어",
    "砂入り人工芝用": "인조잔디용", "人工芝": "인조잔디",
    "ベルト付き": "벨트부착",
}


def _translate_katakana(name: str) -> str:
    """카타카나 단어를 한국어로 변환 (내장 사전 + 사용자 사전)"""
    # 사용자 사전 먼저 적용 (우선순위 높음)
    for ja, ko in _user_dict.items():
        name = name.replace(ja, ko)
    # 내장 사전 적용 (긴 단어 먼저 매칭하여 부분 치환 방지)
    for ja, ko in sorted(_KATAKANA_MAP.items(), key=lambda x: len(x[0]), reverse=True):
        name = name.replace(ja, ko)
    # 남은 장음부호(ー) 제거
    name = name.replace("ー", "")
    return name


# 한국어 표기 랜덤 변환 (러닝화/런닝화 반반 사용)
_KOREAN_RANDOM = {
    "러닝화": ("러닝화", "런닝화"),
    "러닝": ("러닝", "런닝"),
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
    "런닝화", "러닝화", "트레이닝화", "워킹화", "조깅화", "농구화", "축구화",
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
    for key, (opt_a, opt_b) in _KOREAN_RANDOM.items():
        if key in name:
            name = name.replace(key, random.choice([opt_a, opt_b]))
    # 카테고리 키워드 제거 (런닝화, 트레이닝화, 클럽활동 등)
    for cat in _CATEGORY_REMOVE:
        name = name.replace(cat, '')
    name = re.sub(r'\s+', ' ', name).strip()
    return name


def _has_japanese(text: str) -> bool:
    """텍스트에 일본어(히라가나/카타카나/장음부호/한자)가 포함되어 있는지 확인"""
    return bool(re.search(r'[\u3040-\u309F\u30A0-\u30FF\u30FC\u4E00-\u9FFF]', text))


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

    provider = _ai_config["provider"]
    has_key = (provider == "gemini" and _ai_config["gemini_key"]) or \
              (provider == "claude" and _ai_config["claude_key"]) or \
              (provider == "openai" and _ai_config.get("openai_key"))
    if provider == "none" or not has_key:
        return text

    # 번역 전 일본어 단어 추출
    ja_words = _extract_japanese_words(text)

    try:
        prompt = f"""아래 상품명에 포함된 일본어 부분만 한국어로 번역해주세요.
이미 한국어/영어/숫자인 부분은 그대로 유지하세요.
번역 결과만 출력하세요. 설명이나 부연은 절대 쓰지 마세요.

상품명: {text}"""
        if provider == "gemini":
            result = _call_gemini(prompt)
        elif provider == "openai":
            result = _call_openai(prompt)
        else:
            result = _call_claude(prompt)
        result = result.strip().split("\n")[0].strip()  # 첫 줄만
        if result and not _has_japanese(result):
            logger.info(f"AI 번역: {text} → {result}")

            # 단어별 매핑 추출하여 사전 자동 저장
            _auto_save_translations(text, result, ja_words)

            return result
    except Exception as e:
        logger.warning(f"AI 번역 실패: {e}")

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

_TITLE_SUFFIXES = [
    "직구로 만나보세요!",
    "일본 직배송",
    "한정 사이즈 안내",
    "현지 직송 정품",
    "재고 확인 완료!",
    "무료배송",
    "정품 구매 안내",
    "지금 바로 구매 가능",
    "사이즈 한정 수량!",
    "일본 현지 정품",
    "빠른 직배송",
    "구매대행 추천",
    "일본 한정 컬러",
    "국내 미발매 모델",
    "일본 정가 구매",
    "사이즈 재고 있음",
    "일본 직구 정품",
    "한국 미출시 제품",
    "일본 공식 스토어 정품",
    "세일 가격 구매대행",
    "일본 매장 직접 구매",
    "정품 보장 직배송",
]


def make_title(product: dict) -> str:
    """게시글 제목: 키워드 배열 랜덤 + 자연스러운 마무리"""
    brand = product.get("brand_ko", "") or product.get("brand", "")
    name = product.get("name_ko", "") or product.get("name", "")
    code = product.get("product_code", "")
    name = _clean_name(name, code)

    # 상품명 일본어 번역 (최대 2회 시도)
    if _has_japanese(name):
        name = _gemini_translate_name(name)
    if _has_japanese(name):
        logger.warning(f"⚠️ 제목 1차 번역 후에도 일본어 잔존 — 2차 시도: {name}")
        name = _gemini_translate_name(name)

    # 브랜드명도 일본어면 번역
    if _has_japanese(brand):
        brand = _gemini_translate_name(brand)

    # 최종 확인: 그래도 일본어가 남아있으면 카타카나 사전으로 치환
    if _has_japanese(name):
        logger.warning(f"⚠️ 제목 번역 최종 실패 — 카타카나 사전 치환: {name}")
        name = _translate_katakana(name)

    # 핵심 키워드 조합 (빈 값 제외)
    keywords = [k for k in ["일본구매대행", brand, name, code] if k]

    # 키워드 배열 랜덤 변경 (일본구매대행은 앞 2자리 내에서만 이동)
    if len(keywords) >= 3:
        first = keywords[0]  # 일본구매대행
        rest = keywords[1:]
        random.shuffle(rest)
        insert_pos = random.choice([0, 1])
        rest.insert(insert_pos, first)
        keywords = rest

    suffix = random.choice(_TITLE_SUFFIXES)
    title = " ".join(keywords) + " " + suffix

    # 연속 공백 정리
    title = " ".join(title.split())
    if len(title) > 100:
        title = title[:97] + "..."
    return title


def make_tags(product: dict) -> list:
    """태그: #일본구매대행 #브랜드명 #품번 (기본 3개)"""
    tags = ["일본구매대행"]
    brand = product.get("brand_ko", "") or product.get("brand", "")
    if brand:
        tags.append(brand)
    code = product.get("product_code", "")
    if code:
        tags.append(code)
    return tags


def _extract_ai_tags(content: str) -> list:
    """AI 응답 마지막에서 [추천태그] 줄을 파싱하여 태그 리스트 반환"""
    import re
    for line in reversed(content.strip().split("\n")):
        line = line.strip()
        m = re.match(r'\[추천태그\]\s*(.+)', line)
        if m:
            raw = m.group(1)
            tags = [t.strip().lstrip("#") for t in raw.split(",") if t.strip()]
            return tags[:7]  # 최대 7개
    return []


def _remove_tag_line(content: str) -> str:
    """본문에서 [추천태그] 줄 제거"""
    lines = content.strip().split("\n")
    result = []
    for line in lines:
        if "[추천태그]" in line:
            continue
        result.append(line)
    return "\n".join(result).strip()


# ── 프롬프트 생성 ──────────────────────────

def _build_prompt(product: dict, price_info: dict) -> str:
    """AI 공통 프롬프트"""
    name_ja = product.get("name", "")
    brand = product.get("brand_ko") or product.get("brand", "")
    link = product.get("link", "")
    code = product.get("product_code", "")
    price_krw = price_info.get("price_final", 0)
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
        logger.info(f"📝 [{code}] 상세 설명 일본어 감지 — Gemini 번역 시도")
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
- 주문 가능 사이즈: {size_text}
- 상품 링크: {link}
- 상세 설명(반드시 한국어로 번역하여 사용할 것): {description[:800] if description else "없음"}

[작성 규칙]
1. 아래 형식을 반드시 그대로 따를 것 — 고정 문구는 절대 변경하지 말 것
2. 이모지 적절히 활용, 친근하고 신뢰감 있는 톤
3. 이미지 태그나 HTML은 절대 포함하지 말 것
4. 마크다운 문법(**, ##, - 등) 절대 사용하지 말 것 — 순수 텍스트만 출력
5. 절대로 일본어(히라가나, 카타카나, 한자)를 출력하지 말 것 — 모든 일본어는 반드시 한국어로 번역하여 출력
6. 소재명, 기술명 등 일본어로 된 전문 용어도 반드시 한국어로 번역 (예: 合成繊維→합성섬유, ゴム底→고무밑창)
7. 절대로 공급사/매입처 이름을 언급하지 말 것 (Xebio, 제비오, SuperSports, ABC마트 등 절대 금지)
8. "일본 공식 유통 정품", "일본 현지 매장 정품" 등 일반적인 표현만 사용

[출력 형식 - 이 형식 그대로 출력]
{intro_line1}
{intro_line2}

가격 : {format_price(price_krw)} (무료배송)
배송일 : 대략 4-7일 소요

주문 가능 사이즈
{size_text}


🔍 상품 상세 정보

(여기에 모델명, 특징, 적합 용도, 스펙을 간결한 표 형식으로 정리. 예:
모델명: 나이키 에어 줌 보메로 18
용도: 데일리 러닝 / 장거리 조깅
무게: 약 279g (27.0cm 기준)
발볼: 레귤러 (2E 상당)
드롭: 10mm
소재: 엔지니어드 메쉬
이런 식으로 항목별 한 줄씩 깔끔하게)

💎 핵심 구매 포인트

(여기에 3~4가지 셀링 포인트를 짧고 핵심만 담아 작성. 예:
✔ 극강의 쿠셔닝 — ReactX + ZoomX 이중 폼으로 부드럽고 반발력 있는 착화감
✔ 부드러운 무게 이동 — 향상된 트랙션 패턴으로 발뒤꿈치→발가락 자연스러운 전환
✔ 뛰어난 통기성 — 엔지니어드 메쉬 어퍼로 쾌적한 러닝
✔ 레이스 트레이닝 최적화 — 장거리 훈련부터 대회까지 활용 가능
이런 식으로 ✔ 키워드 — 한 줄 설명 형태로 간결하게)

⚠️ 구매 전 확인 사항

사이즈 추천: (상품 특성에 맞게 작성. 내용이 길면 자연스럽게 줄바꿈하세요)
(예시: 농구화 특성상 발을 안정적으로 잡아주는 핏이므로 정사이즈 주문을 권장합니다.
발볼이 넓으신 분은 반 사이즈 업도 고려해 보세요.)
정품 안내: 일본 공식 유통 정품이며, 미개봉 새상품으로 발송됩니다.
해외 구매대행 상품이라 교환/반품이 어려울 수 있는 점 유의 부탁드려요!!


위 형식에서 (🔍 상품 상세 정보)와 (💎 핵심 구매 포인트) 부분만 채워서 전체를 출력하세요.
👉 구매 문의 섹션이나 네이버 폼 URL은 절대 포함하지 마세요 (별도 템플릿에서 처리됨).
⚠️ 구매 전 확인 사항의 사이즈 추천도 상품에 맞게 상세하게 채워주세요.
마크다운 문법은 절대 쓰지 마세요.
일본어는 절대 출력하지 마세요.
공급사/매입처(Xebio, SuperSports 등) 이름은 절대 언급하지 마세요.

마지막 줄에 추천 태그 7개를 쉼표로 구분해서 작성하세요 (검색 키워드용, #은 빼고).
형식: [추천태그] {random.choice(["러닝화","런닝화"])}추천,나이키{random.choice(["러닝","런닝"])},데일리{random.choice(["러닝화","런닝화"])},조깅화,쿠셔닝{random.choice(["러닝화","런닝화"])},가벼운운동화,일본직구신발"""


# ── AI 키 검증 ────────────────────────────

def verify_ai_key() -> dict:
    """현재 AI provider의 API 키가 정상 작동하는지 확인
    Returns: {"ok": bool, "provider": str, "message": str}
    """
    provider = _ai_config["provider"]

    if provider == "none":
        return {"ok": False, "provider": "none", "message": "AI provider가 'none'으로 설정되어 있습니다. 기본 템플릿이 사용됩니다."}

    if provider == "gemini":
        key = _ai_config["gemini_key"]
        if not key:
            return {"ok": False, "provider": "gemini", "message": "GEMINI_API_KEY가 설정되지 않았습니다."}
        try:
            result = _call_gemini("테스트입니다. '확인'이라고만 답해주세요.")
            if result:
                return {"ok": True, "provider": "gemini", "message": f"Gemini API 정상 작동 (응답: {result[:20]})"}
        except Exception as e:
            return {"ok": False, "provider": "gemini", "message": f"Gemini API 오류: {e}"}

    elif provider == "claude":
        key = _ai_config["claude_key"]
        if not key:
            return {"ok": False, "provider": "claude", "message": "ANTHROPIC_API_KEY가 설정되지 않았습니다."}
        try:
            result = _call_claude("테스트입니다. '확인'이라고만 답해주세요.")
            if result:
                return {"ok": True, "provider": "claude", "message": f"Claude API 정상 작동 (응답: {result[:20]})"}
        except Exception as e:
            err = str(e)
            if "credit balance is too low" in err:
                return {"ok": False, "provider": "claude", "message": "Claude API 크레딧 잔액 부족 — console.anthropic.com 에서 충전 필요"}
            return {"ok": False, "provider": "claude", "message": f"Claude API 오류: {e}"}

    elif provider == "openai":
        key = _ai_config.get("openai_key", "")
        if not key:
            return {"ok": False, "provider": "openai", "message": "OPENAI_API_KEY가 설정되지 않았습니다. 대시보드에서 키를 입력하고 저장 버튼을 눌러주세요."}
        try:
            # openai 패키지 설치 확인
            try:
                from openai import OpenAI
            except ImportError:
                return {"ok": False, "provider": "openai", "message": "openai 패키지가 설치되지 않았습니다. 서버에서 'pip install openai'를 실행해주세요."}
            logger.info(f"🧪 OpenAI 테스트 시작 — key: {key[:8]}...{key[-4:]}")
            result = _call_openai("테스트입니다. '확인'이라고만 답해주세요.")
            if result:
                return {"ok": True, "provider": "openai", "message": f"OpenAI API 정상 작동 (응답: {result[:20]})"}
            return {"ok": False, "provider": "openai", "message": "OpenAI API 응답이 비어있습니다."}
        except ImportError as e:
            logger.error(f"🧪 OpenAI 패키지 오류: {e}")
            return {"ok": False, "provider": "openai", "message": f"openai 패키지 오류: {e}. 서버에서 'pip install openai'를 실행해주세요."}
        except Exception as e:
            import traceback
            logger.error(f"🧪 OpenAI 테스트 실패: {traceback.format_exc()}")
            return {"ok": False, "provider": "openai", "message": f"OpenAI API 오류: {type(e).__name__}: {e}"}

    return {"ok": False, "provider": provider, "message": f"알 수 없는 provider: {provider}"}


# ── AI 채팅 (대시보드 채팅 위젯용) ──────────────

def chat_with_ai(message: str, history: list = None) -> dict:
    """대시보드 채팅 위젯에서 AI와 대화
    Args:
        message: 사용자 메시지
        history: [{"role":"user"|"assistant","content":"..."},...] 이전 대화
    Returns: {"ok": bool, "provider": str, "reply": str}
    """
    provider = _ai_config["provider"]
    if provider == "none":
        return {"ok": False, "provider": "none", "reply": "AI provider가 설정되지 않았습니다. 설정에서 AI를 선택해주세요."}

    key_map = {"gemini": "gemini_key", "claude": "claude_key", "openai": "openai_key"}
    if not _ai_config.get(key_map.get(provider, "")):
        return {"ok": False, "provider": provider, "reply": f"{provider} API 키가 설정되지 않았습니다."}

    system_msg = "당신은 일본 구매대행 쇼핑몰 운영을 돕는 AI 어시스턴트입니다. 친절하고 간결하게 답변해주세요. 한국어로 답변합니다."

    try:
        if provider == "gemini":
            # Gemini — history를 하나의 프롬프트로 합침
            parts = [system_msg + "\n"]
            if history:
                for h in history[-10:]:  # 최근 10개만
                    role = "사용자" if h["role"] == "user" else "AI"
                    parts.append(f"{role}: {h['content']}")
            parts.append(f"사용자: {message}")
            parts.append("AI:")
            reply = _call_gemini("\n".join(parts))

        elif provider == "claude":
            client = _get_claude()
            messages = []
            if history:
                for h in history[-10:]:
                    messages.append({"role": h["role"], "content": h["content"]})
            messages.append({"role": "user", "content": message})
            with client.messages.stream(
                model="claude-sonnet-4-6",
                max_tokens=2000,
                system=system_msg,
                messages=messages,
            ) as stream:
                reply = stream.get_final_message().content[0].text.strip()

        elif provider == "openai":
            client = _get_openai()
            messages = [{"role": "system", "content": system_msg}]
            if history:
                for h in history[-10:]:
                    messages.append({"role": h["role"], "content": h["content"]})
            messages.append({"role": "user", "content": message})
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                max_tokens=2000,
                messages=messages,
            )
            reply = response.choices[0].message.content.strip()
        else:
            return {"ok": False, "provider": provider, "reply": f"알 수 없는 provider: {provider}"}

        return {"ok": True, "provider": provider, "reply": reply}

    except Exception as e:
        logger.error(f"채팅 AI 오류 [{provider}]: {e}")
        err_str = str(e)
        # 사용자 친화적 에러 메시지
        if "credit balance is too low" in err_str or "billing" in err_str.lower():
            return {"ok": False, "provider": provider, "reply": f"⚠️ [{provider}] API 크레딧 잔액이 부족합니다. 해당 서비스의 결제 페이지에서 크레딧을 충전해주세요."}
        if "invalid_api_key" in err_str or "Incorrect API key" in err_str:
            return {"ok": False, "provider": provider, "reply": f"⚠️ [{provider}] API 키가 유효하지 않습니다. 설정에서 키를 확인해주세요."}
        if "rate_limit" in err_str or "429" in err_str:
            return {"ok": False, "provider": provider, "reply": f"⚠️ [{provider}] API 요청 한도 초과입니다. 잠시 후 다시 시도해주세요."}
        return {"ok": False, "provider": provider, "reply": f"❌ [{provider}] AI 오류: {err_str}"}


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


def _call_openai(prompt: str) -> str:
    """OpenAI API 호출"""
    client = _get_openai()
    logger.info("🤖 OpenAI API 호출 시작...")
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        max_tokens=3000,
        messages=[{"role": "user", "content": prompt}],
        timeout=30.0,  # 요청 타임아웃
    )
    logger.info("🤖 OpenAI API 응답 수신 완료")
    return response.choices[0].message.content.strip()


def _translate_description(desc: str) -> str:
    """상품 상세 설명(일본어)을 AI로 한국어 번역"""
    if not _has_japanese(desc):
        return desc
    provider = _ai_config["provider"]
    has_key = (provider == "gemini" and _ai_config["gemini_key"]) or \
              (provider == "claude" and _ai_config["claude_key"]) or \
              (provider == "openai" and _ai_config.get("openai_key"))
    if provider == "none" or not has_key:
        return desc

    try:
        prompt = f"""아래 일본어 상품 설명을 한국어로 번역해주세요.
소재명, 기술명 등 전문 용어도 모두 한국어로 번역하세요.
예: 合成繊維→합성섬유, ゴム底→고무밑창, 合成樹脂→합성수지, ベトナム製→베트남제
번역 결과만 출력하세요.

{desc[:800]}"""
        result = _call_gemini(prompt) if provider == "gemini" else _call_openai(prompt) if provider == "openai" else _call_claude(prompt)
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
    """본문에 남은 일본어를 AI로 재번역"""
    if not _has_japanese(content):
        return content
    provider = _ai_config["provider"]
    has_key = (provider == "gemini" and _ai_config["gemini_key"]) or \
              (provider == "claude" and _ai_config["claude_key"]) or \
              (provider == "openai" and _ai_config.get("openai_key"))
    if provider == "none" or not has_key:
        return content

    try:
        prompt = f"""아래 텍스트에서 일본어로 된 부분을 모두 한국어로 번역해주세요.
이미 한국어/영어/숫자인 부분은 그대로 유지하세요.
전체 문맥과 형식을 그대로 유지하면서 일본어만 한국어로 바꿔주세요.
번역 결과만 출력하세요.

{content}"""
        result = _call_gemini(prompt) if provider == "gemini" else _call_openai(prompt) if provider == "openai" else _call_claude(prompt)
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


def _remove_purchase_inquiry_section(content: str) -> str:
    """👉 구매 문의 섹션과 네이버 폼 URL 제거 (에디터 템플릿에서 처리되므로)"""
    lines = content.split("\n")
    result = []
    skip = False
    for line in lines:
        stripped = line.strip()
        # 👉 구매 문의 섹션 시작 → 이후 줄 스킵
        if "👉 구매 문의" in stripped:
            skip = True
            continue
        # 네이버 폼 URL 줄 제거
        if "naver.me" in stripped or "네이버 폼" in stripped:
            continue
        # 스킵 중이면 빈 줄이 아닌 새 섹션이 나올 때까지 계속 스킵
        if skip:
            if stripped == "":
                continue
            skip = False
        result.append(line)
    return "\n".join(result).rstrip()


def _clean_ai_response(content: str) -> str:
    """AI 응답에서 구분자 제거 + 불필요한 줄 삭제"""
    if content.startswith("---"):
        content = content[3:].strip()
    if content.endswith("---"):
        content = content[:-3].strip()
    # 마크다운 코드블록 제거
    if content.startswith("```"):
        content = re.sub(r'^```\w*\n?', '', content)
        content = re.sub(r'\n?```$', '', content)
        content = content.strip()
    # AI가 임의로 생성하는 불필요한 줄 제거
    lines = content.split("\n")
    lines = [l for l in lines if "네이버 폼 신청서" not in l]
    lines = [l for l in lines if "서포트 센터장" not in l]
    content = "\n".join(lines)
    return content


def _split_content(content: str) -> tuple:
    """본문을 인트로(가격/사이즈까지)와 상세(🔍부터)로 분리
    Returns: (intro, detail) — detail이 비어있으면 분리 불가"""
    # "🔍 상품 상세 정보" 섹션을 기준으로 분리
    markers = ["🔍 상품 상세 정보", "🔍 상품 상세", "💎 핵심 구매 포인트"]
    for marker in markers:
        idx = content.find(marker)
        if idx > 0:
            intro = content[:idx].rstrip()
            detail = content[idx:]
            return intro, detail
    # 마커가 없으면 빈 줄 2개 이상 연속되는 첫 지점에서 분리 (사이즈 목록 이후)
    m = re.search(r'\n\n\n', content)
    if m and m.start() > 100:
        intro = content[:m.start()].rstrip()
        detail = content[m.end():].lstrip('\n')
        return intro, detail
    return content, ""


# ── 메인 함수 ──────────────────────────────

def generate_cafe_post(product: dict, price_info: dict) -> dict:
    """
    AI로 카페 게시글 제목 + 본문 생성

    Returns:
        {"title": str, "content": str, "tags": list[str]}
    """
    title = make_title(product)
    tags = make_tags(product)
    code = product.get("product_code", "?")

    provider = _ai_config["provider"]
    logger.info(f"📝 [{code}] 게시글 생성 시작 — AI: {provider}")

    if provider == "none":
        logger.info(f"📝 [{code}] AI=none — 기본 템플릿 사용")
        fb = _make_fallback_content(product, price_info)
        fb_intro, fb_detail = _split_content(fb)
        return {"title": title, "content": fb, "tags": tags,
                "content_intro": fb_intro, "content_detail": fb_detail}

    prompt = _build_prompt(product, price_info)

    # AI 호출 순서: Claude → Gemini → OpenAI (3번 시도)
    ai_providers = [
        ("claude", _ai_config.get("claude_key"), _call_claude),
        ("gemini", _ai_config.get("gemini_key"), _call_gemini),
        ("openai", _ai_config.get("openai_key"), _call_openai),
    ]
    # 설정된 provider를 1순위로 정렬
    ai_providers.sort(key=lambda x: 0 if x[0] == provider else 1)

    errors = []
    content = None

    for prov_name, prov_key, prov_call in ai_providers:
        if not prov_key:
            errors.append(f"{prov_name}: API 키 미설정")
            continue
        try:
            logger.info(f"📝 [{code}] {prov_name} API 호출 중...")
            content = prov_call(prompt)
            logger.info(f"✅ [{code}] {prov_name} 게시글 생성 완료 ({len(content)}자)")
            break  # 성공하면 중단
        except Exception as e:
            err_msg = f"{prov_name}: {type(e).__name__}: {e}"
            errors.append(err_msg)
            logger.warning(f"⚠️ [{code}] {err_msg} — 다음 AI 시도")
            content = None

    # 모든 AI 실패
    if not content:
        error_detail = "\n".join(f"  {i+1}. {err}" for i, err in enumerate(errors))
        logger.error(f"❌ [{code}] 모든 AI 실패:\n{error_detail}\n→ 기본 템플릿 사용")

        # 텔레그램 알림 — 상세 오류 정보
        try:
            from notifier import send_telegram
            brand = product.get("brand_ko") or product.get("brand", "")
            name = product.get("name_ko") or product.get("name", "")
            tg_msg = (
                f"🚨 <b>AI 글쓰기 실패</b>\n\n"
                f"📦 상품: {brand} {name}\n"
                f"🔢 품번: {code}\n\n"
                f"❌ <b>시도 결과:</b>\n{error_detail}\n\n"
                f"📝 기본 템플릿으로 대체됩니다."
            )
            send_telegram(tg_msg)
        except Exception as tg_err:
            logger.warning(f"텔레그램 알림 실패: {tg_err}")

        fb = _make_fallback_content(product, price_info)
        fb_intro, fb_detail = _split_content(fb)
        return {"title": title, "content": fb, "tags": tags,
                "content_intro": fb_intro, "content_detail": fb_detail}

    content = _clean_ai_response(content)

    # AI 응답에서 추천 태그 추출
    ai_tags = _extract_ai_tags(content)
    if ai_tags:
        tags.extend(ai_tags)
        logger.info(f"🏷️ [{code}] 추천 태그 {len(ai_tags)}개 추가: {ai_tags}")
        # 본문에서 태그 줄 제거
        content = _remove_tag_line(content)

    # 본문에 일본어가 남아있으면 재번역 시도
    if _has_japanese(content):
        logger.warning(f"⚠️ [{code}] 본문에 일본어 잔존 — 재번역 시도")
        content = _retranslate_content(content)
    else:
        logger.info(f"✅ [{code}] 일본어 없음 — 번역 OK")

    # 👉 구매 문의 & 네이버 폼 섹션 제거 (에디터 템플릿에서 처리)
    content = _remove_purchase_inquiry_section(content)

    # 최종 제목 일본어 잔존 확인
    if _has_japanese(title):
        logger.warning(f"⚠️ [{code}] 최종 제목에 일본어 잔존 — 재번역 시도")
        title = _gemini_translate_name(title)
        if _has_japanese(title):
            title = _translate_katakana(title)
            logger.warning(f"⚠️ [{code}] 제목 최종: {title}")

    # 본문을 인트로 / 상세로 분리 (첫 이미지 삽입 위치용)
    intro, detail = _split_content(content)

    return {"title": title, "content": content, "tags": tags,
            "content_intro": intro, "content_detail": detail}


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
    if description and _has_japanese(description):
        description = _translate_description(description)
    desc_section = ""
    if description:
        desc_lines = description.strip().split("\n")
        desc_section = "\n".join(line.strip() for line in desc_lines if line.strip())

    full_name = f"{name_ko} {code}".strip()
    intro_line1, intro_line2 = _pick_intro(full_name)

    content = f"""{intro_line1}
{intro_line2}

가격 : {format_price(price_krw)} (무료배송)
배송일 : 대략 4-7일 소요

주문 가능 사이즈
{size_text}"""

    if desc_section:
        # 공급사 이름 제거
        for supplier in ["Xebio", "xebio", "제비오", "SuperSports", "ABC마트", "abc-mart"]:
            desc_section = desc_section.replace(supplier, "")
        content += f"\n\n{desc_section}"

    content += """

⚠️ 구매 전 확인 사항

정품 안내: 일본 공식 유통 정품이며, 미개봉 새상품으로 발송됩니다.
해외 구매대행 상품이라 교환/반품이 어려울 수 있는 점 유의 부탁드려요!!"""

    return content.strip()


# ── 이미지 URL ─────────────────────────────

def get_detail_image_urls(product: dict) -> list:
    """상세 이미지 URL 목록 (detail_images 우선, 없으면 img_url 폴백)"""
    images = []
    thumb = product.get("img_url", "")
    detail = product.get("detail_images", [])

    if detail:
        # detail_images가 있으면 썸네일 제외한 나머지 사용
        for url in detail:
            if url and url != thumb:
                images.append(url)
        # detail_images에 썸네일만 있었으면 (전부 필터됨) 썸네일이라도 사용
        if not images and thumb:
            images.append(thumb)
    elif thumb:
        # detail_images가 아예 없으면 썸네일을 대표 이미지로 사용
        images.append(thumb)

    return images[:8]
