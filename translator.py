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
except Exception:
    TRANSLATE_AVAILABLE = False
    _translator = None
    logger.warning("⚠️ googletrans 초기화 실패 — 커스텀 단어장만 사용")

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
    "ゴルフウェア": "골프웨어",
    "ゴルフ"      : "골프",
    "シャツ"    : "셔츠",
    "パンツ"    : "팬츠",
    "ジャケット" : "자켓",
    "ウェア"    : "웨어",
    "ソックス"  : "양말",
    "キャップ"  : "캡",

    # 아웃도어/등산
    "ゴアテックス"    : "고어텍스",
    "トレッキングシューズ": "트레킹화",
    "トレッキング"   : "트레킹",
    "ハイキングシューズ" : "하이킹화",
    "ハイキング"     : "하이킹",
    "登山靴"        : "등산화",
    "登山"          : "등산",
    "トレイルランニング": "트레일러닝",
    "トレイル"       : "트레일",
    "ウォータープルーフ": "방수",
    "防水"          : "방수",
    "フリース"       : "플리스",

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


_ai_rate_limited_until = 0  # rate limit 발생 시 대기 시간

def _translate_with_ai(text: str) -> str:
    """AI API로 일본어 → 한국어 번역 (Gemini/OpenAI/Claude, rate limit 대응)"""
    global _ai_rate_limited_until
    # rate limit 쿨다운 중이면 스킵
    if time.time() < _ai_rate_limited_until:
        return ""
    try:
        from post_generator import get_ai_config, _call_gemini, _call_claude, _call_openai
        config = get_ai_config()
        provider = config.get("provider", "none")
        if provider == "none":
            return ""

        prompt = f"""다음 일본어 중고 명품 상품 정보를 자연스러운 한국어로 번역해주세요.
규칙:
- 카타카나를 한국어 발음으로 음역하지 마세요 (예: ショルダーバッグ→숄더백 금지 → 어깨가방 또는 한국에서 통용되는 자연스러운 표현 사용)
- 의미를 살려 자연스러운 한국어로 번역
- 브랜드명(PRADA, LOUIS VUITTON 등)은 그대로 유지
- 모델명, 영문/숫자 코드는 그대로 유지
- 색상 약어(BLK, GRY, RED, NVY 등)는 그대로 유지
- / 구분자 구조를 유지
- 번역 결과만 출력 (설명 없이)

원문: {text}"""

        # 우선순위: Gemini → OpenAI → Claude (rate limit 분산)
        providers = []
        if config.get("gemini_key"):
            providers.append(("gemini", _call_gemini))
        if config.get("openai_key"):
            providers.append(("openai", _call_openai))
        if config.get("claude_key"):
            providers.append(("claude", _call_claude))

        # 현재 설정된 provider를 맨 앞으로
        providers.sort(key=lambda x: 0 if x[0] == provider else 1)

        for pname, pfunc in providers:
            try:
                result = pfunc(prompt)
                if result and len(result) > 1:
                    return result
            except Exception as e:
                err = str(e)
                if "429" in err or "rate_limit" in err.lower() or "RateLimit" in err:
                    _ai_rate_limited_until = time.time() + 60  # 60초 쿨다운
                    logger.warning(f"⚠️ {pname} rate limit — 60초 대기, 다음 provider 시도")
                    continue
                logger.debug(f"{pname} 번역 실패: {e}")
                continue
    except Exception as e:
        logger.debug(f"AI 번역 실패: {e}")
    return ""


def translate_ja_ko(text: str, retries: int = 3) -> str:
    """
    일본어 → 한국어 번역
    1. AI API 번역 (Gemini/OpenAI/Claude) — 최우선
    2. AI 실패 시 → 사전 번역 + 구글 번역 (폴백)
    """
    if not text or not text.strip():
        return text

    # 캐시 확인
    if text in _cache:
        return _cache[text]

    # 1단계: AI API 번역 (최우선)
    ai_result = _translate_with_ai(text)
    if ai_result and len(ai_result) > 1:
        _cache[text] = ai_result
        return ai_result

    # 2단계: 사전 번역 (AI 실패 시 폴백)
    pre_translated = apply_custom_dict(text)

    import re as _re
    if not _re.search(r'[\u3040-\u30FF\u4E00-\u9FFF]', pre_translated):
        _cache[text] = pre_translated
        return pre_translated

    # 3단계: 구글 번역 (최종 폴백)
    if TRANSLATE_AVAILABLE:
        for attempt in range(retries):
            try:
                result = _translator.translate(pre_translated, src="ja", dest="ko")
                translated = result.text.strip()
                _cache[text] = translated
                return translated
            except Exception as e:
                if attempt < retries - 1:
                    time.sleep(1)
                else:
                    logger.debug(f"구글 번역 실패: {e}")

    _cache[text] = pre_translated
    return pre_translated


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


# ── 빈티지 상품명 사전 번역 ──────────────────
_VINTAGE_DICT = {
    # ── 가방 종류 ──
    "ショルダーバッグ": "숄더백", "トートバッグ": "토트백", "ハンドバッグ": "핸드백",
    "リュック": "백팩", "ポーチ": "파우치", "ボストンバッグ": "보스턴백",
    "クラッチバッグ": "클러치백", "ウエストバッグ": "웨이스트백",
    "ボディバッグ": "바디백", "バックパック": "백팩",
    "バニティバッグ": "베니티백", "ミニバッグ": "미니백",
    "2WAYバッグ": "2WAY백", "3WAYバッグ": "3WAY백",
    "ワンショルダーバッグ": "원숄더백", "メッセンジャーバッグ": "메신저백",
    "ブリーフケース": "서류가방", "セカンドバッグ": "세컨드백",
    "バッグ": "가방",

    # ── 의류 ──
    "ジャケット": "자켓", "コート": "코트", "ブルゾン": "블루종",
    "ダウンジャケット": "다운자켓", "ダウンベスト": "다운베스트",
    "テーラードジャケット": "테일러드자켓", "ライダースジャケット": "라이더스자켓",
    "ステンカラーコート": "스텐카라코트", "トレンチコート": "트렌치코트",
    "ピーコート": "피코트", "ダッフルコート": "더플코트",
    "シャツ": "셔츠", "ブラウス": "블라우스", "Tシャツ": "T셔츠",
    "ポロシャツ": "폴로셔츠", "長袖シャツ": "긴팔셔츠", "半袖シャツ": "반팔셔츠",
    "パンツ": "팬츠", "デニムパンツ": "데님팬츠", "スラックス": "슬랙스",
    "ショートパンツ": "반바지", "スカート": "스커트",
    "ワンピース": "원피스", "ドレス": "드레스",
    "ニット": "니트", "セーター": "스웨터", "カーディガン": "가디건",
    "パーカー": "후드", "スウェット": "스웨트", "ベスト": "베스트",
    "ジップパーカー": "집업후드", "プルオーバー": "풀오버",
    "マフラー": "머플러", "ストール": "스톨", "スカーフ": "스카프",
    "アウター": "아우터", "ウェア": "웨어",

    # ── 신발 ──
    "スニーカー": "스니커즈", "ブーツ": "부츠", "サンダル": "샌들",
    "パンプス": "펌프스", "ローファー": "로퍼", "スリッポン": "슬리폰",
    "ミュール": "뮬", "フラットシューズ": "플랫슈즈",
    "レインブーツ": "레인부츠", "シューズ": "슈즈",

    # ── 시계 ──
    "腕時計": "손목시계", "クォーツ": "쿼츠", "自動巻き": "오토매틱",
    "アナログ": "아날로그", "デジタル": "디지털",

    # ── 악세서리/소품 ──
    "ネックレス": "목걸이", "ブレスレット": "팔찌", "リング": "반지",
    "ピアス": "피어싱", "イヤリング": "귀걸이", "バングル": "뱅글",
    "ベルト": "벨트", "サングラス": "선글라스", "メガネ": "안경",
    "キーケース": "키케이스", "キーリング": "키링", "キーホルダー": "키홀더",
    "財布": "지갑", "長財布": "장지갑", "二つ折り財布": "반지갑",
    "コインケース": "동전지갑", "カードケース": "카드케이스",
    "名刺入れ": "명함케이스", "手帳カバー": "다이어리커버",
    "帽子": "모자", "キャップ": "캡", "ハット": "햇",
    "手袋": "장갑", "グローブ": "글러브",

    # ── 소재 ──
    "レザー": "가죽", "ナイロン": "나일론", "キャンバス": "캔버스",
    "デニム": "데님", "スエード": "스웨이드", "ファー": "퍼",
    "ラフィア": "라피아", "ビニール": "비닐", "サテン": "새틴",
    "コットン": "코튼", "ウール": "울", "シルク": "실크",
    "カシミヤ": "캐시미어", "ポリエステル": "폴리에스터",
    "ラム": "램", "パイソン": "파이썬(뱀가죽)", "クロコ": "크로커다일",
    "エナメル": "에나멜", "パテント": "페이턴트",
    "モノグラム": "모노그램", "ダミエ": "다미에", "エピ": "에피",
    "ヴェルニ": "베르니", "タイガ": "타이가",
    "PVC": "PVC", "GG": "GG",

    # ── 색상 ──
    "ブラック": "블랙", "ホワイト": "화이트", "レッド": "레드",
    "ブルー": "블루", "グリーン": "그린", "ピンク": "핑크",
    "ベージュ": "베이지", "ブラウン": "브라운", "グレー": "그레이",
    "ネイビー": "네이비", "オレンジ": "오렌지", "イエロー": "옐로",
    "パープル": "퍼플", "ゴールド": "골드", "シルバー": "실버",
    "カーキ": "카키", "ボルドー": "보르도", "クリーム": "크림",
    "アイボリー": "아이보리", "ワイン": "와인", "マルチカラー": "멀티컬러",
    "ライトブルー": "라이트블루", "ライトグレー": "라이트그레이",
    "ダークブラウン": "다크브라운",

    # ── 패턴/무늬 ──
    "無地": "무지", "柄": "패턴", "総柄": "올오버패턴",
    "チェック": "체크", "ストライプ": "스트라이프", "ドット": "도트",
    "花柄": "꽃무늬", "迷彩": "카모",

    # ── 기타 ──
    "ビジュー": "비쥬", "ロゴ": "로고", "チェーン": "체인",
    "金具": "금장", "三角ロゴ": "삼각 로고", "トライアングル": "트라이앵글",
    "カナパ": "카나파", "プラダ": "프라다",
    "中古": "중고", "新品": "신품", "未使用": "미사용",

    # ── 사이즈 표기 ──
    "マチ": "폭", "高さ": "높이", "幅": "너비", "持ち手": "손잡이",
    "その他サイズ": "기타 사이즈", "実寸サイズ": "실측사이즈",

    # ── 브랜드 모델명 (번역 안 함 — 원문 유지용) ──
    # 이건 사전에 넣지 않음 (영문은 그대로 유지됨)
}


def translate_vintage_name(name: str) -> str:
    """빈티지 상품명 사전 기반 번역 (구글 번역 불필요)"""
    if not name:
        return name
    result = name
    for ja, ko in _VINTAGE_DICT.items():
        result = result.replace(ja, ko)
    return result