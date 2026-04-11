import os
import platform

# .env 파일 로드 (있으면)
_env_path = os.path.join(os.path.dirname(__file__), ".env")
if os.path.exists(_env_path):
    with open(_env_path, encoding="utf-8") as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _k, _v = _line.split("=", 1)
                os.environ.setdefault(_k.strip(), _v.strip())

# =============================================
# 전체 프로젝트 설정값
# =============================================

# ── 서버 설정 ──────────────────────────────
SERVER_HOST = "0.0.0.0"
SERVER_PORT = 3002
URL_PREFIX  = ""   # 대시보드 접속 경로 (루트)

# ── Xebio 스크래핑 설정 ────────────────────
XEBIO_BASE_URL  = "https://www.supersports.com/ja-jp/xebio"
XEBIO_DOMAIN    = "https://www.supersports.com"   # 링크 조합용 도메인
XEBIO_SALE_PATH = "/c/xb_sale"           # セール 카테고리 경로 (확인 후 수정)
TARGET_BRAND    = "NIKE"                  # 필터할 브랜드

# 한 페이지당 최대 상품 수 (사이트 기본값)
PRODUCTS_PER_PAGE = 48

# 스크래핑 딜레이 (초) - 서버 부하 방지
SCRAPE_DELAY = 1.5

# ── 자동 스케줄 설정 ──────────────────────
AUTO_SCHEDULE_HOUR   = 9    # 매일 오전 9시 자동 실행
AUTO_SCHEDULE_MINUTE = 0

# ── 환율 설정 ─────────────────────────────
EXCHANGE_API_URL = "https://api.exchangerate-api.com/v4/latest/JPY"
# ── 가격 계산 설정 ──────────────────────────
MARGIN_RATE                = 1.2    # 마진율 (1.2 = 20%) - 대시보드에서 변경 가능

# 일본 내부 비용 (엔화)
JP_FREE_SHIPPING_THRESHOLD = 3980   # 이 금액 이상이면 일본 내 배송 무료
JP_DOMESTIC_SHIPPING       = 550    # 일본 내 배송료 (엔)
JP_PLATFORM_FEE_RATE       = 0.03   # 일본 업체 수수료 3%
JP_INTL_SHIPPING           = 1500   # 국제 배송비 (엔)
EXCHANGE_RATE_MARKUP       = 1.015  # 송금 환율 마진 1.5%

# ── AI API 설정 ─────────────────────────
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
GEMINI_API_KEY    = os.environ.get("GEMINI_API_KEY", "")
OPENAI_API_KEY    = os.environ.get("OPENAI_API_KEY", "")
AI_PROVIDER       = os.environ.get("AI_PROVIDER", "gemini")  # "gemini" | "claude" | "openai" | "none"

# ── 텔레그램 알림 설정 ─────────────────────
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")   # @BotFather에서 발급
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID", "")     # 개인 또는 그룹 채팅 ID

# ── 네이버 카페 설정 ──────────────────────
CAFE_URL       = "https://cafe.naver.com/sohosupport"
CAFE_ID        = "28938799"        # 카페 고유 ID
CAFE_MENU_ID   = "100"            # 게시판 메뉴 ID
CAFE_MENU_NAME = "브랜드 구매대행"    # 게시판 이름
CAFE_MY_NICKNAME = "서포트센터장"  # 내 닉네임 (내 글은 알림 제외)

# ── 네이버 블로그 설정 ─────────────────────
BLOG_ID        = ""                # 블로그 아이디 (예: myblogid)
BLOG_CATEGORY  = ""                # 블로그 카테고리 (선택)

# ── 네이버 쿠키 기반 로그인 ──────────────
NAVER_COOKIE_PATH = "naver_cookies.json"   # 쿠키 저장 경로
NAVER_LOGIN_TIMEOUT = 120                   # 수동 로그인 대기 시간 (초)

# ── 로그인 설정 ──────────────────────────
APP_ENV = "production" if platform.system() == "Darwin" else "test"

LOGIN_USERS = {
    "admin": "0000",            # 관리자 계정 (기본값)
}

# .env에서 관리자 비밀번호 로드
import os as _os
_env_path = _os.path.join(_os.path.dirname(__file__), ".env")
if _os.path.exists(_env_path):
    with open(_env_path, encoding="utf-8") as _f:
        for _line in _f:
            if _line.strip().startswith("ADMIN_PASSWORD="):
                LOGIN_USERS["admin"] = _line.strip().split("=", 1)[1].strip()
                break

SECRET_KEY = "jp-sourcing-secret-key-change-me"  # 세션 암호화 키 (운영 시 변경)

# ── 출력 경로 (외부 저장소 기반) ─────────────
from data_manager import get_path

OUTPUT_DIR = get_path("outputs")
IMAGE_DIR  = os.path.join(get_path("outputs"), "images")
LOG_DIR    = get_path("logs")
DB_DIR     = get_path("db")