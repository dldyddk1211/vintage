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

# ── PC 역할 설정 ──────────────────────────────
# "server"  = Mac 서버 (웹서버, 쇼핑몰, DB 병합)
# "crawl"   = Windows 수집 PC (상품 크롤링 전용)
# "fresh"   = Windows 최신화 PC (품절 체크 전용)
#
# 설정 방법: 각 PC에서 pc_role.json 파일 생성
#   {"role": "crawl"}  또는  {"role": "fresh"}
import platform as _pf
import json as _jr
import os as _or2

if _pf.system() == "Darwin":
    PC_ROLE = "server"
else:
    PC_ROLE = "crawl"  # 기본값
    _role_file = _or2.path.join(_or2.path.dirname(__file__), "pc_role.json")
    if _or2.path.exists(_role_file):
        try:
            with open(_role_file, "r") as _rf:
                PC_ROLE = _jr.load(_rf).get("role", "crawl")
        except Exception:
            pass

# NAS 내보내기 파일명 (역할별 분리)
NAS_EXPORT_DB = {
    "crawl": "products_crawl.db",
    "fresh": "products_fresh.db",
}

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

# ── AI 상품명/태그 자동 분석 (스크래핑 완료 후) ────
AUTO_AI_ENRICH_ON_SCRAPE = False  # 스크래핑 완료 후 자동 AI 분석 ON/OFF (수집PC에서는 OFF)
AUTO_AI_ENRICH_LIMIT     = 0      # 0 = 제한 없음 (수집된 미분석 상품 전부 처리)

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
    "kabinet": "0000",          # 부관리자 — 카비넷 전용
}

# 부관리자 메뉴 권한 (admin은 전체, 나머지는 지정된 메뉴만)
ADMIN_MENU_ACCESS = {
    "admin": ["vintage", "brand", "kabinet", "setting"],  # 전체
    "kabinet": ["kabinet"],                                # 카비넷만
    # 추가 예: "partner1": ["brand", "kabinet"],
}

# 관리자 비밀번호 로드: NAS 공유 설정 → .env 순서
import os as _os
import json as _json

# 1순위: NAS 공유 폴더의 admin_config.json (모든 디바이스 공유)
try:
    from data_manager import get_nas_path
    _admin_cfg_path = _os.path.join(get_nas_path("db"), "admin_config.json")
    if _os.path.exists(_admin_cfg_path):
        with open(_admin_cfg_path, encoding="utf-8") as _af:
            _admin_cfg = _json.load(_af)
            if _admin_cfg.get("admin_password"):
                LOGIN_USERS["admin"] = _admin_cfg["admin_password"]
            if _admin_cfg.get("secret_key"):
                SECRET_KEY = _admin_cfg["secret_key"]
except Exception:
    pass

# 2순위: 로컬 .env (NAS 없을 때 폴백)
_env_path = _os.path.join(_os.path.dirname(__file__), ".env")
if _os.path.exists(_env_path):
    with open(_env_path, encoding="utf-8") as _f:
        for _line in _f:
            if _line.strip().startswith("ADMIN_PASSWORD="):
                _env_pw = _line.strip().split("=", 1)[1].strip()
                if _env_pw and LOGIN_USERS["admin"] == "0000":
                    LOGIN_USERS["admin"] = _env_pw
                break

SECRET_KEY = "jp-sourcing-secret-key-change-me"  # 세션 암호화 키 (운영 시 변경)

# ── 출력 경로 (외부 저장소 기반) ─────────────
from data_manager import get_path

OUTPUT_DIR = get_path("outputs")
IMAGE_DIR  = os.path.join(get_path("outputs"), "images")
LOG_DIR    = get_path("logs")
DB_DIR     = get_path("db")