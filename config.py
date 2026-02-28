# =============================================
# 전체 프로젝트 설정값
# =============================================

# ── 서버 설정 ──────────────────────────────
SERVER_HOST = "0.0.0.0"
SERVER_PORT = 3002
URL_PREFIX  = "/jp_sourcing"   # 대시보드 접속 경로

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

# ── 네이버 카페 설정 ──────────────────────
NAVER_ID       = "your_naver_id"        # ← 실제 아이디로 변경
NAVER_PW       = "your_naver_password"  # ← 실제 비밀번호로 변경
CAFE_URL       = "https://cafe.naver.com/your_cafe"  # ← 카페 주소로 변경
CAFE_MENU_NAME = "일본직구/구매대행"    # ← 올릴 게시판 이름으로 변경

# ── 출력 경로 ─────────────────────────────
OUTPUT_DIR = "output"
IMAGE_DIR  = "output/images"
LOG_DIR    = "logs"