# =============================================
# JP 소싱 대시보드 - 설치 및 실행 가이드
# =============================================

# 1. 라이브러리 설치
pip install -r requirements.txt

# 2. Playwright 브라우저 설치 (최초 1회)
playwright install chromium

# 3. config.py 수정 (필수!)
#    - NAVER_ID, NAVER_PW 본인 계정으로 변경
#    - CAFE_URL 본인 카페 주소로 변경
#    - CAFE_MENU_NAME 올릴 게시판 이름으로 변경

# 4. 서버 실행
python app.py

# 5. 브라우저에서 접속
# http://yaglobal.iptime.org:3000/jp_sourcing


# =============================================
# 폴더 구조
# =============================================
# xebio_cafe_uploader/
# ├── app.py              ← Flask 서버 (메인 실행 파일)
# ├── scraper.py          ← Xebio 스크래핑
# ├── cafe_uploader.py    ← 네이버 카페 업로드
# ├── exchange.py         ← 엔화 환율 계산
# ├── config.py           ← 설정값 (계정, URL 등)
# ├── requirements.txt    ← 필요 라이브러리
# ├── templates/
# │   └── dashboard.html  ← 대시보드 화면
# └── output/
#     └── latest.json     ← 최신 수집 결과


# =============================================
# 스크래핑 순서
# =============================================
# 1. Xebio 메인 (https://www.supersports.com/ja-jp/xebio) 접속
# 2. セール 카테고리 클릭
# 3. ブランドで絞り込む → NIKE 선택
# 4. 필터 적용된 3029개 상품 전체 수집


# =============================================
# ⚠️ 주의사항
# =============================================
# - 네이버 로그인 시 캡차/2단계 인증이 걸릴 수 있음
#   → headless=False 로 설정하면 창이 보여서 수동 처리 가능
# - Xebio 선택자는 사이트 업데이트 시 변경될 수 있음
#   → 개발자도구(F12)로 실제 선택자 확인 후 scraper.py 수정
# - 과도한 스크래핑은 IP 차단 위험 (SCRAPE_DELAY 조절)