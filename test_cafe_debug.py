"""
카페 글쓰기 직접 URL 시도
"""
import asyncio
import json
import os
import sys
sys.stdout.reconfigure(encoding='utf-8')

from playwright.async_api import async_playwright

COOKIE_PATH = "naver_cookies.json"
CAFE_ID = "28938799"
MENU_ID = "100"

# 네이버 카페 글쓰기 직접 URL 후보들
WRITE_URLS = [
    f"https://cafe.naver.com/ca-fe/cafes/{CAFE_ID}/articles/write?boardType=L&menuId={MENU_ID}",
    f"https://cafe.naver.com/f-e/cafes/{CAFE_ID}/articles/write?menuId={MENU_ID}",
    f"https://cafe.naver.com/f-e/cafes/{CAFE_ID}/menus/{MENU_ID}/articles/write",
    f"https://cafe.naver.com/sohosupport?iframe_url=/ArticleWrite.nhn%3Fclubid%3D{CAFE_ID}%26menuid%3D{MENU_ID}",
]


async def main():
    if not os.path.exists(COOKIE_PATH):
        print("쿠키 없음")
        return
    with open(COOKIE_PATH, encoding="utf-8") as f:
        cookies = json.load(f)

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=False, args=["--window-size=1280,900"])
        ctx = await browser.new_context(
            viewport={"width": 1280, "height": 900},
            locale="ko-KR",
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        )
        await ctx.add_cookies(cookies)
        page = await ctx.new_page()

        # 글쓰기 직접 URL 시도
        for url in WRITE_URLS:
            print(f"\n시도: {url}")
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=15000)
                await asyncio.sleep(3)
                final_url = page.url
                print(f"  최종 URL: {final_url}")

                # 에디터가 나타나는지 확인
                for sel in ["textarea.textarea_input", "textarea[placeholder*='제목']", "input[placeholder*='제목']", ".se-content", "[contenteditable=true]"]:
                    try:
                        cnt = await page.locator(sel).count()
                        if cnt > 0:
                            print(f"  ✅ 에디터 요소 발견: {sel}")
                    except:
                        pass

                # 페이지 HTML에서 write/editor 관련 키워드 확인
                html = await page.content()
                for keyword in ["textarea", "contenteditable", "editor", "se-content", "article-write"]:
                    if keyword in html.lower():
                        print(f"  HTML에 '{keyword}' 포함됨")

            except Exception as e:
                print(f"  에러: {e}")

        # 마지막으로 메뉴 페이지에서 href에 write가 포함된 링크 탐색
        menu_url = f"https://cafe.naver.com/f-e/cafes/{CAFE_ID}/menus/{MENU_ID}?viewType=L"
        print(f"\n\n메뉴 페이지에서 글쓰기 관련 요소 탐색: {menu_url}")
        await page.goto(menu_url, wait_until="domcontentloaded", timeout=20000)
        await asyncio.sleep(3)

        # 모든 a 태그의 href 출력 (write/article 포함하는 것만)
        links = await page.evaluate("""
            [...document.querySelectorAll('a')].map(e => ({
                text: e.innerText.trim().substring(0, 40),
                href: e.href || '',
            })).filter(e => e.href.includes('write') || e.href.includes('Write') || e.href.includes('article'))
        """)
        print(f"  write/article 관련 링크: {len(links)}개")
        for l in links:
            print(f"    text='{l['text']}' href='{l['href'][:100]}'")

        # svg, img 등에 포함된 글쓰기 아이콘 버튼 확인
        floating_btns = await page.evaluate("""
            [...document.querySelectorAll('[class*="float"], [class*="Float"], [class*="fixed"], [class*="Fixed"], [class*="fab"], [class*="Fab"], [class*="write"], [class*="Write"]')]
            .map(e => ({
                tag: e.tagName,
                cls: e.className.substring(0, 80),
                text: e.innerText.trim().substring(0, 30),
                visible: e.offsetParent !== null
            }))
        """)
        print(f"\n  floating/write 클래스 요소: {len(floating_btns)}개")
        for b in floating_btns:
            print(f"    <{b['tag']}> cls='{b['cls']}' text='{b['text']}' visible={b['visible']}")

        print("\n15초 후 종료...")
        await asyncio.sleep(15)
        await browser.close()


asyncio.run(main())
