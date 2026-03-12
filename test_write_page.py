"""글쓰기 페이지 에디터 구조 확인"""
import asyncio, json, os, sys
sys.stdout.reconfigure(encoding='utf-8')
from playwright.async_api import async_playwright

COOKIE_PATH = "naver_cookies.json"
WRITE_URL = "https://cafe.naver.com/f-e/cafes/28938799/articles/write?menuId=100"

async def main():
    with open(COOKIE_PATH, encoding="utf-8") as f:
        cookies = json.load(f)

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=False, args=["--window-size=1280,900"])
        ctx = await browser.new_context(
            viewport={"width": 1280, "height": 900}, locale="ko-KR",
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        )
        await ctx.add_cookies(cookies)
        page = await ctx.new_page()

        print(f"이동: {WRITE_URL}")
        await page.goto(WRITE_URL, wait_until="domcontentloaded", timeout=20000)
        await asyncio.sleep(5)
        print(f"현재 URL: {page.url}")

        # 모든 input, textarea, contenteditable 요소 탐색
        print("\n=== input/textarea 요소 ===")
        inputs = await page.evaluate("""
            [...document.querySelectorAll('input, textarea')].map(e => ({
                tag: e.tagName, type: e.type||'', cls: e.className.substring(0,80),
                placeholder: (e.placeholder||'').substring(0,40), name: e.name||'',
                id: e.id||'', visible: e.offsetParent !== null
            }))
        """)
        for el in inputs:
            print(f"  <{el['tag']}> type={el['type']} cls='{el['cls']}' ph='{el['placeholder']}' name='{el['name']}' id='{el['id']}' visible={el['visible']}")

        print("\n=== contenteditable 요소 ===")
        editables = await page.evaluate("""
            [...document.querySelectorAll('[contenteditable=true]')].map(e => ({
                tag: e.tagName, cls: e.className.substring(0,80),
                role: e.getAttribute('role')||'', text: e.innerText.trim().substring(0,30),
                visible: e.offsetParent !== null
            }))
        """)
        for el in editables:
            print(f"  <{el['tag']}> cls='{el['cls']}' role='{el['role']}' text='{el['text']}' visible={el['visible']}")

        # iframe 확인
        print(f"\n=== iframe ({len(page.frames)}개) ===")
        for i, f in enumerate(page.frames):
            print(f"  [{i}] name='{f.name}' url='{f.url[:80]}'")
            if f != page.main_frame and 'about:blank' not in f.url:
                try:
                    inner = await f.evaluate("""
                        [...document.querySelectorAll('input, textarea, [contenteditable=true]')].map(e => ({
                            tag: e.tagName, cls: e.className.substring(0,60),
                            placeholder: (e.placeholder||'').substring(0,40),
                        }))
                    """)
                    for el in inner:
                        print(f"    iframe> <{el['tag']}> cls='{el['cls']}' ph='{el['placeholder']}'")
                except:
                    pass

        # 게시판 선택 드롭다운 확인
        print("\n=== 게시판 선택 관련 ===")
        board_els = await page.evaluate("""
            [...document.querySelectorAll('[class*="board"], [class*="Board"], [class*="category"], [class*="Category"], [class*="menu"], [class*="Menu"], select')].map(e => ({
                tag: e.tagName, cls: e.className.substring(0,80), text: e.innerText.trim().substring(0,40),
                visible: e.offsetParent !== null
            })).filter(e => e.visible)
        """)
        for el in board_els[:15]:
            print(f"  <{el['tag']}> cls='{el['cls']}' text='{el['text']}'")

        print("\n20초 후 종료...")
        await asyncio.sleep(20)
        await browser.close()

asyncio.run(main())
