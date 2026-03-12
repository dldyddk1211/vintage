"""iframe 내 에디터 접근 테스트"""
import asyncio, json, sys
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
            permissions=["clipboard-read", "clipboard-write"],
        )
        await ctx.add_cookies(cookies)
        page = await ctx.new_page()

        print(f"1. 글쓰기 페이지 이동...")
        await page.goto(WRITE_URL, wait_until="domcontentloaded", timeout=20000)
        await asyncio.sleep(3)
        print(f"   URL: {page.url}")

        # iframe 접근
        frame = page.frame_locator("iframe[name='cafe_main']")

        print("2. 제목 입력란 대기...")
        try:
            await frame.locator("textarea.textarea_input").first.wait_for(timeout=15000)
            print("   ✅ 제목 입력란 발견!")
        except Exception as e:
            print(f"   ❌ 실패: {e}")
            await asyncio.sleep(10)
            await browser.close()
            return

        print("3. 제목 입력 시도...")
        el = frame.locator("textarea.textarea_input").first
        await el.click()
        await asyncio.sleep(0.5)
        await el.fill("테스트 제목 - 자동입력")
        val = await el.input_value()
        print(f"   제목값: '{val}'")

        print("4. 본문 에디터 찾기...")
        for sel in [".se-content", ".se-section-text", "[contenteditable=true]"]:
            try:
                cnt = await frame.locator(sel).count()
                if cnt > 0:
                    print(f"   ✅ 에디터 발견: {sel} (count={cnt})")
            except:
                pass

        print("5. 등록 버튼 찾기...")
        for sel in ["button.BaseButton--submit", "button:has-text('등록')", "button:has-text('확인')"]:
            try:
                cnt = await frame.locator(sel).count()
                if cnt > 0:
                    print(f"   ✅ 등록 버튼 발견: {sel}")
            except:
                pass

        print("\n✅ 테스트 완료! 10초 후 종료...")
        await asyncio.sleep(10)
        await browser.close()

asyncio.run(main())
