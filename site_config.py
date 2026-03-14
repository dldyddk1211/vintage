"""
site_config.py
사이트 / 카테고리 설정 — 크롤링 대상 관리
"""

# ── 사이트 & 카테고리 정의 ─────────────────────
# 구조: SITES[site_id] → { name, base_url, categories: { cat_id: {name, params} } }
#
# URL 생성: base_url + "/products/?" + urlencode(params)
# 예: https://www.supersports.com/ja-jp/xebio/products/?discount=sale
#     https://www.supersports.com/ja-jp/xebio/products/?category=running

SITES = {
    "xebio": {
        "name": "제비오 (Xebio)",
        "domain": "https://www.supersports.com",
        "base_url": "https://www.supersports.com/ja-jp/xebio",
        "scraper": "xebio_search",          # 사용할 스크래퍼 모듈
        "categories": {
            "sale": {
                "name": "세일",
                "name_ja": "セール",
                "params": {"discount": "sale"},
            },
            "running": {
                "name": "런닝",
                "name_ja": "ランニング",
                "params": {"category": "running"},
            },
            "soccer-futsal": {
                "name": "축구/풋살",
                "name_ja": "サッカー・フットサル",
                "params": {"category": "soccer-futsal"},
            },
            "basketball": {
                "name": "농구",
                "name_ja": "バスケットボール",
                "params": {"category": "basketball"},
            },
            "tennis": {
                "name": "테니스",
                "name_ja": "テニス",
                "params": {"category": "tennis"},
            },
            "golf": {
                "name": "골프",
                "name_ja": "ゴルフ",
                "params": {"category": "golf"},
            },
            "training": {
                "name": "트레이닝",
                "name_ja": "トレーニング",
                "params": {"category": "training"},
            },
            # ── 세일 + 브랜드별 ──
            "sale-nike": {
                "name": "세일 > 나이키",
                "name_ja": "セール > ナイキ",
                "params": {"discount": "sale", "brand": "004278"},
            },
            "sale-jordan": {
                "name": "세일 > 조던",
                "name_ja": "セール > ジョーダン",
                "params": {"discount": "sale", "brand": "007009"},
            },
            "sale-newbalance": {
                "name": "세일 > 뉴발란스",
                "name_ja": "セール > ニューバランス",
                "params": {"discount": "sale", "brand": "004150"},
            },
            "sale-adidas": {
                "name": "세일 > 아디다스",
                "name_ja": "セール > アディダス",
                "params": {"discount": "sale", "brand": "004277"},
            },
            "sale-mizuno": {
                "name": "세일 > 미즈노",
                "name_ja": "セール > ミズノ",
                "params": {"discount": "sale", "brand": "004052"},
            },
            "sale-asics": {
                "name": "세일 > 아식스",
                "name_ja": "セール > アシックス",
                "params": {"discount": "sale", "brand": "004048"},
            },
            "sale-northface": {
                "name": "세일 > 노스페이스",
                "name_ja": "セール > ノースフェイス",
                "params": {"discount": "sale", "brand": "004065"},
            },
            "sale-underarmour": {
                "name": "세일 > 언더아머",
                "name_ja": "セール > アンダーアーマー",
                "params": {"discount": "sale", "brand": "005495"},
            },
            "sale-puma": {
                "name": "세일 > 푸마",
                "name_ja": "セール > プーマ",
                "params": {"discount": "sale", "brand": "004059"},
            },
            "sale-descente": {
                "name": "세일 > 데상트",
                "name_ja": "セール > デサント",
                "params": {"discount": "sale", "brand": "004044"},
            },
            "sale-yonex": {
                "name": "세일 > 요넥스",
                "name_ja": "セール > ヨネックス",
                "params": {"discount": "sale", "brand": "004069"},
            },
            "sale-hoka": {
                "name": "세일 > 호카",
                "name_ja": "セール > ホカ",
                "params": {"discount": "sale", "brand": "009630"},
            },
        },
    },
    # ── 향후 추가 사이트 ──
    # "abc_mart": {
    #     "name": "ABC마트",
    #     "domain": "https://www.abc-mart.net",
    #     "base_url": "https://www.abc-mart.net",
    #     "scraper": "abc_search",
    #     "categories": {
    #         "running": {"name": "런닝", "params": {...}},
    #         "sneakers": {"name": "스니커즈", "params": {...}},
    #     },
    # },
}


def get_site(site_id: str) -> dict:
    """사이트 설정 반환"""
    return SITES.get(site_id)


def get_category(site_id: str, cat_id: str) -> dict:
    """카테고리 설정 반환"""
    site = SITES.get(site_id)
    if not site:
        return None
    return site["categories"].get(cat_id)


def build_url(site_id: str, cat_id: str) -> str:
    """사이트 + 카테고리로 스크래핑 URL 생성"""
    site = SITES.get(site_id)
    if not site:
        return ""
    cat = site["categories"].get(cat_id)
    if not cat:
        return ""
    from urllib.parse import urlencode
    return f"{site['base_url']}/products/?{urlencode(cat['params'])}"


def get_sites_for_ui() -> list:
    """대시보드 UI용 사이트/카테고리 트리 반환"""
    result = []
    for site_id, site in SITES.items():
        cats = []
        for cat_id, cat in site["categories"].items():
            cats.append({
                "id": cat_id,
                "name": cat["name"],
            })
        result.append({
            "id": site_id,
            "name": site["name"],
            "categories": cats,
        })
    return result
