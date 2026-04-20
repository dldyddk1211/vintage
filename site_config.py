"""
site_config.py
사이트 / 카테고리 / 브랜드 설정 — 크롤링 대상 관리
"""

# ── 사이트 & 카테고리 정의 ─────────────────────
# 구조: SITES[site_id] → { name, base_url, categories, brands }
#
# URL 생성: base_url + "/products/?" + urlencode(params) + (brand 선택 시 &brand=코드)
# 예: https://www.supersports.com/ja-jp/xebio/products/?discount=sale
#     https://www.supersports.com/ja-jp/xebio/products/?discount=sale&brand=004278

SITES = {
    "xebio": {
        "name": "제비오 (Xebio)",
        "domain": "https://www.supersports.com",
        "base_url": "https://www.supersports.com/ja-jp/xebio",
        "scraper": "xebio_search",
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
        },
        "brands": {
            "004278": "나이키",
            "007009": "조던",
            "004150": "뉴발란스",
            "004277": "아디다스",
            "004052": "미즈노",
            "004048": "아식스",
            "004065": "노스페이스",
            "005495": "언더아머",
            "004059": "푸마",
            "004044": "데상트",
            "004069": "요넥스",
            "009630": "호카",
        },
    },
    "2ndstreet": {
        "name": "세컨드스트리트 (2nd STREET)",
        "domain": "https://www.2ndstreet.jp",
        "base_url": "https://www.2ndstreet.jp",
        "source_type": "vintage",
        "categories": {
            "950001": {"name": "가방", "name_ja": "バッグ", "params": {"category": "950001"}},
            "950002": {"name": "의류", "name_ja": "衣類", "params": {"category": "950002"}},
            "950003": {"name": "신발", "name_ja": "シューズ", "params": {"category": "950003"}},
            "950004": {"name": "시계", "name_ja": "時計", "params": {"category": "950004"}},
            "950005": {"name": "악세서리", "name_ja": "アクセサリー", "params": {"category": "950005"}},
        },
        "brands": {
            "000931": "LOUIS VUITTON",
            "000363": "GUCCI",
            "000299": "CHROME HEARTS",
            "000279": "CHANEL",
            "000615": "PRADA",
            "000395": "HERMES",
            "000266": "CELINE",
            "000339": "FENDI",
            "000256": "BALENCIAGA",
            "000462": "LOEWE",
            "000648": "SAINT LAURENT",
            "000592": "PORTER",
            "000260": "BOTTEGA VENETA",
            "000285": "Christian Dior",
            "000257": "BURBERRY",
            "001355": "FERRAGAMO",
            "kw:Issey Miyake": "Issey Miyake",
            "001545": "Tiffany & Co.",
        },
    },
    "kindal": {
        "name": "킨달 (KINDAL)",
        "domain": "https://www.kindal.jp",
        "base_url": "https://www.kindal.jp",
        "source_type": "vintage",
        "categories": {
            "bag": {"name": "가방", "name_ja": "バッグ", "params": {"category": "bag"}},
            "clothing": {"name": "의류", "name_ja": "衣類", "params": {"category": "clothing"}},
            "shoes": {"name": "신발", "name_ja": "シューズ", "params": {"category": "shoes"}},
            "watch": {"name": "시계", "name_ja": "時計", "params": {"category": "watch"}},
            "accessory": {"name": "악세서리", "name_ja": "アクセサリー", "params": {"category": "accessory"}},
        },
        "brands": {},
    },
    "brandoff": {
        "name": "브랜드오프 (BRAND OFF)",
        "domain": "https://www.brandoff.co.jp",
        "base_url": "https://www.brandoff.co.jp",
        "source_type": "vintage",
        "categories": {
            "bag": {"name": "가방", "name_ja": "バッグ", "params": {"category": "bag"}},
            "clothing": {"name": "의류", "name_ja": "衣類", "params": {"category": "clothing"}},
            "shoes": {"name": "신발", "name_ja": "シューズ", "params": {"category": "shoes"}},
            "watch": {"name": "시계", "name_ja": "時計", "params": {"category": "watch"}},
            "accessory": {"name": "악세서리", "name_ja": "アクセサリー", "params": {"category": "accessory"}},
        },
        "brands": {},
    },
    "komehyo": {
        "name": "코메효 (KOMEHYO)",
        "domain": "https://komehyo.jp",
        "base_url": "https://komehyo.jp",
        "source_type": "vintage",
        "categories": {
            "bag": {"name": "가방", "name_ja": "バッグ", "params": {"category": "bag"}},
            "clothing": {"name": "의류", "name_ja": "衣類", "params": {"category": "clothing"}},
            "watch": {"name": "시계", "name_ja": "時計", "params": {"category": "watch"}},
            "accessory": {"name": "악세서리", "name_ja": "アクセサリー", "params": {"category": "accessory"}},
        },
        "brands": {},
    },
    "kabinet": {
        "name": "카비넷 (KABINET)",
        "domain": "",
        "base_url": "",
        "source_type": "vintage",
        "categories": {},
        "brands": {},
    },
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


def get_brands(site_id: str) -> dict:
    """사이트의 브랜드 목록 반환 {코드: 이름}"""
    site = SITES.get(site_id)
    if not site:
        return {}
    return site.get("brands", {})


def build_url(site_id: str, cat_id: str, brand_code: str = "") -> str:
    """사이트 + 카테고리 + 브랜드로 스크래핑 URL 생성"""
    site = SITES.get(site_id)
    if not site:
        return ""
    cat = site["categories"].get(cat_id)
    if not cat:
        return ""
    from urllib.parse import urlencode
    params = dict(cat["params"])
    if brand_code:
        params["brand"] = brand_code
    return f"{site['base_url']}/products/?{urlencode(params)}"


def get_sites_for_ui() -> list:
    """대시보드 UI용 사이트/카테고리/브랜드 트리 반환"""
    result = []
    for site_id, site in SITES.items():
        cats = []
        for cat_id, cat in site["categories"].items():
            cats.append({
                "id": cat_id,
                "name": cat["name"],
            })
        brands = []
        for code, name in site.get("brands", {}).items():
            brands.append({
                "code": code,
                "name": name,
            })
        result.append({
            "id": site_id,
            "name": site["name"],
            "categories": cats,
            "brands": brands,
        })
    return result
