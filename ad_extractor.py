"""Extract sponsored placements from a saved Google SERP.

Two ad formats are recognised, matching how Google marks them up:

* Text ads  — `[data-text-ad]` blocks, labelled "Sponsored"/"Sponsored result".
              Live inside `#tads` (above the organic results) or `#bottomads`
              (below them). Placement is recorded because a top slot is worth
              far more than a bottom one.
* PLA / Shopping — `.pla-unit` cards inside a `[data-pla]` carousel headed
              "Sponsored products". Each card is one product listing.

Destination URLs are recovered from the `adurl=` parameter of Google's `/aclk?`
click-tracking redirect, which is what the advertiser actually paid to send you to.

Usage:
    python ad_extractor.py google_iphone_16_pro.html
    python ad_extractor.py *.html --json out.json
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

from bs4 import BeautifulSoup

PRICE_RE = re.compile(r"[£$€]\s?\d[\d,]*(?:\.\d{2})?")


@dataclass
class TextAd:
    """One `[data-text-ad]` block."""

    title: str = ""
    advertiser: str = ""
    display_url: str = ""
    destination_url: str = ""
    description: str = ""
    placement: str = ""  # "top" | "bottom" | "unknown"
    slot: int = 0  # 1-based rank within its placement group
    sitelinks: list[str] = field(default_factory=list)


@dataclass
class ProductAd:
    """One `.pla-unit` Shopping card."""

    title: str = ""
    price: str = ""
    merchant: str = ""
    destination_url: str = ""
    placement: str = ""  # "top" | "right" | "bottom" | "unknown"
    slot: int = 0
    extras: str = ""  # delivery/condition/returns blurbs


@dataclass
class SerpAds:
    """Everything sponsored on a single SERP, plus the derived per-SERP rates."""

    query: str = ""
    source_file: str = ""
    text_ads: list[TextAd] = field(default_factory=list)
    product_ads: list[ProductAd] = field(default_factory=list)
    organic_count: int = 0
    is_captcha: bool = False

    @property
    def text_ad_count(self) -> int:
        return len(self.text_ads)

    @property
    def pla_count(self) -> int:
        return len(self.product_ads)

    @property
    def total_ads(self) -> int:
        return self.text_ad_count + self.pla_count

    @property
    def has_text_ads(self) -> bool:
        return self.text_ad_count > 0

    @property
    def has_plas(self) -> bool:
        return self.pla_count > 0

    @property
    def top_text_ads(self) -> int:
        return sum(1 for a in self.text_ads if a.placement == "top")

    def to_dict(self) -> dict:
        return {
            "query": self.query,
            "source_file": self.source_file,
            "is_captcha": self.is_captcha,
            "organic_count": self.organic_count,
            "text_ad_count": self.text_ad_count,
            "pla_count": self.pla_count,
            "total_ads": self.total_ads,
            "has_text_ads": self.has_text_ads,
            "has_plas": self.has_plas,
            "top_text_ads": self.top_text_ads,
            "text_ads": [asdict(a) for a in self.text_ads],
            "product_ads": [asdict(a) for a in self.product_ads],
        }


# Card furniture that sits between the price and the seller name.
_MERCHANT_NOISE = re.compile(
    r"^(?:free\b|\+?\s*[£$€]|delivery|returns?|refurbished|used|new\b|pre-owned|"
    r"in stock|out of stock|energy:?|sponsored|by\s|save\s|was\s|from\s|"
    r"\(|\d+(?:\.\d+)?\s*(?:out of|/)\s*5|rating)",
    re.I,
)


def _is_merchant_candidate(part: str) -> bool:
    """True when a card segment plausibly names the seller rather than a price,
    delivery blurb, condition tag or rating."""
    if not part or len(part) > 60:
        return False
    if PRICE_RE.fullmatch(part.replace(" ", "")) or _MERCHANT_NOISE.match(part):
        return False
    # A segment that is mostly digits/punctuation is a price or a review count.
    letters = sum(ch.isalpha() for ch in part)
    return letters >= 2 and letters / len(part) > 0.4


def _clean(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def unwrap_aclk(href: str) -> str:
    """Pull the advertiser's real landing page out of a Google `/aclk?` redirect.

    Falls back to the raw href when there is no `adurl`/`ludocid` payload — some
    PLA cards point at a Google Shopping interstitial instead.
    """
    if not href:
        return ""
    try:
        qs = parse_qs(urlparse(href).query)
    except ValueError:
        return href
    for key in ("adurl", "url", "q"):
        if key in qs and qs[key]:
            return unquote(qs[key][0])
    return href


def _domain(url: str) -> str:
    try:
        host = urlparse(url).netloc.lower()
    except ValueError:
        return ""
    return host[4:] if host.startswith("www.") else host


def _placement_of(node) -> str:
    """Walk up the tree to find which Google ad container this sits in."""
    for parent in node.parents:
        pid = parent.get("id") if hasattr(parent, "get") else None
        if not pid:
            continue
        if pid in ("tads", "tvcap", "taw"):
            return "top"
        if pid == "bottomads":
            return "bottom"
        if pid == "rhs":
            return "right"
    return "unknown"


def _strip_noise(soup: BeautifulSoup) -> None:
    for tag in soup(["script", "style", "noscript", "template"]):
        tag.decompose()


def is_captcha_html(html: str) -> bool:
    return 'id="captcha-form"' in html and "solveSimpleChallenge" in html


def _extract_query(soup: BeautifulSoup, html: str) -> str:
    box = soup.select_one('textarea[name="q"], input[name="q"]')
    if box:
        val = box.get("value") or box.get_text(strip=True)
        if val:
            return _clean(val)
    m = re.search(r"<title>(.*?)\s*-\s*Google (?:Search|Zoeken)", html, re.S | re.I)
    return _clean(m.group(1)) if m else ""


def extract_text_ads(soup: BeautifulSoup) -> list[TextAd]:
    ads: list[TextAd] = []
    seen: set[int] = set()
    by_placement: dict[str, int] = {}

    for node in soup.select("[data-text-ad]"):
        if id(node) in seen:
            continue
        seen.add(id(node))

        headings = [_clean(h.get_text(" ")) for h in node.select("[role=heading]")]
        # "My Ad Centre" is Google's own control link, never the ad's headline.
        headings = [h for h in headings if h and "Ad Centre" not in h and "Ad Center" not in h]
        title = headings[0] if headings else ""

        link = node.select_one('a[href*="aclk"]')
        dest = unwrap_aclk(link.get("href", "")) if link else ""

        # The visible green URL — first http(s) string in the rendered text.
        text = node.get_text(" | ", strip=True)
        m = re.search(r"https?://[^\s|]+", text)
        display_url = m.group(0) if m else ""
        advertiser = _domain(display_url) or _domain(dest)

        # Description: the longest text segment that is not chrome or the headline.
        parts = [_clean(p) for p in text.split("|")]
        skip = {"My Ad Centre", "My Ad Center", "Rating", title, advertiser, display_url}
        body = [p for p in parts if p and p not in skip and not p.startswith("http")]
        description = max(body, key=len) if body else ""

        sitelinks = [
            _clean(a.get_text(" "))
            for a in node.select('a[href*="aclk"]')[1:]
            if _clean(a.get_text(" "))
        ]

        placement = _placement_of(node)
        by_placement[placement] = by_placement.get(placement, 0) + 1

        ads.append(
            TextAd(
                title=title,
                advertiser=advertiser,
                display_url=display_url,
                destination_url=dest,
                description=description,
                placement=placement,
                slot=by_placement[placement],
                sitelinks=sitelinks[:8],
            )
        )
    return ads


def extract_product_ads(soup: BeautifulSoup) -> list[ProductAd]:
    ads: list[ProductAd] = []
    by_placement: dict[str, int] = {}

    for card in soup.select(".pla-unit"):
        text = card.get_text(" | ", strip=True)
        parts = [_clean(p) for p in text.split("|") if _clean(p)]
        if not parts:
            continue

        title = parts[0]
        price_m = PRICE_RE.search(text)
        price = price_m.group(0).replace(" ", "") if price_m else ""

        # Merchant is the first non-price segment after the price. Cards often carry a
        # second price (was-price, monthly instalment, delivery), so "the part right
        # after the price" alone would capture "£40.00" instead of the seller name.
        merchant = ""
        price_idx = next(
            (i for i, p in enumerate(parts)
             if price and price.replace(" ", "") in p.replace(" ", "")),
            None,
        )
        if price_idx is not None:
            for p in parts[price_idx + 1:]:
                if _is_merchant_candidate(p):
                    merchant = p
                    break
        if not merchant:
            merchant = next((p for p in parts[1:] if _is_merchant_candidate(p)), "")

        link = card.select_one('a[href*="aclk"]')
        dest = unwrap_aclk(link.get("href", "")) if link else ""

        placement = _placement_of(card)
        by_placement[placement] = by_placement.get(placement, 0) + 1

        consumed = {title, price, merchant}
        extras = "; ".join(p for p in parts[1:] if p not in consumed)[:200]

        ads.append(
            ProductAd(
                title=title,
                price=price,
                merchant=merchant,
                destination_url=dest,
                placement=placement,
                slot=by_placement[placement],
                extras=extras,
            )
        )
    return ads


def count_organic(soup: BeautifulSoup) -> int:
    """Approximate organic result count — `.g` blocks holding a real headline,
    excluding anything inside an ad container."""
    n = 0
    for block in soup.select("#search .g, #rso .g, #search div[data-hveid] h3"):
        if block.find_parent(attrs={"data-text-ad": True}) or block.find_parent(class_="pla-unit"):
            continue
        if _placement_of(block) in ("top", "bottom", "right"):
            continue
        n += 1
    return n


def extract_ads(html: str, source_file: str = "", query: str = "") -> SerpAds:
    """Parse one SERP's HTML into a `SerpAds` record."""
    if is_captcha_html(html):
        return SerpAds(query=query, source_file=source_file, is_captcha=True)

    soup = BeautifulSoup(html, "lxml")
    _strip_noise(soup)

    return SerpAds(
        query=query or _extract_query(soup, html),
        source_file=source_file,
        text_ads=extract_text_ads(soup),
        product_ads=extract_product_ads(soup),
        organic_count=count_organic(soup),
    )


def extract_file(path: Path) -> SerpAds:
    html = path.read_text(encoding="utf-8", errors="replace")
    return extract_ads(html, source_file=str(path))


def main() -> int:
    ap = argparse.ArgumentParser(description="Extract sponsored results (text ads + PLAs) from saved Google SERPs.")
    ap.add_argument("files", nargs="+", type=Path, help="Saved SERP HTML file(s).")
    ap.add_argument("--json", type=Path, default=None, help="Write full records to this JSON file.")
    args = ap.parse_args()

    records = []
    for path in args.files:
        if not path.exists():
            print(f"skip (missing): {path}", file=sys.stderr)
            continue
        rec = extract_file(path)
        records.append(rec)
        tag = "CAPTCHA" if rec.is_captcha else f"{rec.text_ad_count} text ads, {rec.pla_count} PLAs"
        print(f"{path.name}: {rec.query!r} — {tag}")
        for a in rec.text_ads:
            print(f"    [text/{a.placement}#{a.slot}] {a.advertiser} — {a.title[:60]}")
        for p in rec.product_ads[:5]:
            print(f"    [pla#{p.slot}] {p.price} {p.merchant} — {p.title[:50]}")
        if rec.pla_count > 5:
            print(f"    … and {rec.pla_count - 5} more PLA cards")

    if args.json:
        args.json.write_text(
            json.dumps([r.to_dict() for r in records], indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        print(f"\nWrote {args.json}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
