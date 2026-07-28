"""Search Google (UK) for a term and save the fully-rendered HTML.

Replicates a Chrome-omnibox navigation (`sourceid=chrome&source=chrome.crn.obic`)
as observed in a real HAR, with matching UA + client hints + `x-browser-*` headers.
Handles the EU/UK consent dialog. Cookies persist between runs via a user-data dir.

Usage:
    python google_search_html.py "iPhone 16 Pro"
    python google_search_html.py "iPhone 16 Pro" --headed
    python google_search_html.py "iPhone 16 Pro" \\
        --browser-validation gW2BAPoBimaFKAjp8VA69YjSAbY=
"""

import argparse
import re
import sys
from pathlib import Path

from playwright.sync_api import Page, TimeoutError as PlaywrightTimeoutError, sync_playwright
from playwright_stealth import Stealth


STEALTH_INIT_SCRIPT = """
Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
Object.defineProperty(navigator, 'plugins', {
  get: () => [1, 2, 3, 4, 5].map(() => ({ name: 'Chrome PDF Plugin' })),
});
Object.defineProperty(navigator, 'languages', { get: () => ['en-GB', 'en'] });
window.chrome = window.chrome || { runtime: {} };
const origQ = window.navigator.permissions && window.navigator.permissions.query;
if (origQ) {
  window.navigator.permissions.query = (p) => (
    p.name === 'notifications'
      ? Promise.resolve({ state: Notification.permission })
      : origQ(p)
  );
}
"""

CONSENT_ACCEPT_LABELS = ("Accept all", "I agree", "Agree to all", "Accept the use of cookies")
CONSENT_REJECT_LABELS = ("Reject all", "Reject the use of cookies")


class CaptchaError(RuntimeError):
    """Google served a reCAPTCHA interstitial instead of results."""

    def __init__(self, saved_path: Path):
        self.saved_path = saved_path
        super().__init__(f"Google returned a CAPTCHA page (saved to {saved_path})")


def slugify(text: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", text.strip()).strip("_").lower()
    return slug or "search"


def is_captcha_page(html: str) -> bool:
    return 'id="captcha-form"' in html and "solveSimpleChallenge" in html


def handle_consent(page: Page, reject: bool = True) -> bool:
    """Dismiss Google's cookie consent — full-page redirect or in-page modal.

    Returns True if a consent action was taken.
    """
    labels = CONSENT_REJECT_LABELS if reject else CONSENT_ACCEPT_LABELS

    # Case 1: full-page redirect to consent.google.com. Wait for it, then click.
    if "consent.google" in page.url:
        try:
            page.wait_for_load_state("domcontentloaded", timeout=10000)
        except PlaywrightTimeoutError:
            pass

    for label in labels:
        # Try button role first (most consent buttons are proper <button>s).
        try:
            btn = page.get_by_role("button", name=label, exact=False)
            if btn.count():
                btn.first.click(timeout=3000)
                page.wait_for_load_state("domcontentloaded", timeout=10000)
                return True
        except Exception:
            pass

        # Some variants use <form> with a submit input labelled the same.
        try:
            form_btn = page.locator(f'form button:has-text("{label}"), input[type=submit][value*="{label}"]').first
            if form_btn.count():
                form_btn.click(timeout=3000)
                page.wait_for_load_state("domcontentloaded", timeout=10000)
                return True
        except Exception:
            pass

    return False


def scroll_to_bottom(page: Page, step_px: int = 800, pause_ms: int = 400, max_steps: int = 30) -> None:
    last_height = page.evaluate("() => document.body.scrollHeight")
    for _ in range(max_steps):
        page.evaluate(f"() => window.scrollBy(0, {step_px})")
        page.wait_for_timeout(pause_ms)
        new_height = page.evaluate("() => document.body.scrollHeight")
        at_bottom = page.evaluate(
            "() => (window.innerHeight + window.scrollY) >= document.body.scrollHeight - 2"
        )
        if at_bottom and new_height == last_height:
            break
        last_height = new_height


DEFAULT_PROFILE_DIR = Path.home() / ".cache" / "google_search_html_profile"

# Captured from a real Chrome 150 macOS ARM omnibox request (HAR analysis).
CHROME_UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36"
)

# Applied to every request. Only headers that are genuinely constant across all
# request types belong here — see NAV_ONLY_HEADERS for the rest.
CHROME_HEADERS = {
    "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
}

# Chrome sends these ONLY on the top-level document navigation. Putting them in
# extra_http_headers stamps them onto every subresource and XHR too, so Google sees
# fetch/XHR requests claiming `Sec-Fetch-Dest: document` — a contradiction no real
# browser produces, and a reliable bot tell. They are injected per-request instead.
#
# Sec-Ch-Ua* and Accept are deliberately absent: real Chrome derives them itself and
# varies Accept by resource type. Overriding them can only introduce a mismatch.
NAV_ONLY_HEADERS = {
    # Chrome-proprietary headers Google validates. We can't sign x-browser-validation
    # ourselves — a real Chrome binary generates it — but sending a captured value is
    # strictly better than sending nothing.
    "X-Browser-Channel": "stable",
    "X-Browser-Copyright": "Copyright 2026 Google LLC. All Rights Reserved.",
    "X-Browser-Year": "2026",
}


def build_omnibox_url(query: str) -> str:
    """Match the URL shape Chrome sends when you type a query in the address bar.
    Google trusts requests with sourceid=chrome + source=chrome.crn.obic more than
    a search-box submit from google.com (source=hp). We omit `udm=50` (Shopping tab)
    and `aep=48` from the raw HAR so the default 'All' (web) tab is returned."""
    q = query.replace(" ", "+")
    return (
        f"https://www.google.com/search?q={q}"
        "&sourceid=chrome&ie=UTF-8&source=chrome.crn.obic"
    )


def fetch_google_html(
    query: str,
    output_path: Path,
    headless: bool = True,
    reject_cookies: bool = True,
    profile_dir: Path = DEFAULT_PROFILE_DIR,
    browser_validation: str | None = None,
) -> Path:
    """Replicates a Chrome-omnibox navigation to google.com/search as closely as
    Playwright allows. Uses a persistent user-data dir so cookies (NID, CONSENT)
    accumulate across runs."""
    profile_dir.mkdir(parents=True, exist_ok=True)

    nav_headers = dict(NAV_ONLY_HEADERS)
    if browser_validation:
        nav_headers["X-Browser-Validation"] = browser_validation

    stealth = Stealth()
    with stealth.use_sync(sync_playwright()) as p:
        launch_kwargs = {
            "user_data_dir": str(profile_dir),
            "headless": headless,
            "args": [
                "--disable-blink-features=AutomationControlled",
                "--disable-features=IsolateOrigins,site-per-process",
            ],
            "viewport": {"width": 1440, "height": 900},
            "locale": "en-GB",
            "timezone_id": "Europe/London",
            "extra_http_headers": dict(CHROME_HEADERS),
        }
        # Prefer the real Chrome binary: it emits correct Sec-Ch-Ua/Accept headers and a
        # matching UA on its own. Only spoof the UA on the bundled-Chromium fallback,
        # where the real UA would say "HeadlessChrome"/"Chromium".
        try:
            context = p.chromium.launch_persistent_context(channel="chrome", **launch_kwargs)
        except Exception:
            context = p.chromium.launch_persistent_context(user_agent=CHROME_UA, **launch_kwargs)

        # Stamp the Chrome-only navigation headers onto the top-level document request
        # only, exactly as Chrome does.
        def _add_nav_headers(route, request):
            if request.resource_type == "document" and request.is_navigation_request():
                route.continue_(headers={**request.all_headers(), **nav_headers})
            else:
                route.continue_()

        context.route("**/*", _add_nav_headers)

        context.add_init_script(STEALTH_INIT_SCRIPT)
        page = context.pages[0] if context.pages else context.new_page()

        # Go straight to the omnibox-style URL — no homepage warm-up, no search-box
        # typing, because the omnibox request is what Google trusts.
        page.goto(build_omnibox_url(query), wait_until="domcontentloaded", timeout=30000)

        # Consent — full-page redirect variant.
        if "consent.google" in page.url or page.locator('form[action*="consent.google"]').count():
            print(f"Consent screen detected — clicking '{'Reject all' if reject_cookies else 'Accept all'}'", file=sys.stderr)
            handle_consent(page, reject=reject_cookies)
            page.wait_for_timeout(1000)

        # Consent — in-page modal variant.
        if page.locator('div[aria-modal="true"]').count():
            handle_consent(page, reject=reject_cookies)
            page.wait_for_timeout(500)

        try:
            page.wait_for_selector("div#search, div#rso, div#main", timeout=10000)
        except PlaywrightTimeoutError:
            pass

        # Step 4: scroll to trigger lazy-loaded content.
        scroll_to_bottom(page)
        page.wait_for_timeout(500)

        # Step 5: save HTML.
        html = page.content()
        output_path.write_text(html, encoding="utf-8")

        context.close()

    if is_captcha_page(html):
        raise CaptchaError(output_path)

    return output_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch and save the HTML of a Google search results page (UK).")
    parser.add_argument("query", help="The search term to query on Google.")
    parser.add_argument(
        "--output", "-o", default=None,
        help="Output HTML path. Defaults to google_<slug>.html in the current directory.",
    )
    parser.add_argument(
        "--headed", action="store_true",
        help="Run with a visible browser window.",
    )
    parser.add_argument(
        "--accept-cookies", action="store_true",
        help="Click 'Accept all' on the consent screen instead of the default 'Reject all'.",
    )
    parser.add_argument(
        "--profile-dir", default=str(DEFAULT_PROFILE_DIR),
        help="Persistent Chrome user-data directory (cookies persist between runs).",
    )
    parser.add_argument(
        "--browser-validation", default=None,
        help=(
            "Value of the 'x-browser-validation' header captured from a working Chrome "
            "request (DevTools → Network → the /search request → Request Headers). "
            "Chrome-only header that Google validates server-side."
        ),
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output = Path(args.output) if args.output else Path.cwd() / f"google_{slugify(args.query)}.html"
    output.parent.mkdir(parents=True, exist_ok=True)

    kwargs = dict(
        reject_cookies=not args.accept_cookies,
        profile_dir=Path(args.profile_dir),
        browser_validation=args.browser_validation,
    )

    try:
        saved = fetch_google_html(args.query, output, headless=not args.headed, **kwargs)
    except CaptchaError as exc:
        if args.headed:
            print(f"ERROR: {exc}", file=sys.stderr)
            return 2
        # Headless Chrome is the single strongest bot signal Google keys on. A visible
        # window usually clears the interstitial on the first retry.
        print("CAPTCHA in headless mode — retrying with a visible window…", file=sys.stderr)
        try:
            saved = fetch_google_html(args.query, output, headless=False, **kwargs)
        except CaptchaError as exc2:
            print(f"ERROR: {exc2}", file=sys.stderr)
            return 2

    print(f"Saved {saved} ({saved.stat().st_size:,} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
