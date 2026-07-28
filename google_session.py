"""A reusable Google search session.

`google_search_html.fetch_google_html` launches and tears down a whole browser per
query, which costs several seconds each time — fine for one search, wasteful across
a thousand. `GoogleSession` keeps one persistent context open and reuses it, which
also helps evade blocking: a single warm profile accumulating cookies looks far more
like a real user than a thousand cold browser launches.

    with GoogleSession(headless=True) as s:
        html = s.search("iPhone 16 Pro")
"""

from __future__ import annotations

from pathlib import Path

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError, sync_playwright
from playwright_stealth import Stealth

from google_search_html import (
    CHROME_HEADERS,
    CHROME_UA,
    DEFAULT_PROFILE_DIR,
    NAV_ONLY_HEADERS,
    STEALTH_INIT_SCRIPT,
    CaptchaError,
    build_omnibox_url,
    handle_consent,
    is_captcha_page,
    scroll_to_bottom,
)


class GoogleSession:
    def __init__(
        self,
        headless: bool = True,
        reject_cookies: bool = True,
        profile_dir: Path = DEFAULT_PROFILE_DIR,
        browser_validation: str | None = None,
        scroll: bool = True,
    ):
        self.headless = headless
        self.reject_cookies = reject_cookies
        self.profile_dir = Path(profile_dir)
        self.scroll = scroll
        self._consent_done = False

        self._nav_headers = dict(NAV_ONLY_HEADERS)
        if browser_validation:
            self._nav_headers["X-Browser-Validation"] = browser_validation

        self._stealth = None
        self._pw = None
        self._context = None
        self._page = None

    def __enter__(self) -> "GoogleSession":
        self.profile_dir.mkdir(parents=True, exist_ok=True)
        self._stealth = Stealth()
        self._pw = self._stealth.use_sync(sync_playwright()).__enter__()

        launch_kwargs = {
            "user_data_dir": str(self.profile_dir),
            "headless": self.headless,
            "args": [
                "--disable-blink-features=AutomationControlled",
                "--disable-features=IsolateOrigins,site-per-process",
            ],
            "viewport": {"width": 1440, "height": 900},
            "locale": "en-GB",
            "timezone_id": "Europe/London",
            "extra_http_headers": dict(CHROME_HEADERS),
        }
        try:
            self._context = self._pw.chromium.launch_persistent_context(channel="chrome", **launch_kwargs)
        except Exception:
            self._context = self._pw.chromium.launch_persistent_context(user_agent=CHROME_UA, **launch_kwargs)

        def _add_nav_headers(route, request):
            if request.resource_type == "document" and request.is_navigation_request():
                route.continue_(headers={**request.all_headers(), **self._nav_headers})
            else:
                route.continue_()

        self._context.route("**/*", _add_nav_headers)
        self._context.add_init_script(STEALTH_INIT_SCRIPT)
        self._page = self._context.pages[0] if self._context.pages else self._context.new_page()
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def restart(self, headless: bool | None = None) -> "GoogleSession":
        """Tear the browser down and bring it back up, optionally switching modes.

        Used to escalate from headless to headed after Google starts blocking:
        headless Chrome is the strongest bot signal it keys on, so a visible window
        will often clear an interstitial that no amount of backoff will.
        """
        self.close()
        if headless is not None:
            self.headless = headless
        self._consent_done = False
        return self.__enter__()

    def close(self) -> None:
        if self._context is not None:
            try:
                self._context.close()
            except Exception:
                pass
        if self._pw is not None:
            try:
                self._pw.__exit__(None, None, None)
            except Exception:
                pass
        self._context = self._pw = self._page = None

    def search(self, query: str, timeout_ms: int = 30000) -> str:
        """Run one search and return the rendered HTML.

        Raises `CaptchaError` when Google serves the reCAPTCHA interstitial.
        """
        if self._page is None:
            raise RuntimeError("GoogleSession used outside its context manager")

        page = self._page
        page.goto(build_omnibox_url(query), wait_until="domcontentloaded", timeout=timeout_ms)

        # Consent only ever appears until the cookie is set for this profile.
        if not self._consent_done:
            if "consent.google" in page.url or page.locator('form[action*="consent.google"]').count():
                handle_consent(page, reject=self.reject_cookies)
                page.wait_for_timeout(1000)
                self._consent_done = True
            if page.locator('div[aria-modal="true"]').count():
                handle_consent(page, reject=self.reject_cookies)
                page.wait_for_timeout(500)
                self._consent_done = True

        try:
            page.wait_for_selector("div#search, div#rso, div#main", timeout=10000)
        except PlaywrightTimeoutError:
            pass

        if self.scroll:
            # Ads and PLA carousels lazy-load; without this the SERP under-reports.
            scroll_to_bottom(page)
            page.wait_for_timeout(400)

        html = page.content()
        if is_captcha_page(html):
            raise CaptchaError(Path(f"<in-memory:{query}>"))
        return html
