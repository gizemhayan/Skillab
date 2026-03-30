"""
Kariyer.net Scraper — Concrete implementation of BaseScraper.

Fetches job search results and full job detail pages from Kariyer.net,
Turkey's primary online employment platform. Implements browser automation
via Selenium 4.x with realistic user behavior (headless=False, random delays,
User-Agent rotation) to bypass anti-bot protections.

Pipeline position:  Stage 1 — Data Extraction
Output models:      JobListing  (search-results pass)
                    JobDetail   (detail-page pass)

Scalability note:  To add LinkedIn or Indeed support, create a new file
                   (e.g. linkedin_scraper.py) that inherits from BaseScraper
                   and follows the same pattern. No other code changes needed.

Author: Skillab Turkey Team
Project: EU Horizon Skill Intelligence Hub
"""

from __future__ import annotations

import os
import random
import re
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import pandas as pd
from bs4 import BeautifulSoup, Tag
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
import undetected_chromedriver as uc

from src.scraper.base_scraper import BaseScraper
from src.utils.models import JobDetail, JobListing
from src.utils.checkpoint_utils import (
    save_checkpoint,
    load_checkpoint,
    append_job_to_excel,
    append_job_to_recovery_csv,
    log_progress,
    beep_alert,
    detect_cloudflare_challenge,
    EXCEL_APPEND_PATH,
    FULL_RECOVERY_CSV_PATH,
)
from src.analyzer.turkish_concepts import TurkishConceptAnalyzer


class KariyerScraper(BaseScraper):
    """
    Kariyer.net concrete scraper using Selenium 4.x for browser automation.

    Handles two distinct scraping passes per full pipeline run:
      1. fetch_jobs()       — paginated search results → List[JobListing]
      2. fetch_job_detail() — individual detail page  → JobDetail

    Realistic user behavior is enforced via:
      - Headless=False (visible browser window for debugging)
      - Random inter-request delays to avoid rate limiting
      - User-Agent rotation to appear as different browsers
      - Element wait strategies with randomised timeouts

    Attributes:
        PLATFORM_NAME: Platform identifier embedded in every model record.
        BASE_URL:      Kariyer.net job search endpoint.
        DETAIL_BASE:   Root URL used to resolve relative href values.
        driver:        Selenium WebDriver instance.
        wait:          WebDriverWait helper for element synchronization.
    """

    PLATFORM_NAME: str = "kariyer.net"
    BASE_URL: str = "https://www.kariyer.net/is-ilanlari"
    DETAIL_BASE: str = "https://www.kariyer.net"

    # User-Agent pool — rotate to appear as different browser profiles
    USER_AGENTS: List[str] = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.7680.165 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.7412.177 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_6_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.7680.165 Safari/537.36",
    ]

    # Randomised delay bounds (seconds) — keeps us below rate-limit thresholds
    _DELAY_INITIAL: tuple[float, float] = (1.0, 2.5)
    _DELAY_PRE_NAVIGATION: tuple[float, float] = (2.0, 6.0)
    _DELAY_BETWEEN_PAGES: tuple[float, float] = (2.0, 4.0)
    _DELAY_BETWEEN_DETAILS: tuple[float, float] = (5.0, 12.0)  # Per job detail fetch
    _DELAY_PER_5_JOBS: tuple[float, float] = (30.0, 30.0)  # EXACTLY 30s after every 5 jobs
    _ELEMENT_WAIT_TIMEOUT: int = 15  # seconds for WebDriverWait

    def __init__(self) -> None:
        super().__init__()  # initialises self.logger via BaseScraper
        self.driver: Optional[uc.Chrome] = None
        self.wait: Optional[WebDriverWait] = None
        self.concept_analyzer: TurkishConceptAnalyzer = TurkishConceptAnalyzer()
        self._init_driver()
        self.logger.info(
            "scraper_initialized",
            platform=self.PLATFORM_NAME,
            base_url=self.BASE_URL,
            browser="Chrome",
        )

    def _init_driver(self) -> None:
        """Initialize undetected Chrome using local binaries only (no downloads)."""
        # Assign a realistic Chrome User-Agent.
        user_agent = random.choice(self.USER_AGENTS)
        chrome_binary = self._resolve_chrome_binary()
        chromedriver_binary = self._resolve_chromedriver_binary()
        
        try:
            # First attempt: try standard initialization
            options = uc.ChromeOptions()
            
            # Keep browser visible to pass interactive anti-bot checks.
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-blink-features=AutomationControlled")
            options.add_argument("--disable-blink-features")
            options.add_argument("--start-maximized")
            options.add_argument(f"user-agent={user_agent}")
            options.add_argument("--acceptinsecureSSLCerts")

            chrome_kwargs: Dict[str, Any] = {
                "options": options,
                "browser_executable_path": str(chrome_binary),
                "use_subprocess": False,
                "suppress_welcome": True,
                "no_sandbox": True,
                "headless": False,
            }
            if chromedriver_binary is not None:
                chrome_kwargs["driver_executable_path"] = str(chromedriver_binary)

            try:
                self.driver = uc.Chrome(**chrome_kwargs)
            except Exception as first_exc:
                # If local/auto resolution fails due to version mismatch, retry with fresh options and version pin.
                self.logger.warning(
                    "webdriver_init_retry_with_version_main",
                    detail=str(first_exc),
                    version_main=146,
                )
                
                # Create fresh options for retry (the old options object cannot be reused)
                options_retry = uc.ChromeOptions()
                options_retry.add_argument("--no-sandbox")
                options_retry.add_argument("--disable-dev-shm-usage")
                options_retry.add_argument("--disable-blink-features=AutomationControlled")
                options_retry.add_argument("--disable-blink-features")
                options_retry.add_argument("--start-maximized")
                options_retry.add_argument(f"user-agent={user_agent}")
                options_retry.add_argument("--acceptinsecureSSLCerts")
                
                chrome_kwargs_retry: Dict[str, Any] = {
                    "options": options_retry,
                    "browser_executable_path": str(chrome_binary),
                    "use_subprocess": False,
                    "suppress_welcome": True,
                    "no_sandbox": True,
                    "headless": False,
                    "version_main": 146,
                }
                if chromedriver_binary is not None:
                    chrome_kwargs_retry["driver_executable_path"] = str(chromedriver_binary)
                
                self.driver = uc.Chrome(**chrome_kwargs_retry)

            self.driver.execute_cdp_cmd(
                "Page.addScriptToEvaluateOnNewDocument",
                {
                    "source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
                },
            )

            self.driver.set_page_load_timeout(30)
            self.wait = WebDriverWait(self.driver, self._ELEMENT_WAIT_TIMEOUT)
            self.logger.info(
                "webdriver_initialized",
                user_agent=user_agent[:50],
                chrome_binary=str(chrome_binary),
                chromedriver_binary=(str(chromedriver_binary) if chromedriver_binary else "auto"),
            )
        except Exception as exc:
            self.logger.error(
                "webdriver_init_failed",
                error_type=type(exc).__name__,
                detail=str(exc),
                exc_info=True,
            )
            raise

    def _rotate_user_agent(self) -> str:
        """Randomize User-Agent before navigation to reduce repeated fingerprints."""
        user_agent = random.choice(self.USER_AGENTS)
        try:
            self.driver.execute_cdp_cmd("Network.enable", {})
            self.driver.execute_cdp_cmd(
                "Network.setUserAgentOverride",
                {"userAgent": user_agent},
            )
        except Exception as exc:
            self.logger.debug("user_agent_rotation_failed", detail=str(exc))
        return user_agent

    def _resolve_chrome_binary(self) -> Path:
        """Resolve local Chrome executable path without network access."""
        env_path = os.getenv("CHROME_BINARY_PATH", "").strip()
        candidates: List[Path] = []
        if env_path:
            candidates.append(Path(env_path))

        candidates.extend(
            [
                Path("C:/Program Files/Google/Chrome/Application/chrome.exe"),
                Path("C:/Program Files (x86)/Google/Chrome/Application/chrome.exe"),
                Path.home() / "AppData/Local/Google/Chrome/Application/chrome.exe",
            ]
        )

        for candidate in candidates:
            if candidate.exists():
                return candidate

        raise FileNotFoundError(
            "Chrome binary not found. Set CHROME_BINARY_PATH in .env to local chrome.exe path."
        )

    def _resolve_chromedriver_binary(self) -> Optional[Path]:
        """Resolve optional local chromedriver path; return None to let uc auto-resolve."""
        env_path = os.getenv("CHROMEDRIVER_PATH", "").strip()
        candidates: List[Path] = []
        if env_path:
            candidates.append(Path(env_path))

        workspace_root = Path(__file__).resolve().parents[2]
        candidates.extend(
            [
                workspace_root / "drivers" / "chromedriver.exe",
                workspace_root / "chromedriver.exe",
                Path("C:/WebDriver/bin/chromedriver.exe"),
                Path.home() / "chromedriver.exe",
            ]
        )

        # Pick up locally cached drivers to avoid online auto-download timeouts.
        home = Path.home()
        candidates.extend(
            sorted(
                home.glob(".wdm/drivers/chromedriver/win64/*/chromedriver-win32/chromedriver.exe"),
                key=lambda p: p.stat().st_mtime,
                reverse=True,
            )
        )
        candidates.extend(
            sorted(
                home.glob(".cache/selenium/chromedriver/win64/*/chromedriver.exe"),
                key=lambda p: p.stat().st_mtime,
                reverse=True,
            )
        )

        for candidate in candidates:
            if candidate.exists():
                return candidate

        return None

    # -----------------------------------------------------------------------
    # Public Interface  (implements BaseScraper)
    # -----------------------------------------------------------------------

    def fetch_jobs(
        self,
        keyword: str,
        page_count: Optional[int] = 1,
        on_page_complete: Optional[Callable[[int, List[JobListing]], None]] = None,
    ) -> List[JobListing]:
        """
        Scrape paginated search results using Selenium browser automation.

        Args:
            keyword:    Turkish or English job title / skill keyword.
            page_count: Number of result pages to traverse. If None, scrape
                        until the last available page.
            on_page_complete: Optional callback fired after each page parse.

        Returns:
            Deduplicated list of JobListing instances ordered by page then
            position on page.
        """
        self.logger.info(
            "fetch_jobs_started",
            keyword=keyword,
            page_count=page_count,
        )

        all_listings: List[JobListing] = []
        seen_urls: set[str] = set()

        time.sleep(random.uniform(*self._DELAY_INITIAL))

        dynamic_mode = page_count is None or page_count <= 0
        max_pages = page_count if not dynamic_mode else None
        consecutive_page_failures = 0
        max_consecutive_page_failures = 5

        page = 1
        while True:
            if max_pages is not None and page > max_pages:
                break

            self.logger.info("fetching_page", keyword=keyword, page=page, total=page_count)
            page_success = False

            try:
                # Build search URL with pagination parameter
                search_url = f"{self.BASE_URL}?kw={keyword}&cp={page}"
                self.logger.debug("navigating_to_url", url=search_url)

                pre_nav_delay = random.uniform(*self._DELAY_PRE_NAVIGATION)
                self.logger.debug("pre_navigation_delay", seconds=round(pre_nav_delay, 2), page=page)
                time.sleep(pre_nav_delay)

                rotated_ua = self._rotate_user_agent()
                self.logger.debug("user_agent_rotated", page=page, user_agent=rotated_ua[:70])
                self.driver.get(search_url)
                
                # Wait for job listings to load
                try:
                    self.wait.until(
                        EC.presence_of_all_elements_located(
                            (By.CSS_SELECTOR, "a.k-ad-card[data-test='ad-card-item']")
                        )
                    )
                    self.logger.debug("job_listings_loaded", page=page)
                except TimeoutException:
                    self.logger.warning("job_listings_timeout", page=page)
                
                # Give page a bit more time for JavaScript to finish rendering
                delay = random.uniform(0.5, 1.2)
                time.sleep(delay)
                
                # Get page source and parse with BeautifulSoup
                page_source = self.driver.page_source
                lower_source = page_source.lower()

                # On 503/no-results blocks, wait 3 minutes then refresh same page and retry.
                if (
                    "503" in page_source
                    or "service unavailable" in lower_source
                    or "sonuç bulunamadı" in lower_source
                ):
                    self.logger.warning(
                        "page_block_detected",
                        keyword=keyword,
                        page=page,
                        reason="503_or_no_results",
                        retry_wait_seconds=180,
                    )
                    time.sleep(180)
                    self.driver.refresh()
                    consecutive_page_failures += 1
                    continue

                soup = BeautifulSoup(page_source, "lxml")
                
                page_listings = self._parse_search_results(soup, keyword, page)

                # Deduplicate across pages
                for listing in page_listings:
                    if listing.url not in seen_urls:
                        seen_urls.add(listing.url)
                        all_listings.append(listing)

                self.logger.info(
                    "page_processed",
                    page=page,
                    new_listings=len(page_listings),
                    cumulative=len(all_listings),
                )
                page_success = True
                consecutive_page_failures = 0

                if on_page_complete:
                    on_page_complete(page, all_listings)

                if dynamic_mode:
                    if not page_listings:
                        self.logger.info(
                            "dynamic_pagination_stopped",
                            keyword=keyword,
                            page=page,
                            reason="empty_page",
                        )
                        break
                    last_page = self._extract_last_page_number(soup)
                    if last_page is not None and page >= last_page:
                        self.logger.info(
                            "dynamic_pagination_stopped",
                            keyword=keyword,
                            page=page,
                            reason="reached_last_page",
                            last_page=last_page,
                        )
                        break
                    if not self._has_next_page(soup, page):
                        self.logger.info(
                            "dynamic_pagination_stopped",
                            keyword=keyword,
                            page=page,
                            reason="no_next_page_link",
                        )
                        break

            except TimeoutException:
                self.logger.error(
                    "page_timeout", keyword=keyword, page=page, timeout_seconds=30
                )
                consecutive_page_failures += 1
            except WebDriverException as exc:
                self.logger.error(
                    "webdriver_error", keyword=keyword, page=page, detail=str(exc)
                )
                consecutive_page_failures += 1
            except Exception as exc:
                self.logger.error(
                    "unexpected_page_error",
                    keyword=keyword,
                    page=page,
                    error_type=type(exc).__name__,
                    detail=str(exc),
                    exc_info=True,
                )
                consecutive_page_failures += 1

            if not page_success and consecutive_page_failures >= max_consecutive_page_failures:
                self.logger.warning(
                    "page_fetch_aborted_after_retries",
                    keyword=keyword,
                    page=page,
                    retries=max_consecutive_page_failures,
                )
                if dynamic_mode:
                    break
                page += 1
                continue

            if not page_success:
                self.logger.warning(
                    "page_retry_same_position",
                    keyword=keyword,
                    page=page,
                    retry_wait_seconds=180,
                )
                time.sleep(180)
                continue

            if max_pages is None or page < max_pages:
                delay = random.uniform(*self._DELAY_BETWEEN_PAGES)
                self.logger.debug("inter_page_delay", seconds=round(delay, 2))
                time.sleep(delay)

            page += 1

        self.logger.info(
            "fetch_jobs_completed",
            keyword=keyword,
            total_listings=len(all_listings),
        )
        return all_listings

    def fetch_job_detail(self, listing: JobListing, job_index: int = 0, current_page: int = 1, keyword: str = "") -> JobDetail:
        """
        Fetch and parse the full job description from an individual detail page via Selenium.
        
        Enhanced with:
        - Auto-retry on 503 errors (60s wait + retry same job)
        - Cloudflare challenge detection + audio alert
        - Checkpoint saving for resume capability
        - Turkish concept extraction

        Args:
            listing: A JobListing previously returned by fetch_jobs().
            job_index: Index in the global job list (for throttling tracking).
            current_page: Current page number for checkpoint saving.
            keyword: Search keyword for checkpoint saving.

        Returns:
            JobDetail always — description and raw skill lists default to empty
            on parse failure so downstream stages can still process the record.
        """
        self.logger.debug("fetching_job_detail", url=listing.url, title=listing.title, index=job_index)

        # Every 5 jobs, rest for 30s to avoid 503 throttling.
        if job_index > 0 and job_index % 5 == 0:
            rest_delay = 30.0
            self.logger.info("anti_503_rest", job_count=job_index, rest_seconds=round(rest_delay, 2))
            time.sleep(rest_delay)

        # Per-page delay: 10 seconds (from user requirements)
        time.sleep(10.0)

        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                rotated_ua = self._rotate_user_agent()
                self.logger.debug("detail_user_agent_rotated", index=job_index, user_agent=rotated_ua[:70])
                self.driver.get(listing.url)
                
                # Wait for detail content to load
                try:
                    self.wait.until(
                        EC.presence_of_all_elements_located(
                            (By.TAG_NAME, "body")
                        )
                    )
                except TimeoutException:
                    self.logger.warning("detail_page_timeout", url=listing.url)
                
                # Give page a bit more time for JavaScript rendering
                delay = random.uniform(0.5, 1.0)
                time.sleep(delay)
                
                page_source = self.driver.page_source
                
                # Check for 503 or Cloudflare challenge
                if "503" in page_source or "Service Unavailable" in page_source:
                    self.logger.warning("503_detected", url=listing.url, retry=retry_count + 1)
                    if retry_count < max_retries - 1:
                        self.logger.info("503_retry_sleeping", seconds=180)
                        time.sleep(180)
                        self.driver.refresh()
                        retry_count += 1
                        continue
                
                # Detect Cloudflare challenge
                if detect_cloudflare_challenge(page_source):
                    self.logger.warning("cloudflare_challenge_detected", url=listing.url)
                    beep_alert("Cloudflare Challenge Detected! Please handle manually.")
                    # User has time to handle the challenge while we beep
                    time.sleep(5)
                    # Try to continue; if page didn't load, retry after waiting
                    if "cf_challenge" in page_source or "checking your browser" in page_source:
                        self.logger.info("cloudflare_retry_after_wait", seconds=180)
                        time.sleep(180)
                        self.driver.refresh()
                        retry_count += 1
                        continue
                
                soup = BeautifulSoup(page_source, "lxml")
                detail = self._parse_detail_page(soup, listing)
                
                # Extract Turkish concepts
                digital_concepts = list(self.concept_analyzer.extract_digital_concepts(detail.full_description))
                green_concepts = list(self.concept_analyzer.extract_green_concepts(detail.full_description))

                # Append to Excel immediately (for resilience)
                job_data = {
                    "platform": detail.listing.platform,
                    "title": detail.listing.title,
                    "company": detail.listing.company,
                    "location": detail.listing.location,
                    "search_keyword": keyword,
                    "url": detail.listing.url,
                    "scraped_at": detail.listing.scraped_at.isoformat() if detail.listing.scraped_at else "",
                    "full_description": detail.full_description,
                    "required_skills_raw": " | ".join(detail.required_skills_raw),
                    "preferred_skills_raw": " | ".join(detail.preferred_skills_raw),
                    "experience_years": detail.experience_years or "",
                    "education_level": detail.education_level or "",
                    "salary_range": detail.salary_range or "",
                    "employment_type": detail.employment_type or "",
                    "department": detail.department or "",
                    "digital_concepts": " | ".join(digital_concepts),
                    "green_concepts": " | ".join(green_concepts),
                }
                
                try:
                    append_job_to_excel(job_data, output_path=EXCEL_APPEND_PATH)
                except Exception as e:
                    self.logger.warning("excel_append_failed", url=listing.url, detail=str(e))

                try:
                    append_job_to_recovery_csv(job_data, output_path=FULL_RECOVERY_CSV_PATH)
                except Exception as e:
                    self.logger.warning("recovery_csv_append_failed", url=listing.url, detail=str(e))
                
                # Save checkpoint for resume capability
                if keyword and current_page > 0:
                    try:
                        save_checkpoint(keyword, current_page, job_index)
                    except Exception as e:
                        self.logger.warning("checkpoint_save_failed", detail=str(e))
                
                # Log progress
                try:
                    log_progress(job_index, 985, listing.company, listing.title)  # 985 is the target
                except Exception as e:
                    self.logger.warning("progress_log_failed", detail=str(e))

                self.logger.debug(
                    "job_detail_fetched",
                    title=listing.title,
                    description_length=len(detail.full_description),
                    raw_skills_count=len(detail.required_skills_raw),
                    digital_concepts=len(digital_concepts),
                    green_concepts=len(green_concepts),
                )
                self.logger.info("saved_to_recovery_csv", file=str(FULL_RECOVERY_CSV_PATH), title=listing.title)
                return detail

            except TimeoutException:
                self.logger.warning("detail_timeout", url=listing.url, retry=retry_count + 1)
                if retry_count < max_retries - 1:
                    self.logger.info("timeout_retry_after_wait", seconds=180)
                    time.sleep(180)
                    self.driver.refresh()
                    retry_count += 1
                    continue
            except WebDriverException as exc:
                self.logger.warning("detail_webdriver_error", url=listing.url, detail=str(exc), retry=retry_count + 1)
                if retry_count < max_retries - 1:
                    retry_count += 1
                    time.sleep(180)
                    self.driver.refresh()
                    continue
            except Exception as exc:
                self.logger.error(
                    "detail_fetch_failed",
                    url=listing.url,
                    error_type=type(exc).__name__,
                    detail=str(exc),
                    exc_info=True,
                )
                if retry_count < max_retries - 1:
                    retry_count += 1
                    time.sleep(180)
                    self.driver.refresh()
                    continue
        
        # Max retries exhausted - return partial model
        self.logger.warning("detail_fetch_max_retries_exhausted", url=listing.url, retries=max_retries)
        return JobDetail(listing=listing)

    def fetch_all_details(
        self,
        listings: List[JobListing],
        max_jobs: Optional[int] = None,
        keyword: str = "",
        current_page: int = 1,
    ) -> List[JobDetail]:
        """
        Batch-fetch detail pages for a list of JobListings with ethical delays and checkpoint support.
        
        Supports resuming from checkpoint if previous scraping was interrupted.

        Args:
            listings: Output of fetch_jobs().
            max_jobs: Cap the number of detail fetches (useful during development).
            keyword: Search keyword for checkpoint saving.
            current_page: Current page number for checkpoint saving.

        Returns:
            List of JobDetail records, same order as input listings.
        """
        targets = listings[:max_jobs] if max_jobs else listings
        total = len(targets)

        # Load checkpoint if available
        checkpoint = load_checkpoint()
        start_index = 0
        if checkpoint:
            self.logger.info(
                "checkpoint_loaded",
                keyword_checkpoint=checkpoint.get("keyword"),
                page_checkpoint=checkpoint.get("page"),
                job_index_checkpoint=checkpoint.get("job_index"),
            )
            if checkpoint.get("keyword") == keyword:
                start_index = checkpoint.get("job_index", 0)
                self.logger.info("resuming_from_checkpoint", start_index=start_index, total_jobs=total)

        self.logger.info("batch_detail_fetch_started", total_jobs=total, start_index=start_index)
        details: List[JobDetail] = []

        for idx, listing in enumerate(targets, start=1):
            # Skip jobs already processed
            if idx <= start_index:
                continue
            
            detail = self.fetch_job_detail(
                listing,
                job_index=idx,
                current_page=current_page,
                keyword=keyword,
            )
            details.append(detail)

            # Every 10 jobs, log progress
            if idx % 10 == 0:
                self.logger.info(
                    "batch_detail_progress",
                    completed=idx,
                    remaining=total - idx,
                )

        self.logger.info("batch_detail_fetch_completed", fetched=len(details), resumed_from=start_index)
        return details

    def deep_scrape_jobs(
        self,
        keyword: str,
        page_count: Optional[int] = 1,
        max_jobs: Optional[int] = None,
        on_page_complete: Optional[Callable[[int, List[JobListing]], None]] = None,
    ) -> pd.DataFrame:
        """
        Run a complete deep scrape and return a structured DataFrame.

        This method first fetches listing cards, then follows every job URL to
        extract full description text and metadata from detail pages.
        
        Enhanced with checkpoint support for resuming interrupted scrapes.

        Args:
            keyword: Search term used in Kariyer.net results.
            page_count: Number of pages to scrape.
            max_jobs: Optional cap for detail-page scraping.

        Returns:
            Pandas DataFrame containing one row per job with full description.
        """
        # Track current page for checkpoint
        current_page = 1
        
        listings = self.fetch_jobs(
            keyword=keyword,
            page_count=page_count,
            on_page_complete=lambda page, listings_so_far: (
                setattr(self, '_current_page', page),
                on_page_complete(page, listings_so_far) if on_page_complete else None
            )[-1],
        )
        
        # Get the last page that was scraped
        current_page = getattr(self, '_current_page', 1)
        
        # Pass keyword and page to fetch_all_details for checkpoint support
        self.fetch_all_details(
            listings=listings,
            max_jobs=max_jobs,
            keyword=keyword,
            current_page=current_page,
        )

        if FULL_RECOVERY_CSV_PATH.exists():
            df = pd.read_csv(FULL_RECOVERY_CSV_PATH, encoding="utf-8-sig")
        else:
            df = pd.DataFrame()

        if not df.empty:
            df["full_description"] = df["full_description"].fillna("").astype(str)
            df = df[df["full_description"].str.strip() != ""].reset_index(drop=True)

        self.logger.info(
            "deep_scrape_completed",
            keyword=keyword,
            page_count=page_count,
            rows=len(df),
        )
        return df

    def _extract_last_page_number(self, soup: BeautifulSoup) -> Optional[int]:
        """Parse pagination controls and return the detected last page number."""
        page_candidates: List[int] = []
        for link in soup.find_all("a", href=True):
            href = link.get("href", "")
            if "cp=" not in href:
                continue
            match = re.search(r"[?&]cp=(\d+)", href)
            if match:
                page_candidates.append(int(match.group(1)))
        return max(page_candidates) if page_candidates else None

    def _has_next_page(self, soup: BeautifulSoup, current_page: int) -> bool:
        """Return True when a next-page navigation link is present."""
        expected_next = current_page + 1
        for link in soup.find_all("a", href=True):
            href = link.get("href", "")
            if re.search(rf"[?&]cp={expected_next}(?:&|$)", href):
                return True
            rel_value = link.get("rel")
            if rel_value and "next" in rel_value:
                return True
            text = link.get_text(" ", strip=True).lower()
            if text in {"next", "sonraki", ">", "›"}:
                return True
        return False

    def close(self) -> None:
        """Close the Selenium WebDriver and release all held resources."""
        if self.driver:
            try:
                self.driver.quit()
                self.logger.info("webdriver_closed", platform=self.PLATFORM_NAME)
            except Exception as exc:
                self.logger.warning("webdriver_close_failed", detail=str(exc))



    def _parse_search_results(
        self,
        soup: BeautifulSoup,
        keyword: str,
        page: int,
    ) -> List[JobListing]:
        """Parse all job listing cards from a search results page."""
        listings: List[JobListing] = []

        # Primary selector — Kariyer.net uses data-test attributes for listing cards
        cards: List[Tag] = soup.find_all(
            "a", {"class": "k-ad-card", "data-test": "ad-card-item"}
        )
        if not cards:
            # Fallback: any anchor pointing to a job detail url
            cards = soup.find_all("a", href=lambda h: h and "/is-ilani/" in h)

        self.logger.debug(
            "cards_found", keyword=keyword, page=page, count=len(cards)
        )

        for card in cards:
            listing = self._extract_listing_from_card(card)
            if listing:
                listings.append(listing)

        return listings

    def _extract_listing_from_card(self, card: Tag) -> Optional[JobListing]:
        """Extract a JobListing from a single search-result card element."""
        try:
            title_tag = card.find("span", {"data-test": "ad-card-title"})
            title = title_tag.get_text(strip=True) if title_tag else ""

            company_img = card.find("img", {"data-test": "company-image"})
            company = company_img.get("alt", "").strip() if company_img else ""

            location_tag = card.find("span", {"data-test": "location"})
            location = location_tag.get_text(strip=True) if location_tag else ""

            raw_href: str = card.get("href", "") or ""
            url = (
                f"{self.DETAIL_BASE}{raw_href}"
                if raw_href.startswith("/")
                else raw_href
            )

            if not (title and url):
                return None

            return JobListing(
                title=title,
                company=company or "Belirtilmemiş",
                location=location or "Belirtilmemiş",
                url=url,
                platform=self.PLATFORM_NAME,
            )
        except Exception as exc:
            self.logger.debug("card_parse_failed", error=str(exc))
            return None

    def _parse_detail_page(self, soup: BeautifulSoup, listing: JobListing) -> JobDetail:
        """
        Extract structured fields from an individual job detail page.

        Kariyer.net detail pages present information in a mix of structured
        metadata blocks and free-text description sections. This method
        applies multiple selector strategies with graceful fallbacks.
        """
        full_description = self._extract_description(soup)
        required_skills = self._extract_skill_list(soup, required=True)
        preferred_skills = self._extract_skill_list(soup, required=False)
        experience_years = self._extract_experience_years(full_description)
        education = self._extract_education_level(soup, full_description)
        salary = self._extract_salary(soup)
        employment_type = self._extract_employment_type(soup)
        department = self._extract_department(soup)

        return JobDetail(
            listing=listing,
            full_description=full_description,
            required_skills_raw=required_skills,
            preferred_skills_raw=preferred_skills,
            experience_years=experience_years,
            education_level=education,
            salary_range=salary,
            employment_type=employment_type,
            department=department,
        )

    def _extract_description(self, soup: BeautifulSoup) -> str:
        """Extract the full job description text using cascading selectors."""
        # Ordered by specificity / likelihood of containing the primary description
        selectors = [
            {"attrs": {"data-test": "job-description"}},
            {"class_": "detail-info-container"},
            {"class_": "job-description"},
            {"class_": "ilan-detay-description"},
            {"attrs": {"id": "jobDescriptionContainer"}},
            {"class_": "description"},
        ]
        for sel in selectors:
            tag = None
            if "attrs" in sel:
                tag = soup.find(attrs=sel["attrs"])
            elif "class_" in sel:
                tag = soup.find(class_=sel["class_"])
            if tag:
                return tag.get_text(separator=" ", strip=True)

        # Last resort: largest <div> by text length
        all_divs = soup.find_all("div")
        if all_divs:
            best = max(all_divs, key=lambda d: len(d.get_text()), default=None)
            if best:
                return best.get_text(separator=" ", strip=True)
        return ""

    def _extract_skill_list(self, soup: BeautifulSoup, required: bool) -> List[str]:
        """Parse bullet-list skill items from the structured requirements block."""
        skills: List[str] = []

        # Kariyer.net separates required vs. preferred in labelled sections
        section_keywords_tr = (
            ["gereksinimler", "aranan", "zorunlu"] if required
            else ["tercih edilir", "tercih", "avantaj"]
        )

        for tag in soup.find_all(["h3", "h4", "strong", "b"]):
            text_lower = tag.get_text(strip=True).lower()
            if any(kw in text_lower for kw in section_keywords_tr):
                container = tag.find_next_sibling(["ul", "ol"])
                if container:
                    for li in container.find_all("li"):
                        skill_text = li.get_text(strip=True)
                        if skill_text:
                            skills.append(skill_text)
                    break

        return skills

    def _extract_experience_years(self, description: str) -> Optional[int]:
        """Infer minimum required experience years from description text."""
        patterns = [
            r"(\d+)\+?\s*yıl",
            r"(\d+)\+?\s*year",
            r"en az\s+(\d+)\s*yıl",
            r"minimum\s+(\d+)\s*yıl",
        ]
        for pattern in patterns:
            match = re.search(pattern, description, re.IGNORECASE)
            if match:
                try:
                    return int(match.group(1))
                except ValueError:
                    continue
        return None

    def _extract_education_level(
        self, soup: BeautifulSoup, description: str
    ) -> Optional[str]:
        """Extract required education level from metadata or description text."""
        education_tag = soup.find(attrs={"data-test": "education-level"})
        if education_tag:
            return education_tag.get_text(strip=True)

        # Pattern fallback in description
        patterns = [
            r"(lisans|bachelor|önlisans|associate|yüksek lisans|master|doktora|phd|lise|high school)",
        ]
        for pattern in patterns:
            match = re.search(pattern, description, re.IGNORECASE)
            if match:
                return match.group(1).title()
        return None

    def _extract_salary(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract disclosed salary range from metadata block."""
        tag = soup.find(attrs={"data-test": "salary"})
        if tag:
            return tag.get_text(strip=True)
        return None

    def _extract_employment_type(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract employment/contract type from metadata block."""
        tag = (
            soup.find(attrs={"data-test": "employment-type"})
            or soup.find(attrs={"data-test": "work-type"})
        )
        if tag:
            return tag.get_text(strip=True)
        return None

    def _extract_department(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract department or business unit designation."""
        tag = (
            soup.find(attrs={"data-test": "department"})
            or soup.find(attrs={"data-test": "sector"})
        )
        if tag:
            return tag.get_text(strip=True)
        return None

