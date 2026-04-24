#!/usr/bin/env python3
"""
LinkedIn Job Scraper
Scrapes job postings from LinkedIn and saves to Excel.

Usage:
    python linkedin_scraper.py [--url URL] [--pages N] [--output FILE] [--wait N]

Requirements:
    pip install selenium openpyxl webdriver-manager
    Chrome browser must be installed.
"""

import argparse
import json
import random
import re
import sys
import time
from collections import Counter
from datetime import datetime

from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter

try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.common.exceptions import (
        TimeoutException,
        NoSuchElementException,
        StaleElementReferenceException,
        ElementClickInterceptedException,
    )
    from webdriver_manager.chrome import ChromeDriverManager
except ImportError as e:
    print(f"Missing dependency: {e}")
    print("Run: pip install selenium openpyxl webdriver-manager")
    sys.exit(1)


SELECTORS = {
    "job_cards": [
        "li.jobs-search-results__list-item",
        "div.job-search-card",
        "li[data-occludable-job-id]",
        "li.scaffold-layout__list-item",
    ],
    "job_title": [
        "h1.job-details-jobs-unified-top-card__job-title",
        "h1.t-24",
        "h2.job-details-jobs-unified-top-card__job-title",
        "h1[data-test-id='job-title']",
    ],
    "company": [
        "div.job-details-jobs-unified-top-card__company-name a",
        "span.job-details-jobs-unified-top-card__company-name",
        "a.topcard__org-name-link",
        "span[data-test-id='job-poster-company-name']",
    ],
    "location": [
        "div.job-details-jobs-unified-top-card__primary-description-without-tagline span.tvm__text",
        "span.job-details-jobs-unified-top-card__bullet",
        "span[data-test-id='job-location']",
        "div.jobs-unified-top-card__primary-description > div > span",
    ],
    "employment_type": [
        "span.job-details-jobs-unified-top-card__job-insight-text",
        "li.job-details-jobs-unified-top-card__job-insight span",
        "span[data-test-id='job-employment-type']",
    ],
    "description": [
        "div.jobs-description__content",
        "div#job-details",
        "article.jobs-description__container",
        "div[class*='description']",
    ],
    "applicants": [
        "span.jobs-unified-top-card__applicant-count",
        "figcaption.jobs-unified-top-card__applicant-count",
    ],
    "posted_date": [
        "span.jobs-unified-top-card__posted-date",
        "time",
        "span[data-test-id='job-posted-date']",
    ],
    "seniority": [
        "span.job-details-jobs-unified-top-card__job-insight-text",
    ],
    "next_page": [
        "button[aria-label='View next page']",
        "button.jobs-search-pagination__button--next",
        "li.artdeco-pagination__indicator--number.selected + li button",
    ],
    "page_state": [
        "li.artdeco-pagination__indicator--number.selected",
    ],
}


def try_find(driver, selectors, parent=None, multiple=False, text_only=True):
    """Try multiple CSS selectors and return first successful match.
    
    Args:
        driver: Selenium WebDriver instance
        selectors: List of CSS selectors to try
        parent: Optional parent element to search within
        multiple: If True, return all matches as list; if False, return first match
        text_only: If True, return text content; if False, return element objects
    
    Returns:
        Extracted text/elements, or empty string/list if nothing found
    """
    ctx = parent if parent else driver
    for sel in selectors:
        try:
            if multiple:
                els = ctx.find_elements(By.CSS_SELECTOR, sel)
                if els:
                    return [e.text.strip() for e in els] if text_only else els
            else:
                el = ctx.find_element(By.CSS_SELECTOR, sel)
                if el:
                    return el.text.strip() if text_only else el
        except (NoSuchElementException, StaleElementReferenceException):
            continue
    return [] if multiple else ""


def build_driver(headless=False, user_data_dir=None):
    """Create and configure Chrome WebDriver."""
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")

    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.add_argument("--window-size=1440,900")
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    )
    if user_data_dir:
        opts.add_argument(f"--user-data-dir={user_data_dir}")

    # Try cached chromedriver first, fallback to download if needed
    try:
        from pathlib import Path
        cache_path = Path.home() / ".wdm" / "drivers" / "chromedriver" / "win64"
        if cache_path.exists():
            # Get the latest cached version
            versions = sorted([d for d in cache_path.iterdir() if d.is_dir()], reverse=True)
            if versions:
                driver_path = versions[0] / "chromedriver-win32" / "chromedriver.exe"
                if driver_path.exists():
                    service = Service(str(driver_path))
                else:
                    service = Service(ChromeDriverManager().install())
            else:
                service = Service(ChromeDriverManager().install())
        else:
            service = Service(ChromeDriverManager().install())
    except (OSError, ValueError, FileNotFoundError):
        service = Service(ChromeDriverManager().install())
    
    driver = webdriver.Chrome(service=service, options=opts)
    driver.execute_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    )
    return driver


def wait_for_jobs_list(driver, timeout=15):
    """Wait until at least one job card is present."""
    wait = WebDriverWait(driver, timeout)
    for sel in SELECTORS["job_cards"]:
        try:
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, sel)))
            return sel
        except TimeoutException:
            continue
    return None


def get_job_cards(driver, working_selector=None):
    """Return all job card elements on the current page."""
    selectors = ([working_selector] + SELECTORS["job_cards"]) if working_selector else SELECTORS["job_cards"]
    for sel in selectors:
        cards = driver.find_elements(By.CSS_SELECTOR, sel)
        if cards:
            return cards, sel
    return [], None


def scroll_into_view_and_click(driver, element):
    """Scroll element into view and click it."""
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", element)
    time.sleep(0.3)
    try:
        element.click()
    except ElementClickInterceptedException:
        driver.execute_script("arguments[0].click();", element)


# ─────── Skill Extraction ─────────────────────────────────────────────────────

def _extract_skills(description: str):
    """Extract simple raw skill tokens from the description text.
    
    Args:
        description: Job description text
        
    Returns:
        List of detected skill keywords (lowercase)
    """
    if not description:
        return []

    patterns = [
        r"\bpython\b",
        r"\bjava\b",
        r"\bc\+\+\b",
        r"\bc#\b",
        r"\bjavascript\b",
        r"\btypescript\b",
        r"\breact\b",
        r"\bnode\.js\b",
        r"\bdjango\b",
        r"\bflask\b",
        r"\bsql\b",
        r"\bpostgresql\b",
        r"\bmysql\b",
        r"\baws\b",
        r"\bazure\b",
        r"\bdocker\b",
        r"\bkubernetes\b",
        r"\bgit\b",
    ]

    text = description.lower()
    found = []
    for pattern in patterns:
        if re.search(pattern, text):
            found.append(re.sub(r"\\b", "", pattern).replace("\\", ""))
    return found


# ─────── Role Filtering Keywords ──────────────────────────────────────────────

POSITIVE_ROLE_KEYWORDS = [
    "yazilim",
    "yazılım",
    "yazilim muhendisi",
    "yazılım mühendisi",
    "bilgisayar muhendisi",
    "bilgisayar mühendisi",
    "yapay zeka",
    "veri bilimi",
    "makine ogrenmesi",
    "makine öğrenmesi",
    "gelistirici",
    "geliştirici",
    "software",
    "developer",
    "engineer",
    "engineering",
    "backend",
    "frontend",
    "full stack",
    "fullstack",
    "data",
    "ai",
    "ml",
    "devops",
    "cloud",
    "cyber",
    "security",
    "robotics",
    "automation",
    "iot",
]

NEGATIVE_ROLE_KEYWORDS = [
    "satis",
    "satış",
    "satis temsil",
    "satış temsil",
    "musteri temsil",
    "müşteri temsil",
    "call center",
    "cagri merkezi",
    "çağrı merkezi",
    "pazarlama",
    "marketing",
    "client advisor",
    "tahsilat",
]

TRANSFORMATION_KEYWORDS = [
    "dijital donusum",
    "dijital dönüşüm",
    "digital transformation",
    "green transformation",
    "yesil donusum",
    "yeşil dönüşüm",
    "surdurulebilir",
    "sürdürülebilir",
    "sustainability",
    "decarbon",
    "net zero",
    "energy efficiency",
    "enerji verimliligi",
    "enerji verimliliği",
]


def _contains_any(text: str, keywords):
    """Check if text contains any of the keywords (case-insensitive)."""
    text = (text or "").lower()
    return any(keyword in text for keyword in keywords)


def is_relevant_job(title: str, description: str = ""):
    """Return (is_relevant, reason) for role-level filtering.
    
    Args:
        title: Job title
        description: Job description
        
    Returns:
        Tuple of (bool, str) - (is_relevant, reason_if_filtered)
    """
    title_l = (title or "").lower()
    desc_l = (description or "").lower()
    combined = f"{title_l} {desc_l}"

    if _contains_any(title_l, NEGATIVE_ROLE_KEYWORDS):
        return False, "negative_title_keyword"

    has_role_signal = _contains_any(combined, POSITIVE_ROLE_KEYWORDS)
    has_transformation_signal = _contains_any(combined, TRANSFORMATION_KEYWORDS)

    if not (has_role_signal or has_transformation_signal):
        return False, "no_relevant_keywords"

    return True, "ok"


def _extract_skills(description: str):
    """Extract simple raw skill tokens from the description text."""
    if not description:
        return []

    patterns = [
        r"\bpython\b",
        r"\bjava\b",
        r"\bc\+\+\b",
        r"\bc#\b",
        r"\bjavascript\b",
        r"\btypescript\b",
        r"\breact\b",
        r"\bnode\.js\b",
        r"\bdjango\b",
        r"\bflask\b",
        r"\bsql\b",
        r"\bpostgresql\b",
        r"\bmysql\b",
        r"\baws\b",
        r"\bazure\b",
        r"\bdocker\b",
        r"\bkubernetes\b",
        r"\bgit\b",
    ]

    text = description.lower()
    found = []
    for pattern in patterns:
        if re.search(pattern, text):
            found.append(re.sub(r"\\b", "", pattern).replace("\\", ""))
    return found


POSITIVE_ROLE_KEYWORDS = [
    "yazilim",
    "yazılım",
    "yazilim muhendisi",
    "yazılım mühendisi",
    "bilgisayar muhendisi",
    "bilgisayar mühendisi",
    "yapay zeka",
    "veri bilimi",
    "makine ogrenmesi",
    "makine öğrenmesi",
    "gelistirici",
    "geliştirici",
    "software",
    "developer",
    "engineer",
    "engineering",
    "backend",
    "frontend",
    "full stack",
    "fullstack",
    "data",
    "ai",
    "ml",
    "devops",
    "cloud",
    "cyber",
    "security",
    "robotics",
    "automation",
    "iot",
]

NEGATIVE_ROLE_KEYWORDS = [
    "satis",
    "satış",
    "satis temsil",
    "satış temsil",
    "musteri temsil",
    "müşteri temsil",
    "call center",
    "cagri merkezi",
    "çağrı merkezi",
    "pazarlama",
    "marketing",
    "client advisor",
    "tahsilat",
]

TRANSFORMATION_KEYWORDS = [
    "dijital donusum",
    "dijital dönüşüm",
    "digital transformation",
    "green transformation",
    "yesil donusum",
    "yeşil dönüşüm",
    "surdurulebilir",
    "sürdürülebilir",
    "sustainability",
    "decarbon",
    "net zero",
    "energy efficiency",
    "enerji verimliligi",
    "enerji verimliliği",
]


def _contains_any(text: str, keywords):
    text = (text or "").lower()
    return any(keyword in text for keyword in keywords)


def is_relevant_job(title: str, description: str = ""):
    """Return (is_relevant, reason) for role-level filtering."""
    title_l = (title or "").lower()
    desc_l = (description or "").lower()
    combined = f"{title_l} {desc_l}"

    if _contains_any(title_l, NEGATIVE_ROLE_KEYWORDS):
        return False, "negative_title_keyword"

    has_role_signal = _contains_any(combined, POSITIVE_ROLE_KEYWORDS)
    has_transformation_signal = _contains_any(combined, TRANSFORMATION_KEYWORDS)

    if not (has_role_signal or has_transformation_signal):
        return False, "no_relevant_keywords"

    return True, "ok"


def scrape_job_detail(driver, default_keyword="Yazılım|Mühendislik"):
    """Extract all available info from the currently-open job detail panel."""
    time.sleep(random.uniform(1.0, 2.0))

    job = {}
    job["platform"] = "linkedin"
    job["title"] = try_find(driver, SELECTORS["job_title"])
    job["company"] = try_find(driver, SELECTORS["company"])

    locs = try_find(driver, SELECTORS["location"], multiple=True)
    job["location"] = " · ".join(locs) if locs else ""

    insights = try_find(driver, SELECTORS["employment_type"], multiple=True)
    job["employment_type"] = insights[0] if insights else ""

    desc_el = try_find(driver, SELECTORS["description"], text_only=False)
    description = desc_el.text.strip()[:5000] if desc_el else ""

    raw_skills = _extract_skills(description)

    job["url"] = driver.current_url
    job["scraped_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    job["full_description"] = description
    job["required_skills_raw"] = json.dumps(raw_skills, ensure_ascii=False)
    job["preferred_skills_raw"] = json.dumps([], ensure_ascii=False)
    job["experience_years"] = ""
    job["education_level"] = ""
    job["salary_range"] = ""
    job["department"] = ""

    # Digital & Green skills from job insight sections.
    try:
        all_insights = driver.find_elements(
            By.CSS_SELECTOR,
            "div.job-details-jobs-unified-top-card__job-insight, "
            "li.job-details-jobs-unified-top-card__job-insight",
        )

        digital_skills = []
        green_skills = []

        for insight in all_insights:
            outer = (insight.get_attribute("outerHTML") or "").lower()
            text = insight.text.strip()

            if not text:
                continue

            if any(k in outer for k in ["digital", "dijital", "technology", "tech-skill"]):
                digital_skills.append(text)
            elif any(k in outer for k in ["green", "sustainability", "yeşil", "sustainable"]):
                green_skills.append(text)

        # Fallback: skill-match section if top-card insights are empty.
        if not digital_skills and not green_skills:
            skill_tags = driver.find_elements(
                By.CSS_SELECTOR,
                "a.job-details-skill-match-status-list__unmatched-skill-link, "
                "span.job-details-skill-match-status-list__skill-text",
            )
            all_text = [s.text.strip() for s in skill_tags if s.text.strip()]
            if all_text:
                digital_skills = all_text

        job["digital_concepts"] = " | ".join(sorted(set(digital_skills)))
        job["green_concepts"] = " | ".join(sorted(set(green_skills)))

    except Exception as e:
        job["digital_concepts"] = ""
        job["green_concepts"] = ""
        print(f"  ⚠ Skills parse hatası: {e}")

    job["search_keyword"] = default_keyword

    return job


def scrape_page(driver, working_sel, verbose=True):
    """Scrape all job cards on the current page."""
    jobs = []
    cards, working_sel = get_job_cards(driver, working_sel)

    if not cards:
        print("  No job cards found on this page.")
        return jobs, working_sel

    print(f"  Found {len(cards)} job cards.")

    for i, card in enumerate(cards, 1):
        try:
            # Get job title from card for logging
            card_title = ""
            for title_sel in ["h3", "a.job-card-list__title", "a[data-control-name='job_card_title']", "a"]:
                try:
                    card_title = card.find_element(By.CSS_SELECTOR, title_sel).text.strip()
                    if card_title:
                        break
                except NoSuchElementException:
                    continue

            if verbose:
                print(f"  [{i}/{len(cards)}] Clicking: {card_title[:60] or '(untitled)'}")

            scroll_into_view_and_click(driver, card)
            time.sleep(random.uniform(1.5, 2.5))

            job_data = scrape_job_detail(driver)

            # Fallback title from card if not found in detail
            if not job_data["title"] and card_title:
                job_data["title"] = card_title

            jobs.append(job_data)

        except (StaleElementReferenceException, Exception) as e:
            print(f"  ⚠ Error on card {i}: {e}")
            # Re-fetch cards since DOM may have changed
            cards, working_sel = get_job_cards(driver, working_sel)
            continue

    return jobs, working_sel


def go_to_next_page(driver):
    """Click the next-page button. Returns True on success."""
    for sel in SELECTORS["next_page"]:
        try:
            btn = driver.find_element(By.CSS_SELECTOR, sel)
            if btn and btn.is_enabled() and btn.is_displayed():
                scroll_into_view_and_click(driver, btn)
                time.sleep(random.uniform(2.0, 3.5))
                return True
        except (NoSuchElementException, ElementClickInterceptedException):
            continue
    return False


def save_to_excel(jobs, output_path):
    """Save jobs in Kariyer.net-compatible schema."""
    wb = Workbook()
    ws = wb.active
    ws.title = "Jobs"

    columns = [
        ("Platform", 14),
        ("Title", 35),
        ("Company", 28),
        ("Location", 24),
        ("URL", 55),
        ("Scraped At", 20),
        ("Full Description", 90),
        ("Required Skills", 35),
        ("Preferred Skills", 35),
        ("Experience Years", 16),
        ("Education Level", 20),
        ("Salary Range", 20),
        ("Employment Type", 20),
        ("Department", 20),
        ("Digital Concepts", 30),
        ("Green Concepts", 30),
    ]

    header_fill = PatternFill("solid", start_color="4472C4")
    header_font = Font(bold=True, color="FFFFFF", name="Arial", size=11)
    header_align = Alignment(horizontal="center", vertical="center", wrap_text=True)
    thin_border = Border(
        bottom=Side(style="thin", color="CCCCCC"),
        right=Side(style="thin", color="CCCCCC"),
    )

    for col_idx, (header, width) in enumerate(columns, 1):
        cell = ws.cell(row=1, column=col_idx, value=header)
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = header_align
        cell.border = thin_border
        ws.column_dimensions[get_column_letter(col_idx)].width = width

    ws.row_dimensions[1].height = 28
    ws.freeze_panes = "A2"

    alt_fill = PatternFill("solid", start_color="EBF3FB")
    normal_fill = PatternFill("solid", start_color="FFFFFF")
    data_font = Font(name="Arial", size=10)
    link_font = Font(name="Arial", size=10, color="0A66C2", underline="single")
    wrap_align = Alignment(vertical="top", wrap_text=True)

    field_map = [
        "platform",
        "title",
        "company",
        "location",
        "url",
        "scraped_at",
        "full_description",
        "required_skills_raw",
        "preferred_skills_raw",
        "experience_years",
        "education_level",
        "salary_range",
        "employment_type",
        "department",
        "digital_concepts",
        "green_concepts",
    ]

    for row_idx, job in enumerate(jobs, 2):
        fill = alt_fill if row_idx % 2 == 0 else normal_fill
        ws.row_dimensions[row_idx].height = 60

        for col_idx, field in enumerate(field_map, 1):
            value = job.get(field, "")
            cell = ws.cell(row=row_idx, column=col_idx, value=value)
            cell.fill = fill
            cell.alignment = wrap_align
            cell.border = thin_border
            cell.font = link_font if field == "url" else data_font

    ws2 = wb.create_sheet("Summary")
    ws2["A1"] = "LinkedIn Scrape Summary"
    ws2["A1"].font = Font(bold=True, size=14, name="Arial", color="0A66C2")
    ws2["A3"] = "Total jobs scraped:"
    ws2["B3"] = len(jobs)
    ws2["A4"] = "Scrape date:"
    ws2["B4"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ws2["A6"] = "Top Companies"
    ws2["A6"].font = Font(bold=True, name="Arial")

    companies = Counter(j.get("company", "Unknown") for j in jobs)
    for i, (company, count) in enumerate(companies.most_common(20), 7):
        ws2[f"A{i}"] = company
        ws2[f"B{i}"] = count

    ws2.column_dimensions["A"].width = 35
    ws2.column_dimensions["B"].width = 12

    wb.save(output_path)
    print(f"\nSaved {len(jobs)} jobs to {output_path}")


def main():
    parser = argparse.ArgumentParser(description="LinkedIn Job Scraper")
    parser.add_argument(
        "--url",
        default="https://www.linkedin.com/jobs/search/?keywords=Yaz%C4%B1l%C4%B1m%20M%C3%BChendislik&location=T%C3%BCrkiye&f_TPR=r604800&position=1&pageNum=0",
        help="LinkedIn jobs search URL to scrape",
    )
    parser.add_argument("--pages", type=int, default=5, help="Max pages to scrape (default: 5)")
    parser.add_argument("--output", default="data/linkedin_jobs.xlsx", help="Output Excel filename")
    parser.add_argument("--headless", action="store_true", help="Run Chrome in headless mode")
    parser.add_argument("--wait", type=int, default=20, help="Page load timeout seconds (default: 20)")
    parser.add_argument("--user-data-dir", help="Chrome user data dir (use your profile to stay logged in)")
    args = parser.parse_args()

    print("=" * 60)
    print("  LinkedIn Job Scraper")
    print("=" * 60)
    print(f"  URL    : {args.url[:80]}")
    print(f"  Pages  : {args.pages}")
    print(f"  Output : {args.output}")
    print(f"  Headless: {args.headless}")
    print("=" * 60)
    print()

    if not args.headless:
        print("TIP: If LinkedIn shows authwall/login, sign in manually in the opened browser.")
        print("     Use --user-data-dir to keep session cookies and avoid repeated login prompts.")
        print()

    driver = build_driver(headless=args.headless, user_data_dir=args.user_data_dir)
    all_jobs = []
    working_sel = None

    try:
        print(f"Opening: {args.url}")
        driver.get(args.url)
        time.sleep(random.uniform(3, 5))

        if "authwall" in driver.current_url or "login" in driver.current_url:
            print("\nWARNING: LinkedIn authwall/login detected.")
            print("1) Keep this browser open and complete login manually.")
            print("2) After login lands on jobs page, return here and press Enter.")
            print("3) Next runs: pass --user-data-dir with your logged-in Chrome profile.")
            input("Press Enter after login to continue (or Ctrl+C to stop): ")
            logged_in = False
            for _ in range(6):
                driver.get(args.url)
                time.sleep(3)
                if "authwall" not in driver.current_url and "login" not in driver.current_url:
                    if wait_for_jobs_list(driver, timeout=5):
                        logged_in = True
                        break

            if not logged_in:
                print("\nWARNING: Login was not confirmed. Finish verification in the browser and try again.")
                return 1

        for page_num in range(1, args.pages + 1):
            print(f"\n{'-' * 50}")
            print(f"  PAGE {page_num}")
            print(f"{'-' * 50}")

            working_sel = wait_for_jobs_list(driver, timeout=args.wait)
            if not working_sel:
                print("Could not find job cards. Page may require login or LinkedIn DOM changed.")
                break

            page_jobs, working_sel = scrape_page(driver, working_sel)
            all_jobs.extend(page_jobs)
            print(f"Page {page_num}: {len(page_jobs)} jobs. Total: {len(all_jobs)}")

            if page_num < args.pages:
                print("Navigating to next page...")
                if not go_to_next_page(driver):
                    print("No more pages found.")
                    break
                time.sleep(random.uniform(2, 4))

    except KeyboardInterrupt:
        print("\nInterrupted by user.")
    finally:
        driver.quit()

    if all_jobs:
        save_to_excel(all_jobs, args.output)
    else:
        print("\nNo jobs were scraped. Check login status or URL validity.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
