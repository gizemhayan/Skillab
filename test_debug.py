#!/usr/bin/env python
"""
Debug mode - inspect Kariyer.net HTML structure
"""

from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions
import random
import time

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
]

def debug():
    print("\n[DEBUG] Starting Chrome...")
    options = ChromeOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument(f"user-agent={random.choice(USER_AGENTS)}")
    
    driver = webdriver.Chrome(options=options)
    
    try:
        url = "https://www.kariyer.net/is-ilanlari?kw=Python+Developer&cp=1"
        print(f"\n[DEBUG] Opening: {url}")
        driver.get(url)
        
        print("[DEBUG] Waiting 5 seconds for JS render...")
        time.sleep(5)
        
        html = driver.page_source
        
        # Save HTML to file for inspection
        with open("debug_page_source.html", "w", encoding="utf-8") as f:
            f.write(html)
        print("[DEBUG] HTML saved to debug_page_source.html")
        
        # Check for common selectors
        selectors_to_check = [
            "a.k-ad-card",
            "[data-test='ad-card-item']",
            ".job-card",
            ".ilan-card",
            "a[href*='/is-ilani/']",
            ".job-listing",
            "[class*='card']",
        ]
        
        print("\n[DEBUG] Checking selectors:")
        for selector in selectors_to_check:
            try:
                elements = driver.find_elements("css selector", selector)
                print(f"  {selector}: {len(elements)} found")
            except:
                print(f"  {selector}: error")
        
        # Print first 2000 chars of HTML body
        print("\n[DEBUG] Sample HTML (first 2000 chars):")
        start = html.find("<body")
        end = start + 2000 if start != -1 else 2000
        print(html[start:end][:1000])
        
    finally:
        driver.quit()
        print("\n[DEBUG] Browser closed")

if __name__ == "__main__":
    debug()
