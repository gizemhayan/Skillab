#!/usr/bin/env python
"""
Minimal Selenium scraper test - NO dependencies on publication_analysis
"""

import os
os.environ['PAGE_COUNT'] = '1'
os.environ['MAX_JOBS'] = '2'

from src.scraper.kariyer_scraper import KariyerScraper

def test():
    print("\n" + "="*70)
    print("SELENIUM SCRAPER TEST - Kariyer.net")
    print("="*70 + "\n")
    
    keywords = ["Python Developer", "Data Scientist"]
    
    with KariyerScraper() as scraper:
        for keyword in keywords:
            print(f"\n[KEYWORD] {keyword}")
            print("-" * 50)
            
            listings = scraper.fetch_jobs(keyword=keyword, page_count=1)
            print(f"Found {len(listings)} listings")
            
            if listings:
                print(f"First job: {listings[0].title}")
                detail = scraper.fetch_job_detail(listings[0])
                print(f"Description length: {len(detail.full_description)}")
                print(f"Required skills: {len(detail.required_skills_raw)}")
            
            break  # Only test first keyword
    
    print("\n" + "="*70)
    print("[DONE] Scraper test completed successfully!")
    print("="*70 + "\n")

if __name__ == "__main__":
    test()
