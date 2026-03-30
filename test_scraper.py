"""
Quick test script to fetch Veri Analisti job listings
"""

try:
    from src.scraper.kariyer_scraper import KariyerScraper
    
    print('\n🔍 Fetching job listings for: Veri Analisti\n')
    
    scraper = KariyerScraper()
    jobs = scraper.fetch_jobs(keyword='Veri Analisti', page_count=1)
    
    if jobs:
        print(f'✅ Found {len(jobs)} job listings:\n')
        print('=' * 80)
        for idx, job in enumerate(jobs, 1):
            print(f'\n• Job #{idx}')
            print(f'  Title:    {job["title"]}')
            print(f'  Company:  {job["company"]}')
            print(f'  Location: {job["location"]}')
        print('\n' + '=' * 80)
    else:
        print('⚠️  No job listings found.')
    
    scraper.close()
    
except Exception as e:
    print(f'❌ Error: {e}')
    import traceback
    traceback.print_exc()
