"""
Debug script to examine Kariyer.net HTML structure
"""

try:
    from src.scraper.kariyer_scraper import KariyerScraper
    from bs4 import BeautifulSoup
    
    print('\n🔍 Fetching HTML to debug structure...\n')
    
    scraper = KariyerScraper()
    
    # Manually fetch the page
    import time
    import random
    time.sleep(random.uniform(0.5, 1.5))
    
    response = scraper.session.get(
        scraper.base_url,
        params={'kw': 'Veri Analisti', 'cp': 1},
        headers={
            'Referer': 'https://www.kariyer.net/',
            'Sec-Fetch-Site': 'none',
        },
        timeout=15,
        allow_redirects=True
    )
    
    print(f"Status Code: {response.status_code}")
    print(f"Content Length: {len(response.content)} bytes\n")
    
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Save HTML to file
    with open('debug_output.html', 'w', encoding='utf-8') as f:
        f.write(soup.prettify())
    print("✅ HTML saved to debug_output.html\n")
    
    # Look for common job listing patterns
    print("=" * 80)
    print("Searching for potential job listing elements...\n")
    
    # Try to find div elements that might contain jobs
    job_containers = [
        soup.find_all('div', class_=lambda x: x and 'job' in x.lower()),
        soup.find_all('div', class_=lambda x: x and 'list' in x.lower()),
        soup.find_all('article'),
        soup.find_all('li', class_=lambda x: x and 'job' in x.lower() if x else False),
        soup.find_all('a', href=lambda x: x and '/is-ilani/' in x if x else False)
    ]
    
    for idx, container_list in enumerate(job_containers):
        if container_list:
            print(f"\nPattern {idx + 1}: Found {len(container_list)} elements")
            if container_list:
                print(f"First element classes: {container_list[0].get('class', 'No class')}")
                print(f"First element tag: {container_list[0].name}")
                if idx == 4:  # Links
                    print(f"First link href: {container_list[0].get('href', 'No href')}")
    
    print("\n" + "=" * 80)
    
    scraper.close()
    
except Exception as e:
    print(f'❌ Error: {e}')
    import traceback
    traceback.print_exc()
