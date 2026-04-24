from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
import time

opts = Options()
opts.add_argument("--user-data-dir=C:/Users/Gizem/AppData/Local/Google/Chrome/User Data")
opts.add_argument("--no-sandbox")
opts.add_argument("--disable-dev-shm-usage")
opts.add_argument("--disable-blink-features=AutomationControlled")
service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=opts)

driver.get("https://www.linkedin.com/jobs/search/")
time.sleep(5)

# Try to open the first visible job card automatically.
opened = False
for sel in [
    "li.jobs-search-results__list-item a",
    "a.job-card-list__title",
    "li.scaffold-layout__list-item a",
]:
    cards = driver.find_elements(By.CSS_SELECTOR, sel)
    if cards:
        try:
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", cards[0])
            time.sleep(0.5)
            cards[0].click()
            opened = True
            break
        except Exception:
            continue

if not opened:
    input("Bir job'a tıkla, sağ panel açılsın, sonra Enter'a bas...")
else:
    time.sleep(3)

# Tüm insight elementlerini yakala
items = driver.find_elements(By.CSS_SELECTOR, "li.job-details-jobs-unified-top-card__job-insight")

print(f"\n{len(items)} insight bulundu\n")
for i, el in enumerate(items):
    html = el.get_attribute("outerHTML")
    print(f"--- Insight {i} ---")
    print(html[:500])
    print()

# HTML'i dosyaya yaz
with open("debug_skills.html", "w", encoding="utf-8") as f:
    f.write(driver.page_source)
print("\nTam HTML -> debug_skills.html")
driver.quit()
