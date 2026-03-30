985 İŞ İLANI TARAMA VE VERİ KURTARMA: TAM REHBERİ
=====================================================
985 Job Listings Scraping & Data Recovery: Complete Guide

---

## 🎯 PROJE HEDEFİ / PROJECT OBJECTIVE

Bu sistem, Kariyer.net'ten "Yazılım" anahtar kelimesi için ~985 iş ilanını temel alarak Türkiye'nin **Dijital Dönüşüm** ve **Yeşil Dönüşüm** gereksiniğini haritalandırır.

This system scrapes ~985 job listings from Kariyer.net using the keyword "Yazılım" (Software) and maps Turkey's Digital Transformation and Green Transformation skill requirements.

---

## ⚙️ KURULUM / SETUP

### 1. Ortam Hazırlığı / Environment Setup

```bash
# .venv'yi etkinleştir / Activate venv
.\.venv\Scripts\Activate.ps1

# requirements.txt yükle / Install requirements
pip install -r requirements.txt
```

### 2. Tarayıcı ve Sürücü Yapılandırması / Browser & Driver Configuration

**Dosya: `.env`**
```
# İsteğe bağlı: Chrome binary yolu (otomatik algılanır) / Optional: Chrome binary path
CHROME_BINARY_PATH=C:\Program Files\Google\Chrome\Application\chrome.exe

# İsteğe bağlı: Chromedriver yolu (otomatik yüklenir) / Optional: Chromedriver path
CHROMEDRIVER_PATH=C:\path\to\chromedriver.exe
```

---

## 🚀 ÇALIŞTIRIM / EXECUTION

### Tam Tarama Başlat / Start Full Scraping

```bash
# Anahtar kelime: "Yazılım" - Dinamik sayfa sayısı (sonraki buton kaybolana kadar)
# Keyword: "Yazılım" - Dynamic pages (until next button disappears)
python main.py --run
```

---

## 📊 ÖZELLIKLER / FEATURES

### 1. **Dayanıklılık ve Kontrol Noktaları / Resilience & Checkpoints**

- ✅ **Checkpoint Sistemi**: Her ilandan sonra kontrol noktası kaydedilir. Program kesintiye uğrarsa kaldığı yerden devam eder.
- ✅ **Checkpoint System**: Saves checkpoint after each job. Resume from interruption point.
- 📁 **Dosya**: `data/checkpoints/scrape_checkpoint_985.json`

**Örnek / Example:**
```json
{
  "keyword": "Yazılım",
  "page": 12,
  "job_index": 245,
  "timestamp": 1711270000.123
}
```

### 2. **Anlık Excel Kayıt / Immediate Excel Appending**

- ✅ Her ilandan sonra **ANINDA** Excel dosyasına eklenir (Append)
- ✅ Each job **immediately** appended to Excel after analysis
- ✅ Veri kaybı riski: Minimum / Data loss risk: Minimal
- 📁 **Dosya**: `data/TURKIYE_YAZILIM_985_FINAL.xlsx`

**Sütunlar / Columns:**
- Platform, Title, Company, Location, URL, Scraped At
- Full Description, Required Skills, Preferred Skills
- Experience Years, Education Level, Salary Range
- Employment Type, Department
- **Digital Concepts** (Yeni / New), **Green Concepts** (Yeni / New)

### 3. **503 Hata Otomatik İyileşmesi / Auto-Recovery from 503 Errors**

- ✅ **Kolay Kolay**: 503 hatası algılandığında program kapatılmaz
- ✅ **Easy Fix**: When 503 detected, program does NOT close
- ✅ **Otomatik Bekleme**: 60 saniye bekle ve aynı ilandan devam et
- ✅ **Auto-Wait**: 60 seconds + retry same job
- ✅ **Maksimum 3 deneme**: Max 3 retries per job

**Throttling Stratejisi / Throttling Strategy:**
- Her 5 ilandan sonra: **20 saniye dinlen** (Her 5 jobs: rest 20s)
- Her sayfa geçişinde: **10 saniye bekle** (Per page transition: wait 10s)
- Başarısız request sonrası: **30-60 saniye bekleme** (After failed request: wait 30-60s)

### 4. **Bot Tespiti Önlemleri / Anti-Bot Defenses**

- ✅ Gerçek Chrome User-Agents (v146, v145)
- ✅ Real Chrome User-Agents (v146, v145)
- ✅ Tarayıcı görünür modda (headless=False)
- ✅ Browser in visible mode (headless=False)
- ✅ CDP navigator.webdriver spoofing
- ✅ undetected_chromedriver kullanımı
- ✅ Using undetected_chromedriver

### 5. **Cloudflare Challenge Tespiti / Cloudflare Challenge Detection**

- ✅ Cloudflare "Basılı Tut" düğmesi tespit edilirse uyarı verilir
- ✅ Cloudflare "Hold Down" button detected → Alert given
- ✅ **Sesli Uyarı**: Bilgisayardan üç "Bip" sesi çıkar (Windows)
- ✅ **Audio Alert**: Three "Beep" sounds (Windows)
- Kullanıcı müdahale edebilmesi için programın devamı bekler
- Waits for user intervention (visible browser mode)

```python
# Triggers: winsound.Beep(1000, 500) x 3
# Reason: beep_alert("Cloudflare Challenge Detected!")
```

### 6. **Terminal İlerleme Günlüğü / Terminal Progress Logging**

Terminalde her ilandan sonra şu bilgi yazılır:

```
[İlerleme: 45 / 985] - [Acme Corp] (Senior Software Engineer) taranıyor...
[İlerleme: 46 / 985] - [Tech Solutions] (Junior Developer) taranıyor...
...
[İlerleme: 985 / 985] - [Last Company] (Final Job Title) taranıyor...
```

### 7. **Türkçe Kavram Analizi / Turkish Concepts Analysis**

#### 🔵 DİJİTAL DÖNÜŞÜM KAVRAMLARI / DIGITAL TRANSFORMATION

- Dijital Dönüşüm / Digital Transformation
- Endüstri 4.0 / Industry 4.0
- Veri Analitiği / Data Analytics
- Siber Güvenlik / Cybersecurity
- Bulut Bilişim / Cloud Computing
- Yapay Zeka / Artificial Intelligence
- Otomasyon / Automation
- IoT / Internet of Things
- Kişisel Veriler / GDPR/Privacy

**Sonuç**: Her ilandan çıkarılır ve Excel'e yazılır
**Outcome**: Extracted from each job and written to Excel

#### 🟢 YEŞIL DÖNÜŞÜM KAVRAMLARI / GREEN TRANSFORMATION

- Yeşil Dönüşüm / Green Transformation
- Sürdürülebilirlik / Sustainability
- Karbon Ayak İzi / Carbon Footprint
- Enerji Verimliliği / Energy Efficiency
- Yenilenebilir Enerji / Renewable Energy
- Çevresel Etki / Environmental Impact
- Dairesel Ekonomi / Circular Economy
- Su Yönetimi / Water Management
- Atık Yönetimi / Waste Management

### 8. **Türkiye'nin Dönüşüm Karnesi / Turkey's Transformation Scorecard**

Tarama bittiğinde otomatik olarak oluşturulan rapor:

📄 **Dosya**: `outputs/TURKIYE_DONUSUM_KARNESI_{TARİH}.png`

**İçeriği / Contents:**
1. **Özet İstatistikler** - Toplam ilanlar, Dijital %, Yeşil %, Şirket sayısı
2. **Summary Statistics** - Total jobs, Digital %, Green %, Company count
3. **Dijital Dönüşüm Kavramları** - Top 8 (Bar chart)
4. **Digital Transformation Concepts** - Top 8 (Bar chart)
5. **Yeşil Dönüşüm Kavramları** - Top 6 (Bar chart)  
6. **Green Transformation Concepts** - Top 6 (Bar chart)
7. **Dönüşüm Dağılımı** - Dijital vs Yeşil vs Diğer (Pie chart)
8. **Transformation Distribution** - Digital vs Green vs Other (Pie chart)
9. **İşverenler ve Dönüşümleri** - Top 8 şirket × Dijital/Yeşil analizi
10. **Employer Transformation Match** - Top 8 companies × Digital/Green analysis

---

## 📁 ÇIKTI DOSYALARI / OUTPUT FILES

Tüm çıktılar **tarihe damgalanır** (format: `DD_MM_YYYY`)
All outputs are **date-stamped** (format: `DD_MM_YYYY`)

### Excel Veri Dosyası (Anlık Güncelleme)
### Excel Data File (Real-time Update)
```
data/TURKIYE_YAZILIM_985_FINAL.xlsx
```
- **Sütunlar / Columns**: 16 (Platform, Title, Company, ... , Digital Concepts, Green Concepts)
- **Güncelleme / Update**: Her ilanın hemen ardından appended
- **Güncelleme / Update**: Appended immediately after each job

### CSV Ham Veri
### CSV Raw Data  
```
data/raw_jobs_24_03_2026.csv
```
- Nihai derlenmiş veri / Final aggregated data
- Tamamlandığında bir sefer yazılır / Written once at completion

### Analiz Raporu (Türkçe + İngilizce)
### Analysis Report (Turkish + English)
```
data/analysis_report_24_03_2026.txt
```
- Özet istatistikler / Summary statistics
- Top 10 şirketler / Top 10 companies
- Dijital kavramları frekans / Digital concepts frequency
- Yeşil kavramları frekans / Green concepts frequency

### Dönüşüm Karnesi (Görsel)
### Transformation Scorecard (Visual)
```
outputs/TURKIYE_DONUSUM_KARNESI_24_03_2026.png
```
- Grafik ve istatistiklerle kapsamlı analiz
- Comprehensive analysis with charts and stats
- 5 alt grafik /  5 subplots

### Yetenek Envanteri (Excel)
### Skill Inventory (Excel)
```
data/skill_inventory_24_03_2026.xlsx
```
- NLP ile çıkarılan beceriler
- Skills extracted via NLP
- ESCO eşlemesi (apsicyon)
- ESCO mapping (if applicable)

### Kontrol Noktası (JSON)
### Checkpoint (JSON)
```
data/checkpoints/scrape_checkpoint_985.json
```
- Devam etmek için gerekli bilgiler
- Resume information

---

## 🔧 DİNAMİK AYARLAR / DYNAMIC SETTINGS

**Dosya: `.env` (isteğe bağlı / optional)**

```bash
# Anahtar kelimeler (virgülle ayrılmış / comma-separated)
SEARCH_KEYWORDS=Yazılım

# Sayfa sayısı: "DYNAMIC" = sonraki buton kaybolana kadar
PAGE_COUNT=DYNAMIC

# Maksimum iş sayısı (0 = sınırsız / unlimited)
MAX_JOBS=0
```

---

## 📋 İŞ AKIŞI / WORKFLOW

```
START
  ↓
[1] Keyword = "Yazılım" ayarla / Set keyword
  ↓
[2] Checkpoint yükle (varsa) / Load checkpoint if exists
  ↓
[3] Kariyer.net arama sonuçlarını taray / Scrape search results
       ├─ Sayfa 1, 2, 3, ... (sonraki buton kaybolana kadar)
       ├─ Page 1, 2, 3, ... (until next button disappears)
       ├─ Dinamik pagination / Dynamic pagination
       └─ Her 10 sayfada checkpoint kayde / Save checkpoint every 10 pages
  ↓
[4] Her ilanın tam açıklamasını getir / Fetch full job description
       ├─ [Başarısız] 503 hatası? → 60s bekle, tekrar dene (max 3x)
       ├─ [Failed] 503 error? → Wait 60s, retry (max 3x)
       ├─ [Başarısız] Cloudflare? → Sesli uyarı, beş dev wait
       ├─ [Failed] Cloudflare? → Audio alert, manual wait
       ├─ Dijital kavramları çıkar / Extract digital concepts
       ├─ Yeşil kavramları çıkar / Extract green concepts
       ├─ Excel'e ANINDA ekle / Append to Excel IMMEDIATELY
       ├─ Checkpoint güncelle / Update checkpoint
       ├─ Terminal'e ilerleme yaz / Log progress to terminal
       └─ Her 5 ilandan sonra 20s dinlen / Rest 20s every 5 jobs
  ↓
[5] Tüm ilanlar tamamlandı / All jobs completed
  ↓
[6] CSV dosyasına nihai veri yaz / Write final data to CSV
  ↓
[7] NLP skill extractor çalıştır / Run NLP skill extraction
  ↓
[8] ESCO eşlemesi yap (opsiyonel) / ESCO mapping (optional)
  ↓
[9] Türkçe analiz: Dijital ve Yeşil kavramları analiz et
  ↓
[9] Turkish analysis: Analyze Digital and Green concepts
  ↓
[10] Dönüşüm Karnesi grafiği oluştur / Generate scorecard
  ↓
[11] Final rapor yaz / Write final report
  ↓
END

✅ Tüm çıktılar data/ ve outputs/ klasörlerinde
✅ All outputs in data/ and outputs/ directories
```

---

## ⚠️ SORUN GİDERME / TROUBLESHOOTING

### Sorun 1: Chrome/Chromedriver Bulunamadı
### Problem 1: Chrome/Chromedriver not found

**Çözüm / Solution:**
```bash
# .env dosyasına ekle / Add to .env:
CHROME_BINARY_PATH=C:\Program Files\Google\Chrome\Application\chrome.exe
CHROMEDRIVER_PATH=C:\path\to\chromedriver.exe
```

### Sorun 2: 503 Hatası Alınıyor
### Problem 2: Receiving 503 errors

**Çözüm / Solution:**
- Program otomatik olarak 60s bekler ve yeniden dener
- Program automatically waits 60s and retries
- Eğer devam etmezse, manuel olarak başta Cloudflare'yi çöz
- If it continues, manually solve Cloudflare first
- Kontrol noktası sayesinde kaldığınız yerden devam edebilirsiniz
- Checkpoint allows resuming from where you left

### Sorun 3: Ctrl+C ile program kesintiye uğradı
### Problem 3: Program interrupted with Ctrl+C

**Çözüm / Solution:**
```bash
# Baştan başla - kontrol noktasından devam edecektir
# Restart - will resume from checkpoint
python main.py --run
```

### Sorun 4: Excel dosyası kilitli / Excel file locked

**Çözüm / Solution:**
- Excel dosyasını kapatın  
- Close the Excel file
- Programı yeniden başlatın
- Restart the program

---

## 🎓 TÜRKÇE KAVRAMLAR LÜGATİ / TURKISH CONCEPTS GLOSSARY

### Dijital Dönüşüm / Digital Transformation
- **Dijital Dönüşüm**: İş süreçlerinin teknoloji aracılığıyla modernizasyonu
- **Endüstri 4.0**: Sanayinin dördüncü devrim aşaması (IoT, Big Data, AI)
- **Veri Analitiği**: İş kararlarını veri ile destekleme
- **Siber Güvenlik**: Dijital tehditlerden koruma

### Yeşil Dönüşüm / Green Transformation
- **Sürdürülebilirlik**: Çevre ve ekonominin uzun vadeli dengesi
- **Karbon Ayak İzi**: İşletme faaliyetlerinin iklime etkisi
- **Enerji Verimliliği**: Daha az enerjiyle daha çok çıktı
- **Dairesel Ekonomi**: Atıkları en aza indir, yeniden kullan

---

## 📞 DESTEK / SUPPORT

- **Hata Bildir / Report Bugs**: Terminalde görülen hata mesajlarını kaydedin
- **Proje Reposu / Project Repo**: https://github.com/skillab-turkiye/...  
- **İletişim / Contact**: [contact info]

---

## ✨ NOTLAR / NOTES

1. ✅ **Excel Güvenliği**: Excel dosyası açık olsa bile append çalışır (yadırı değil, ama tavsiye edilmez)
2. ✅ **Excel Safety**: Excel append works even if file is open (unusual but not recommended)
3. ✅ **Kontrollü Hız**: Sistem tasarım gereği yavaş hareket eder (anti-bot koruması)
4. ✅ **Controlled Speed**: System intentionally moves slowly (anti-bot protection)
5. ✅ **Tarayıcı Görünür**: Cloudflare çözmek için tarayıcı görünür kalır
6. ✅ **Browser Visible**: Browser stays visible for Cloudflare solving
7. ✅ **Otomatik Yeniden Başlatma**: Hatalarda program KAPANMAZ, otomatik devam eder
8. ✅ **Auto-Resume**: Errors don't close program, auto-continues

---

**Başarılı Tarama Dilerim! / Good luck with your scraping!**

Generated: 24.03.2026
Skillab Türkiye Team
