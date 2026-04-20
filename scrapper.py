import json
import time
import random
from datetime import datetime
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

def scrape_all_wired_categories():
    print("--- Memulai Proses Scraping Semua Kategori Wired.com ---")
    
    options = uc.ChromeOptions()
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--headless=new") 

    print("Membuka browser anti-deteksi...")
    driver = uc.Chrome(options=options, use_subprocess=True)
    
    articles_data = []
    seen_urls = set()
    
    categories = [
        "security", 
        "politics", 
        "business", 
        "science", 
        "culture",
        "reviews"
    ] 
    
    start_page = 1
    end_page = 2

    try:
        for category in categories:
            print(f" KATEGORI: {category.upper()}")
            
            for page in range(start_page, end_page + 1):
                target_url = f"https://www.wired.com/category/{category}/?page={page}"
                print(f"[{category.upper()} - Halaman {page}] Mengakses: {target_url}")
                
                driver.get(target_url)
                
                if page == 1 and category == categories[0]:
                    print("Menunggu sistem bypass JS Blocker (10 detik)...")
                    time.sleep(10)
                
                try:
                    WebDriverWait(driver, 15).until(
                        EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'SummaryItemWrapper')]"))
                    )
                except TimeoutException:
                    print(f"Halaman {page} untuk {category.upper()} kosong/tertahan blocker. Lanjut ke halaman berikutnya...")
                    continue

                elements = driver.find_elements(By.XPATH, "//div[contains(@class, 'SummaryItemWrapper')]")
                
                new_found = 0
                for elemen in elements:
                    try:
                        link_el = elemen.find_element(By.CSS_SELECTOR, "a[class*='SummaryItemHedLink'], a")
                        url = link_el.get_attribute("href")

                        if url and url not in seen_urls:
                            title = elemen.find_element(By.TAG_NAME, "h3").text.strip()
                            if not title or "Play/Pause" in title or len(title) < 5:
                                continue

                            try:
                                desc = elemen.find_element(By.CSS_SELECTOR, "[class*='SummaryItemDek']").text.strip()
                            except:
                                desc = "-"

                            try:
                                author_raw = elemen.find_element(By.CSS_SELECTOR, "[class*='BylineName']").text.strip()
                                author = f"By {author_raw.replace('By', '').strip()}"
                            except:
                                author = "By Unknown"

                            articles_data.append({
                                "title": title,
                                "url": url,
                                "category": category.capitalize(),
                                "description": desc,
                                "author": author,
                                "page_source": page,
                                "scraped_at": datetime.now().isoformat(),
                                "source": "Wired.com"
                            })
                            seen_urls.add(url)
                            new_found += 1
                    except:
                        continue

                print(f"Berhasil mengambil {new_found} data unik. (Total memori: {len(articles_data)})")
                
                time.sleep(random.uniform(3.5, 6.0))

        output = {
            "session_id": f"wired_all_cat_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            "timestamp": datetime.now().isoformat(),
            "categories_scraped": categories,
            "pages_per_category": f"{start_page} to {end_page}",
            "total_articles_count": len(articles_data),
            "articles": articles_data 
        }

        with open("wired_all_categories.json", "w", encoding="utf-8") as f:
            json.dump(output, f, indent=4, ensure_ascii=False)

        print(f"\nSCRAPING SELESAI ")
        print(f"Berhasil menyimpan {len(articles_data)} artikel ke wired_all_categories.json")

    except Exception as e:
        print(f"Terjadi error utama: {e}")
    finally:
        driver.quit()

if __name__ == "__main__":
    scrape_all_wired_categories()