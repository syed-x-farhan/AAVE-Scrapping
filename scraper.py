from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException, StaleElementReferenceException
import time
import pandas as pd
from datetime import datetime

# Set up Chrome options
chrome_options = Options()
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--headless")  # Runs Chrome in headless mode

# Initialize WebDriver
driver = webdriver.Chrome(options=chrome_options)
driver.maximize_window()

# Define target date (October 3, 2020) as timestamp
target_date = datetime(2020, 10, 3).timestamp() * 1000  # Convert to milliseconds

try:
    # Open the CoinMarketCap community page for AAVE
    url = "https://coinmarketcap.com/community/search/latest/aave/"
    print(f"Opening URL: {url}")
    driver.get(url)
    
    # Wait for content to load
    time.sleep(10)
    
    posts_data = []
    oldest_timestamp_ms = float('inf')
    scroll_count = 0
    max_scrolls = 100
    
    print("Starting data collection...")
    
    while scroll_count < max_scrolls:
        post_wrappers = driver.find_elements(By.CSS_SELECTOR, "div.post-wrapper.community")
        
        if not post_wrappers:
            print("No more posts found, stopping.")
            break
            
        new_posts_found = False
        
        for wrapper in post_wrappers:
            try:
                post_id = wrapper.get_attribute("data-post-id")
                timestamp_ms = wrapper.get_attribute("data-post-time")
                
                if timestamp_ms:
                    timestamp_ms = float(timestamp_ms)
                    if timestamp_ms < oldest_timestamp_ms:
                        oldest_timestamp_ms = timestamp_ms
                    
                    if timestamp_ms < target_date:
                        print("Reached target date, stopping.")
                        break
                    
                    timestamp_date = datetime.fromtimestamp(timestamp_ms / 1000)
                    formatted_timestamp = timestamp_date.strftime("%Y-%m-%d %H:%M:%S")
                else:
                    formatted_timestamp = "Unknown"
                
                try:
                    content_element = wrapper.find_element(By.CSS_SELECTOR, "div.post-text-wrapper.community")
                    content_text = content_element.text if content_element else "No content"
                except NoSuchElementException:
                    content_text = "Could not find content"
                
                posts_data.append({
                    "post_id": post_id,
                    "timestamp": formatted_timestamp,
                    "content_text": content_text,
                    "scrape_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })
                
                new_posts_found = True
                print(f"Collected post from {formatted_timestamp}")
                
            except Exception as e:
                print(f"Error processing post: {str(e)}")
                continue
        
        if not new_posts_found:
            print("No new posts found, stopping.")
            break
            
        print(f"Scrolling... ({scroll_count + 1}/{max_scrolls})")
        driver.execute_script("window.scrollBy(0, 1000);")
        time.sleep(3)
        scroll_count += 1
    
    if posts_data:
        df = pd.DataFrame(posts_data)
        df.to_csv("aave_posts_complete.csv", index=False)
        print(f"Saved {len(posts_data)} posts to CSV")
    else:
        print("No posts collected.")

except Exception as e:
    print(f"Error: {e}")

finally:
    driver.quit()
    print("Browser closed")
