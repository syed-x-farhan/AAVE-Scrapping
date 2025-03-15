from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import NoSuchElementException, StaleElementReferenceException, TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import random
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

# Define target date (October 5, 2020) as timestamp in milliseconds
target_date = datetime(2020, 10, 5).timestamp() * 1000  

# Open the CoinMarketCap community page for AAVE
url = "https://coinmarketcap.com/community/search/latest/aave/"
print(f"Opening URL: {url}")
driver.get(url)

try:
    # Wait for initial content to load
    WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.CSS_SELECTOR, "div.post-wrapper.community")))
    print("Page loaded successfully.")
    
    posts_data = []
    oldest_timestamp_ms = float('inf')
    scroll_count = 0

    print("Starting data collection...")

    while True:
        post_wrappers = driver.find_elements(By.CSS_SELECTOR, "div.post-wrapper.community")

        if not post_wrappers:
            print("No more posts found, stopping.")
            break

        new_posts_found = False

        for wrapper in post_wrappers:
            try:
                post_id = wrapper.get_attribute("data-post-id")

                # Skip if already processed
                if any(post['post_id'] == post_id for post in posts_data):
                    continue

                timestamp_ms = wrapper.get_attribute("data-post-time")

                if timestamp_ms:
                    timestamp_ms = float(timestamp_ms)
                    if timestamp_ms < oldest_timestamp_ms:
                        oldest_timestamp_ms = timestamp_ms

                    if timestamp_ms < target_date:
                        print("Reached target date (October 5, 2020), stopping.")
                        break

                    timestamp_date = datetime.fromtimestamp(timestamp_ms / 1000)
                    formatted_timestamp = timestamp_date.strftime("%Y-%m-%d %H:%M:%S")
                else:
                    formatted_timestamp = "Unknown"

                # Extract post content
                try:
                    content_element = wrapper.find_element(By.CSS_SELECTOR, "div.post-text-wrapper.community")
                    content_text = content_element.text.strip() if content_element else "No content"
                except NoSuchElementException:
                    content_text = "Could not find content"

                posts_data.append({
                    "post_id": post_id,
                    "timestamp": formatted_timestamp,
                    "timestamp_ms": timestamp_ms,
                    "content_text": content_text,
                    "scrape_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })

                new_posts_found = True
                print(f"Collected post from {formatted_timestamp}")

            except (NoSuchElementException, StaleElementReferenceException):
                print("Encountered a stale or missing element, skipping.")
                continue
            except Exception as e:
                print(f"Error processing post: {str(e)}")
                continue

        if not new_posts_found:
            print("No new posts found, stopping.")
            break

        # Anti-ban measures: short break every 20 scrolls
        if scroll_count % 20 == 0 and scroll_count > 0:
            print("Taking a short break to avoid rate limiting...")
            time.sleep(10)

        # Random wait time before next scroll
        wait_time = random.uniform(1.5, 3.0)
        print(f"Waiting {wait_time:.2f} seconds before scrolling...")
        time.sleep(wait_time)

        # Smooth scrolling
        ActionChains(driver).scroll_by_amount(0, 1000).perform()

        scroll_count += 1

    if posts_data:
        df = pd.DataFrame(posts_data)
        df.to_csv("aave_posts_complete.csv", index=False, encoding='utf-8')
        print(f"Saved {len(posts_data)} posts to CSV")
    else:
        print("No posts collected.")

except TimeoutException:
    print("Page took too long to load, exiting.")
except Exception as e:
    print(f"Unexpected error: {e}")
finally:
    driver.quit()
    print("Browser closed")
