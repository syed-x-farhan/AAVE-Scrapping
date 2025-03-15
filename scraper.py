from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.common.exceptions import (
    NoSuchElementException, StaleElementReferenceException, TimeoutException, ElementClickInterceptedException
)
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

# Define target date (October 4, 2020) as timestamp
target_date = datetime(2020, 10, 4).timestamp() * 1000  # Convert to milliseconds

# Read All button CSS selector
read_all_selector = "span.read-all"

try:
    # Open the CoinMarketCap community page for AAVE
    url = "https://coinmarketcap.com/community/search/latest/aave/"
    print(f"Opening URL: {url}")
    driver.get(url)

    # Wait for posts to appear dynamically
    try:
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.post-wrapper.community"))
        )
        print("Posts loaded successfully.")
    except TimeoutException:
        print("Timed out waiting for posts to load. Exiting.")
        driver.quit()
        exit()

    posts_data = []
    collected_post_ids = set()
    oldest_timestamp_ms = float('inf')
    scroll_count = 0

    print("Starting data collection...")

    while True:
        try:
            # Wait for new posts to load after scrolling
            WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.post-wrapper.community"))
            )
        except TimeoutException:
            print("No more posts loading, stopping.")
            break

        post_wrappers = driver.find_elements(By.CSS_SELECTOR, "div.post-wrapper.community")

        if not post_wrappers:
            print("No more posts found, stopping.")
            break

        new_posts_found = False
        reached_target_date = False

        for wrapper in post_wrappers:
            try:
                post_id = wrapper.get_attribute("data-post-id")

                if post_id in collected_post_ids:
                    continue

                timestamp_ms = wrapper.get_attribute("data-post-time")
                if timestamp_ms:
                    timestamp_ms = float(timestamp_ms)
                    if timestamp_ms < oldest_timestamp_ms:
                        oldest_timestamp_ms = timestamp_ms

                    # Check if we've reached our target date
                    if timestamp_ms < target_date:
                        print(f"Reached target date (October 4, 2020), stopping. Post timestamp: {datetime.fromtimestamp(timestamp_ms/1000).strftime('%Y-%m-%d')}")
                        reached_target_date = True
                        break

                    timestamp_date = datetime.fromtimestamp(timestamp_ms / 1000)
                    formatted_timestamp = timestamp_date.strftime("%Y-%m-%d %H:%M:%S")
                else:
                    formatted_timestamp = "Unknown"
                    timestamp_ms = None

                # Try to click "Read all" button if it exists within this post
                try:
                    read_all_buttons = wrapper.find_elements(By.CSS_SELECTOR, read_all_selector)

                    if read_all_buttons:
                        for button in read_all_buttons:
                            try:
                                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", button)
                                time.sleep(0.5)

                                max_retries = 3
                                for attempt in range(max_retries):
                                    try:
                                        button.click()
                                        print("Successfully clicked 'Read all' button")
                                        time.sleep(1)
                                        break
                                    except (ElementClickInterceptedException, StaleElementReferenceException) as e:
                                        if attempt < max_retries - 1:
                                            print(f"Click failed, retry {attempt+1}/{max_retries}: {str(e)}")
                                            time.sleep(0.5)
                                        else:
                                            print(f"Failed to click 'Read all' button after {max_retries} attempts: {str(e)}")
                            except Exception as e:
                                print(f"Error clicking Read all button: {str(e)}")
                except Exception as e:
                    print(f"Error handling Read all button: {str(e)}")

                # Now get the content after potentially expanding it
                try:
                    content_element = wrapper.find_element(By.CSS_SELECTOR, "div.post-text-wrapper.community")
                    content_text = content_element.text if content_element else "No content"
                except NoSuchElementException:
                    content_text = "Could not find content"

                posts_data.append({
                    "post_id": post_id,
                    "timestamp": formatted_timestamp,
                    "timestamp_ms": timestamp_ms,
                    "content_text": content_text,
                    "scrape_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })

                collected_post_ids.add(post_id)
                new_posts_found = True
                print(f"Collected post from {formatted_timestamp}")

            except StaleElementReferenceException:
                print("Encountered a stale element reference, skipping this post.")
                continue
            except NoSuchElementException as e:
                print(f"Element not found: {str(e)}")
                continue
            except Exception as e:
                print(f"Error processing post: {str(e)}")
                continue

        if reached_target_date:
            print("Breaking main loop after reaching target date")
            break

        if not new_posts_found:
            print("No new posts found in this scroll, stopping.")
            break

        # Save intermediate results every 100 posts
        if len(posts_data) % 100 == 0 and len(posts_data) > 0:
            print(f"Saving intermediate results with {len(posts_data)} posts...")
            temp_df = pd.DataFrame(posts_data)
            temp_df.to_csv(f"aave_posts_intermediate_{len(posts_data)}.csv", index=False)

        # Anti-ban measures
        if scroll_count % 20 == 0 and scroll_count > 0:
            pause_time = random.uniform(8.0, 15.0)
            print(f"Taking a longer break ({pause_time:.2f}s) to avoid rate limiting...")
            time.sleep(pause_time)

        # Random wait time before next scroll
        wait_time = random.uniform(1.5, 3.0)
        print(f"Waiting {wait_time:.2f} seconds before scrolling...")
        time.sleep(wait_time)

        # Scroll down
        driver.execute_script("window.scrollBy(0, 1000);")
        scroll_count += 1

        if scroll_count % 5 == 0:
            print(f"Completed {scroll_count} scrolls, collected {len(posts_data)} posts so far.")
            if oldest_timestamp_ms != float('inf'):
                print(f"Oldest post timestamp: {datetime.fromtimestamp(oldest_timestamp_ms/1000).strftime('%Y-%m-%d')}")

    if posts_data:
        df = pd.DataFrame(posts_data)
        df.to_csv("aave_posts_complete.csv", index=False)
        print(f"Saved {len(posts_data)} posts to CSV")
    else:
        print("No posts collected.")

except Exception as e:
    print(f"Critical error: {e}")
    if posts_data:
        print(f"Saving {len(posts_data)} posts collected before error...")
        pd.DataFrame(posts_data).to_csv("aave_posts_error_recovery.csv", index=False)

finally:
    driver.quit()
    print("Browser closed")
