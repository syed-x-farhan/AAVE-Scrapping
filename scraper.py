import os
import random
import time
import logging
import pandas as pd
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.common.exceptions import (
    NoSuchElementException, 
    StaleElementReferenceException, 
    TimeoutException, 
    ElementClickInterceptedException
)
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("scraper.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def setup_chrome_options():
    """Setup and return Chrome options."""
    chrome_options = Options()
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    
    # Unique user data directory
    chrome_options.add_argument(f"--user-data-dir=/tmp/chrome-user-data-{random.randint(1, 10000)}")
    
    # Optional: Run in headless mode (uncomment if needed)
    # chrome_options.add_argument("--headless")
    
    # User agent to mimic a regular browser
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    return chrome_options

def scrape_coinmarketcap_posts():
    """
    Scrape AAVE community posts from CoinMarketCap.
    
    Returns:
    list: A list of dictionaries containing scraped post data
    """
    # Define target date (October 4, 2020) as timestamp
    target_date = datetime(2020, 10, 4).timestamp() * 1000  # Convert to milliseconds
    
    # Read All button CSS selector
    read_all_selector = "span.read-all"
    
    # Setup Chrome options
    chrome_options = setup_chrome_options()
    
    # Initialize posts data storage
    posts_data = []
    collected_post_ids = set()
    oldest_timestamp_ms = float('inf')
    scroll_count = 0
    no_new_posts_count = 0
    
    # Initialize WebDriver
    try:
        # Use WebDriverManager to handle driver installation
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        driver.maximize_window()
    except Exception as e:
        logger.error(f"WebDriver initialization error: {e}")
        return []
    
    try:
        # Open the CoinMarketCap community page for AAVE
        url = "https://coinmarketcap.com/community/search/latest/aave/"
        logger.info(f"Opening URL: {url}")
        driver.get(url)

        # Wait for posts to load
        try:
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.post-wrapper.community"))
            )
            logger.info("Posts loaded successfully.")
        except TimeoutException:
            logger.error("Timed out waiting for posts to load.")
            return []

        logger.info("Starting data collection...")

        while True:
            # Wait for posts to load
            try:
                WebDriverWait(driver, 15).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.post-wrapper.community"))
                )
            except TimeoutException:
                logger.warning("Timeout waiting for posts. Attempting to continue...")
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(random.uniform(3.0, 5.0))
                continue

            # Get current page height
            last_height = driver.execute_script("return document.body.scrollHeight")
            
            # Find post wrappers
            post_wrappers = driver.find_elements(By.CSS_SELECTOR, "div.post-wrapper.community")

            if not post_wrappers:
                no_new_posts_count += 1
                if no_new_posts_count >= 5:
                    logger.info("No posts found after multiple attempts, stopping.")
                    break
                logger.warning(f"No posts found on attempt {no_new_posts_count}")
                time.sleep(random.uniform(3.0, 5.0))
                continue

            reached_target_date = False

            for wrapper in post_wrappers:
                try:
                    # Scroll to element
                    driver.execute_script("arguments[0].scrollIntoView({block: 'center', behavior: 'smooth'});", wrapper)
                    time.sleep(0.3)
                    
                    # Get post ID
                    post_id = wrapper.get_attribute("data-post-id")
                    if post_id in collected_post_ids:
                        continue

                    # Get timestamp
                    timestamp_ms = wrapper.get_attribute("data-post-time")
                    if timestamp_ms:
                        timestamp_ms = float(timestamp_ms)
                        
                        # Track oldest timestamp
                        if timestamp_ms < oldest_timestamp_ms:
                            oldest_timestamp_ms = timestamp_ms

                        # Check if reached target date
                        if timestamp_ms < target_date:
                            logger.info(f"Reached target date (October 4, 2020)")
                            reached_target_date = True
                            break

                        timestamp_date = datetime.fromtimestamp(timestamp_ms / 1000)
                        formatted_timestamp = timestamp_date.strftime("%Y-%m-%d %H:%M:%S")
                    else:
                        formatted_timestamp = "Unknown"
                        timestamp_ms = None

                    # Expand full post content
                    try:
                        read_all_buttons = wrapper.find_elements(By.CSS_SELECTOR, read_all_selector)
                        for button in read_all_buttons:
                            try:
                                driver.execute_script("arguments[0].click();", button)
                                time.sleep(0.5)
                            except Exception as e:
                                logger.warning(f"Error expanding post: {e}")
                    except Exception as e:
                        logger.warning(f"Error finding 'Read all' button: {e}")

                    # Get post content
                    try:
                        content_element = wrapper.find_element(By.CSS_SELECTOR, "div.post-text-wrapper.community")
                        content_text = content_element.text if content_element else "No content"
                    except NoSuchElementException:
                        content_text = "Could not find content"

                    # Get author name
                    try:
                        author_element = wrapper.find_element(By.CSS_SELECTOR, "span.community-author")
                        author_name = author_element.text if author_element else "Unknown"
                    except NoSuchElementException:
                        author_name = "Unknown"

                    # Store post data
                    posts_data.append({
                        "post_id": post_id,
                        "author": author_name,
                        "timestamp": formatted_timestamp,
                        "timestamp_ms": timestamp_ms,
                        "content_text": content_text,
                        "scrape_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    })

                    collected_post_ids.add(post_id)
                    logger.info(f"Collected post from {formatted_timestamp} by {author_name}")

                except Exception as e:
                    logger.warning(f"Error processing post: {e}")
                    continue

            if reached_target_date:
                break

            # Scroll and wait
            scroll_amount = random.randint(800, 1500)
            driver.execute_script(f"window.scrollBy(0, {scroll_amount});")
            time.sleep(random.uniform(2.0, 4.0))

            # Check if new content loaded
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                logger.warning("No new content loaded after scroll")
                break

            scroll_count += 1
            if scroll_count % 10 == 0:
                logger.info(f"Completed {scroll_count} scrolls, collected {len(posts_data)} posts")

        # Save collected posts
        if posts_data:
            df = pd.DataFrame(posts_data)
            df.to_csv("aave_posts_complete.csv", index=False)
            logger.info(f"Saved {len(posts_data)} posts to CSV")
        else:
            logger.warning("No posts collected.")

        return posts_data

    except Exception as e:
        logger.error(f"Critical scraping error: {e}")
        return posts_data

    finally:
        # Always close the driver
        driver.quit()
        logger.info("Browser closed")

def main():
    """Main function to run the scraper."""
    try:
        # Run the scraper
        scraped_posts = scrape_coinmarketcap_posts()
        print(f"Total posts scraped: {len(scraped_posts)}")
    except Exception as e:
        logger.error(f"Scraping failed: {e}")

if __name__ == "__main__":
    main()
