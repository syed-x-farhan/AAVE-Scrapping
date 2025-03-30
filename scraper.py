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
import logging
import os
import sys
import tempfile
import shutil

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("scraper.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger()

def setup_driver():
    """Set up and return the Chrome WebDriver with appropriate options for GitHub Actions"""
    # Set up Chrome options
    chrome_options = Options()
    
    # Essential options for GitHub Actions
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--headless")  # Always run headless in GitHub Actions
    
    # Create a unique temporary directory that will be automatically cleaned up
    temp_dir = tempfile.mkdtemp(prefix="chrome-data-")
    logger.info(f"Created temporary directory for Chrome: {temp_dir}")
    
    # Use the temporary directory for Chrome data
    chrome_options.add_argument(f"--user-data-dir={temp_dir}")
    
    # Add user agent to appear more like a regular browser
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    # Additional options to improve stability
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-features=NetworkService")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-notifications")
    
    # Add these options for running in CI environment
    chrome_options.add_argument("--disable-setuid-sandbox")
    chrome_options.add_argument("--disable-software-rasterizer")
    
    try:
        # Initialize Chrome driver
        driver = webdriver.Chrome(options=chrome_options)
        return driver, temp_dir
    except Exception as e:
        logger.error(f"Failed to initialize WebDriver: {str(e)}")
        # Clean up the directory we created
        try:
            shutil.rmtree(temp_dir, ignore_errors=True)
        except Exception:
            pass
        sys.exit(1)

def click_read_all_button(driver, button, max_retries=3):
    """Attempt to click a 'Read all' button with retries"""
    for attempt in range(max_retries):
        try:
            # Scroll to make button visible
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", button)
            time.sleep(0.5)
            
            # Use JavaScript click as it's more reliable
            driver.execute_script("arguments[0].click();", button)
            logger.info("Successfully clicked 'Read all' button")
            time.sleep(1)
            return True
        except (ElementClickInterceptedException, StaleElementReferenceException) as e:
            if attempt < max_retries - 1:
                logger.warning(f"Click failed, retry {attempt+1}/{max_retries}: {str(e)}")
                time.sleep(0.5)
            else:
                logger.warning(f"Failed to click 'Read all' button after {max_retries} attempts: {str(e)}")
    return False

def save_data(posts_data, filename, intermediate=False):
    """Save the collected data to a CSV file"""
    if not posts_data:
        logger.warning("No posts to save")
        return
        
    df = pd.DataFrame(posts_data)
    
    # Sort by timestamp so oldest posts are last
    try:
        df['timestamp_ms'] = pd.to_numeric(df['timestamp_ms'], errors='coerce')
        df = df.sort_values(by='timestamp_ms', ascending=False)
    except Exception as e:
        logger.warning(f"Error sorting data: {str(e)}")
    
    # Create output directory if it doesn't exist
    os.makedirs("output", exist_ok=True)
    
    # Save to CSV
    if intermediate:
        outfile = f"output/aave_posts_intermediate_{len(posts_data)}.csv"
    else:
        outfile = "output/aave_posts_complete.csv"
        
    df.to_csv(outfile, index=False)
    logger.info(f"Saved {len(posts_data)} posts to {outfile}")

def scrape_coinmarketcap():
    """Main function to scrape CoinMarketCap community posts"""
    # Define target date (October 4, 2020) as timestamp
    target_date = datetime(2020, 10, 4).timestamp() * 1000  # Convert to milliseconds
    
    # Read All button CSS selector
    read_all_selector = "span.read-all"
    
    driver = None
    temp_dir = None
    posts_data = []
    
    # Set a maximum runtime for GitHub Actions (3 hours)
    max_runtime = 3 * 60 * 60  # 3 hours in seconds
    start_time = time.time()
    
    try:
        # Initialize WebDriver
        driver, temp_dir = setup_driver()
        
        # Open the CoinMarketCap community page for AAVE
        url = "https://coinmarketcap.com/community/search/latest/aave/"
        logger.info(f"Opening URL: {url}")
        driver.get(url)
        
        # Handle cookie consent if present
        try:
            WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "button[id*='cookie'], button[id*='consent']"))
            ).click()
            logger.info("Clicked cookie consent button")
        except (TimeoutException, NoSuchElementException):
            logger.info("No cookie consent dialog found")
        
        # Wait for posts to appear dynamically
        try:
            WebDriverWait(driver, 30).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.post-wrapper.community"))
            )
            logger.info("Posts loaded successfully.")
        except TimeoutException:
            logger.error("Timed out waiting for posts to load. Exiting.")
            if driver:
                driver.quit()
            return
    
        collected_post_ids = set()
        oldest_timestamp_ms = float('inf')
        scroll_count = 0
        no_new_posts_count = 0  # Track consecutive scrolls with no new posts
        
        logger.info("Starting data collection...")
    
        while True:
            # Check if we've exceeded the maximum runtime
            current_runtime = time.time() - start_time
            if current_runtime > max_runtime:
                logger.info(f"Reached maximum runtime of {max_runtime/3600:.1f} hours, stopping.")
                break
                
            try:
                # Wait for new posts to load after scrolling with increased timeout
                WebDriverWait(driver, 20).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.post-wrapper.community"))
                )
            except TimeoutException:
                logger.warning("Timeout waiting for posts. Attempting to continue...")
                # Try a different scroll approach
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(random.uniform(3.0, 5.0))
                continue
    
            # Get the current height for comparison after scrolling
            last_height = driver.execute_script("return document.body.scrollHeight")
            
            post_wrappers = driver.find_elements(By.CSS_SELECTOR, "div.post-wrapper.community")
    
            if not post_wrappers:
                no_new_posts_count += 1
                if no_new_posts_count >= 5:
                    logger.info("No posts found after multiple attempts, stopping.")
                    break
                logger.warning(f"No posts found on attempt {no_new_posts_count}, trying again...")
                time.sleep(random.uniform(3.0, 5.0))
                continue
    
            prev_posts_count = len(collected_post_ids)
            reached_target_date = False
    
            for wrapper in post_wrappers:
                try:
                    # Scroll element into view to ensure it's properly loaded
                    driver.execute_script("arguments[0].scrollIntoView({block: 'center', behavior: 'smooth'});", wrapper)
                    time.sleep(0.3)  # Small pause after scrolling to element
                    
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
                            logger.info(f"Reached target date (October 4, 2020), stopping. Post timestamp: {datetime.fromtimestamp(timestamp_ms/1000).strftime('%Y-%m-%d')}")
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
                                    # Ensure button is clickable
                                    WebDriverWait(driver, 5).until(
                                        EC.element_to_be_clickable((By.CSS_SELECTOR, read_all_selector))
                                    )
                                    if click_read_all_button(driver, button):
                                        # Successfully clicked, give time for content to expand
                                        time.sleep(1)
                                except Exception as e:
                                    logger.warning(f"Error with Read all button: {str(e)}")
                    except Exception as e:
                        logger.warning(f"Error handling Read all button: {str(e)}")
    
                    # Now get the content after potentially expanding it
                    try:
                        content_element = wrapper.find_element(By.CSS_SELECTOR, "div.post-text-wrapper.community")
                        content_text = content_element.text if content_element else "No content"
                    except NoSuchElementException:
                        content_text = "Could not find content"
    
                    # Try to get the author name
                    try:
                        author_element = wrapper.find_element(By.CSS_SELECTOR, "span.community-author")
                        author_name = author_element.text if author_element else "Unknown"
                    except NoSuchElementException:
                        author_name = "Unknown"
    
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
    
                except StaleElementReferenceException:
                    logger.warning("Encountered a stale element reference, skipping this post.")
                    continue
                except NoSuchElementException as e:
                    logger.warning(f"Element not found: {str(e)}")
                    continue
                except Exception as e:
                    logger.warning(f"Error processing post: {str(e)}")
                    continue
    
            if reached_target_date:
                logger.info("Breaking main loop after reaching target date")
                break
    
            # Reset counter if we found new posts
            if len(collected_post_ids) > prev_posts_count:
                no_new_posts_count = 0
            else:
                no_new_posts_count += 1
                if no_new_posts_count >= 5:
                    logger.info(f"No new posts found after {no_new_posts_count} scrolls, stopping.")
                    break
                logger.warning(f"No new posts in this scroll ({no_new_posts_count}/5 attempts)")
    
            # Save intermediate results every 50 posts
            if len(posts_data) % 50 == 0 and len(posts_data) > 0:
                logger.info(f"Saving intermediate results with {len(posts_data)} posts...")
                save_data(posts_data, f"aave_posts_intermediate_{len(posts_data)}.csv", intermediate=True)
    
            # Anti-ban measures
            if scroll_count % 10 == 0 and scroll_count > 0:
                pause_time = random.uniform(10.0, 20.0)
                logger.info(f"Taking a longer break ({pause_time:.2f}s) to avoid rate limiting...")
                time.sleep(pause_time)
    
            # Random wait time before next scroll
            wait_time = random.uniform(2.0, 4.0)
            logger.info(f"Waiting {wait_time:.2f} seconds before scrolling...")
            time.sleep(wait_time)
    
            # Try multiple scroll methods to be more effective
            if scroll_count % 3 == 0:
                # Method 1: Scroll to bottom
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            else:
                # Method 2: Scroll by a random amount
                scroll_amount = random.randint(800, 1500)
                driver.execute_script(f"window.scrollBy(0, {scroll_amount});")
            
            scroll_count += 1
    
            # Check if scroll was effective
            time.sleep(3)  # Give more time for potential new content to load
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                logger.warning("Page height didn't change after scroll, trying a different approach...")
                # Try clicking a "Load more" button if it exists
                try:
                    load_more_buttons = driver.find_elements(By.XPATH, "//button[contains(text(), 'Load') or contains(text(), 'more') or contains(text(), 'Show')]")
                    if load_more_buttons:
                        for button in load_more_buttons:
                            try:
                                driver.execute_script("arguments[0].click();", button)
                                logger.info("Clicked a 'Load more' button")
                                time.sleep(3)
                                break
                            except Exception as e:
                                logger.warning(f"Failed to click 'Load more' button: {str(e)}")
                except Exception as e:
                    logger.warning(f"Error finding 'Load more' button: {str(e)}")
    
            if scroll_count % 5 == 0:
                logger.info(f"Completed {scroll_count} scrolls, collected {len(posts_data)} posts so far.")
                if oldest_timestamp_ms != float('inf'):
                    logger.info(f"Oldest post timestamp: {datetime.fromtimestamp(oldest_timestamp_ms/1000).strftime('%Y-%m-%d')}")
    
        # Save the final results
        save_data(posts_data, "aave_posts_complete.csv", intermediate=False)
        
    except Exception as e:
        logger.error(f"Critical error: {e}")
        if posts_data:
            logger.info(f"Saving {len(posts_data)} posts collected before error...")
            save_data(posts_data, "aave_posts_error_recovery.csv", intermediate=False)
    
    finally:
        if driver:
            driver.quit()
            logger.info("Browser closed")
        
        # Clean up the temporary directory
        if temp_dir and os.path.exists(temp_dir):
            try:
                shutil.rmtree(temp_dir, ignore_errors=True)
                logger.info(f"Cleaned up temporary directory: {temp_dir}")
            except Exception as e:
                logger.warning(f"Failed to clean up temporary directory: {str(e)}")

if __name__ == "__main__":
    try:
        scrape_coinmarketcap()
    except KeyboardInterrupt:
        logger.info("Scraping stopped by user")
    except Exception as e:
        logger.critical(f"Unexpected error: {str(e)}")
