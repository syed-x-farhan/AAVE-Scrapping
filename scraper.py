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
from datetime import datetime, timedelta
import logging
import os
import sys
import tempfile
import shutil
import json

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("scraper_update.log"),
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
    
    # Add option to disable animations which can cause stale elements
    chrome_options.add_argument("--disable-animations")
    
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

# Custom wait condition for stale-safe element presence
class element_has_css_class(object):
    def __init__(self, locator, css_class):
        self.locator = locator
        self.css_class = css_class
        
    def __call__(self, driver):
        try:
            element = driver.find_element(*self.locator)
            return self.css_class in element.get_attribute("class")
        except StaleElementReferenceException:
            return False

def safe_find_elements(driver, by, value, wait_time=10):
    """Safely find elements with retry logic for stale elements"""
    end_time = time.time() + wait_time
    while time.time() < end_time:
        try:
            elements = driver.find_elements(by, value)
            # Verify elements aren't stale by doing a simple attribute check
            for element in elements:
                _ = element.is_enabled()  # This will throw if stale
            return elements
        except StaleElementReferenceException:
            time.sleep(0.5)
    # If we got here, we timed out
    return []

def safe_get_attribute(element, attribute, default=None, max_retries=3):
    """Safely get an attribute from an element with retry logic"""
    for attempt in range(max_retries):
        try:
            return element.get_attribute(attribute)
        except StaleElementReferenceException:
            if attempt < max_retries - 1:
                time.sleep(0.5)
            else:
                return default
    return default

def safe_get_text(element, default="", max_retries=3):
    """Safely get text from an element with retry logic"""
    for attempt in range(max_retries):
        try:
            return element.text
        except StaleElementReferenceException:
            if attempt < max_retries - 1:
                time.sleep(0.5)
            else:
                return default
    return default

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

def load_existing_data():
    """Load existing data from the complete CSV file"""
    posts_data = []
    collected_post_ids = set()
    newest_timestamp_ms = 0  # Track the newest timestamp in the existing data
    
    try:
        if os.path.exists("output/aave_posts_complete.csv"):
            df = pd.read_csv("output/aave_posts_complete.csv")
            
            # Ensure timestamp_ms is numeric for comparison
            df['timestamp_ms'] = pd.to_numeric(df['timestamp_ms'], errors='coerce')
            
            # Find the newest timestamp in the dataset
            if 'timestamp_ms' in df.columns and not df['timestamp_ms'].empty:
                newest_timestamp_ms = df['timestamp_ms'].max()
                logger.info(f"Newest timestamp in existing data: {datetime.fromtimestamp(newest_timestamp_ms/1000).strftime('%Y-%m-%d %H:%M:%S')}")
            
            posts_data = df.to_dict('records')
            collected_post_ids = set(df['post_id'].astype(str).tolist())
            logger.info(f"Loaded {len(posts_data)} existing posts from output file")
            return posts_data, collected_post_ids, newest_timestamp_ms
    except Exception as e:
        logger.warning(f"Error loading existing data: {str(e)}")
    
    logger.info("No existing data found, starting fresh")
    return [], set(), 0

def process_post(driver, wrapper, collected_post_ids, read_all_selector):
    """Process a single post wrapper element with stale-safe operations"""
    try:
        # Scroll element into view to ensure it's properly loaded - use a less aggressive scroll
        driver.execute_script("arguments[0].scrollIntoView({block: 'center', behavior: 'auto'});", wrapper)
        time.sleep(0.5)  # Increased pause after scrolling to element
        
        post_id = safe_get_attribute(wrapper, "data-post-id")
        if not post_id:
            logger.warning("Could not get post_id, skipping...")
            return None, None

        if post_id in collected_post_ids:
            logger.debug(f"Post {post_id} already collected, skipping...")
            return None, None

        timestamp_ms = safe_get_attribute(wrapper, "data-post-time")
        if timestamp_ms:
            timestamp_ms = float(timestamp_ms)
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
                        # Ensure button is clickable with a shorter wait
                        WebDriverWait(driver, 3).until(
                            EC.element_to_be_clickable((By.CSS_SELECTOR, read_all_selector))
                        )
                        if click_read_all_button(driver, button):
                            # Successfully clicked, give time for content to expand
                            time.sleep(1)
                    except Exception as e:
                        logger.debug(f"Error with Read all button: {str(e)}")
        except Exception as e:
            logger.debug(f"Error handling Read all button: {str(e)}")

        # Now get the content after potentially expanding it
        content_text = "No content"
        try:
            content_element = WebDriverWait(driver, 3).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.post-text-wrapper.community"))
            )
            content_text = safe_get_text(content_element, "No content")
        except (TimeoutException, NoSuchElementException, StaleElementReferenceException):
            try:
                # Second attempt with direct find
                content_element = wrapper.find_element(By.CSS_SELECTOR, "div.post-text-wrapper.community")
                content_text = safe_get_text(content_element, "No content")
            except (NoSuchElementException, StaleElementReferenceException):
                content_text = "Could not find content"

        # Try to get the author name
        author_name = "Unknown"
        try:
            author_element = wrapper.find_element(By.CSS_SELECTOR, "span.community-author")
            author_name = safe_get_text(author_element, "Unknown")
        except (NoSuchElementException, StaleElementReferenceException):
            author_name = "Unknown"

        post_data = {
            "post_id": post_id,
            "author": author_name,
            "timestamp": formatted_timestamp,
            "timestamp_ms": timestamp_ms,
            "content_text": content_text,
            "scrape_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        logger.info(f"Collected post from {formatted_timestamp} by {author_name}")
        return post_id, post_data
        
    except StaleElementReferenceException:
        # If the wrapper itself is stale
        logger.debug("Post element became stale during processing")
        return None, None
    except Exception as e:
        logger.warning(f"Error processing post: {str(e)}")
        return None, None

def save_updated_data(posts_data, filename="aave_posts_complete.csv"):
    """Save the updated data to a CSV file"""
    if not posts_data:
        logger.warning("No posts to save")
        return
        
    df = pd.DataFrame(posts_data)
    
    # Sort by timestamp so newest posts are first
    try:
        df['timestamp_ms'] = pd.to_numeric(df['timestamp_ms'], errors='coerce')
        df = df.sort_values(by='timestamp_ms', ascending=False)
    except Exception as e:
        logger.warning(f"Error sorting data: {str(e)}")
    
    # Create output directory if it doesn't exist
    os.makedirs("output", exist_ok=True)
    
    # Save backup of previous file
    if os.path.exists(f"output/{filename}"):
        backup_filename = f"output/backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}_aave_posts.csv"
        shutil.copy2(f"output/{filename}", backup_filename)
        logger.info(f"Created backup of existing data at {backup_filename}")
    
    # Save to CSV
    outfile = f"output/{filename}"
    df.to_csv(outfile, index=False)
    logger.info(f"Saved {len(posts_data)} posts to {outfile}")
        
def scrape_new_coinmarketcap_posts():
    """Function to scrape only new CoinMarketCap community posts"""
    # Read All button CSS selector
    read_all_selector = "span.read-all"
    
    driver = None
    temp_dir = None
    
    # Load existing data - we need the post IDs and the newest timestamp
    existing_posts, collected_post_ids, newest_timestamp_ms = load_existing_data()
    
    if not existing_posts:
        logger.warning("No existing data found. Please run the original scraper first.")
        return
        
    # Convert newest timestamp to datetime for better logging
    newest_timestamp_dt = datetime.fromtimestamp(newest_timestamp_ms / 1000) if newest_timestamp_ms > 0 else None
    logger.info(f"Scraping posts newer than {newest_timestamp_dt}")
    
    # Track new posts
    new_posts_data = []
    new_posts_count = 0
    
    # Set a maximum runtime (30 minutes should be enough for new posts)
    max_runtime = 30 * 60  # 30 minutes in seconds
    start_time = time.time()
    
    try:
        # Initialize WebDriver
        driver, temp_dir = setup_driver()
        
        # Open the CoinMarketCap community page for AAVE
        url = "https://coinmarketcap.com/community/search/latest/aave/"
        logger.info(f"Opening URL: {url}")
        driver.get(url)
        
        # Set page load timeout
        driver.set_page_load_timeout(30)
        
        # Handle cookie consent if present
        try:
            WebDriverWait(driver, 5).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "button[id*='cookie'], button[id*='consent']"))
            ).click()
            logger.info("Clicked cookie consent button")
        except (TimeoutException, NoSuchElementException):
            logger.info("No cookie consent dialog found")
        
        # Wait for posts to appear
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
    
        # Initialize tracking variables
        no_new_posts_count = 0
        page_number = 1
        scroll_count = 0
        found_overlap = False
        
        # Process the first page
        while True:
            # Check if we've exceeded maximum runtime
            current_runtime = time.time() - start_time
            if current_runtime > max_runtime:
                logger.info(f"Reached maximum runtime of {max_runtime/60:.1f} minutes, stopping.")
                break
                
            # Get all visible post wrappers
            try:
                post_wrappers = safe_find_elements(driver, By.CSS_SELECTOR, "div.post-wrapper.community")
                logger.info(f"Found {len(post_wrappers)} posts on current view")
            except Exception as e:
                logger.warning(f"Error finding posts: {str(e)}")
                post_wrappers = []
    
            if not post_wrappers:
                no_new_posts_count += 1
                if no_new_posts_count >= 3:
                    logger.info("No posts found after multiple attempts, stopping.")
                    break
                logger.warning("No posts found, trying again...")
                time.sleep(3)
                continue
    
            # Process posts - check if any of them are newer than our newest post
            new_in_batch = 0
            overlap_found = False
            
            # Process posts in smaller batches to reduce stale element issues
            batch_size = 5
            for i in range(0, len(post_wrappers), batch_size):
                batch = post_wrappers[i:i+batch_size]
                
                for wrapper in batch:
                    post_id, post_data = process_post(driver, wrapper, collected_post_ids, read_all_selector)
                    
                    if post_id and post_data:
                        timestamp_ms = post_data.get("timestamp_ms", 0)
                        
                        # Check if this post is newer than our newest existing post
                        if timestamp_ms and timestamp_ms > newest_timestamp_ms:
                            collected_post_ids.add(post_id)
                            new_posts_data.append(post_data)
                            new_in_batch += 1
                            logger.info(f"Found new post from {post_data.get('timestamp')} by {post_data.get('author')}")
                        else:
                            # We've reached posts that are already in our dataset
                            logger.info(f"Found overlap with existing data at post from {post_data.get('timestamp')}")
                            overlap_found = True
                            found_overlap = True
                            break
                
                if overlap_found:
                    break
                    
                # Take a short break between batches
                time.sleep(0.5)
            
            logger.info(f"Found {new_in_batch} new posts in this batch")
            new_posts_count += new_in_batch
            
            # If we found overlap with existing data, we can stop
            if overlap_found:
                logger.info("Found overlap with existing data, no more new posts to scrape")
                break
                
            # If no new posts were found in this batch, we might be at the end
            if new_in_batch == 0:
                no_new_posts_count += 1
                if no_new_posts_count >= 3:
                    logger.info("No new posts found after multiple attempts, stopping.")
                    break
            else:
                no_new_posts_count = 0
                
            # Scroll down to load more posts - use a mix of scrolling techniques
            if scroll_count % 3 == 0:
                # Scroll towards the bottom - but not completely, to avoid older posts
                driver.execute_script("window.scrollTo({top: document.body.scrollHeight * 0.7, behavior: 'smooth'});")
            else:
                # Scroll a fixed amount
                scroll_amount = random.randint(600, 1000)
                driver.execute_script(f"window.scrollBy({{top: {scroll_amount}, behavior: 'smooth'}});")
                
            # Wait after scrolling
            time.sleep(3)
            scroll_count += 1
            
            # Limit to prevent infinite loops
            if scroll_count >= 15 and not found_overlap:
                logger.info("Reached maximum scroll count without finding overlap with existing data.")
                break
        
        # Now merge the new posts with the existing ones
        if new_posts_data:
            merged_posts = new_posts_data + existing_posts
            logger.info(f"Added {len(new_posts_data)} new posts to existing {len(existing_posts)} posts")
            save_updated_data(merged_posts)
        else:
            logger.info("No new posts found to add")
        
    except Exception as e:
        logger.error(f"Critical error: {e}")
        if new_posts_data:
            # Save whatever we've collected so far
            merged_posts = new_posts_data + existing_posts
            logger.info(f"Saving {len(new_posts_data)} new posts collected before error...")
            save_updated_data(merged_posts, "aave_posts_error_recovery.csv")
    
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
        scrape_new_coinmarketcap_posts()
        logger.info("Scraping of new posts completed successfully")
    except KeyboardInterrupt:
        logger.info("Scraping stopped by user")
    except Exception as e:
        logger.critical(f"Unexpected error: {str(e)}")
