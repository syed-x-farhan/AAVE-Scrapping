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
import json

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

def save_checkpoint(collected_post_ids, oldest_timestamp_ms, scroll_count):
    """Save checkpoint data to resume scraping if interrupted"""
    checkpoint_data = {
        "collected_post_ids": list(collected_post_ids),
        "oldest_timestamp_ms": oldest_timestamp_ms,
        "scroll_count": scroll_count,
        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    
    os.makedirs("checkpoints", exist_ok=True)
    
    with open("checkpoints/scraper_checkpoint.json", "w") as f:
        json.dump(checkpoint_data, f)
    
    logger.info(f"Saved checkpoint with {len(collected_post_ids)} post IDs")

def load_checkpoint():
    """Load checkpoint data to resume scraping"""
    try:
        if os.path.exists("checkpoints/scraper_checkpoint.json"):
            with open("checkpoints/scraper_checkpoint.json", "r") as f:
                checkpoint_data = json.load(f)
            
            collected_post_ids = set(checkpoint_data.get("collected_post_ids", []))
            oldest_timestamp_ms = checkpoint_data.get("oldest_timestamp_ms", float('inf'))
            scroll_count = checkpoint_data.get("scroll_count", 0)
            
            logger.info(f"Loaded checkpoint with {len(collected_post_ids)} post IDs, oldest timestamp: {datetime.fromtimestamp(oldest_timestamp_ms/1000).strftime('%Y-%m-%d') if oldest_timestamp_ms != float('inf') else 'None'}")
            
            return collected_post_ids, oldest_timestamp_ms, scroll_count
        else:
            logger.info("No checkpoint file found, starting fresh")
            return set(), float('inf'), 0
    except Exception as e:
        logger.warning(f"Error loading checkpoint: {str(e)}, starting fresh")
        return set(), float('inf'), 0

def load_existing_data():
    """Load existing data to resume scraping"""
    posts_data = []
    collected_post_ids = set()
    
    try:
        if os.path.exists("output/aave_posts_complete.csv"):
            df = pd.read_csv("output/aave_posts_complete.csv")
            posts_data = df.to_dict('records')
            collected_post_ids = set(df['post_id'].astype(str).tolist())
            logger.info(f"Loaded {len(posts_data)} existing posts from output file")
            return posts_data, collected_post_ids
        else:
            # Check for intermediate files
            intermediate_files = [f for f in os.listdir("output") if f.startswith("aave_posts_intermediate_") and f.endswith(".csv")]
            if intermediate_files:
                # Get the most recent one (with highest number of posts)
                most_recent = sorted(intermediate_files, key=lambda x: int(x.split("_")[-1].split(".")[0]), reverse=True)[0]
                df = pd.read_csv(os.path.join("output", most_recent))
                posts_data = df.to_dict('records')
                collected_post_ids = set(df['post_id'].astype(str).tolist())
                logger.info(f"Loaded {len(posts_data)} existing posts from intermediate file {most_recent}")
                return posts_data, collected_post_ids
    except Exception as e:
        logger.warning(f"Error loading existing data: {str(e)}")
    
    logger.info("No existing data found, starting fresh")
    return [], set()

def process_post(driver, wrapper, collected_post_ids, read_all_selector):
    """Process a single post wrapper element with stale-safe operations"""
    try:
        # More gentle scrolling to avoid triggering too many loads at once
        driver.execute_script(
            "arguments[0].scrollIntoView({block: 'center', behavior: 'smooth'});", 
            wrapper
        )
        time.sleep(random.uniform(0.8, 1.5))  # Longer pause
        
        post_id = safe_get_attribute(wrapper, "data-post-id")
        if not post_id:
            logger.warning("Could not get post_id, skipping...")
            return None, None

        if post_id in collected_post_ids:
            return None, None

        timestamp_ms = safe_get_attribute(wrapper, "data-post-time")
        if timestamp_ms:
            timestamp_ms = float(timestamp_ms)
            timestamp_date = datetime.fromtimestamp(timestamp_ms / 1000)
            formatted_timestamp = timestamp_date.strftime("%Y-%m-%d %H:%M:%S")
        else:
            formatted_timestamp = "Unknown"
            timestamp_ms = None

        # Try multiple selectors for "Read all" buttons - sites sometimes change their structure
        read_all_selectors = [
            read_all_selector,
            "span.read-more",
            "button[class*='read']",
            "div[class*='expand']",
            "a[class*='more']"
        ]
        
        for selector in read_all_selectors:
            try:
                read_all_buttons = wrapper.find_elements(By.CSS_SELECTOR, selector)
                
                if read_all_buttons:
                    for button in read_all_buttons:
                        if "read" in safe_get_text(button, "").lower() or "more" in safe_get_text(button, "").lower():
                            # Multiple click attempts with different methods
                            for attempt in range(3):
                                try:
                                    if attempt == 0:
                                        # Try normal click
                                        button.click()
                                    elif attempt == 1:
                                        # Try JavaScript click
                                        driver.execute_script("arguments[0].click();", button)
                                    else:
                                        # Try action chains
                                        actions = webdriver.ActionChains(driver)
                                        actions.move_to_element(button).click().perform()
                                        
                                    logger.info(f"Successfully clicked 'Read all' button with method {attempt+1}")
                                    time.sleep(random.uniform(1.0, 2.0))  # Longer wait for expansion
                                    break
                                except Exception as e:
                                    logger.debug(f"Click attempt {attempt+1} failed: {str(e)}")
            except Exception as e:
                logger.debug(f"Error with selector {selector}: {str(e)}")

        # Try multiple content selectors to be more resilient to site changes
        content_selectors = [
            "div.post-text-wrapper.community",
            "div.post-text-wrapper",
            "div.post-text",
            "div[class*='post-content']",
            "div[class*='message']",
            "div[class*='text-content']"
        ]
        
        content_text = "No content"
        for selector in content_selectors:
            try:
                content_elements = wrapper.find_elements(By.CSS_SELECTOR, selector)
                if content_elements:
                    for element in content_elements:
                        text = safe_get_text(element, "")
                        if text and len(text) > len(content_text):
                            content_text = text
            except (NoSuchElementException, StaleElementReferenceException):
                continue
                
        if content_text == "No content":
            # Last resort - try to get any text content from the post
            try:
                all_text = safe_get_text(wrapper, "No content")
                if len(all_text) > 10:  # If it has substantial text
                    content_text = all_text
            except:
                pass

        # Try multiple author selectors
        author_selectors = [
            "span.community-author",
            "a.community-author",
            "div[class*='author']",
            "span[class*='username']",
            "a[class*='user']"
        ]
        
        author_name = "Unknown"
        for selector in author_selectors:
            try:
                author_elements = wrapper.find_elements(By.CSS_SELECTOR, selector)
                if author_elements:
                    author_name = safe_get_text(author_elements[0], "Unknown")
                    if author_name != "Unknown":
                        break
            except:
                continue

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
        logger.debug("Post element became stale during processing")
        return None, None
    except Exception as e:
        logger.warning(f"Error processing post: {str(e)}")
        return None, None

def scrape_coinmarketcap():
    """Main function to scrape CoinMarketCap community posts"""
    # Define target date (January 1, 2022) as timestamp
    target_date = datetime(2022, 1, 1).timestamp() * 1000  # Convert to milliseconds
    
    # Read All button CSS selector
    read_all_selector = "span.read-all"
    
    driver = None
    temp_dir = None
    
    # Try to load existing data and checkpoint
    posts_data, collected_ids_from_data = load_existing_data()
    collected_post_ids_checkpoint, oldest_timestamp_checkpoint, scroll_count_checkpoint = load_checkpoint()
    
    # Merge post IDs from both sources
    collected_post_ids = collected_ids_from_data.union(collected_post_ids_checkpoint)
    oldest_timestamp_ms = oldest_timestamp_checkpoint
    scroll_count = scroll_count_checkpoint
    
    # Set a maximum runtime for GitHub Actions (3 hours)
    max_runtime = 3 * 60 * 60  # 3 hours in seconds
    start_time = time.time()
    
    # Set checkpoint saving interval (save every 5 minutes)
    checkpoint_interval = 5 * 60  # 5 minutes in seconds
    last_checkpoint_time = start_time
    
    # Track if we were interrupted in the previous run
    interrupted = len(collected_post_ids) > 0
    
    # Increased tolerance for no new posts
    no_posts_max_attempts = 10  # Increased from 5
    
    # This will track consecutive scrolls with the same number of posts
    same_post_count_scrolls = 0
    
    try:
        # Initialize WebDriver
        driver, temp_dir = setup_driver()
        
        # Reduce logging level for stale elements to debug to minimize log spam
        selenium_logger = logging.getLogger('selenium.webdriver.remote.remote_connection')
        selenium_logger.setLevel(logging.WARNING)
        
        # Open the CoinMarketCap community page for AAVE
        url = "https://coinmarketcap.com/community/search/latest/pancakeswap/"
        logger.info(f"Opening URL: {url}")
        driver.get(url)
        
        # Set page load timeout to avoid hanging indefinitely
        driver.set_page_load_timeout(30)
        
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
    
        no_new_posts_count = 0  # Track consecutive scrolls with no new posts
        stale_count = 0  # Count stale elements
        
        logger.info(f"Starting data collection with {len(collected_post_ids)} already collected posts...")
        if interrupted:
            logger.info(f"Resuming from previous run. Oldest timestamp: {datetime.fromtimestamp(oldest_timestamp_ms/1000).strftime('%Y-%m-%d') if oldest_timestamp_ms != float('inf') else 'None'}")
        
        # Scroll down to load more content if we're resuming
        if interrupted:
            # Scroll down multiple times to get closer to where we left off
            initial_scrolls = min(50, scroll_count // 2)  # Do roughly half the scrolls we did before
            logger.info(f"Doing {initial_scrolls} initial scrolls to get back to previous position...")
            for i in range(initial_scrolls):
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(0.5)
                if i % 10 == 0:
                    logger.info(f"Initial scroll progress: {i+1}/{initial_scrolls}")
    
        while True:
            # Check if we've exceeded the maximum runtime
            current_runtime = time.time() - start_time
            if current_runtime > max_runtime:
                logger.info(f"Reached maximum runtime of {max_runtime/3600:.1f} hours, stopping.")
                break
                
            # Save checkpoint periodically
            current_time = time.time()
            if current_time - last_checkpoint_time > checkpoint_interval:
                save_checkpoint(collected_post_ids, oldest_timestamp_ms, scroll_count)
                last_checkpoint_time = current_time
                
            # First wait to ensure the page is fully loaded before getting posts
            time.sleep(random.uniform(3.0, 5.0))
                
            try:
                # More robust wait for posts - increased timeout and more explicit condition
                post_wrappers = WebDriverWait(driver, 30).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div.post-wrapper.community"))
                )
            except TimeoutException:
                logger.warning("Timeout waiting for posts. Attempting to recover...")
                
                # Try multiple recovery methods
                # 1. Check for "No results found" messages
                try:
                    no_results = driver.find_elements(By.XPATH, "//*[contains(text(), 'No results') or contains(text(), 'Nothing found')]")
                    if no_results:
                        logger.info("Found 'No results' message on page, but this might be incorrect. Trying to refresh...")
                        driver.refresh()
                        time.sleep(random.uniform(8.0, 12.0))
                        continue
                except:
                    pass
                    
                # 2. Try a more aggressive scroll approach
                for _ in range(3):
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(random.uniform(2.0, 3.0))
                
                # 3. Try a middle-of-page scroll to trigger lazy loading
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight/2);")
                time.sleep(random.uniform(5.0, 7.0))
                continue
    
            # Get the current height for comparison after scrolling
            last_height = driver.execute_script("return document.body.scrollHeight")
            
            # Use our safe find elements function with longer timeout
            post_wrappers = safe_find_elements(driver, By.CSS_SELECTOR, "div.post-wrapper.community", wait_time=15)
    
            if not post_wrappers:
                no_new_posts_count += 1
                if no_new_posts_count >= no_posts_max_attempts:
                    logger.info(f"No posts found after {no_posts_max_attempts} attempts, but this could be incorrect.")
                    # Instead of stopping, try refreshing the page
                    driver.refresh()
                    time.sleep(random.uniform(8.0, 12.0))
                    no_new_posts_count = 0  # Reset counter
                    continue
                logger.warning(f"No posts found on attempt {no_new_posts_count}, trying again...")
                time.sleep(random.uniform(5.0, 8.0))  # Increased wait time
                continue
    
            prev_posts_count = len(collected_post_ids)
            reached_target_date = False
            
            # Process posts in smaller batches to reduce stale element issues
            batch_size = 3  # Reduced batch size
            for i in range(0, len(post_wrappers), batch_size):
                batch = post_wrappers[i:i+batch_size]
                
                for wrapper in batch:
                    # Improved scroll behavior - only scroll the element partly into view
                    # to avoid triggering too many loads at once
                    try:
                        driver.execute_script(
                            "arguments[0].scrollIntoView({block: 'center', behavior: 'smooth'});", 
                            wrapper
                        )
                        # Random wait between processing elements to appear more human-like
                        time.sleep(random.uniform(0.8, 1.5))  # Increased individual post processing time
                    except:
                        logger.debug("Error scrolling to element, continuing")
                    
                    post_id, post_data = process_post(driver, wrapper, collected_post_ids, read_all_selector)
                    
                    if post_id and post_data:
                        collected_post_ids.add(post_id)
                        posts_data.append(post_data)
                        
                        # Check if we've reached our target date
                        timestamp_ms = post_data.get("timestamp_ms")
                        if timestamp_ms and timestamp_ms < oldest_timestamp_ms:
                            oldest_timestamp_ms = timestamp_ms
                            
                        if timestamp_ms and timestamp_ms < target_date:
                            logger.info(f"Reached target date (January 1, 2022), stopping. Post timestamp: {datetime.fromtimestamp(timestamp_ms/1000).strftime('%Y-%m-%d')}")
                            reached_target_date = True
                            break
                            
                # Take a longer break between batches
                time.sleep(random.uniform(1.0, 2.0))  # Increased inter-batch wait
                
                if reached_target_date:
                    break
    
            if reached_target_date:
                logger.info("Breaking main loop after reaching target date")
                break
    
            # Improved detection of "stuck" scraping
            if len(collected_post_ids) > prev_posts_count:
                no_new_posts_count = 0
                same_post_count_scrolls = 0
            else:
                no_new_posts_count += 1
                same_post_count_scrolls += 1
                
                # More aggressive recovery if we're stuck for a while
                if same_post_count_scrolls >= 3:
                    logger.warning(f"No new posts for {same_post_count_scrolls} scrolls. Attempting recovery...")
                    
                    # Try multiple recovery methods
                    # 1. More aggressive scroll pattern
                    for _ in range(3):
                        scroll_dist = random.randint(300, 1200)
                        driver.execute_script(f"window.scrollBy(0, {scroll_dist});")
                        time.sleep(random.uniform(1.0, 2.0))
                    
                    # 2. Attempt to find and click "Load more" or "Show more" buttons
                    for button_text in ["Load", "more", "Show", "View"]:
                        try:
                            buttons = driver.find_elements(By.XPATH, f"//button[contains(text(), '{button_text}')]")
                            if buttons:
                                for button in buttons:
                                    try:
                                        # Scroll to button
                                        driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", button)
                                        time.sleep(1)
                                        # Try to click
                                        driver.execute_script("arguments[0].click();", button)
                                        logger.info(f"Clicked a '{button_text}' button")
                                        time.sleep(random.uniform(3.0, 5.0))
                                        break
                                    except Exception as e:
                                        logger.debug(f"Failed to click button: {str(e)}")
                        except Exception:
                            pass
                    
                    # 3. If we've been stuck for a long time, refresh the page
                    if same_post_count_scrolls >= 5:
                        logger.info("Page might be stuck, refreshing...")
                        driver.refresh()
                        time.sleep(random.uniform(8.0, 12.0))  # Longer wait after refresh
                        same_post_count_scrolls = 0  # Reset counter
                        
                        # Re-handle cookie consent if needed
                        try:
                            WebDriverWait(driver, 5).until(
                                EC.element_to_be_clickable((By.CSS_SELECTOR, "button[id*='cookie'], button[id*='consent']"))
                            ).click()
                        except:
                            pass
                            
                # Only stop if we've exhausted all recovery options and still no new posts
                if no_new_posts_count >= no_posts_max_attempts and same_post_count_scrolls >= 8:
                    logger.info(f"No new posts found after extensive recovery attempts, stopping.")
                    break
                
                logger.warning(f"No new posts in this scroll ({no_new_posts_count}/{no_posts_max_attempts} attempts)")
    
            # Add random mouse movements to appear more human-like
            if scroll_count % 5 == 0:
                try:
                    # Move mouse to random position on screen
                    action = webdriver.ActionChains(driver)
                    x, y = random.randint(100, 800), random.randint(100, 600)
                    action.move_by_offset(x, y).perform()
                    logger.debug("Performed random mouse movement")
                except Exception:
                    pass  # Ignore if this fails in headless mode
    
            # Save intermediate results every 25 posts (increased frequency)
            if len(posts_data) % 25 == 0 and len(posts_data) > 0:
                logger.info(f"Saving intermediate results with {len(posts_data)} posts...")
                save_data(posts_data, f"aave_posts_intermediate_{len(posts_data)}.csv", intermediate=True)
    
            # Anti-ban measures - more varied pauses
            if scroll_count % 8 == 0 and scroll_count > 0:
                pause_time = random.uniform(15.0, 30.0)  # Increased maximum pause time
                logger.info(f"Taking a longer break ({pause_time:.2f}s) to avoid rate limiting...")
                time.sleep(pause_time)
    
            # More varied wait times before next scroll
            if scroll_count % 3 == 0:
                wait_time = random.uniform(5.0, 8.0)  # Longer wait every 3rd scroll
            else:
                wait_time = random.uniform(3.0, 5.0)  # Normal wait
                
            logger.info(f"Waiting {wait_time:.2f} seconds before scrolling...")
            time.sleep(wait_time)
    
            # More varied scroll methods
            scroll_method = random.randint(1, 4)
            
            if scroll_method == 1:
                # Full scroll to bottom
                driver.execute_script("window.scrollTo({top: document.body.scrollHeight, behavior: 'smooth'});")
            elif scroll_method == 2:
                # Partial scroll - random distance
                scroll_amount = random.randint(500, 1500)
                driver.execute_script(f"window.scrollBy({{top: {scroll_amount}, behavior: 'smooth'}});")
            elif scroll_method == 3:
                # Multiple small scrolls to simulate human behavior
                for _ in range(3):
                   small_scroll = random.randint(200, 400)
                   driver.execute_script(f"window.scrollBy(0, {small_scroll});")
                   time.sleep(random.uniform(0.5, 1.2))
            else:
                # Scroll to 70-90% of page height - often triggers lazy loading better
                scroll_percent = random.uniform(0.7, 0.9)
                driver.execute_script(f"window.scrollTo(0, document.body.scrollHeight * {scroll_percent});")
            
            scroll_count += 1
            
            # Wait longer after scrolling to allow page to stabilize
            time.sleep(random.uniform(4.0, 6.0))  # Increased post-scroll wait time
    
            # Check if scroll was effective
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                logger.warning("Page height didn't change after scroll, trying a different approach...")
                
                # Try clicking a "Load more" button if it exists
                try:
                    # Look for a wider variety of load more button patterns
                    load_more_selectors = [
                        "//button[contains(text(), 'Load') or contains(text(), 'more') or contains(text(), 'Show')]",
                        "//a[contains(text(), 'Load') or contains(text(), 'more') or contains(text(), 'Show')]",
                        "//div[contains(@class, 'load') or contains(@class, 'more')]",
                        "//span[contains(text(), 'Load') or contains(text(), 'more')]"
                    ]
                    
                    clicked = False
                    for selector in load_more_selectors:
                        if clicked:
                            break
                            
                        buttons = driver.find_elements(By.XPATH, selector)
                        if buttons:
                            for button in buttons:
                                try:
                                    # First make sure it's visible
                                    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", button)
                                    time.sleep(1.5)
                                    
                                    # Try different click methods
                                    try:
                                        # Regular click
                                        button.click()
                                    except:
                                        # JavaScript click as fallback
                                        driver.execute_script("arguments[0].click();", button)
                                        
                                    logger.info(f"Clicked a button matching '{selector}'")
                                    time.sleep(random.uniform(3.0, 5.0))
                                    clicked = True
                                    break
                                except Exception as e:
                                    logger.debug(f"Failed to click button: {str(e)}")
                                    
                    if not clicked:
                        # Try a more aggressive approach - search for clickable elements at the bottom of the page
                        try:
                            # Scroll to bottom
                            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                            time.sleep(2)
                            
                            # Get elements near the bottom
                            bottom_elements = driver.execute_script("""
                                const bottom = document.body.scrollHeight - 300;
                                return Array.from(document.querySelectorAll('button, a')).filter(el => {
                                    const rect = el.getBoundingClientRect();
                                    const elemTop = rect.top + window.scrollY;
                                    return elemTop > bottom;
                                });
                            """)
                            
                            if bottom_elements:
                                for elem in bottom_elements[:3]:  # Try the first few elements
                                    try:
                                        driver.execute_script("arguments[0].click();", elem)
                                        logger.info("Clicked an element at the bottom of the page")
                                        time.sleep(3)
                                        break
                                    except:
                                        pass
                        except Exception as e:
                            logger.debug(f"Bottom element click approach failed: {str(e)}")
                except Exception as e:
                    logger.warning(f"Error finding 'Load more' button: {str(e)}")
                    
                # If still no change, try a variety of tactics
                if scroll_count % 15 == 0 and new_height == last_height:
                    recovery_method = random.randint(1, 3)
                    
                    if recovery_method == 1:
                        # Refresh the page
                        logger.info("Page might be stuck, refreshing...")
                        driver.refresh()
                        time.sleep(random.uniform(8.0, 12.0))  # Wait for page to reload
                    elif recovery_method == 2:
                        # Try going back and forth in history
                        logger.info("Trying back-and-forth navigation...")
                        current_url = driver.current_url
                        driver.execute_script("window.history.go(-1)")
                        time.sleep(3)
                        driver.execute_script("window.history.go(1)")
                        time.sleep(3)
                        # Make sure we're back at the right URL
                        if driver.current_url != current_url:
                            driver.get(current_url)
                            time.sleep(5)
                    else:
                        # Try adding a URL parameter to refresh content
                        logger.info("Adding URL parameter to refresh content...")
                        current_url = driver.current_url
                        random_param = f"?refresh={int(time.time())}"
                        if "?" in current_url:
                            modified_url = current_url + "&" + random_param
                        else:
                            modified_url = current_url + random_param
                        driver.get(modified_url)
                        time.sleep(5)
                    
                    # Re-handle cookie consent if needed after any of these recovery methods
                    try:
                        WebDriverWait(driver, 5).until(
                            EC.element_to_be_clickable((By.CSS_SELECTOR, "button[id*='cookie'], button[id*='consent']"))
                        ).click()
                    except:
                        pass
    
            if scroll_count % 5 == 0:
                logger.info(f"Completed {scroll_count} scrolls, collected {len(posts_data)} posts so far.")
                if oldest_timestamp_ms != float('inf'):
                    logger.info(f"Oldest post timestamp: {datetime.fromtimestamp(oldest_timestamp_ms/1000).strftime('%Y-%m-%d')}")
    
        # Save the final results and checkpoint
        save_data(posts_data, "aave_posts_complete.csv", intermediate=False)
        save_checkpoint(collected_post_ids, oldest_timestamp_ms, scroll_count)
        
    except Exception as e:
        logger.error(f"Critical error: {e}")
        if posts_data:
            logger.info(f"Saving {len(posts_data)} posts collected before error...")
            save_data(posts_data, "aave_posts_error_recovery.csv", intermediate=False)
            save_checkpoint(collected_post_ids, oldest_timestamp_ms, scroll_count)
    
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
