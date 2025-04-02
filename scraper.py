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
        outfile = f"output/{filename}_intermediate_{len(posts_data)}.csv"
    else:
        outfile = f"output/{filename}_complete.csv"
        
    df.to_csv(outfile, index=False)
    logger.info(f"Saved {len(posts_data)} posts to {outfile}")

def save_checkpoint(collected_post_ids, oldest_timestamp_ms, scroll_count, page_url):
    """Save checkpoint data to resume scraping if interrupted"""
    checkpoint_data = {
        "collected_post_ids": list(collected_post_ids),
        "oldest_timestamp_ms": oldest_timestamp_ms,
        "scroll_count": scroll_count,
        "page_url": page_url,
        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    
    os.makedirs("checkpoints", exist_ok=True)
    
    checkpoint_file = f"checkpoints/scraper_checkpoint_{page_url.split('/')[-2]}.json"
    with open(checkpoint_file, "w") as f:
        json.dump(checkpoint_data, f)
    
    logger.info(f"Saved checkpoint with {len(collected_post_ids)} post IDs to {checkpoint_file}")

def load_checkpoint(page_url):
    """Load checkpoint data to resume scraping"""
    try:
        checkpoint_file = f"checkpoints/scraper_checkpoint_{page_url.split('/')[-2]}.json"
        if os.path.exists(checkpoint_file):
            with open(checkpoint_file, "r") as f:
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

def load_existing_data(filename):
    """Load existing data to resume scraping"""
    posts_data = []
    collected_post_ids = set()
    
    try:
        complete_file = f"output/{filename}_complete.csv"
        if os.path.exists(complete_file):
            df = pd.read_csv(complete_file)
            posts_data = df.to_dict('records')
            collected_post_ids = set(df['post_id'].astype(str).tolist())
            logger.info(f"Loaded {len(posts_data)} existing posts from output file {complete_file}")
            return posts_data, collected_post_ids
        else:
            # Check for intermediate files
            intermediate_files = [f for f in os.listdir("output") if f.startswith(f"{filename}_intermediate_") and f.endswith(".csv")]
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
        # Scroll element into view to ensure it's properly loaded - use a less aggressive scroll
        driver.execute_script("arguments[0].scrollIntoView({block: 'center', behavior: 'auto'});", wrapper)
        time.sleep(0.5)  # Increased pause after scrolling to element
        
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

def get_visible_post_ids(driver):
    """Get IDs of all currently visible posts on the page"""
    visible_ids = set()
    try:
        post_wrappers = safe_find_elements(driver, By.CSS_SELECTOR, "div.post-wrapper.community")
        for wrapper in post_wrappers:
            post_id = safe_get_attribute(wrapper, "data-post-id")
            if post_id:
                visible_ids.add(post_id)
    except Exception as e:
        logger.warning(f"Error getting visible post IDs: {str(e)}")
    return visible_ids

def scroll_in_smaller_increments(driver, increment=300, pauses=2):
    """Scroll down in smaller increments to ensure posts are loaded properly"""
    # Get the height of the viewport
    viewport_height = driver.execute_script("return window.innerHeight")
    
    # Scroll down in smaller increments
    for i in range(pauses):
        # Calculate scroll amount (fraction of viewport height)
        scroll_amount = increment if increment > 0 else viewport_height / abs(increment)
        driver.execute_script(f"window.scrollBy(0, {scroll_amount});")
        time.sleep(0.5)  # Small pause between incremental scrolls
    
    # Wait a bit after the last scroll
    time.sleep(1)

def scrape_coinmarketcap(url="https://coinmarketcap.com/community/search/latest/dydx-chain/", 
                         output_filename="crypto_posts",
                         target_date=datetime(2022, 1, 1)):
    """Main function to scrape CoinMarketCap community posts"""
    # Convert target date to timestamp in milliseconds
    target_date_ms = target_date.timestamp() * 1000
    
    # Read All button CSS selector
    read_all_selector = "span.read-all"
    
    driver = None
    temp_dir = None
    
    # Try to load existing data and checkpoint
    posts_data, collected_ids_from_data = load_existing_data(output_filename)
    collected_post_ids_checkpoint, oldest_timestamp_checkpoint, scroll_count_checkpoint = load_checkpoint(url)
    
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
    
    try:
        # Initialize WebDriver
        driver, temp_dir = setup_driver()
        
        # Reduce logging level for stale elements to debug to minimize log spam
        selenium_logger = logging.getLogger('selenium.webdriver.remote.remote_connection')
        selenium_logger.setLevel(logging.WARNING)
        
        # Open the CoinMarketCap community page
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
        previously_seen_ids = set()  # Track IDs we've already seen for gap detection
        
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
                save_checkpoint(collected_post_ids, oldest_timestamp_ms, scroll_count, url)
                last_checkpoint_time = current_time
                
            try:
                # Get all posts currently visible on page
                visible_post_ids = get_visible_post_ids(driver)
                
                # Check if we have any new posts
                new_ids = visible_post_ids - previously_seen_ids
                if new_ids:
                    logger.info(f"Found {len(new_ids)} new post IDs in this scroll")
                else:
                    logger.warning("No new post IDs found in this scroll")
                
                # Process each post on the page
                processed_in_this_loop = 0
                all_post_wrappers = safe_find_elements(driver, By.CSS_SELECTOR, "div.post-wrapper.community")
                
                # Log the mapping of post IDs to indices to help debug missing posts
                if logger.level <= logging.DEBUG:
                    id_mapping = {}
                    for idx, wrapper in enumerate(all_post_wrappers):
                        pid = safe_get_attribute(wrapper, "data-post-id")
                        if pid:
                            id_mapping[pid] = idx
                    logger.debug(f"Post ID to index mapping: {id_mapping}")
                
                # First, identify all post IDs for gap detection
                all_visible_ids = []
                for wrapper in all_post_wrappers:
                    post_id = safe_get_attribute(wrapper, "data-post-id")
                    if post_id:
                        all_visible_ids.append(post_id)
                
                # Process posts in smaller batches to reduce stale element issues
                batch_size = 5
                for i in range(0, len(all_post_wrappers), batch_size):
                    batch = all_post_wrappers[i:i+batch_size]
                    
                    for wrapper in batch:
                        # Double-check for posts that might have been missed
                        post_id = safe_get_attribute(wrapper, "data-post-id")
                        
                        # Skip processing if we already have this post
                        if post_id in collected_post_ids:
                            continue
                        
                        # Process the post
                        post_id, post_data = process_post(driver, wrapper, collected_post_ids, read_all_selector)
                        
                        if post_id and post_data:
                            collected_post_ids.add(post_id)
                            previously_seen_ids.add(post_id)
                            posts_data.append(post_data)
                            processed_in_this_loop += 1
                            
                            # Check if we've reached our target date
                            timestamp_ms = post_data.get("timestamp_ms")
                            if timestamp_ms and timestamp_ms < oldest_timestamp_ms:
                                oldest_timestamp_ms = timestamp_ms
                                
                            if timestamp_ms and timestamp_ms < target_date_ms:
                                logger.info(f"Reached target date ({target_date.strftime('%Y-%m-%d')}), stopping. Post timestamp: {datetime.fromtimestamp(timestamp_ms/1000).strftime('%Y-%m-%d')}")
                                save_data(posts_data, output_filename, intermediate=False)
                                save_checkpoint(collected_post_ids, oldest_timestamp_ms, scroll_count, url)
                                return
                                
                    # Take a short break between batches
                    time.sleep(0.5)
                
                # Update previously seen IDs with all visible IDs
                previously_seen_ids.update(visible_post_ids)
                
                # Track if we found new posts in this iteration
                if processed_in_this_loop > 0:
                    logger.info(f"Processed {processed_in_this_loop} new posts in this iteration")
                    no_new_posts_count = 0
                else:
                    no_new_posts_count += 1
                    logger.warning(f"No new posts processed in this iteration ({no_new_posts_count}/5)")
                    if no_new_posts_count >= 5:
                        # Try refreshing the page before giving up
                        if no_new_posts_count == 5:
                            logger.info("Refreshing page to attempt to load more posts...")
                            driver.refresh()
                            time.sleep(5)  # Wait for page to reload
                            # Re-handle cookie consent if needed
                            try:
                                WebDriverWait(driver, 5).until(
                                    EC.element_to_be_clickable((By.CSS_SELECTOR, "button[id*='cookie'], button[id*='consent']"))
                                ).click()
                            except:
                                pass
                            no_new_posts_count = 0  # Reset counter after refresh
                            continue
                        else:
                            logger.info(f"No new posts found after {no_new_posts_count} attempts including refresh, stopping.")
                            break
    
                # Save intermediate results every 25 posts
                if len(posts_data) % 25 == 0 and len(posts_data) > 0:
                    logger.info(f"Saving intermediate results with {len(posts_data)} posts...")
                    save_data(posts_data, output_filename, intermediate=True)
    
                # Anti-ban measures
                if scroll_count % 10 == 0 and scroll_count > 0:
                    pause_time = random.uniform(10.0, 20.0)
                    logger.info(f"Taking a longer break ({pause_time:.2f}s) to avoid rate limiting...")
                    time.sleep(pause_time)
    
                # Get the current height for comparison after scrolling
                last_height = driver.execute_script("return document.body.scrollHeight")
                
                # Use smaller incremental scrolls instead of one big scroll 
                # This is KEY to preventing missed posts
                if scroll_count % 3 == 0:
                    # Every third scroll, do a full scroll to bottom to ensure we're not missing anything
                    logger.info("Performing full scroll to bottom...")
                    driver.execute_script("window.scrollTo({top: document.body.scrollHeight, behavior: 'smooth'});")
                    time.sleep(2)
                else:
                    # Use our improved smaller incremental scroll function
                    logger.info("Performing incremental scroll...")
                    scroll_in_smaller_increments(driver, increment=300, pauses=3)
                
                scroll_count += 1
                
                # Wait for page to stabilize after scrolling
                time.sleep(2)
    
                # Check if scroll was effective
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
                        
                    # Try using smaller scroll increments
                    scroll_in_smaller_increments(driver, increment=200, pauses=4)
                    time.sleep(2)
                    
                    # Check if we need to refresh the page
                    if scroll_count % 15 == 0 and new_height == last_height:
                        logger.info("Page might be stuck, refreshing...")
                        driver.refresh()
                        time.sleep(5)  # Wait for page to reload
                        # Re-handle cookie consent if needed
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
    
            except Exception as e:
                logger.error(f"Error during main loop: {e}")
                # Save progress before continuing
                if posts_data:
                    save_data(posts_data, f"{output_filename}_error_recovery", intermediate=True)
                    save_checkpoint(collected_post_ids, oldest_timestamp_ms, scroll_count, url)
                # Let's refresh the page and continue
                try:
                    driver.refresh()
                    time.sleep(5)
                    logger.info("Page refreshed after error, continuing...")
                except:
                    logger.error("Failed to refresh page after error")

        # Save the final results and checkpoint
        save_data(posts_data, output_filename, intermediate=False)
        save_checkpoint(collected_post_ids, oldest_timestamp_ms, scroll_count, url)
        
    except Exception as e:
        logger.error(f"Critical error: {e}")
        if posts_data:
            logger.info(f"Saving {len(posts_data)} posts collected before error...")
            save_data(posts_data, f"{output_filename}_error_recovery", intermediate=False)
            save_checkpoint(collected_post_ids, oldest_timestamp_ms, scroll_count, url)
    
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
    # Set up command line arguments
    import argparse
    
    parser = argparse.ArgumentParser(description='Scrape CoinMarketCap community posts')
    parser.add_argument('--url', type=str, 
                        default="https://coinmarketcap.com/community/search/latest/dydx-chain/",
                        help='URL of the CoinMarketCap community page to scrape')
    parser.add_argument('--output', type=str, default="crypto_posts",
                        help='Base filename for output (without extension)')
    parser.add_argument('--target-date', type=str, default="2022-01-01",
                        help='Target date to scrape back to (YYYY-MM-DD format)')
    parser.add_argument('--max-runtime', type=int, default=10800,
                        help='Maximum runtime in seconds (default: 3 hours)')
    
    args = parser.parse_args()
    
    # Parse target date
    try:
        target_date = datetime.strptime(args.target_date, "%Y-%m-%d")
    except ValueError:
        logger.error(f"Invalid target date format: {args.target_date}. Using default (2022-01-01).")
        target_date = datetime(2022, 1, 1)
    
    try:
        scrape_coinmarketcap(url=args.url, 
                           output_filename=args.output,
                           target_date=target_date)
    except KeyboardInterrupt:
        logger.info("Scraping stopped by user")
    except Exception as e:
        logger.critical(f"Unexpected error: {str(e)}")
