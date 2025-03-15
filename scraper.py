from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException
import time
import pandas as pd
from datetime import datetime
import re

# Set up Chrome options
chrome_options = Options()
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--lang=en-US,en;q=0.9")

# Initialize WebDriver
driver = webdriver.Chrome(options=chrome_options)
driver.maximize_window()

# Define target date (October 3, 2020) as timestamp
target_date = datetime(2020, 10, 3).timestamp() * 1000  # Convert to milliseconds for comparison

try:
    # Open the CoinMarketCap community page for AAVE
    url = "https://coinmarketcap.com/community/search/latest/aave/"
    print(f"Opening URL: {url}")
    driver.get(url)
    
    # Wait for content to load
    print("Waiting for page to load...")
    time.sleep(10)
    
    # Initialize data collection
    posts_data = []
    oldest_timestamp_ms = float('inf')
    scroll_count = 0
    max_scrolls = 100  # Set a reasonable maximum to prevent infinite scrolling
    
    print("Beginning data collection from October 3, 2020 onwards...")
    
    # Keep scrolling until we reach the target date or max scrolls
    while scroll_count < max_scrolls:
        # Find all currently loaded post wrappers
        post_wrappers = driver.find_elements(By.CSS_SELECTOR, "div.post-wrapper.community")
        
        if not post_wrappers:
            print("No more posts found, exiting scroll loop")
            break
            
        # Process posts that haven't been processed yet
        new_posts_found = False
        current_posts_count = len(posts_data)
        
        print(f"Found {len(post_wrappers)} posts on screen, processing new ones...")
        
        for wrapper in post_wrappers:
            try:
                # Get post ID for tracking
                post_id = wrapper.get_attribute("data-post-id")
                
                # Skip if we've already processed this post
                if any(post['post_id'] == post_id for post in posts_data):
                    continue
                    
                # Get timestamp and check if it's after our target date
                timestamp_ms = wrapper.get_attribute("data-post-time")
                
                if timestamp_ms:
                    timestamp_ms = float(timestamp_ms)
                    if timestamp_ms < oldest_timestamp_ms:
                        oldest_timestamp_ms = timestamp_ms
                    
                    # Convert timestamp to readable format
                    timestamp_date = datetime.fromtimestamp(timestamp_ms / 1000)
                    formatted_timestamp = timestamp_date.strftime("%Y-%m-%d %H:%M:%S")
                    
                    # If the post is older than our target date, skip it
                    if timestamp_ms < target_date:
                        print(f"Reached post from {formatted_timestamp}, which is before our target date")
                        continue
                else:
                    formatted_timestamp = "Unknown"
                
                # Check if there is a "Read more" button and click it
                try:
                    read_more_buttons = wrapper.find_elements(By.CSS_SELECTOR, "span.read-all")
                    if read_more_buttons:
                        driver.execute_script("arguments[0].click();", read_more_buttons[0])
                        time.sleep(1)
                    else:
                        read_more_buttons = wrapper.find_elements(By.XPATH, ".//span[contains(@class, 'read-all')]")
                        if read_more_buttons:
                            driver.execute_script("arguments[0].click();", read_more_buttons[0])
                            time.sleep(1)
                except (NoSuchElementException, StaleElementReferenceException) as e:
                    # Just log and continue if we can't click the "Read more" button
                    print(f"Couldn't click 'Read more' for post {post_id}: {str(e)[:50]}...")
                
                # Get post content
                try:
                    content_element = wrapper.find_element(By.CSS_SELECTOR, "div.post-text-wrapper.community")
                    content_html = driver.execute_script("return arguments[0].innerHTML;", content_element)
                    content_text = content_element.text if content_element else "No content"
                except NoSuchElementException:
                    try:
                        content_element = wrapper.find_element(By.CSS_SELECTOR, "div.post-content")
                        content_html = driver.execute_script("return arguments[0].innerHTML;", content_element)
                        content_text = content_element.text if content_element else "No content"
                    except NoSuchElementException:
                        content_html = "Could not find content element"
                        content_text = "Could not find content element"
                
                # Find relative time display
                try:
                    relative_time_elements = wrapper.find_elements(By.XPATH, ".//span[contains(text(), 'minute') or contains(text(), 'hour') or contains(text(), 'day') or contains(text(), 'month') or contains(text(), 'year')]")
                    relative_time = relative_time_elements[0].text if relative_time_elements else "Unknown"
                except:
                    relative_time = "Unknown"
                
                # Add to our collected data
                posts_data.append({
                    "post_id": post_id,
                    "timestamp": formatted_timestamp,
                    "timestamp_ms": timestamp_ms,
                    "relative_time": relative_time,
                    "content_text": content_text,
                    "content_html": content_html,
                    "scrape_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })
                
                new_posts_found = True
                print(f"Processed post from {formatted_timestamp}")
                
            except Exception as e:
                # Log the error but continue with other posts
                print(f"Error processing a post: {str(e)[:100]}... Continuing with next post.")
                continue
        
        # If we found no new posts in this scroll, we might be at the end
        if not new_posts_found:
            print("No new posts found in this scroll")
        
        # If we've reached posts older than our target date, we can stop
        if oldest_timestamp_ms < target_date:
            print(f"Reached posts from before target date. Stopping scroll.")
            break
            
        # Save intermediate results every 20 scrolls
        if scroll_count % 20 == 0 and posts_data:
            intermediate_df = pd.DataFrame(posts_data)
            intermediate_df.to_csv(f"posts_intermediate_{scroll_count}.csv", index=False)
            print(f"Saved intermediate results with {len(posts_data)} posts")
        
        # Print progress
        print(f"Completed scroll {scroll_count+1}/{max_scrolls}. Posts collected so far: {len(posts_data)}")
        print(f"Oldest post timestamp so far: {datetime.fromtimestamp(oldest_timestamp_ms/1000).strftime('%Y-%m-%d')}")
        
        # Scroll down to load more
        driver.execute_script("window.scrollBy(0, 1000);")
        time.sleep(3)  # Wait longer for content to load
        scroll_count += 1
    
    # Final save of results
    if posts_data:
        # Sort by timestamp (newest first)
        posts_data.sort(key=lambda x: x.get('timestamp_ms', 0), reverse=True)
        
        df = pd.DataFrame(posts_data)
        # Remove timestamp_ms from final output
        if 'timestamp_ms' in df.columns:
            df = df.drop('timestamp_ms', axis=1)
            
        df.to_csv("aave_posts_complete.csv", index=False)
        print(f"Successfully saved {len(posts_data)} posts to CSV")
        
        # Generate a summary
        start_date = min(datetime.strptime(post['timestamp'], "%Y-%m-%d %H:%M:%S") for post in posts_data if post['timestamp'] != 'Unknown')
        end_date = max(datetime.strptime(post['timestamp'], "%Y-%m-%d %H:%M:%S") for post in posts_data if post['timestamp'] != 'Unknown')
        print(f"\nData collection summary:")
        print(f"Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        print(f"Total posts collected: {len(posts_data)}")
    else:
        print("No posts data was collected")
    
except Exception as e:
    print(f"An error occurred in the main execution: {e}")
    # Even if we get an error, try to save what we've collected so far
    if posts_data:
        recovery_df = pd.DataFrame(posts_data)
        recovery_df.to_csv("aave_posts_recovery.csv", index=False)
        print(f"Saved {len(posts_data)} posts to recovery file")

finally:
    # Close the browser when done
    driver.quit()
    print("Browser closed")
