import ssl
ssl._create_default_https_context = ssl._create_unverified_context
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException, 
    NoSuchElementException,
    StaleElementReferenceException,
    WebDriverException
)
from twocaptcha import TwoCaptcha
from undetected_chromedriver import Chrome as Driver
import time
import random
import pandas as pd
import os
import re
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import logging
import schedule
import traceback

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("amazon_scraper.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("AmazonScraper")

class SimpleDatabaseManager:
    def __init__(self, dbname="amazon_scraper", user="postgres", password="Talha", host="localhost", port="5432"):
        self.conn = None
        self.db_params = {
            "dbname": dbname,
            "user": user,
            "password": password,
            "host": host,
            "port": port
        }
        self.connect()
        self.create_tables()
        self.add_unique_constraint()  # Add unique constraint if it doesn't exist
        
    def connect(self):
        try:
            self.conn = psycopg2.connect(**self.db_params)
            logger.info("Successfully connected to PostgreSQL database")
        except Exception as e:
            logger.error(f"Error connecting to database: {str(e)}")
            raise

    def create_tables(self):
        try:
            with self.conn.cursor() as cur:
                # Create a single, simple table to store all product data with date
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS daily_amazon_data (
                        id SERIAL PRIMARY KEY,
                        asin VARCHAR(10) NOT NULL,
                        scan_date DATE NOT NULL DEFAULT CURRENT_DATE,
                        price DECIMAL(10,2),
                        minimum_price DECIMAL(10,2),
                        offers INTEGER,
                        best_seller_rank TEXT
                    )
                """)
                
                # Create a simple index on asin and date for quicker lookups
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_daily_data_asin_date 
                    ON daily_amazon_data (asin, scan_date)
                """)
                
                # Create a checkpoint table to track progress
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS scraper_checkpoint (
                        id SERIAL PRIMARY KEY,
                        scan_date DATE NOT NULL DEFAULT CURRENT_DATE,
                        last_asin VARCHAR(10),
                        last_index INTEGER,
                        completed BOOLEAN DEFAULT FALSE
                    )
                """)
                
                self.conn.commit()
                logger.info("Tables created successfully")
        except Exception as e:
            logger.error(f"Error creating tables: {str(e)}")
            self.conn.rollback()
            raise

    def add_unique_constraint(self):
        """Add unique constraint for asin and scan_date if it doesn't exist"""
        try:
            with self.conn.cursor() as cur:
                # Check if constraint exists
                cur.execute("""
                    SELECT constraint_name 
                    FROM information_schema.table_constraints 
                    WHERE table_name = 'daily_amazon_data' 
                    AND constraint_type = 'UNIQUE'
                    AND constraint_name = 'unique_asin_date'
                """)
                
                if cur.fetchone() is None:
                    # Constraint doesn't exist, so add it
                    cur.execute("""
                        ALTER TABLE daily_amazon_data 
                        ADD CONSTRAINT unique_asin_date UNIQUE (asin, scan_date)
                    """)
                    self.conn.commit()
                    logger.info("Added unique constraint on asin and scan_date")
                else:
                    logger.info("Unique constraint already exists")
        except Exception as e:
            logger.error(f"Error adding unique constraint: {str(e)}")
            self.conn.rollback()

    def save_product_data(self, batch_results):
        """
        Save only the current batch of results to the database.
        Each call will save just the results passed in this call.
        """
        try:
            with self.conn.cursor() as cur:
                # Filter out results with None ASINs
                valid_results = [r for r in batch_results if r['asin']]
                
                if not valid_results:
                    logger.warning("No valid results in this batch to save to database")
                    return
                
                # Get today's date (date only, no time component)
                today_date = datetime.now().date()
                
                # Prepare data for insertion - only the current batch
                data_to_insert = []
                for result in valid_results:
                    try:
                        offers = int(result['offers']) if result['offers'] else None
                    except (ValueError, TypeError):
                        offers = None
                        
                    data_to_insert.append((
                        result['asin'],
                        today_date,
                        result['price'],
                        result['minimum_price'],
                        offers,
                        result['best_seller_rank']
                    ))
                
                # Insert only the current batch for today
                execute_values(cur, """
                    INSERT INTO daily_amazon_data 
                    (asin, scan_date, price, minimum_price, offers, best_seller_rank)
                    VALUES %s
                    ON CONFLICT (asin, scan_date) DO UPDATE SET
                    price = EXCLUDED.price,
                    minimum_price = EXCLUDED.minimum_price,
                    offers = EXCLUDED.offers,
                    best_seller_rank = EXCLUDED.best_seller_rank
                """, data_to_insert)
                
                self.conn.commit()
                logger.info(f"Successfully saved batch of {len(valid_results)} products to database for date: {today_date}")
        except Exception as e:
            logger.error(f"Error saving batch product data to database: {str(e)}")
            self.conn.rollback()
            raise

    def save_checkpoint(self, asin, index, completed=False):
        """Save progress checkpoint to resume from in case of interruption"""
        try:
            with self.conn.cursor() as cur:
                today_date = datetime.now().date()
                
                # Check if we have a checkpoint for today
                cur.execute("""
                    SELECT id FROM scraper_checkpoint 
                    WHERE scan_date = %s
                """, (today_date,))
                
                checkpoint_exists = cur.fetchone()
                
                if checkpoint_exists:
                    # Update existing checkpoint
                    cur.execute("""
                        UPDATE scraper_checkpoint 
                        SET last_asin = %s, last_index = %s, completed = %s
                        WHERE scan_date = %s
                    """, (asin, index, completed, today_date))
                else:
                    # Create new checkpoint
                    cur.execute("""
                        INSERT INTO scraper_checkpoint 
                        (scan_date, last_asin, last_index, completed)
                        VALUES (%s, %s, %s, %s)
                    """, (today_date, asin, index, completed))
                
                self.conn.commit()
                logger.info(f"Checkpoint saved: ASIN={asin}, Index={index}, Completed={completed}")
        except Exception as e:
            logger.error(f"Error saving checkpoint: {str(e)}")
            self.conn.rollback()

    def get_last_checkpoint(self):
        """Get the last checkpoint to resume from"""
        try:
            with self.conn.cursor() as cur:
                today_date = datetime.now().date()
                
                cur.execute("""
                    SELECT last_asin, last_index, completed 
                    FROM scraper_checkpoint 
                    WHERE scan_date = %s
                """, (today_date,))
                
                result = cur.fetchone()
                
                if result:
                    return {
                        'last_asin': result[0],
                        'last_index': result[1],
                        'completed': result[2]
                    }
                return None
        except Exception as e:
            logger.error(f"Error retrieving checkpoint: {str(e)}")
            return None

    def get_scraped_asins_for_today(self):
        """Get list of ASINs already scraped today to avoid duplicates"""
        try:
            with self.conn.cursor() as cur:
                today_date = datetime.now().date()
                
                cur.execute("""
                    SELECT asin FROM daily_amazon_data 
                    WHERE scan_date = %s
                """, (today_date,))
                
                results = cur.fetchall()
                return [r[0] for r in results] if results else []
        except Exception as e:
            logger.error(f"Error retrieving scraped ASINs: {str(e)}")
            return []

    def close(self):
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")


def delete_all_cookies(driver):
    """Deletes all cookies before starting the script"""
    try:
        driver.delete_all_cookies()
        logger.info("All cookies deleted successfully.")
    except Exception as e:
        logger.error(f"Error deleting cookies: {str(e)}")


def login_and_setup(driver, max_attempts=3):
    """Initialize Amazon session and handle initial setup with retry logic"""
    delete_all_cookies(driver)
    
    for attempt in range(max_attempts):
        try:
            url = "https://www.amazon.com"
            driver.get(url)
            
            # Short wait for initial page load
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//div")))
            
            # Handle captcha if present
            if "Type the characters you see in this image" in driver.page_source:
                logger.info("Initial captcha detected during setup")
                if not handle_captcha(driver):
                    logger.warning(f"Failed to solve initial captcha, retrying setup (attempt {attempt+1}/{max_attempts})")
                    driver.delete_all_cookies()
                    time.sleep(random.uniform(2, 4))
                    continue
                else:
                    # Give additional time after solving captcha
                    time.sleep(random.uniform(3, 5))
            
            # Wait for location element with a reasonable timeout
            try:
                location_element = WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.XPATH, "//div[@id='glow-ingress-block']"))
                )
                
                # Check and set location if needed
                location_text = location_element.text.strip()
                if "11229" not in location_text:
                    try:
                        # Accept cookies if present
                        try:
                            cookie_button = WebDriverWait(driver, 3).until(
                                EC.element_to_be_clickable((By.XPATH, "//input[@id='sp-cc-accept']"))
                            )
                            cookie_button.click()
                        except:
                            pass  # No cookie acceptance needed
                        
                        # Set location
                        location_element.click()
                        
                        zip_input = WebDriverWait(driver, 8).until(
                            EC.presence_of_element_located((By.XPATH, "//input[@id='GLUXZipUpdateInput']"))
                        )
                        zip_input.clear()
                        zip_input.send_keys("11229")
                        
                        update_button = WebDriverWait(driver, 5).until(
                            EC.element_to_be_clickable((By.XPATH, "//span[@data-action='GLUXPostalUpdateAction']/input[@class='a-button-input']"))
                        )
                        update_button.click()
                        
                        # Wait for location to be applied
                        time.sleep(2)
                    except Exception as e:
                        logger.warning(f"Error setting location: {str(e)}")
            except TimeoutException:
                logger.warning("Location element not found, continuing without setting location")
            
            driver.refresh()
            time.sleep(2)
            logger.info("Amazon setup completed successfully")
            return driver

        except KeyboardInterrupt:
            raise KeyboardInterrupt
        except Exception as e:
            logger.error(f"Setup error (attempt {attempt+1}/{max_attempts}): {str(e)}")
            if attempt < max_attempts - 1:
                time.sleep(random.uniform(3, 6))
                continue
            else:
                raise  # Re-raise the exception if all attempts failed
    
    raise Exception("Failed to complete Amazon setup after maximum attempts")


def handle_captcha(driver, max_attempts=3):
    """
    Handles Amazon captcha with multiple retry attempts.
    Returns True if captcha was successfully handled or not present, False otherwise.
    """
    solver = TwoCaptcha(os.getenv('APIKEY_2CAPTCHA', 'b6bf51f9305ea298f4f2e8946bf46773'))
    attempts = 0
    
    while attempts < max_attempts:
        try:
            # Check if captcha is present on the page
            if "Type the characters you see in this image" in driver.page_source:
                logger.info(f"Captcha detected. Attempt {attempts + 1} of {max_attempts}")
                
                # Try to find and solve the captcha
                captcha_image = driver.find_element(By.XPATH, "//form[@action='/errors/validateCaptcha']//img")
                captcha_image.screenshot("captcha.png")
                
                # Solve with 2Captcha
                result = solver.normal('captcha.png')
                if not result.get("code"):
                    logger.error("Failed to get captcha solution code")
                    attempts += 1
                    time.sleep(random.uniform(2, 4))
                    continue
                    
                solved_captcha = result["code"]
                logger.info(f"Captcha solution received: {solved_captcha}")
                
                # Enter the solution
                captcha_input = driver.find_element(By.XPATH, "//input[@id='captchacharacters']")
                captcha_input.clear()
                captcha_input.send_keys(solved_captcha)
                
                # Submit the form
                submit_button = driver.find_element(By.XPATH, "//button[@type='submit']")
                submit_button.click()
                
                # Wait for page to load after submission
                time.sleep(random.uniform(2, 3))
                
                # Check if captcha is still present (indicating failure)
                if "Type the characters you see in this image" in driver.page_source:
                    logger.warning("Captcha solution was incorrect, trying again")
                    attempts += 1
                else:
                    logger.info("Captcha solved successfully")
                    return True
            else:
                # No captcha detected
                return True
                
        except Exception as e:
            logger.error(f"Error handling captcha: {str(e)}")
            attempts += 1
            time.sleep(random.uniform(2, 3))
    
    logger.error(f"Failed to solve captcha after {max_attempts} attempts")
    return False


def initialize_driver():
    """Initialize and return the WebDriver with proper configuration"""
    try:
        driver = Driver(uc=True)
        logger.info("WebDriver initialized successfully")
        return driver
    except Exception as e:
        logger.error(f"Error initializing WebDriver: {str(e)}")
        raise


class AmazonProductScraper:
    def __init__(self, driver, db_manager, excel_file='cleaned_asin.xlsx'):
        self.driver = driver
        self.db_manager = db_manager
        self.solver = TwoCaptcha(os.getenv('APIKEY_2CAPTCHA', 'b6bf51f9305ea298f4f2e8946bf46773'))
        self.load_asins(excel_file)
        self.captcha_failures = 0
        self.consecutive_errors = 0
        self.max_consecutive_errors = 5
        
    def load_asins(self, excel_file):
        try:
            df = pd.read_excel(excel_file)
            self.asins = df['ASIN'].dropna().astype(str).tolist()
            logger.info(f"Successfully loaded {len(self.asins)} ASINs from {excel_file}")
        except Exception as e:
            logger.error(f"Error reading Excel file: {str(e)}")
            self.asins = []

    def extract_price(self):
        try:
            # Try the standard price element first
            try:
                price_whole = self.driver.find_element(By.XPATH, "//span[@class='a-price-whole']").text
                price_fraction = self.driver.find_element(By.XPATH, "//span[@class='a-price-fraction']").text
                price = f"{price_whole}.{price_fraction}" if price_whole else None
            except NoSuchElementException:
                # Try alternative price elements
                try:
                    price_element = self.driver.find_element(By.XPATH, "//*[@id='corePriceDisplay_desktop_feature_div']/div[1]/span/span[1]")
                    price = price_element.text.strip().replace('$', '')
                except NoSuchElementException:
                    try:
                        price_element = self.driver.find_element(By.XPATH, "//span[@id='price_inside_buybox']")
                        price = price_element.text.strip().replace('$', '')
                    except:
                        price = None
            
            return float(price) if price and price.replace('.', '').isdigit() else None
        except:
            return None

    def extract_best_seller_rank(self):
        try:
            # Try multiple XPaths for best seller rank
            for xpath in [
                "//th[contains(text(), 'Best Sellers Rank')]/following-sibling::td",
                "//span[contains(text(), 'Best Sellers Rank')]/following::span[1]",
                "//*[contains(text(), 'Best Sellers Rank')]/.."
            ]:
                try:
                    rank_section = self.driver.find_element(By.XPATH, xpath).text
                    if rank_section:
                        cleaned_rank = re.sub(r'<[^>]+>', '', rank_section).strip()
                        cleaned_rank = re.sub(r'\s*\([^)]*\)', '', cleaned_rank)
                        ranks = []
                        for rank in cleaned_rank.split('#'):
                            if rank.strip():
                                ranks.append('#' + rank.strip())
                        best_seller_rank = ', '.join(ranks).strip()
                        return best_seller_rank
                except NoSuchElementException:
                    continue
            
            return 'Not ranked'
        except:
            return 'Not ranked'

    def extract_offers(self):
        try:
            # Try multiple XPaths for offers
            for xpath in [
                "//div[@id='dynamic-aod-ingress-box']//span[@class='a-declarative']/span[1]",
                "//span[contains(text(), 'New')]/span[contains(text(), 'from')]",
                "//div[contains(@id, 'olp_feature_div')]//span"
            ]:
                try:
                    offers_text = self.driver.find_element(By.XPATH, xpath).text
                    offers_match = re.search(r'\((\d+)\)', offers_text)
                    if offers_match:
                        return offers_match.group(1)
                except NoSuchElementException:
                    continue
            
            # If no offers found but product page loaded, assume at least 1 offer
            return '1'
        except:
            return '1'

    def extract_minimum_price(self):
        try:
            # Try different XPaths for minimum price
            for xpath_pair in [
                (
                    "//div[@id='dynamic-aod-ingress-box']//div//div//a/span[@class='a-declarative']/span[@class='a-price']/span[2]/span[@class='a-price-whole']",
                    "//div[@id='dynamic-aod-ingress-box']//div//div//a/span[@class='a-declarative']/span[@class='a-price']/span[2]//span[@class='a-price-fraction']"
                ),
                (
                    "//div[contains(@id, 'olp_feature_div')]//span[@class='a-price']/span[@class='a-offscreen']",
                    None
                )
            ]:
                try:
                    if xpath_pair[1]:  # Two-part price (whole and fraction)
                        min_price_whole = self.driver.find_element(By.XPATH, xpath_pair[0]).text
                        min_price_fraction = self.driver.find_element(By.XPATH, xpath_pair[1]).text
                        if min_price_whole:
                            min_price = f"{min_price_whole}.{min_price_fraction}"
                            return float(min_price)
                    else:  # Price in a single element
                        price_element = self.driver.find_element(By.XPATH, xpath_pair[0])
                        price_text = price_element.get_attribute("textContent") or price_element.text
                        price_text = price_text.replace('$', '').strip()
                        if price_text:
                            return float(price_text)
                except (NoSuchElementException, ValueError):
                    continue
            
            # If no minimum price is found, return the main price
            return self.extract_price()
        except:
            return self.extract_price()

    def scrape_product(self, asin):
        url = f'https://www.amazon.com/dp/{asin}'
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                self.driver.get(url)
                
                # Use shorter wait for initial page load
                time.sleep(random.uniform(1, 2))
                
                # Check for captcha
                if "Type the characters you see in this image" in self.driver.page_source:
                    logger.info(f"Captcha detected for ASIN {asin}")
                    if not handle_captcha(self.driver):
                        self.captcha_failures += 1
                        if self.captcha_failures >= 3:
                            logger.warning("Multiple captcha failures, need to restart driver")
                            raise Exception("Multiple captcha failures")
                        retry_count += 1
                        continue
                    else:
                        self.captcha_failures = 0  # Reset captcha failure counter after success
                        # Add an extra wait after solving captcha
                        time.sleep(random.uniform(2, 3))
                
                # Short wait after captcha handling
                time.sleep(random.uniform(1, 2))
                
                # Extract product data
                result = {
                    'asin': asin,
                    'price': self.extract_price(),
                    'best_seller_rank': self.extract_best_seller_rank(),
                    'offers': self.extract_offers(),
                    'minimum_price': self.extract_minimum_price()
                }
                
                # Reset consecutive error counter on success
                self.consecutive_errors = 0
                
                # Delay before next request, shorter than original
                time.sleep(random.uniform(1.5, 3))
                
                return result
                
            except Exception as e:
                logger.error(f"Error scraping ASIN {asin}: {str(e)}")
                retry_count += 1
                self.consecutive_errors += 1
                
                # If too many consecutive errors, signal need for driver restart
                if self.consecutive_errors >= self.max_consecutive_errors:
                    logger.warning(f"Too many consecutive errors ({self.consecutive_errors}), need to restart driver")
                    raise Exception("Too many consecutive errors")
                
                if retry_count < max_retries:
                    logger.info(f"Retrying... Attempt {retry_count + 1} of {max_retries}")
                    time.sleep(random.uniform(3, 5))
                else:
                    logger.warning(f"Max retries reached for ASIN {asin}")
                    return {
                        'asin': asin,
                        'price': None,
                        'best_seller_rank': 'Error',
                        'offers': None,
                        'minimum_price': None
                    }

    def scrape_all_products(self, start_index=0):
        batch_results = []
        batch_size = 10  # Batch size for database saves
        
        # Get already scraped ASINs for today to avoid duplicates
        already_scraped = self.db_manager.get_scraped_asins_for_today()
        logger.info(f"Found {len(already_scraped)} ASINs already scraped today")
        
        # Adjust start_index based on checkpoint
        checkpoint = self.db_manager.get_last_checkpoint()
        if checkpoint and checkpoint['last_index'] > start_index:
            start_index = checkpoint['last_index']
            logger.info(f"Resuming from checkpoint at index {start_index}")
        
        for i, asin in enumerate(self.asins[start_index:], start_index + 1):
            # Skip if already scraped today
            if asin in already_scraped:
                logger.info(f"Skipping ASIN {asin} (already scraped today)")
                continue
            
            logger.info(f"Scraping product {i} of {len(self.asins)}: {asin}")
            
            try:
                result = self.scrape_product(asin)
                batch_results.append(result)
                
                # Save checkpoint regularly
                if i % 5 == 0:  # Save checkpoint every 5 products
                    self.db_manager.save_checkpoint(asin, i, completed=False)
                
                # Save progress when batch size is reached
                if len(batch_results) >= batch_size:
                    logger.info(f"Saving batch of {len(batch_results)} products")
                    self.db_manager.save_product_data(batch_results)
                    # Clear the batch after saving
                    batch_results = []
            
            except Exception as e:
                if "Multiple captcha failures" in str(e) or "Too many consecutive errors" in str(e):
                    # Save current batch before restarting
                    if batch_results:
                        logger.info(f"Saving current batch before driver restart")
                        self.db_manager.save_product_data(batch_results)
                    
                    # Save checkpoint so we can resume from this point
                    self.db_manager.save_checkpoint(asin, i-1, completed=False)
                    
                    # Signal the calling function to restart the driver
                    logger.info(f"Need to restart driver and resume from index {i}")
                    return {
                        'status': 'restart_needed',
                        'resume_index': i-1  # Resume from the previous index
                    }
        
        # Save any remaining results in the final batch
        if batch_results:
            logger.info(f"Saving final batch of {len(batch_results)} products")
            self.db_manager.save_product_data(batch_results)
        
        # Mark process as completed
        self.db_manager.save_checkpoint(self.asins[-1] if self.asins else '', len(self.asins), completed=True)
        
        return {
            'status': 'completed',
            'message': "Scraping completed successfully"
        }


def run_scraper_with_recovery():
    """Run the scraper with recovery logic for captchas and errors"""
    logger.info("Starting Amazon product scraper job with recovery logic")
    driver = None
    db_manager = None
    
    try:
        # Initialize database connection
        db_manager = SimpleDatabaseManager()
        
        # Check if today's job is already completed
        checkpoint = db_manager.get_last_checkpoint()
        if checkpoint and checkpoint.get('completed', False):
            logger.info("Today's scraping job already completed")
            return "Already completed"
        
        # Get start index from checkpoint
        start_index = checkpoint['last_index'] if checkpoint else 0
        
        # Maximum number of driver restarts
        max_restarts = 10
        restart_count = 0
        
        while restart_count < max_restarts:
            try:
                # Initialize driver
                if driver:
                    try:
                        driver.quit()
                    except:
                        pass
                
                driver = initialize_driver()
                
                # Setup driver and login
                login_and_setup(driver)
                
                # Create and run scraper
                scraper = AmazonProductScraper(driver, db_manager)
                
                # Run scraper from last checkpoint
                result = scraper.scrape_all_products(start_index)
                
                # Check if we need to restart the driver
                if result['status'] == 'restart_needed':
                    start_index = result['resume_index'] + 1
                    restart_count += 1
                    logger.info(f"Restarting driver (attempt {restart_count}/{max_restarts}), resuming from index {start_index}")
                    # Give a slightly longer pause before restarting
                    time.sleep(random.uniform(5, 10))
                    continue
                
                # If the scraper completed successfully, we're done
                if result['status'] == 'completed':
                    logger.info("Amazon scraping job completed successfully")
                    return "Completed successfully"
            
            except KeyboardInterrupt:
                logger.warning("Scraping interrupted by user")
                raise
            except Exception as e:
                logger.error(f"Unexpected error, restarting driver: {str(e)}")
                logger.error(traceback.format_exc())
                restart_count += 1
                time.sleep(random.uniform(10, 15))  # Longer delay before restart on unexpected error
        
        logger.error(f"Exceeded maximum number of driver restarts ({max_restarts})")
        return "Failed after maximum restarts"
    
    except KeyboardInterrupt:
        logger.warning("Scraping interrupted by user")
        return "Interrupted by user"
    except Exception as e:
        logger.error(f"Critical error in scraper job: {str(e)}")
        logger.error(traceback.format_exc())
        return f"Failed with error: {str(e)}"
    finally:
        # Cleanup resources
        if driver:
            try:
                driver.quit()
                logger.info("WebDriver closed")
            except:
                logger.warning("Error closing WebDriver")
            
        if db_manager:
            db_manager.close()


def schedule_jobs():
    """Schedule the scraper to run daily at specific time"""
    # Set the job to run at 1:00 AM every day
    schedule.every().day.at("00:00").do(run_scraper_with_recovery)
    
    logger.info("Scheduler started. Jobs will run at 12:00 AM daily")
    
    while True:
        try:
            schedule.run_pending()
            time.sleep(60)  # Sleep for 1 minute between checks
        except KeyboardInterrupt:
            logger.info("Scheduler interrupted by user")
            break
        except Exception as e:
            logger.error(f"Scheduler error: {str(e)}")
            time.sleep(300)  # Sleep for 5 minutes on error


def main():
    """
    Main function to either run the scraper immediately or start the scheduler
    """
    import argparse
    
    parser = argparse.ArgumentParser(description='Amazon Product Scraper with Database Storage')
    parser.add_argument('--now', action='store_true', help='Run the scraper immediately')
    parser.add_argument('--schedule', action='store_true', help='Schedule the scraper to run daily')
    parser.add_argument('--from-idx', type=int, default=0, help='Start scraping from specific index')
    
    args = parser.parse_args()
    
    if args.now:
        logger.info(f"Running scraper immediately from index {args.from_idx}")
        run_scraper_with_recovery()
    elif args.schedule:
        logger.info("Starting scheduler")
        schedule_jobs()
    else:
        logger.info("No action specified. Use --now to run immediately or --schedule to schedule daily runs")
        run_scraper_with_recovery()  # Default behavior: run immediately


if __name__ == "__main__":
    main()