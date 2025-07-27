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
import os
import re
import logging
import psycopg2
import pandas as pd
from datetime import datetime
import schedule
import json

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("amazon_realtime_scraper.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("AmazonRealtimeScraper")

# Database configuration
DB_CONFIG = {
    'dbname': 'amazon.com',
    'user': 'postgres',
    'password': 'Talha',
    'host': 'localhost',
    'port': '5432'
}

def get_db_connection():
    """Establish connection to PostgreSQL database"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        logger.error(f"Database connection error: {str(e)}")
        return None

def create_realtimedata_table():
    """Create the realtimedata table if it doesn't exist"""
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS realtimedata (
                    asin VARCHAR(20) PRIMARY KEY,
                    title TEXT,
                    price DECIMAL(10,2),
                    rating VARCHAR(20),
                    reviews_count VARCHAR(50),
                    best_seller_rank TEXT,
                    buybox_shipped_from TEXT,
                    buybox_sold_by TEXT,
                    buybox_price DECIMAL(10,2),
                    other_offers JSONB,
                    last_updated TIMESTAMP
                )
            """)
            conn.commit()
            logger.info("realtimedata table created/verified successfully")
            return True
    except Exception as e:
        logger.error(f"Error creating realtimedata table: {str(e)}")
        return False
    finally:
        conn.close()

class RealtimeAmazonScraper:
    def __init__(self):
        """
        Initialize the real-time scraper
        """
        self.driver = None
        self.solver = TwoCaptcha(os.getenv('APIKEY_2CAPTCHA', 'b6bf51f9305ea298f4f2e8946bf46773'))
        self.captcha_failures = 0
        self.max_captcha_failures = 3
        
    def initialize_driver(self):
        """Initialize and configure the WebDriver"""
        try:
            self.driver = Driver(uc=True)
            logger.info("WebDriver initialized successfully")
            self._setup_amazon_session()
            return True
        except Exception as e:
            logger.error(f"Error initializing WebDriver: {str(e)}")
            return False

    def _delete_all_cookies(self):
        """Deletes all cookies before starting the script"""
        try:
            self.driver.delete_all_cookies()
            logger.info("All cookies deleted successfully.")
        except Exception as e:
            logger.error(f"Error deleting cookies: {str(e)}")

    def _handle_captcha(self, max_attempts=3):
        """
        Handles Amazon captcha with multiple retry attempts.
        Returns True if captcha was successfully handled or not present, False otherwise.
        """
        attempts = 0
        
        while attempts < max_attempts:
            try:
                # Check if captcha is present on the page
                if "Type the characters you see in this image" in self.driver.page_source:
                    logger.info(f"Captcha detected. Attempt {attempts + 1} of {max_attempts}")
                    
                    # Try to find and solve the captcha
                    captcha_image = self.driver.find_element(By.XPATH, "//form[@action='/errors/validateCaptcha']//img")
                    captcha_image.screenshot("captcha.png")
                    
                    # Solve with 2Captcha
                    result = self.solver.normal('captcha.png')
                    if not result.get("code"):
                        logger.error("Failed to get captcha solution code")
                        attempts += 1
                        time.sleep(random.uniform(2, 4))
                        continue
                        
                    solved_captcha = result["code"]
                    logger.info(f"Captcha solution received: {solved_captcha}")
                    
                    # Enter the solution
                    captcha_input = self.driver.find_element(By.XPATH, "//input[@id='captchacharacters']")
                    captcha_input.clear()
                    captcha_input.send_keys(solved_captcha)
                    
                    # Submit the form
                    submit_button = self.driver.find_element(By.XPATH, "//button[@type='submit']")
                    submit_button.click()
                    
                    # Wait for page to load after submission
                    time.sleep(random.uniform(2, 3))
                    
                    # Check if captcha is still present (indicating failure)
                    if "Type the characters you see in this image" in self.driver.page_source:
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

    def _setup_amazon_session(self):
        """Setup Amazon session with proper location and cookies"""
        self._delete_all_cookies()
        
        url = "https://www.amazon.com"
        self.driver.get(url)
        
        # Short wait for initial page load
        WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.XPATH, "//div")))
        
        # Handle captcha if present
        if "Type the characters you see in this image" in self.driver.page_source:
            logger.info("Initial captcha detected during setup")
            if not self._handle_captcha():
                raise Exception("Failed to solve initial captcha")
            else:
                # Give additional time after solving captcha
                time.sleep(random.uniform(3, 5))
        
        # Wait for location element with a reasonable timeout
        try:
            location_element = WebDriverWait(self.driver, 15).until(
                EC.presence_of_element_located((By.XPATH, "//div[@id='glow-ingress-block']"))
            )
            
            # Check and set location if needed
            location_text = location_element.text.strip()
            if "11229" not in location_text:
                try:
                    # Accept cookies if present
                    try:
                        cookie_button = WebDriverWait(self.driver, 3).until(
                            EC.element_to_be_clickable((By.XPATH, "//input[@id='sp-cc-accept']"))
                        )
                        cookie_button.click()
                    except:
                        pass  # No cookie acceptance needed
                    
                    # Set location
                    location_element.click()
                    
                    zip_input = WebDriverWait(self.driver, 8).until(
                        EC.presence_of_element_located((By.XPATH, "//input[@id='GLUXZipUpdateInput']"))
                    )
                    zip_input.clear()
                    zip_input.send_keys("11229")
                    
                    update_button = WebDriverWait(self.driver, 5).until(
                        EC.element_to_be_clickable((By.XPATH, "//span[@data-action='GLUXPostalUpdateAction']/input[@class='a-button-input']"))
                    )
                    update_button.click()
                    
                    # Wait for location to be applied
                    time.sleep(2)
                except Exception as e:
                    logger.warning(f"Error setting location: {str(e)}")
        except TimeoutException:
            logger.warning("Location element not found, continuing without setting location")
        
        self.driver.refresh()
        time.sleep(2)
        logger.info("Amazon setup completed successfully")

    def _safe_find_element(self, by, value, wait_time=5):
        """
        Safely find an element with error handling and wait
        
        :param by: Selenium By method
        :param value: Selector value
        :param wait_time: Time to wait for element
        :return: Element text or None
        """
        try:
            element = WebDriverWait(self.driver, wait_time).until(
                EC.presence_of_element_located((by, value))
            )
            return element.text
        except (NoSuchElementException, TimeoutException):
            logger.debug(f"Element not found: {by}, {value}")
            return None
        except Exception as e:
            logger.debug(f"Error finding element {by}, {value}: {str(e)}")
            return None

    def _safe_find_elements(self, by, value, wait_time=5):
        """
        Safely find multiple elements with error handling and wait
        
        :param by: Selenium By method
        :param value: Selector value
        :param wait_time: Time to wait for elements
        :return: List of elements or empty list
        """
        try:
            WebDriverWait(self.driver, wait_time).until(
                EC.presence_of_element_located((by, value))
            )
            return self.driver.find_elements(by, value)
        except (NoSuchElementException, TimeoutException):
            logger.debug(f"Elements not found: {by}, {value}")
            return []
        except Exception as e:
            logger.debug(f"Error finding elements {by}, {value}: {str(e)}")
            return []

    def _extract_price(self):
        """Extract current price with multiple fallback methods"""
        try:
            # Try the standard price element first
            try:
                price_whole = self._safe_find_element(By.XPATH, "//span[@class='a-price-whole']")
                price_fraction = self._safe_find_element(By.XPATH, "//span[@class='a-price-fraction']")
                if price_whole:
                    return f"{price_whole}.{price_fraction}"
            except:
                pass
                
            # Try alternative price elements
            try:
                price_element = self._safe_find_element(
                    By.XPATH, "//*[@id='corePriceDisplay_desktop_feature_div']/div[1]/span/span[1]"
                )
                if price_element:
                    return price_element.strip().replace('$', '')
            except:
                pass
                
            try:
                price_element = self._safe_find_element(By.XPATH, "//span[@id='price_inside_buybox']")
                if price_element:
                    return price_element.strip().replace('$', '')
            except:
                pass
                
            return None
        except Exception as e:
            logger.error(f"Error extracting price: {str(e)}")
            return None

    def _extract_best_seller_rank(self):
        """Extract best seller rank with multiple fallback methods"""
        try:
            # Try multiple XPaths for best seller rank
            for xpath in [
                "//th[contains(text(), 'Best Sellers Rank')]/following-sibling::td",
                "//span[contains(text(), 'Best Sellers Rank')]/following::span[1]",
                "//*[contains(text(), 'Best Sellers Rank')]/.."
            ]:
                try:
                    rank_section = self._safe_find_element(By.XPATH, xpath)
                    if rank_section:
                        cleaned_rank = re.sub(r'<[^>]+>', '', rank_section).strip()
                        cleaned_rank = re.sub(r'\s*\([^)]*\)', '', cleaned_rank)
                        ranks = []
                        for rank in cleaned_rank.split('#'):
                            if rank.strip():
                                ranks.append('#' + rank.strip())
                        best_seller_rank = ', '.join(ranks).strip()
                        return best_seller_rank
                except:
                    continue
            
            return 'Not ranked'
        except Exception as e:
            logger.error(f"Error extracting best seller rank: {str(e)}")
            return 'Not ranked'

    def _scrape_buybox_offer(self, product_data):
        """Scrape BuyBox offer details"""
        try:
            buybox_data = {
                'shipped_from': self._safe_find_element(
                    By.XPATH, 
                    "//div[@id='offer-display-features']//div[@id='fulfillerInfoFeature_feature_div']/div[2]"
                ),
                'sold_by': self._safe_find_element(
                    By.XPATH, 
                    "//div[@id='offer-display-features']//div[@id='merchantInfoFeature_feature_div']/div[2]"
                ),
                'price': product_data['price']
            }
            return buybox_data
        except Exception as e:
            logger.error(f"Error extracting BuyBox offer: {str(e)}")
            return None

    def _scrape_other_offers(self):
        """Scrape other available offers"""
        try:
            other_offers = []
            
            # Check if panel exists and can be clicked
            try:
                panel = WebDriverWait(self.driver, 5).until(
                    EC.element_to_be_clickable((
                        By.XPATH, 
                        "//div[@id='dynamic-aod-ingress-box']//a"
                    ))
                )
                panel.click()
                time.sleep(2)  # Wait for panel to load
            except (NoSuchElementException, TimeoutException, WebDriverException):
                logger.info("No additional offers panel found or not clickable")
                return other_offers

            # Find available offers
            offers_available = self._safe_find_elements(
                By.XPATH, 
                "//div[@id='aod-offer-list']/div[@id='aod-offer']"
            )
            
            logger.info(f"Found {len(offers_available)} additional offers")

            for offer in offers_available:
                try:
                    offer_type = "New"  # Default value
                    try:
                        offer_type = offer.find_element(
                            By.XPATH, 
                            ".//div[@id='aod-offer-heading']/span"
                        ).text
                    except:
                        pass
                    
                    sold_from = "Unknown"
                    try:
                        sold_from = offer.find_element(
                            By.XPATH, 
                            ".//div[@id='aod-offer-shipsFrom']/div/div/div[2]/span"
                        ).text
                    except:
                        pass
                    
                    seller_name = "Unknown"
                    try:
                        seller_name = offer.find_element(
                            By.XPATH, 
                            ".//div[@id='aod-offer-soldBy']//a"
                        ).text
                    except:
                        try:
                            seller_name = offer.find_element(
                                By.XPATH, 
                                ".//div[@id='aod-offer-soldBy']/div/div/div[2]/span"
                            ).text
                        except:
                            pass
                    
                    seller_price = "Unknown"
                    try:
                        sellerprice_whole = offer.find_element(
                            By.XPATH, 
                            ".//div[@id='aod-offer-price']//span[@class='a-price-whole']"
                        ).text
                        
                        sellerprice_fraction = offer.find_element(
                            By.XPATH, 
                            ".//div[@id='aod-offer-price']//span[@class='a-price-fraction']"
                        ).text
                        
                        seller_price = f"{sellerprice_whole}.{sellerprice_fraction}"
                    except:
                        pass
                    
                    other_offers.append({
                        'type': offer_type,
                        'shipped_from': sold_from,
                        'seller_name': seller_name,
                        'price': seller_price
                    })
                
                except Exception as e:
                    logger.error(f"Error processing an offer: {str(e)}")

            return other_offers

        except Exception as e:
            logger.error(f"Error extracting other offers: {str(e)}")
            return []

    def scrape_product(self, asin):
        """
        Scrape all product details for the specified ASIN
        
        :param asin: Amazon Standard Identification Number
        :return: Dictionary of product data or None if failed
        """
        if not self.driver:
            if not self.initialize_driver():
                logger.error("Failed to initialize driver")
                return None
                
        url = f'https://www.amazon.com/dp/{asin}'
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                logger.info(f"Accessing product page for ASIN {asin}")
                self.driver.get(url)
                
                # Use shorter wait for initial page load
                time.sleep(random.uniform(1, 2))
                
                # Check for captcha
                if "Type the characters you see in this image" in self.driver.page_source:
                    logger.info(f"Captcha detected for ASIN {asin}")
                    if not self._handle_captcha():
                        self.captcha_failures += 1
                        if self.captcha_failures >= self.max_captcha_failures:
                            logger.error("Maximum captcha failures reached")
                            return None
                        retry_count += 1
                        continue
                    else:
                        self.captcha_failures = 0  # Reset counter after success
                        time.sleep(random.uniform(2, 3))
                
                # Initialize product data dictionary
                product_data = {
                    'asin': asin,
                    'title': None,
                    'price': None,
                    'rating': None,
                    'reviews_count': None,
                    'best_seller_rank': None,
                    'buybox_offer': None,
                    'other_offers': [],
                    'last_updated': datetime.now()
                }
                
                # Scrape basic product details
                product_data['title'] = self._safe_find_element(By.ID, 'productTitle')
                product_data['price'] = self._extract_price()
                product_data['rating'] = self._safe_find_element(
                    By.XPATH, 
                    "//div[@id='averageCustomerReviews']//span[@id='acrPopover']//span"
                )
                product_data['reviews_count'] = self._safe_find_element(
                    By.XPATH, 
                    "//div[@id='averageCustomerReviews']//a[@id='acrCustomerReviewLink']//span[@id='acrCustomerReviewText']"
                )
                product_data['best_seller_rank'] = self._extract_best_seller_rank()
                
                # Scrape BuyBox offer
                product_data['buybox_offer'] = self._scrape_buybox_offer(product_data)
                
                # Scrape other offers
                product_data['other_offers'] = self._scrape_other_offers()
                
                logger.info(f"Successfully scraped product data for ASIN {asin}")
                return product_data
                
            except Exception as e:
                logger.error(f"Error scraping ASIN {asin}: {str(e)}")
                retry_count += 1
                
                if retry_count < max_retries:
                    logger.info(f"Retrying... Attempt {retry_count + 1} of {max_retries}")
                    time.sleep(random.uniform(3, 5))
                else:
                    logger.warning(f"Max retries reached for ASIN {asin}")
                    return None

    def save_to_database(self, product_data):
        """Save scraped product data to PostgreSQL database"""
        if not product_data:
            return False
            
        conn = get_db_connection()
        if not conn:
            return False
        
        try:
            with conn.cursor() as cursor:
                # Convert other_offers to JSON string
                other_offers_json = json.dumps(product_data['other_offers']) if product_data['other_offers'] else None
                
                # Convert price to decimal
                try:
                    price = float(product_data['price'].replace('$', '').replace(',', '')) if product_data['price'] else None
                except:
                    price = None
                
                # Convert buybox price to decimal
                try:
                    buybox_price = float(product_data['buybox_offer']['price'].replace('$', '').replace(',', '')) if (product_data['buybox_offer'] and product_data['buybox_offer']['price']) else None
                except:
                    buybox_price = None
                
                # Upsert the data
                cursor.execute("""
                    INSERT INTO realtimedata (
                        asin, title, price, rating, reviews_count, best_seller_rank,
                        buybox_shipped_from, buybox_sold_by, buybox_price, other_offers, last_updated
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (asin) DO UPDATE SET
                        title = EXCLUDED.title,
                        price = EXCLUDED.price,
                        rating = EXCLUDED.rating,
                        reviews_count = EXCLUDED.reviews_count,
                        best_seller_rank = EXCLUDED.best_seller_rank,
                        buybox_shipped_from = EXCLUDED.buybox_shipped_from,
                        buybox_sold_by = EXCLUDED.buybox_sold_by,
                        buybox_price = EXCLUDED.buybox_price,
                        other_offers = EXCLUDED.other_offers,
                        last_updated = EXCLUDED.last_updated
                """, (
                    product_data['asin'],
                    product_data['title'],
                    price,
                    product_data['rating'],
                    product_data['reviews_count'],
                    product_data['best_seller_rank'],
                    product_data['buybox_offer']['shipped_from'] if product_data['buybox_offer'] else None,
                    product_data['buybox_offer']['sold_by'] if product_data['buybox_offer'] else None,
                    buybox_price,
                    other_offers_json,
                    product_data['last_updated']
                ))
                
                conn.commit()
                logger.info(f"Successfully saved data for ASIN {product_data['asin']} to database")
                return True
        except Exception as e:
            logger.error(f"Error saving data to database for ASIN {product_data['asin']}: {str(e)}")
            return False
        finally:
            conn.close()

    def close(self):
        """Close the WebDriver"""
        if self.driver:
            try:
                self.driver.quit()
                logger.info("WebDriver closed successfully")
            except Exception as e:
                logger.error(f"Error closing WebDriver: {str(e)}")

def get_asins_from_excel():
    """Read ASINs from cleaned_asin.xlsx file"""
    try:
        df = pd.read_excel('cleaned_asin.xlsx')
        if 'asin' in df.columns:
            return df['asin'].tolist()
        else:
            logger.error("No 'asin' column found in cleaned_asin.xlsx")
            return []
    except Exception as e:
        logger.error(f"Error reading ASINs from Excel file: {str(e)}")
        return []

def scrape_all_asins():
    """Scrape all ASINs from the Excel file and save to database"""
    asins = get_asins_from_excel()
    if not asins:
        logger.error("No ASINs found to scrape")
        return
    
    logger.info(f"Starting to scrape {len(asins)} ASINs")
    
    # Initialize scraper once
    scraper = RealtimeAmazonScraper()
    
    try:
        for i, asin in enumerate(asins, 1):
            logger.info(f"Processing ASIN {i} of {len(asins)}: {asin}")
            
            # Scrape product data
            product_data = scraper.scrape_product(asin)
            if product_data:
                # Save to database
                scraper.save_to_database(product_data)
                logger.info(f"Successfully processed ASIN {asin}")
            else:
                logger.warning(f"Failed to scrape data for ASIN {asin}")
            
            # Add random delay between ASINs (1-3 seconds)
            time.sleep(random.uniform(1, 2))
            
    except Exception as e:
        logger.error(f"Error during scraping process: {str(e)}")
    finally:
        scraper.close()
    
    logger.info("Finished scraping all ASINs")

def run_scheduled_scrape():
    """Run the scheduled scrape job"""
    logger.info("Starting scheduled daily scrape")
    scrape_all_asins()
    logger.info("Scheduled daily scrape completed")

def schedule_daily_scrape():
    """Schedule the daily scrape to run at a specific time"""
    # Schedule to run every day at 3:00 AM
    schedule.every().day.at("03:00").do(run_scheduled_scrape)
    
    logger.info("Scheduler started. Daily scrape will run at 3:00 AM")
    
    # Keep the script running
    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    # Create the database table if it doesn't exist
    create_realtimedata_table()
    
    # Start the scheduler (uncomment to enable scheduled scraping)
    # schedule_daily_scrape()
    
    # For testing, you can run a one-time scrape of all ASINs
    scrape_all_asins()