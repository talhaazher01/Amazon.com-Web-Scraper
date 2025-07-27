# Amazon.com-Web-Scraper
🕷️ A Python-based Amazon web scraper built for the analytics tool. Extracts real-time product data on schedule bases including pricing, BuyBox, BSR, seller count, and reviews and many more,  all without using third-party APIs.
# 🕷️ Amazon Web Scraper Module

This is a standalone Python-based web scraper for extracting structured product data from Amazon. It works without relying on third-party APIs and is designed for efficient, repeatable, and scalable data collection — ideal for building price trackers, analytics dashboards, or historical product databases.

---

## ⚙️ Core Capabilities

- **📦 Product Details Extraction**
  - Title, price, availability, and ASIN-specific metadata

- **⭐ Review & Rating Data**
  - Total reviews, average rating, and rating breakdown histogram

- **📈 Best Seller Rank (BSR)**
  - Extracts rank data across multiple categories

- **🔍 Offers & Sellers**
  - BuyBox winner, seller count, offer prices, fulfillment type (FBA/FBM)

- **🛠️ Built-in Resilience**
  - Random user-agent rotation  
  - Request throttling  
  - Retry logic for failed requests

---

## 🧰 Tech Stack

- **Language**: Python 3.x  
- **Libraries**:
  - `requests` for HTTP requests  
  - `BeautifulSoup` & `lxml` for parsing  
  - `fake_useragent` for rotating headers  
  - `pandas` for structured output handling  

- **Output Format**: JSON / PostgreSQL row-ready dicts

---

## 🚀 How It Works

1. Accept one or more ASINs as input
2. Send HTTP requests to Amazon product and offer pages
3. Parse HTML content using BeautifulSoup
4. Extract structured fields (price, title, BSR, seller info)
5. Return structured JSON or optionally write to DB




