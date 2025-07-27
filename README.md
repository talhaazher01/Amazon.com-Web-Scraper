# Amazon.com-Web-Scraper
🕷️ A Python-based Amazon web scraper built for the analytics tool. Extracts real-time product data on schedule bases including pricing, BuyBox, BSR, seller count, and reviews and many more,  all without using third-party APIs.
# 🧠 Pulse – Amazon Product Analytics & Intelligence Tool

**Pulse** is a desktop application designed for Amazon sellers to streamline product research and make data-driven decisions. It provides real-time tracking, historical analysis, margin calculators, competitive monitoring, and intelligent recommendations — all powered through efficient in-house web scraping.

---

## 📌 Key Features

* **🔍 Product Tracker**

  * Track product prices, reviews, and rankings over time.
  * Historical charts for ASINs including BuyBox changes and seller fluctuations.

* **📊 Analytics Dashboard**

  * Graphical breakdown of price history, Best Seller Rank (BSR), and offer trends.
  * Monitor and visualize product performance.

* **💡 Smart Recommendations**

  * Discover related products using a recommendation engine (cosine similarity-based).
  * Helps uncover untapped opportunities in related niches.

* **🛒 Offer Comparison**

  * View real-time available offers for each ASIN.
  * Compare FBA, FBM, and pricing strategies of sellers.

* **📦 FBA Calculator**

  * Estimate profit margins using Amazon's FBA fees and your product cost.
  * Break-even and ROI insights for better decision-making.

* **🤖 Built-in Chatbot**

  * Get instant answers to beginner queries related to Amazon FBA and selling process.
  * Regex + cosine similarity based knowledge bot (no AI/LLM dependencies).

* **🔄 Real-time Monitoring**

  * Automatic price, review, and rating monitoring at regular intervals.
  * Cron jobs or scheduled scrapes for up-to-date insights.

* **📁 Search & History**

  * Maintains user interaction logs and previously searched ASINs.
  * Easy backtracking and re-analysis of previous research.

---

## 🧰 Technologies Used

* **Flutter (Desktop App UI)**
* **Python (Web Scraper & Backend)**
* **PostgreSQL (Data Storage)**
* **Matplotlib / Plotly (Graphs & Charts)**
* **Regex + Cosine Similarity (Recommendation & Chatbot Engine)**

---

## 🚀 How It Works

1. **User enters an ASIN** → Data fetched using scrapers.
2. **Scraper collects** → Pricing, offers, BSR, reviews from Amazon.
3. **Data stored** in PostgreSQL → Visualized in UI with real-time updates.
4. **Recommendation engine** kicks in → Shows similar products.
5. **User explores** → Margins, offers, and historical analytics.

---

## 🧪 Screenshots


