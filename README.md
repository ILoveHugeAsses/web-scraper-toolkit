# Web Scraper Toolkit

Production-grade web scraping tools with anti-detection, rate limiting, and robust error handling.

## Features

- **Reddit Scraper**: Mass scraping from multiple subreddits with intelligent rate limiting
- **Market Scraper**: E-commerce product data extraction (BIM, A101, etc.)
- **Anti-ban protection**: User-agent rotation, request delays, cookie persistence
- **JSON export**: Clean, structured output ready for analysis

## Tools

### 1. Reddit Scraper
```bash
python reddit_scraper.py
```
- Scrapes posts from configured subreddits
- Automatic rate limiting (2 requests/second)
- Filters by score, age, keywords
- Deduplication to avoid re-scraping
- Output: `reddit_data.json`

### 2. Market Scraper
```bash
python market_scraper.py
```
- Scrapes product data from Turkish supermarket chains
- BIM scraper implemented (A101, Migros coming soon)
- Extracts: product name, price, image, link
- Output: `market_data.json`

## Requirements

```bash
pip install -r requirements.txt
```

## Configuration

### Reddit API Credentials
Create a `.env` file:
```
REDDIT_CLIENT_ID=your_client_id
REDDIT_CLIENT_SECRET=your_secret
REDDIT_USERNAME=your_username
REDDIT_PASSWORD=your_password
```

### Market Scraper
Edit target URLs in `market_scraper.py`:
```python
BIM_URLS = [
    "https://www.bim.com.tr/kategoriler/...",
]
```

## Usage Examples

### Reddit Scraper
```python
from reddit_scraper import RedditScraper

scraper = RedditScraper(
    subreddits=["python", "webdev"],
    min_score=10,
    max_age_days=7
)
data = scraper.scrape()
scraper.save_json(data, "output.json")
```

### Market Scraper
```python
from market_scraper import MarketScraper

scraper = MarketScraper(store="bim")
products = scraper.scrape()
print(f"Scraped {len(products)} products")
```

## Anti-Detection Features

- Random user-agent rotation
- Request delay randomization (1-3 seconds)
- Cookie jar persistence
- Retry logic with exponential backoff
- Cloudflare bypass (via cloudscraper)

## Output Format

### Reddit Data
```json
{
  "subreddit": "python",
  "title": "Post title",
  "author": "username",
  "score": 150,
  "url": "https://...",
  "created_utc": 1234567890,
  "num_comments": 25,
  "selftext": "Post body..."
}
```

### Market Data
```json
{
  "store": "BIM",
  "product_name": "Product Name",
  "price": "12.95 TL",
  "image_url": "https://...",
  "product_url": "https://..."
}
```

## Legal Notice

This toolkit is for educational and legitimate business use only. Always:
- Respect robots.txt
- Follow platform terms of service
- Implement rate limiting
- Don't overload servers

## License

MIT

## Author

v0id-lab
