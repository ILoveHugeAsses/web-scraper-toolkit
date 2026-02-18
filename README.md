# Web Scraper Toolkit

Production-ready web scraping framework with async support, proxy rotation, and anti-detection.

## Features
- Async scraping with `aiohttp` and `Playwright`
- Automatic proxy rotation
- Rate limiting and retry logic
- Anti-bot detection bypass
- Export to CSV, JSON, Excel
- Resume support for large scrapes

## Quick Start

```python
from scraper import Scraper

s = Scraper(proxy_rotation=True, rate_limit=2.0)
data = await s.scrape("https://example.com/products", selectors={
    "title": "h2.product-title",
    "price": "span.price",
    "rating": "div.rating"
})
s.export(data, "products.csv")
```

## Installation

```bash
pip install -r requirements.txt
playwright install chromium
```

## Use Cases
- E-commerce price monitoring
- Real estate listings
- Job board aggregation
- Social media data collection
- Academic research datasets

## License
MIT
