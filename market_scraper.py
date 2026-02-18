#!/usr/bin/env python3
"""
MARKET INDIRIM SCRAPER v1.0 (MVP)
A101, BIM, SOK haftalik katalog scraper

Strateji:
1. Third-party aggregator (aktuel-urunler.com) - PRIMARY
2. Direkt site scraping (SOK > BIM > A101) - FALLBACK
3. Anti-ban: CloudScraper, delays, UA rotation

Author: Durin
Date: 2026-02-17
"""

import asyncio
import aiohttp
import aiofiles
import json
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import random
from bs4 import BeautifulSoup
import re

# ============================================================================
# CONFIGURATION
# ============================================================================

CONFIG = {
    # Output settings
    "output_file": "market_deals.json",
    "log_file": "market_scraper.log",

    # Scraping settings
    "timeout": 30,
    "max_retries": 3,
    "base_delay": (2, 5),  # Random delay range in seconds

    # Anti-ban
    "user_agents": [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/120.0.0.0 Safari/537.36",
    ],

    # Markets
    "markets": ["a101", "bim", "sok"],

    # Aggregator URLs (PRIMARY METHOD)
    "aggregator_urls": {
        "aktuel-urunler": "https://aktuel-urunler.com",
        "aktuelbul": "https://www.aktuelbul.com",
    },

    # Direct URLs (FALLBACK)
    "direct_urls": {
        "a101": "https://www.a101.com.tr/afisler",
        "bim": "https://www.bim.com.tr/Categories/100/aktuel-urunler.aspx",
        "sok": "https://kurumsal.sokmarket.com.tr/haftanin-firsatlari/firsatlar",
    },
}

# ============================================================================
# LOGGING SETUP
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(CONFIG["log_file"], encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_random_headers() -> Dict[str, str]:
    """Generate random HTTP headers for anti-bot evasion"""
    return {
        "User-Agent": random.choice(CONFIG["user_agents"]),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Cache-Control": "max-age=0",
    }

async def random_delay():
    """Random delay between requests"""
    delay = random.uniform(*CONFIG["base_delay"])
    logger.debug(f"Waiting {delay:.2f}s before next request")
    await asyncio.sleep(delay)

async def fetch_html(session: aiohttp.ClientSession, url: str, retries: int = 0) -> Optional[str]:
    """Fetch HTML content with retry logic"""
    try:
        await random_delay()
        async with session.get(url, headers=get_random_headers(), timeout=CONFIG["timeout"]) as response:
            if response.status == 200:
                logger.info(f"✅ Fetched: {url}")
                return await response.text()
            elif response.status == 429:
                logger.warning(f"⚠️ Rate limited (429) on {url}")
                if retries < CONFIG["max_retries"]:
                    wait_time = (retries + 1) * 10
                    logger.info(f"Retrying in {wait_time}s...")
                    await asyncio.sleep(wait_time)
                    return await fetch_html(session, url, retries + 1)
            elif response.status == 403:
                logger.warning(f"⚠️ CloudFlare block (403) on {url}")
            else:
                logger.error(f"❌ HTTP {response.status} for {url}")
    except asyncio.TimeoutError:
        logger.error(f"❌ Timeout fetching {url}")
        if retries < CONFIG["max_retries"]:
            return await fetch_html(session, url, retries + 1)
    except Exception as e:
        logger.error(f"❌ Error fetching {url}: {str(e)}")

    return None

# ============================================================================
# AGGREGATOR SCRAPERS
# ============================================================================

async def scrape_aktuel_urunler(session: aiohttp.ClientSession) -> List[Dict]:
    """
    Scrape aktuel-urunler.com (PRIMARY METHOD)
    This site aggregates all markets - easier to scrape
    """
    deals = []
    base_url = CONFIG["aggregator_urls"]["aktuel-urunler"]

    for market in CONFIG["markets"]:
        url = f"{base_url}/{market}-aktuel-urunler/"
        logger.info(f"📦 Scraping {market.upper()} from aktuel-urunler.com")

        html = await fetch_html(session, url)
        if not html:
            logger.warning(f"⚠️ Failed to fetch {market} from aggregator")
            continue

        soup = BeautifulSoup(html, 'html.parser')

        # Parse product cards (this is a PLACEHOLDER - actual selectors need inspection)
        # Different aggregators have different HTML structures
        product_cards = soup.find_all(['div', 'article'], class_=re.compile(r'product|item|card|aktuel', re.I))

        for card in product_cards[:50]:  # Limit to 50 products per market
            try:
                # Extract data (PLACEHOLDER - needs real selectors)
                title = card.find(['h2', 'h3', 'h4', 'span'], class_=re.compile(r'title|name|product', re.I))
                price = card.find(['span', 'div'], class_=re.compile(r'price|fiyat', re.I))
                img = card.find('img')

                if title and price:
                    deal = {
                        "market": market.upper(),
                        "title": title.get_text(strip=True),
                        "price": price.get_text(strip=True),
                        "image": img.get('src') if img else None,
                        "scraped_at": datetime.now().isoformat(),
                        "source": "aktuel-urunler.com",
                        "url": url,
                    }
                    deals.append(deal)
            except Exception as e:
                logger.debug(f"Failed to parse product card: {e}")
                continue

        logger.info(f"✅ Found {len([d for d in deals if d['market'] == market.upper()])} deals for {market.upper()}")

    return deals

# ============================================================================
# DIRECT SITE SCRAPERS (FALLBACK)
# ============================================================================

async def scrape_sok_direct(session: aiohttp.ClientSession) -> List[Dict]:
    """
    Scrape SOK directly (EASIEST direct scraping)
    """
    deals = []
    url = CONFIG["direct_urls"]["sok"]
    logger.info(f"📦 Scraping SOK directly from {url}")

    html = await fetch_html(session, url)
    if not html:
        return deals

    soup = BeautifulSoup(html, 'html.parser')

    # SOK product selectors (PLACEHOLDER - needs real inspection)
    product_cards = soup.find_all(['div', 'article'], class_=re.compile(r'product|urun|item|firsat', re.I))

    for card in product_cards[:50]:
        try:
            title = card.find(['h2', 'h3', 'h4', 'span'], class_=re.compile(r'title|name|baslik', re.I))
            price = card.find(['span', 'div'], class_=re.compile(r'price|fiyat', re.I))
            img = card.find('img')

            if title and price:
                deal = {
                    "market": "SOK",
                    "title": title.get_text(strip=True),
                    "price": price.get_text(strip=True),
                    "image": img.get('src') if img else None,
                    "scraped_at": datetime.now().isoformat(),
                    "source": "kurumsal.sokmarket.com.tr",
                    "url": url,
                }
                deals.append(deal)
        except Exception as e:
            logger.debug(f"Failed to parse SOK product: {e}")
            continue

    logger.info(f"✅ Found {len(deals)} deals from SOK direct")
    return deals

async def scrape_bim_direct(session: aiohttp.ClientSession) -> List[Dict]:
    """
    Scrape BIM directly (MEDIUM difficulty)
    REAL SELECTORS: div.product.big > h3, div.priceArea, div.imageArea img
    """
    deals = []
    url = CONFIG["direct_urls"]["bim"]
    logger.info(f"📦 Scraping BIM directly from {url}")

    html = await fetch_html(session, url)
    if not html:
        return deals

    soup = BeautifulSoup(html, 'html.parser')

    # BIM REAL selectors (verified 2026-02-17)
    product_cards = soup.find_all('div', class_='product')
    logger.info(f"Found {len(product_cards)} product cards in BIM HTML")

    for card in product_cards[:100]:  # Increased limit
        try:
            # Title: <h2 class="title"> (NOT h3!)
            title_tag = card.find('h2', class_='title')
            title = title_tag.get_text(strip=True) if title_tag else None

            # Price: div.buttonArea > a.gButton > div.text.quantify + div.kusurArea + span.curr
            button_area = card.find('div', class_='buttonArea')
            if button_area:
                gbutton = button_area.find('a', class_='gButton')
                if gbutton:
                    # Price parts: "14.900," + "00" + "₺"
                    quantify = gbutton.find('div', class_='text')
                    kusur = gbutton.find('div', class_='kusurArea')
                    curr = gbutton.find('span', class_='curr')

                    price_parts = []
                    if quantify:
                        price_parts.append(quantify.get_text(strip=True))
                    if kusur:
                        price_parts.append(kusur.get_text(strip=True))
                    if curr:
                        price_parts.append(curr.get_text(strip=True))
                    price = ''.join(price_parts) if price_parts else None
                else:
                    price = None
            else:
                price = None

            # Image: div.imageArea > a > div.image > img
            image_area = card.find('div', class_='imageArea')
            img_tag = image_area.find('img') if image_area else None
            image = img_tag.get('src') if img_tag else None

            # Only save if we have at least title AND price
            if title and price:
                deal = {
                    "market": "BIM",
                    "title": title,
                    "price": price,
                    "image": image,
                    "scraped_at": datetime.now().isoformat(),
                    "source": "bim.com.tr",
                    "url": url,
                }
                deals.append(deal)
        except Exception as e:
            logger.warning(f"Failed to parse BIM product: {e}")
            continue

    logger.info(f"✅ Found {len(deals)} deals from BIM direct")
    return deals

async def scrape_a101_direct(session: aiohttp.ClientSession) -> List[Dict]:
    """
    Scrape A101 directly (HARDEST - CloudFlare protected)
    NOTE: This will likely fail without CloudScraper
    """
    deals = []
    url = CONFIG["direct_urls"]["a101"]
    logger.info(f"📦 Scraping A101 directly from {url} (WARNING: CloudFlare protected)")

    html = await fetch_html(session, url)
    if not html:
        logger.warning("❌ A101 scraping failed (likely CloudFlare block)")
        return deals

    soup = BeautifulSoup(html, 'html.parser')

    # A101 product selectors (PLACEHOLDER)
    product_cards = soup.find_all(['div', 'article'], class_=re.compile(r'product|urun|item|afis', re.I))

    for card in product_cards[:50]:
        try:
            title = card.find(['h2', 'h3', 'h4', 'span'], class_=re.compile(r'title|name|baslik', re.I))
            price = card.find(['span', 'div'], class_=re.compile(r'price|fiyat', re.I))
            img = card.find('img')

            if title and price:
                deal = {
                    "market": "A101",
                    "title": title.get_text(strip=True),
                    "price": price.get_text(strip=True),
                    "image": img.get('src') if img else None,
                    "scraped_at": datetime.now().isoformat(),
                    "source": "a101.com.tr",
                    "url": url,
                }
                deals.append(deal)
        except Exception as e:
            logger.debug(f"Failed to parse A101 product: {e}")
            continue

    logger.info(f"✅ Found {len(deals)} deals from A101 direct")
    return deals

# ============================================================================
# MAIN SCRAPER ORCHESTRATOR
# ============================================================================

async def scrape_all_markets() -> Dict:
    """
    Main scraper - tries aggregator first, falls back to direct scraping
    """
    start_time = datetime.now()
    logger.info("=" * 60)
    logger.info(f"🚀 MARKET SCRAPER v1.0 - Started at {start_time}")
    logger.info("=" * 60)

    all_deals = []

    async with aiohttp.ClientSession() as session:
        # STRATEGY 1: Try aggregator first (RECOMMENDED)
        logger.info("📡 STRATEGY 1: Aggregator scraping (aktuel-urunler.com)")
        aggregator_deals = await scrape_aktuel_urunler(session)
        all_deals.extend(aggregator_deals)

        # STRATEGY 2: Direct scraping as fallback (if aggregator fails)
        if len(aggregator_deals) == 0:
            logger.warning("⚠️ Aggregator failed, falling back to direct scraping")

            # SOK (easiest)
            sok_deals = await scrape_sok_direct(session)
            all_deals.extend(sok_deals)

            # BIM (medium)
            bim_deals = await scrape_bim_direct(session)
            all_deals.extend(bim_deals)

            # A101 (hardest - will likely fail)
            a101_deals = await scrape_a101_direct(session)
            all_deals.extend(a101_deals)
        else:
            logger.info(f"✅ Aggregator successful, skipping direct scraping")

    # Calculate stats
    duration = (datetime.now() - start_time).total_seconds()

    stats = {
        "total_deals": len(all_deals),
        "by_market": {
            "A101": len([d for d in all_deals if d["market"] == "A101"]),
            "BIM": len([d for d in all_deals if d["market"] == "BIM"]),
            "SOK": len([d for d in all_deals if d["market"] == "SOK"]),
        },
        "scraped_at": start_time.isoformat(),
        "duration_seconds": duration,
    }

    result = {
        "metadata": stats,
        "deals": all_deals,
    }

    logger.info("=" * 60)
    logger.info(f"✅ SCRAPING COMPLETE")
    logger.info(f"Total deals: {stats['total_deals']}")
    logger.info(f"A101: {stats['by_market']['A101']}, BIM: {stats['by_market']['BIM']}, SOK: {stats['by_market']['SOK']}")
    logger.info(f"Duration: {duration:.2f}s")
    logger.info("=" * 60)

    return result

# ============================================================================
# SAVE RESULTS
# ============================================================================

async def save_results(data: Dict):
    """Save results to JSON file"""
    async with aiofiles.open(CONFIG["output_file"], 'w', encoding='utf-8') as f:
        await f.write(json.dumps(data, indent=2, ensure_ascii=False))
    logger.info(f"💾 Results saved to {CONFIG['output_file']}")

# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

async def main():
    """Main entry point"""
    try:
        # Scrape all markets
        results = await scrape_all_markets()

        # Save results
        await save_results(results)

        # Print summary
        print("\n" + "=" * 60)
        print("📊 SCRAPING SUMMARY")
        print("=" * 60)
        print(f"Total deals found: {results['metadata']['total_deals']}")
        print(f"A101: {results['metadata']['by_market']['A101']}")
        print(f"BIM: {results['metadata']['by_market']['BIM']}")
        print(f"SOK: {results['metadata']['by_market']['SOK']}")
        print(f"Duration: {results['metadata']['duration_seconds']:.2f}s")
        print(f"Output: {CONFIG['output_file']}")
        print(f"Log: {CONFIG['log_file']}")
        print("=" * 60)

    except Exception as e:
        logger.error(f"❌ Fatal error: {str(e)}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
