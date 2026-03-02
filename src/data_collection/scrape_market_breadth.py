"""
Scrape market breadth indicators from barchart.com using Selenium.

Fetches:
- NCFD: % of Nasdaq stocks above their 5-day average
- MMFI: % of market stocks above their 50-day moving average
"""

import re
import time
from typing import Optional, Dict
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

from config.settings import CONFIG

NCFD_URL = CONFIG["market_breadth"]["ncfd_url"]
MMFI_URL = CONFIG["market_breadth"]["mmfi_url"]


def get_headless_chrome_driver():
    """Create a headless Chrome WebDriver instance."""
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument(f"user-agent={CONFIG['market_breadth']['user_agent']}")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    return driver


def scrape_barchart_value_selenium(url: str) -> Optional[float]:
    """Scrape a value from barchart.com using Selenium."""
    driver = None
    try:
        driver = get_headless_chrome_driver()
        driver.get(url)

        time.sleep(3)

        selectors = [
            "span.last-change",
            "span[data-ng-bind*='lastPrice']",
            "div.price-value",
            "span.last",
            "div.symbol-price-quote",
            "span.market-price"
        ]

        for selector in selectors:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                for element in elements:
                    text = element.text.strip()
                    if text:
                        match = re.search(r'(\d+\.?\d*)', text)
                        if match and match.group(1) != '.':
                            value = float(match.group(1))
                            print(f"Found value {value} from selector: {selector}")
                            return value
            except Exception:
                continue

        page_source = driver.page_source

        patterns = [
            r'Last[:\s]+(\d+\.?\d+)',
            r'Price[:\s]+(\d+\.?\d+)',
            r'>(\d+\.\d+)%?<',
            r'lastPrice["\']:\s*(\d+\.?\d+)',
        ]

        for pattern in patterns:
            match = re.search(pattern, page_source)
            if match:
                try:
                    value = float(match.group(1))
                    if 0 <= value <= 100:
                        print(f"Found value {value} from pattern: {pattern}")
                        return value
                except ValueError:
                    continue

        print(f"Could not find value in {url}")
        return None

    except Exception as e:
        print(f"Error scraping {url}: {e}")
        return None
    finally:
        if driver:
            driver.quit()


def get_market_breadth() -> Dict[str, Optional[float]]:
    """Get current market breadth indicators using Selenium."""
    print("Fetching market breadth indicators with Selenium...")

    ncfd = scrape_barchart_value_selenium(NCFD_URL)
    mmfi = scrape_barchart_value_selenium(MMFI_URL)

    result = {
        'ncfd': ncfd,
        'mmfi': mmfi,
        'timestamp': datetime.now().isoformat()
    }

    print(f"NCFD: {ncfd}%, MMFI: {mmfi}%")

    return result


if __name__ == '__main__':
    breadth = get_market_breadth()
    print(f"\nMarket Breadth (as of {breadth['timestamp']}):")
    print(f"  NCFD (Nasdaq above 5-day avg): {breadth['ncfd']}%")
    print(f"  MMFI (Stocks above 50-day avg): {breadth['mmfi']}%")
