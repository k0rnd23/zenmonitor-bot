import requests
from bs4 import BeautifulSoup
import logging
import re
from urllib.parse import quote_plus, urljoin
from config import USER_AGENT

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

def clean_price(price_str):
    """Removes non-numeric characters (except '.') and converts to float"""
    if not price_str:
        return None

    cleaned = re.sub(r'[^\d.]', '', str(price_str))
    try:
        if cleaned.count('.') > 1:
            parts = cleaned.split('.')
            cleaned = "".join(parts[:-1]) + "." + parts[-1]
        if not cleaned or cleaned == '.':
            return None

        return float(cleaned)
    except (ValueError, TypeError, IndexError):
        logger.warning(f"Could not convert price string to float: '{price_str}' -> '{cleaned}'")
        return None

def parse_time_remaining(time_str):
    """
    Parses strings like '3 days, 17 hours', '5 hours, 30 minutes', '45 minutes',
    '8 min 30 sec', '< 1 minute' into total remaining minutes (integer)
    Returns -1 if ended, None if parsing fails completely. Ignores seconds if minutes/hours/days are present
    """
    if not time_str:
        return None

    time_str_lower = str(time_str).lower().strip()

    if "minute" in time_str_lower and "<" in time_str_lower:
        return 0
    if "ended" in time_str_lower or "finished" in time_str_lower:
        return -1

    total_minutes = 0
    parsed_successfully = False

    try:


        days_matches = re.findall(r'(\d+)\s+day', time_str_lower)
        hours_matches = re.findall(r'(\d+)\s+hour', time_str_lower)

        minutes_matches = re.findall(r'(\d+)\s+(?:minute|min)', time_str_lower)

        if days_matches:
            total_minutes += sum(int(d) * 24 * 60 for d in days_matches)
            parsed_successfully = True
        if hours_matches:
            total_minutes += sum(int(h) * 60 for h in hours_matches)
            parsed_successfully = True
        if minutes_matches:
            total_minutes += sum(int(m) for m in minutes_matches)
            parsed_successfully = True
        if parsed_successfully:
            return total_minutes
        else:
            logger.warning(f"Could not parse any known time units (day/hour/min) from: '{time_str}'")
            return None

    except Exception as e:
        logger.error(f"Unexpected error parsing time string '{time_str}': {e}", exc_info=True)
        return None

def build_url(platform, query, sort_options=None):
    """Builds the correct ZenMarket URL"""
    try:
        encoded_query = quote_plus(query)
    except TypeError:
        logger.error(f"Invalid query type for URL encoding: {query}")
        return None

    base_urls = {
        'mercari': f"https://zenmarket.jp/en/mercari.aspx?q={encoded_query}",
        'rakuten': f"https://zenmarket.jp/en/rakuten.aspx?q={encoded_query}",
        'yahoo': f"https://zenmarket.jp/en/yahoo.aspx?q={encoded_query}"
    }
    if platform not in base_urls:
        logger.error(f"Invalid platform provided to build_url: {platform}")
        return None

    url = base_urls[platform]

    if sort_options:
        sort_options_str = str(sort_options).strip()
        if '=' in sort_options_str:
            url += f"&{sort_options_str}"
        elif platform == 'mercari' and sort_options_str == 'LaunchDate':
            url += "&sort=LaunchDate"

        elif platform == 'yahoo' and sort_options_str in ['new&order=desc', 'sort=endtime&order=asc']:
             url += f"&{sort_options_str}"
        else:
             logger.warning(f"Sort option '{sort_options_str}' provided but not specifically handled or validated for platform '{platform}'. Appending as is.")
             url += f"&{sort_options_str}"

    return url

def scrape_zenmarket(platform, query, sort_options=None):
    """
    Scrapes ZenMarket search results
    Returns a list of dictionaries: [{'name', 'price', 'url', 'image_url', 'minutes_left' (optional)}]
    Returns an empty list ([]) if no results found or on expected errors like HTTP 404/timeouts
    Returns None only on critical unexpected errors during setup/request/parsing
    """
    search_page_url = build_url(platform, query, sort_options)
    if not search_page_url:
        logger.error(f"Failed to build URL for platform={platform}, query={query}")
        return None
    base_url = "https://zenmarket.jp/"
    logger.info(f"Scraping URL: {search_page_url}")
    headers = {'User-Agent': USER_AGENT}

    try:
        response = requests.get(search_page_url, headers=headers, timeout=30)
        response.raise_for_status()
    except requests.exceptions.Timeout:
        logger.error(f"Timeout fetching URL {search_page_url}")
        return []
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            logger.warning(f"URL not found (404): {search_page_url}. Likely invalid query/params or page removed.")
            return []
        else:
            logger.error(f"HTTP error fetching URL {search_page_url}: {e}")
            return []
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch URL {search_page_url} due to RequestException: {e}")
        return []
    try:
        soup = BeautifulSoup(response.content, 'html.parser')
    except Exception as e:
        logger.error(f"Failed to parse HTML content from {search_page_url}: {e}", exc_info=True)
        return None

    results = []

    item_selector = ""
    name_selector = ""
    link_selector = ""
    name_link_selector = ""
    price_selector = ""
    img_selector = ""
    img_attribute = "src"
    time_selector = ""

    if platform == 'yahoo':
        item_selector = "div.yahoo-search-result"
        name_link_selector = "div.translate a.auction-url"
        price_selector = "div.auction-price span.amount"
        img_selector = "div.img-wrap img"
        time_selector = "div.col-md-7 div:has(> span.glyphicon-time)"

    elif platform == 'mercari':
        item_selector = "div.product"
        name_selector = "h3.item-title"
        link_selector = "a.product-link"
        price_selector = "div.price span.amount"
        img_selector = "div.img-wrap img"
        img_attribute = "src"

    elif platform == 'rakuten':
        item_selector = "div.product"
        name_selector = "h3.item-title"
        link_selector = "a.product-link"
        price_selector = "div.price span.amount"
        img_selector = "div.img-wrap img"
        img_attribute = "src"

    else:
        logger.error(f"Scraping logic not defined for platform: {platform}")
        return None

    items = soup.select(item_selector)
    if not items:
        logger.warning(f"No items found with selector '{item_selector}' on {search_page_url}. Checking for 'no results' message.")
        no_results_indicator = soup.find(text=re.compile("find any items matching|No results found", re.IGNORECASE))
        no_results_element = soup.select_one(".products-not-found-text, .search-results-empty,
        if no_results_indicator or no_results_element:
            logger.info(f"Search returned no results for query '{query}' on {platform}.")
            return []
        else:
            logger.error(f"Could not find item containers using selector '{item_selector}'. Website structure may have changed. Returning empty list for this cycle.")
            return []

    logger.info(f"Found {len(items)} potential items for {platform} query '{query}'.")

    for item_index, item in enumerate(items):
        name = "N/A"
        price = None
        item_url = None
        image_url = None
        minutes_left = None
        try:
            if platform == 'yahoo':
                name_link_element = item.select_one(name_link_selector)
                price_element = item.select_one(price_selector)
                img_element = item.select_one(img_selector)
                time_element = item.select_one(time_selector)
                if name_link_element:
                    name = name_link_element.get_text(strip=True)
                    raw_url = name_link_element.get('href')
                    if raw_url: item_url = urljoin(base_url, raw_url)
                else: logger.debug(f"Yahoo item {item_index}: Name/Link element not found with '{name_link_selector}'")
                if price_element:
                    price_str = price_element.get('data-jpy') or price_element.get_text(strip=True)
                    price = clean_price(price_str)
                    if price_str and price is None:
                        logger.debug(f"Yahoo item {item_index}: Found price element but failed to clean price string: '{price_str}'")
                else: logger.debug(f"Yahoo item {item_index}: Price element not found with '{price_selector}'")
                if img_element:
                    raw_image_url = img_element.get('data-src') or img_element.get('src')
                    if raw_image_url and not raw_image_url.startswith('data:image'):
                        image_url = urljoin(base_url, raw_image_url)
                else: logger.debug(f"Yahoo item {item_index}: Image element not found with '{img_selector}'")
                if time_element:
                    time_str = time_element.get_text(separator=' ', strip=True)
                    minutes_left = parse_time_remaining(time_str)
                else:
                    logger.debug(f"Yahoo item {item_index}: Time element not found using selector '{time_selector}'")

            elif platform == 'mercari' or platform == 'rakuten':
                link_element = item.select_one(link_selector)
                name_element = item.select_one(name_selector)
                price_element = item.select_one(price_selector)
                img_element = item.select_one(img_selector)
                if link_element:
                     raw_url = link_element.get('href')
                     if raw_url: item_url = urljoin(base_url, raw_url)
                else: logger.debug(f"{platform.capitalize()} item {item_index}: Link element not found with '{link_selector}'")
                if name_element:
                    name = name_element.get_text(strip=True)

                    title_attr = name_element.get('title')

                    generic_prefixes = [
                         "Baby and Kids Toys, Educational toys", "Games / Toys / Goods, Character goods",
                         "Toys, Hobbies & Games, Figures", "Comics, Anime", "Other",
                         "Search results",
                    ]

                    if name in generic_prefixes and title_attr and len(title_attr) > len(name):
                        logger.debug(f"{platform.capitalize()} item {item_index}: Using title attribute '{title_attr}' instead of generic name '{name}'")
                        name = title_attr.strip()
                    elif not name and title_attr:
                         logger.debug(f"{platform.capitalize()} item {item_index}: Using title attribute '{title_attr}' as name was empty")
                         name = title_attr.strip()
                else: logger.debug(f"{platform.capitalize()} item {item_index}: Name element not found with '{name_selector}'")
                if price_element:
                    price_str = price_element.get('data-jpy') or price_element.get_text(strip=True)
                    price = clean_price(price_str)
                    if price_str and price is None:
                        logger.debug(f"{platform.capitalize()} item {item_index}: Found price element but failed to clean price string: '{price_str}'")
                else: logger.debug(f"{platform.capitalize()} item {item_index}: Price element not found with '{price_selector}'")
                if img_element:
                    raw_image_url = img_element.get(img_attribute) or img_element.get('src')
                    if raw_image_url and not raw_image_url.startswith('data:image'):
                        image_url = urljoin(base_url, raw_image_url)
                else: logger.debug(f"{platform.capitalize()} item {item_index}: Image element not found with '{img_selector}'")
            if name and name != "N/A" and price is not None and item_url:
                item_data = {
                    'name': name,
                    'price': price,
                    'url': item_url,
                    'image_url': image_url
                }
                if minutes_left is not None:
                    item_data['minutes_left'] = minutes_left
                results.append(item_data)
            elif item_url:
                 logger.debug(f"Skipping item {item_index} due to missing essential data: Name='{name}', Price={price}, URL='{item_url}'")
        except Exception as e:
            logger.error(f"Error processing item details for item index {item_index} on page {search_page_url}: {e}. Skipping item.", exc_info=True)
            continue
    logger.info(f"Successfully parsed {len(results)} items with valid data for {platform} query '{query}'.")
    return results
