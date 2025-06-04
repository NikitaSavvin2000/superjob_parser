import os
import re
import tempfile
import pandas as pd
from tqdm import tqdm
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import cycle
import threading
import logging
import shutil
import requests
import time
import uuid
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from vacancy_content_parser import parse_job_info

# Configuration
MAX_WORKERS = 10
USE_PROXY = True
PROXY_UPDATE_INTERVAL = 300
LINK_COUNT = 0
CURRENT_DF_LEN = 0
TIMEOUT_PER_LINK = 120
MAX_RETRIES = 3
RETRY_BACKOFF = 2
MAX_PAGE_RETRIES = 3  # New constant for pagination retries

# Proxy pool
proxy_pool = []
proxy_lock = threading.Lock()
last_proxy_update = 0

# Logging setup with thread name
def setup_logger(log_path: str, to_console: bool) -> logging.Logger:
    logger = logging.getLogger('vacancy_parser')
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(threadName)s - %(levelname)s - %(message)s')

    logger.handlers = []
    file_handler = logging.FileHandler(log_path, encoding='utf-8')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    if to_console:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger

# Paths
cwd = os.getcwd()
path_to_save_result = os.path.join(cwd, "src", "superjob", "results")
path_to_save_logs = os.path.join(cwd, "src", "superjob", "logs")
os.makedirs(path_to_save_result, exist_ok=True)
os.makedirs(path_to_save_logs, exist_ok=True)

vacancy_path = os.path.join(path_to_save_result, "vacancy.csv")
log_path = os.path.join(path_to_save_logs, "vacancy_parser.log")
result_file = os.path.join(path_to_save_result, "vacancy_links.csv")
progress_file = os.path.join(path_to_save_result, "progress_links.csv")

logger = setup_logger(log_path=log_path, to_console=True)

def validate_proxy(proxy: str) -> bool:
    """Validate a proxy by attempting a simple request."""
    if not proxy:
        return False
    try:
        proxies = {"https": f"http://{proxy}"}
        response = requests.get("https://www.google.com", proxies=proxies, timeout=5)
        logger.debug(f"SUCCESS Proxy {proxy} is good")
        return response.ok
    except Exception as e:
        logger.debug(f"Proxy {proxy} validation failed")
        return False

def update_proxies():
    logger.info(f"STAGE: update_proxies")
    global proxy_pool, last_proxy_update
    try:
        key = os.getenv("KEY")
        proxy_url = f"https://api.best-proxies.ru/proxylist.csv?key={key}&type=https&limit=1000"
        proxy_path = os.path.join(cwd, "src", "proxylist.csv")
        r = requests.get(proxy_url, timeout=10)
        if r.ok:
            with open(proxy_path, "wb") as f:
                f.write(r.content)
            df = pd.read_csv(proxy_path, sep=";", encoding="cp1251")
            df = df.sort_values(by="good checks", ascending=False).head(100)
            proxies = [f"{ip}:{port}" for ip, port in zip(df["ip"], df["port"])]
            logger.info(f"STAGE: validate_proxy")
            valid_proxies = [p for p in proxies if validate_proxy(p)]
            if valid_proxies:
                with proxy_lock:
                    proxy_pool = valid_proxies
                    last_proxy_update = time.time()
                logger.info(f"Updated proxy pool with {len(valid_proxies)} valid proxies")
            else:
                logger.warning("No valid proxies retrieved, using no proxy")
                proxy_pool = [None]
        else:
            logger.error(f"Failed to fetch proxies: HTTP {r.status_code}")
            proxy_pool = [None]
    except Exception as e:
        logger.error(f"Error updating proxies: {e}")
        proxy_pool = [None]
    return cycle(proxy_pool)

def get_proxy():
    logger.info(f"STAGE: get_proxy")
    global proxy_pool, last_proxy_update
    with proxy_lock:
        if not proxy_pool or (time.time() - last_proxy_update) > PROXY_UPDATE_INTERVAL:
            logger.info("Refreshing proxy pool")
            return next(update_proxies())
        return next(cycle(proxy_pool))

def create_driver(proxy=None):
    logger.info(f"STAGE: create_driver")
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")

    prefs = {
        "profile.managed_default_content_settings.images": 2,
        "profile.managed_default_content_settings.fonts": 2
    }
    options.add_experimental_option("prefs", prefs)

    if proxy and USE_PROXY:
        options.add_argument(f'--proxy-server=http://{proxy}')
        logger.info(f"Created driver with proxy: {proxy}")
    else:
        logger.info("Created driver without proxy")

    user_data_dir = os.path.join(tempfile.gettempdir(), f"chrome_profile_{uuid.uuid4()}")
    options.add_argument(f"--user-data-dir={user_data_dir}")

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    driver.implicitly_wait(1)
    return driver, user_data_dir

link_count_lock = threading.Lock()
vacancy_data_lock = threading.Lock()

def save_vacancy_data(data, link):
    logger.info(f"STAGE: save_vacancy_data path = {vacancy_path}")
    global CURRENT_DF_LEN
    with vacancy_data_lock:
        if data and isinstance(data, dict) and any(data.values()):
            data_with_link = {**data, 'link': link}
            df = pd.DataFrame([data_with_link])
            try:
                if os.path.exists(vacancy_path):
                    df.to_csv(vacancy_path, mode='a', index=False, header=False)
                else:
                    df.to_csv(vacancy_path, index=False)
                CURRENT_DF_LEN += 1
                logger.info(f"Saved vacancy data for {link}, total: {CURRENT_DF_LEN}")
            except Exception as e:
                logger.error(f"Failed to write to CSV {vacancy_path}: {e}")
        else:
            logger.warning(f"Skipping invalid or empty data for {link}")

def try_process_link(link, proxy):
    logger.info(f"STAGE: try_process_link")
    global LINK_COUNT
    all_links = set()
    start_time = time.time()
    driver = None
    user_data_dir = None

    try:
        driver, user_data_dir = create_driver(proxy)
        # Get max pages
        for attempt in range(MAX_RETRIES):
            try:
                max_page = None
                current_proxy = proxy
                for page_attempt in range(MAX_PAGE_RETRIES):
                    try:
                        logger.info(f"Attempt {page_attempt + 1} to parse max page for {link} with proxy {current_proxy}")
                        driver.get(link)
                        WebDriverWait(driver, 10).until(
                            EC.presence_of_element_located((By.XPATH, "//*[contains(text(), 'Дальше')]"))
                        )
                        elem = driver.find_element(By.XPATH, "//*[contains(text(), 'Дальше')]")
                        parent_div = elem.find_element(By.XPATH, "./ancestor::div[1]")
                        titles = parent_div.find_elements(By.XPATH, ".//a[@title]")
                        numbers = [int(t.get_attribute("title")) for t in titles if t.get_attribute("title").isdigit()]
                        max_page = max(numbers) if numbers else 1
                        logger.info(f"Found max page {max_page} for {link}")
                        break
                    except (TimeoutException, NoSuchElementException, Exception) as e:
                        logger.error(f"Failed to parse max page for {link}, attempt {page_attempt + 1}: {e}")
                        if page_attempt < MAX_PAGE_RETRIES - 1:
                            time.sleep(RETRY_BACKOFF * (2 ** page_attempt))
                            current_proxy = get_proxy()
                            if driver:
                                driver.quit()
                                if user_data_dir:
                                    shutil.rmtree(user_data_dir, ignore_errors=True)
                            driver, user_data_dir = create_driver(current_proxy)
                        else:
                            logger.warning(f"Exhausted max page retries for {link}, defaulting to max_page = 1")
                            max_page = 1
                if max_page is None:
                    logger.error(f"Failed to determine max page for {link} after retries")
                    return []
                break
            except Exception as e:
                logger.error(f"Attempt {attempt + 1}: Failed to fetch {link}: {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_BACKOFF * (2 ** attempt))
                    proxy = get_proxy()
                    if driver:
                        driver.quit()
                        if user_data_dir:
                            shutil.rmtree(user_data_dir, ignore_errors=True)
                    driver, user_data_dir = create_driver(proxy)
                else:
                    logger.error(f"Exhausted retries for {link}")
                    return []

        # Process each page
        for page in range(1, max_page + 1):
            page_url = f"{link}?page={page}"
            for attempt in range(MAX_RETRIES):
                try:
                    logger.info(f"Page {page}, attempt {attempt + 1}: Processing {page_url}")
                    driver.get(page_url)
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, 'a[href*="/vakansii/"]'))
                    )
                    vacancy_links = driver.find_elements(By.CSS_SELECTOR, 'a[href*="/vakansii/"]')
                    hrefs = [a.get_attribute('href') for a in vacancy_links if a.get_attribute('href')]
                    filtered = [h for h in set(hrefs) if re.search(r'\d+\.html$', h)]
                    logger.info(f"Page {page}: Found {len(filtered)} vacancy links")

                    if not filtered:
                        logger.warning(f"Page {page}: No vacancies found")
                        break

                    for vacancy_link in filtered:
                        for link_attempt in range(MAX_RETRIES):
                            try:
                                link_content = parse_job_info(driver, vacancy_link)
                                if link_content and isinstance(link_content, dict) and any(link_content.values()):
                                    save_vacancy_data(link_content, vacancy_link)
                                    logger.info(f"SUCCESS PARSED vacancy {vacancy_link}")
                                else:
                                    logger.warning(f"Page {page}: No valid content for {vacancy_link}")
                                break
                            except Exception as e:
                                logger.error(f"Page {page}, link attempt {link_attempt + 1}: Failed to parse {vacancy_link}: {e}")
                                if link_attempt < MAX_RETRIES - 1:
                                    time.sleep(RETRY_BACKOFF * (2 ** link_attempt))
                                else:
                                    logger.warning(f"Page {page}: Skipped {vacancy_link} after retries")
                        all_links.add(vacancy_link)
                    break
                except (TimeoutException, NoSuchElementException) as e:
                    logger.error(f"Page {page}, attempt {attempt + 1}: Failed to process {page_url}: {e}")
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(RETRY_BACKOFF * (2 ** attempt))
                        proxy = get_proxy()
                        if driver:
                            driver.quit()
                            if user_data_dir:
                                shutil.rmtree(user_data_dir, ignore_errors=True)
                        driver, user_data_dir = create_driver(proxy)
                    else:
                        logger.warning(f"Page {page}: Skipped after retries")
                        break

        with link_count_lock:
            LINK_COUNT += len(all_links)
        logger.info(f"Completed {link}, collected {len(all_links)} vacancy links")

    except Exception as e:
        logger.error(f"Unexpected error processing {link}: {e}")
    finally:
        if driver:
            driver.quit()
            if user_data_dir:
                shutil.rmtree(user_data_dir, ignore_errors=True)

    if not all_links:
        logger.error(f"No vacancies collected for {link}")
    return list(all_links)

def read_existing_links():
    logger.info(f"STAGE: read_existing_links")
    if os.path.exists(result_file):
        df = pd.read_csv(result_file)
        return set(df['vacancy_links'].dropna().tolist())
    return set()

def append_links_to_csv(links):
    logger.info(f"STAGE: append_links_to_csv")
    existing = read_existing_links()
    unique = list(set(links) - existing)
    if unique:
        df = pd.DataFrame(unique, columns=['vacancy_links'])
        df.to_csv(result_file, mode='a', header=not os.path.exists(result_file), index=False)
        logger.info(f"Appended {len(unique)} new vacancy links to {result_file}")
    else:
        logger.info("No new links to append")

def read_progress():
    logger.info(f"STAGE: read_progress")
    if os.path.exists(progress_file):
        df = pd.read_csv(progress_file)
        if not df.empty:
            return df['link'].tolist()
    return []

def save_progress(processed_links):
    logger.info(f"STAGE: save_progress")
    df = pd.DataFrame(processed_links, columns=['link'])
    df.to_csv(progress_file, index=False)
    logger.info(f"Saved progress for {len(processed_links)} processed links")

def process_level_0_link(link):
    logger.info(f"STAGE: process_level_0_link")
    proxy = get_proxy()
    return link, try_process_link(link, proxy)

def main():
    global proxy_pool
    proxy_pool = update_proxies()

    links_df = pd.read_csv(os.path.join(path_to_save_result, "level_0_links.csv"))
    level_0_links = links_df["level_0_link"].dropna().tolist()
    processed_links = read_progress()
    to_process = [l for l in level_0_links if l not in processed_links]

    all_results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_level_0_link, link): link for link in to_process}
        for future in tqdm(as_completed(futures), total=len(futures), desc="Processing level 0 links"):
            link, links = future.result()
            all_results.extend(links)
            processed_links.append(link)
            save_progress(processed_links)
            logger.info(f"Processed level 0 link {link}, collected {len(links)} vacancy links")

    append_links_to_csv(all_results)
    logger.info(f"Total vacancy links scraped: {LINK_COUNT}, vacancy.csv length: {CURRENT_DF_LEN}")

if __name__ == '__main__':
    main()