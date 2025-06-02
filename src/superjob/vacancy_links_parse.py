import os
import re
import tempfile
import pandas as pd
from tqdm import tqdm
from selenium.webdriver.common.by import By
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import cycle
import threading
import logging
import shutil
import requests
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from vacancy_content_parser import parse_job_info

# Configuration
MAX_WORKERS = 4
USE_PROXY = True
PROXY_UPDATE_INTERVAL = 600
BATCH_SIZE = 50
LINK_COUNT = 0
CURRENT_DF_LEN = 0
TIMEOUT_PER_LINK = 600

# Proxy pool
proxy_pool = []
proxy_lock = threading.Lock()
last_proxy_update = 0

# Logging setup with thread name
def setup_logger(log_path: str, to_console: bool) -> logging.Logger:
    logger = logging.getLogger('vacancy_parser')
    logger.setLevel(logging.INFO)
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

def update_proxies():
    global proxy_pool, last_proxy_update
    try:
        key = os.getenv("KEY")
        proxy_url = f"https://api.best-proxies.ru/proxylist.csv?key={key}&type=https&limit=1000"
        proxy_path = os.path.join(cwd, "src", "proxylist.csv")
        r = requests.get(proxy_url)
        if r.ok:
            with open(proxy_path, "wb") as f:
                f.write(r.content)
            df = pd.read_csv(proxy_path, sep=";", encoding="cp1251")
            df = df.sort_values(by="good checks", ascending=False).head(100)
            proxies = [f"{ip}:{port}" for ip, port in zip(df["ip"], df["port"])]
            if proxies:
                with proxy_lock:
                    proxy_pool = proxies
                    last_proxy_update = time.time()
                logger.info(f"Updated proxy pool with {len(proxies)} proxies")
            else:
                logger.warning("No proxies retrieved, using no proxy")
                proxy_pool = [None]
        else:
            logger.error(f"Failed to fetch proxies: HTTP {r.status_code}")
            proxy_pool = [None]
    except Exception as e:
        logger.error(f"Error updating proxies: {e}")
        proxy_pool = [None]
    return cycle(proxy_pool)

def get_proxy():
    global proxy_pool, last_proxy_update
    with proxy_lock:
        if not proxy_pool or (time.time() - last_proxy_update) > PROXY_UPDATE_INTERVAL:
            logger.info("Refreshing proxy pool")
            return next(update_proxies())
        return next(cycle(proxy_pool))




def create_driver(proxy=None):
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    # Remove --user-data-dir to avoid creating a temporary profile
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

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    return driver, None

link_count_lock = threading.Lock()
vacancy_data_lock = threading.Lock()
batch_data = []

def save_vacancy_data(data, link):
    global CURRENT_DF_LEN
    with vacancy_data_lock:
        data_with_link = {**data, 'link': link}
        df = pd.DataFrame([data_with_link])
        if os.path.exists(vacancy_path):
            df.to_csv(vacancy_path, mode='a', index=False, header=False)
        else:
            df.to_csv(vacancy_path, index=False)
        CURRENT_DF_LEN += 1
        logger.info(f"Saved vacancy data for {link}, total: {CURRENT_DF_LEN}")
    return CURRENT_DF_LEN

def flush_batch():
    global batch_data, CURRENT_DF_LEN
    with vacancy_data_lock:
        if batch_data:
            df = pd.DataFrame(batch_data)
            if os.path.exists(vacancy_path):
                df.to_csv(vacancy_path, mode='a', index=False, header=False)
            else:
                df.to_csv(vacancy_path, index=False)
            CURRENT_DF_LEN += len(batch_data)
            logger.info(f"Flushed final batch of {len(batch_data)} vacancies, total: {CURRENT_DF_LEN}")
            batch_data.clear()

def try_process_link(link, proxy):
    global LINK_COUNT
    all_links = set()
    start_time = time.time()
    attempt = 0

    while time.time() - start_time < TIMEOUT_PER_LINK:
        attempt += 1
        driver = None
        user_data_dir = None
        try:
            driver, user_data_dir = create_driver(proxy)
            logger.info(f"Attempt {attempt}: Fetching {link} with proxy {proxy}")
            driver.get(link)
            try:
                elem = driver.find_element(By.XPATH, "//*[contains(text(), 'Дальше')]")
                parent_div = elem.find_element(By.XPATH, "./ancestor::div[1]")
                titles = parent_div.find_elements(By.XPATH, ".//a[@title]")
                numbers = [int(t.get_attribute("title")) for t in titles if t.get_attribute("title").isdigit()]
                max_page = max(numbers) if numbers else 1
                logger.info(f"Attempt {attempt}: Successfully fetched max page {max_page} for {link} with proxy {proxy}")
            except Exception:
                max_page = 1
                logger.info(f"Attempt {attempt}: No pagination found for {link}, assuming single page")
            break
        except Exception as e:
            logger.error(f"Attempt {attempt}: Failed to fetch {link} with proxy {proxy}: {e}")
            if driver:
                driver.quit()
                if user_data_dir:
                    shutil.rmtree(user_data_dir, ignore_errors=True)
            proxy = get_proxy()
            time.sleep(1)
            continue
    else:
        logger.error(f"Timed out after {TIMEOUT_PER_LINK}s for {link}, no vacancies collected")
        return []

    for page in range(1, max_page + 1):
        page_success = False
        page_attempt = 0
        while not page_success and (time.time() - start_time < TIMEOUT_PER_LINK):
            page_attempt += 1
            try:
                if not driver:
                    driver, user_data_dir = create_driver(proxy)
                url = f"{link}?page={page}"
                logger.info(f"Attempt {attempt}, page {page}, page_attempt {page_attempt}: Processing {url} with proxy {proxy}")
                driver.get(url)
                vacancy_links = driver.find_elements(By.CSS_SELECTOR, 'a[href*="/vakansii/"]')
                hrefs = [a.get_attribute('href') for a in vacancy_links if a.get_attribute('href')]
                filtered = [h for h in set(hrefs) if re.search(r'\d+\.html$', h)]
                logger.info(f"Attempt {attempt}, page {page}: Found {len(filtered)} vacancy links")

                if not filtered:
                    logger.warning(f"Attempt {attempt}, page {page}: No vacancies found, retrying")
                    driver.quit()
                    if user_data_dir:
                        shutil.rmtree(user_data_dir, ignore_errors=True)
                    proxy = get_proxy()
                    driver = None
                    user_data_dir = None
                    time.sleep(1)
                    continue

                for vacancy_link in filtered:
                    link_success = False
                    link_attempt = 0
                    while not link_success and (time.time() - start_time < TIMEOUT_PER_LINK):
                        link_attempt += 1
                        try:
                            link_content = parse_job_info(driver, vacancy_link)
                            save_vacancy_data(link_content, vacancy_link)
                            link_success = True
                            logger.info(f"Attempt {attempt}, page {page}: Successfully parsed vacancy {vacancy_link}")
                        except Exception as e:
                            logger.error(f"Attempt {attempt}, page {page}, link_attempt {link_attempt}: Failed to parse {vacancy_link}: {e}")
                            driver.quit()
                            if user_data_dir:
                                shutil.rmtree(user_data_dir, ignore_errors=True)
                            proxy = get_proxy()
                            driver, user_data_dir = create_driver(proxy)
                            time.sleep(1)
                    if not link_success:
                        logger.warning(f"Attempt {attempt}, page {page}: Failed to parse {vacancy_link} within timeout")

                with link_count_lock:
                    LINK_COUNT += len(filtered)
                all_links.update(filtered)
                page_success = True
                logger.info(f"Attempt {attempt}: Completed page {page} of {link}, total links: {LINK_COUNT}")
            except Exception as e:
                logger.error(f"Attempt {attempt}, page {page}, page_attempt {page_attempt}: Failed to process {url}: {e}")
                if driver:
                    driver.quit()
                    if user_data_dir:
                        shutil.rmtree(user_data_dir, ignore_errors=True)
                proxy = get_proxy()
                driver = None
                user_data_dir = None
                time.sleep(1)

        if not page_success:
            logger.warning(f"Attempt {attempt}: Failed to process page {page} of {link} within timeout")
            continue

    if driver:
        driver.quit()
        if user_data_dir:
            shutil.rmtree(user_data_dir, ignore_errors=True)

    if not all_links:
        logger.error(f"No vacancies collected for {link} after timeout")
    else:
        logger.info(f"Completed processing {link}, collected {len(all_links)} vacancy links")
    return list(all_links)

def read_existing_links():
    if os.path.exists(result_file):
        df = pd.read_csv(result_file)
        return set(df['vacancy_links'].dropna().tolist())
    return set()

def append_links_to_csv(links):
    existing = read_existing_links()
    unique = list(set(links) - existing)
    if unique:
        df = pd.DataFrame(unique, columns=['vacancy_links'])
        df.to_csv(result_file, mode='a', header=not os.path.exists(result_file), index=False)
        logger.info(f"Appended {len(unique)} new vacancy links to {result_file}")
    else:
        logger.info("No new links to append")

def read_progress():
    if os.path.exists(progress_file):
        df = pd.read_csv(progress_file)
        if not df.empty:
            return df['link'].tolist()
    return []

def save_progress(processed_links):
    df = pd.DataFrame(processed_links, columns=['link'])
    df.to_csv(progress_file, index=False)
    logger.info(f"Saved progress for {len(processed_links)} processed links")

def process_level_0_link(link):
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

    flush_batch()
    append_links_to_csv(all_results)
    logger.info(f"Total vacancy links scraped: {LINK_COUNT}, vacancy.csv length: {CURRENT_DF_LEN}")

if __name__ == '__main__':
    main()