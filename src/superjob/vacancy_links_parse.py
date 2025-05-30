import os
import re
import tempfile
import pandas as pd
from tqdm import tqdm
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import cycle
import threading
import logging
from vacancy_content_parser import parse_job_info, init_city_pattern
import shutil
import requests
import time

proxy_cycle = None
last_proxy_update = 0

def update_proxies():
    global proxy_cycle
    try:
        key = os.getenv("KEY")
        proxy_url = f"https://api.best-proxies.ru/proxylist.csv?key={key}&type=https&country=ru&limit=1000"
        proxy_path = os.path.join(cwd, "src", "proxylist.csv")
        r = requests.get(proxy_url)
        if r.ok:
            print("Обновили прокси")
            with open(proxy_path, "wb") as f:
                f.write(r.content)
            df = pd.read_csv(proxy_path, sep=";", encoding="cp1251")
            df = df.sort_values(by="good checks", ascending=False).head(500)
            proxies = [f"{ip}:{port}" for ip, port in zip(df["ip"], df["port"])]
            if proxies:
                proxy_cycle = cycle(proxies)
            else:
                proxy_cycle = cycle([None])
            return proxy_cycle
    except Exception as e:
        logging.error(f"Ошибка при обновлении прокси: {e}")
    if proxy_cycle is None:
        proxy_cycle = cycle([None])
    return proxy_cycle

PROXY_UPDATE_INTERVAL = 300
proxy_lock = threading.Lock()

def get_proxy_cycle():
    global proxy_cycle, last_proxy_update
    with proxy_lock:
        if proxy_cycle is None or (time.time() - last_proxy_update) > PROXY_UPDATE_INTERVAL:
            proxy_cycle = update_proxies()
            last_proxy_update = time.time()
        return proxy_cycle

cwd = os.getcwd()
path_to_save_result = os.path.join(cwd, "results")

vacancy_path = os.path.join(path_to_save_result, "vacancy.csv")

log_path = os.path.join(path_to_save_result, "vacancy_parser.log")

def setup_logger(log_path: str, to_console: bool) -> logging.Logger:
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    file_handler = logging.FileHandler(log_path, encoding='utf-8')
    file_handler.setFormatter(formatter)

    logger.handlers = []
    logger.addHandler(file_handler)

    if to_console:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger

logger = setup_logger(log_path=log_path, to_console=False)


MAX_RETRIES = 50
MAX_WORKERS = 5
USE_PROXY = True


result_file = os.path.join(path_to_save_result, "vacancy_links.csv")
progress_file = os.path.join(path_to_save_result, "progress_links.csv")

driver_path = ChromeDriverManager().install()

link_count_lock = threading.Lock()
LINK_COUNT = 0
CURRENT_DF_LEN = 0

def create_driver(proxy=None):
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument(f"--user-data-dir={tempfile.mkdtemp()}")
    prefs = {
        "profile.managed_default_content_settings.images": 2,
        "profile.managed_default_content_settings.fonts": 2
    }
    options.add_experimental_option("prefs", prefs)
    if proxy:
        options.add_argument(f'--proxy-server=http://{proxy}')
    return webdriver.Chrome(service=Service(driver_path), options=options)


def save_vacancy_data(data, link, path_to_save_result):
    vacancy_path = os.path.join(path_to_save_result, "vacancy.csv")
    data['link'] = link
    df = pd.DataFrame([data])
    if os.path.exists(vacancy_path):
        df.to_csv(vacancy_path, mode='a', index=False, header=False)
        current_len = len(pd.read_csv(vacancy_path))
    else:
        df.to_csv(vacancy_path, index=False)
        current_len = 1
    return current_len


def try_process_link(link, proxy):
    global LINK_COUNT
    global CURRENT_DF_LEN
    proxy_cycle = get_proxy_cycle()
    all_links = set()
    retries = 0
    while retries < MAX_RETRIES:
        try:
            driver = create_driver(proxy)
            driver.get(link)
            elem = driver.find_element(By.XPATH, "//*[contains(text(), 'Дальше')]")
            parent_div = elem.find_element(By.XPATH, "./ancestor::div[1]")
            titles = parent_div.find_elements(By.XPATH, ".//a[@title]")
            numbers = [int(t.get_attribute("title")) for t in titles if t.get_attribute("title").isdigit()]
            max_page = max(numbers) if numbers else 1
            driver.quit()
            # shutil.rmtree(driver.temp_profile, ignore_errors=True)
            break
        except Exception as e:
            logging.error(f"Error fetching max page for link: {link} with proxy {proxy}: {e}")
            driver.quit()
            # shutil.rmtree(driver.temp_profile, ignore_errors=True)
            retries += 1
            proxy = next(proxy_cycle)
    else:
        return []

    for page in range(1, max_page + 1):
        page_retries = 0
        success = False
        while not success:
            try:
                driver = create_driver(proxy)
                url = f"{link}/?page={page}"
                driver.get(url)
                vacancy_links = driver.find_elements(By.CSS_SELECTOR, 'a[href*="/vakansii/"]')
                hrefs = [a.get_attribute('href') for a in vacancy_links if a.get_attribute('href')]
                filtered = [h for h in set(hrefs) if re.search(r'\d+\.html$', h)]

                for link in filtered:
                    link_parse_success = False
                    while not link_parse_success:
                        try:
                            link_content = parse_job_info(driver, link)
                            save_vacancy_data(link_content, link, path_to_save_result)
                            CURRENT_DF_LEN += 1
                            link_parse_success = True
                        except Exception as e:
                            # logging.error(f"Error processing page {page} for link {link} with proxy {proxy}: {e}")
                            driver.quit()
                            shutil.rmtree(driver.temp_profile, ignore_errors=True)
                            page_retries += 1
                            proxy = next(proxy_cycle)

                all_links.update(filtered)
                with link_count_lock:
                    LINK_COUNT += len(filtered)
                driver.quit()
                shutil.rmtree(driver.temp_profile, ignore_errors=True)
                success = True
                massage = f"Current vacancy.csv length: {CURRENT_DF_LEN} должно быть равно LINK_COUNT = {LINK_COUNT}"
                logging.info(massage)
                print(massage)
            except Exception as e:
                # logging.error(f"Error processing page {page} for link {link} with proxy {proxy}: {e}")
                driver.quit()
                shutil.rmtree(driver.temp_profile, ignore_errors=True)
                page_retries += 1
                proxy = next(proxy_cycle)

    logging.info(f"LINK_COUNT={LINK_COUNT} for link={link}")
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
        logging.info(f"Appended {len(unique)} new links")

def init_result_dir():
    os.makedirs(path_to_save_result, exist_ok=True)

def read_progress():
    if os.path.exists(progress_file):
        df = pd.read_csv(progress_file)
        if not df.empty:
            return df['link'].tolist()
    return []

def save_progress(processed_links):
    df = pd.DataFrame(processed_links, columns=['link'])
    df.to_csv(progress_file, index=False)

def process_level_0_link(link, proxy_cycle):
    proxy = next(proxy_cycle)
    return link, try_process_link(link, proxy)

def main():
    global proxy_cycle
    init_result_dir()
    proxy_cycle = update_proxies()

    links_df = pd.read_csv(os.path.join(path_to_save_result, "level_0_links.csv"))
    level_0_links = links_df["level_0_link"].dropna().tolist()

    processed_links = read_progress()
    to_process = [l for l in level_0_links if l not in processed_links]

    all_results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_level_0_link, link, proxy_cycle): link for link in to_process}
        for future in tqdm(as_completed(futures), total=len(futures)):
            link, links = future.result()
            all_results.extend(links)
            processed_links.append(link)
            save_progress(processed_links)


    append_links_to_csv(all_results)
    logging.info(f"Total links scraped: {LINK_COUNT}")

if __name__ == '__main__':
    main()
