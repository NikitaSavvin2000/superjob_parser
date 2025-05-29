
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
from superjob.vacancy_content_parser import parse_job_info, init_city_pattern

cwd = os.getcwd()
path_to_save_result = os.path.join(cwd, "src", "superjob", "results")

log_path = os.path.join(path_to_save_result, "logfile_links_parser.log")
file_handler = logging.FileHandler(log_path, encoding='utf-8')
file_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logging.getLogger().addHandler(file_handler)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

MAX_RETRIES = 5
MAX_WORKERS = 3

result_file = os.path.join(path_to_save_result, "vacancy_links.csv")
progress_file = os.path.join(path_to_save_result, "progress_links.csv")

df_proxies = pd.read_csv(os.path.join(cwd, "src", "proxylist.csv"), sep=';', encoding='cp1251')
df_proxies = df_proxies.sort_values(by="good checks", ascending=False).head(50)
proxies = [f"{ip}:{port}" for ip, port in zip(df_proxies["ip"], df_proxies["port"])]
proxy_cycle = cycle(proxies)
driver_path = ChromeDriverManager().install()

link_count_lock = threading.Lock()
LINK_COUNT = 0

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
    driver = webdriver.Chrome(service=Service(driver_path), options=options)
    driver.set_page_load_timeout(30)
    return driver

def try_process_link(link, proxy):
    global LINK_COUNT
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
            break
        except Exception as e:
            logging.error(f"Error fetching max page for link: {link} with proxy {proxy}: {e}")
            driver.quit()
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
                            print(link)
                            print(link_content)
                            link_parse_success = True
                        except Exception as e:
                            logging.error(f"Error processing page {page} for link {link} with proxy {proxy}: {e}")
                            driver.quit()
                            page_retries += 1
                            proxy = next(proxy_cycle)

                all_links.update(filtered)
                with link_count_lock:
                    LINK_COUNT += len(filtered)
                driver.quit()
                success = True
            except Exception as e:
                logging.error(f"Error processing page {page} for link {link} with proxy {proxy}: {e}")
                driver.quit()
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

def process_level_0_link(link):
    proxy = next(proxy_cycle)
    return link, try_process_link(link, proxy)

def main():
    init_result_dir()
    init_city_pattern()
    links_df = pd.read_csv(os.path.join(path_to_save_result, "level_0_links.csv"))
    level_0_links = links_df["level_0_link"].dropna().tolist()

    processed_links = read_progress()
    to_process = [l for l in level_0_links if l not in processed_links]

    all_results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_level_0_link, link): link for link in to_process}
        for future in tqdm(as_completed(futures), total=len(futures)):
            link, links = future.result()
            all_results.extend(links)
            processed_links.append(link)
            save_progress(processed_links)

    append_links_to_csv(all_results)
    logging.info(f"Total links scraped: {LINK_COUNT}")

if __name__ == '__main__':
    main()
