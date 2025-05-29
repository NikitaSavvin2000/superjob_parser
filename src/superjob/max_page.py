import os
import re
import tempfile
import pandas as pd
from tqdm import tqdm
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from concurrent.futures import ThreadPoolExecutor
from itertools import cycle
import threading
import logging

cwd = os.getcwd()
path_to_save_result = os.path.join(cwd, "src", "superjob", "results")
result_file = os.path.join(path_to_save_result, "vacancy_links.csv")
log_file = os.path.join(path_to_save_result, "logfile.log")

logging.basicConfig(
    filename=log_file,
    filemode="a",
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s"
)

df_proxies = pd.read_csv(os.path.join(cwd, "src", "proxylist.csv"), sep=';', encoding='cp1251')
df_proxies = df_proxies.sort_values(by="good checks", ascending=False).head(50)
proxies = [f"{ip}:{port}" for ip, port in zip(df_proxies["ip"], df_proxies["port"])]

driver_path = ChromeDriverManager().install()

class ProxyManager:
    def __init__(self, proxies):
        self.lock = threading.Lock()
        self.proxy_cycle = cycle(proxies)
        self.current_proxy = next(self.proxy_cycle)

    def get_proxy(self):
        with self.lock:
            return self.current_proxy

    def switch_proxy(self):
        with self.lock:
            self.current_proxy = next(self.proxy_cycle)
            return self.current_proxy

link_count_lock = threading.Lock()
page_count_lock = threading.Lock()
LINK_COUNT = 0
PAGE_COUNT = 0

def create_driver(proxy=None):
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument(f'--user-data-dir={tempfile.mkdtemp()}')
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

def try_process_link(link, proxy_manager):
    global LINK_COUNT, PAGE_COUNT
    all_links = set()
    max_retries = len(proxies)
    retries = 0
    while retries < max_retries:
        proxy = proxy_manager.get_proxy()
        driver = create_driver(proxy)
        try:
            print('Я работаю')
            print(link)
            driver.get(link)
            elem = driver.find_element(By.XPATH, "//*[contains(text(), 'Дальше')]")
            parent_div = elem.find_element(By.XPATH, "./ancestor::div[1]")
            titles = parent_div.find_elements(By.XPATH, ".//a[@title]")
            numbers = [int(t.get_attribute("title")) for t in titles if t.get_attribute("title").isdigit()]
            max_page = max(numbers) if numbers else 1
            print(max_page)

            for page in range(1, max_page + 1):
                url = f"{link}/?page={page}"
                driver.get(url)
                vacancy_links = driver.find_elements(By.CSS_SELECTOR, 'a[href*="/vakansii/"]')
                hrefs = [a.get_attribute('href') for a in vacancy_links if a.get_attribute('href')]
                filtered = [h for h in set(hrefs) if re.search(r'\d+\.html$', h)]
                all_links.update(filtered)
                with page_count_lock:
                    PAGE_COUNT += 1

            with link_count_lock:
                LINK_COUNT += len(all_links)

            logging.info(f"LINKS_COUNT={LINK_COUNT}, PAGE_COUNT={PAGE_COUNT}, LINK={link}")
            driver.quit()
            return list(all_links)

        except Exception as e:
            driver.quit()
            logging.error(f"Error on link: {link}, Proxy: {proxy}, Error: {str(e)}")
            proxy_manager.switch_proxy()
            retries += 1
    return []

def process_level_0_link(link):
    proxy_manager = ProxyManager(proxies)
    return try_process_link(link, proxy_manager)

def read_existing_links():
    if os.path.exists(result_file):
        df = pd.read_csv(result_file)
        return set(df['vacancy_links'].dropna().tolist())
    return set()

def append_links_to_csv(all_links):
    existing_links = read_existing_links()
    unique_links = list(set(all_links) - existing_links)
    if unique_links:
        df = pd.DataFrame(unique_links, columns=['vacancy_links'])
        df.to_csv(result_file, mode='a', header=not os.path.exists(result_file), index=False)
        logging.info(f"Appended {len(unique_links)} new links to CSV")

def init_result_dir():
    os.makedirs(path_to_save_result, exist_ok=True)

def main():
    print('is work')
    init_result_dir()
    links_df = pd.read_csv(os.path.join(path_to_save_result, "level_0_links.csv"))
    print('is work')
    level_0_links = links_df["level_0_link"].dropna().tolist()
    all_results = []

    with ThreadPoolExecutor(max_workers=1) as executor:
        for links in tqdm(executor.map(process_level_0_link, level_0_links), total=len(level_0_links)):
            all_results.extend(links)

    append_links_to_csv(all_results)

if __name__ == '__main__':
    main()
