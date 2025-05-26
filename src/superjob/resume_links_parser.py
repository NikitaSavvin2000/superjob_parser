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
from datetime import datetime, timedelta
import threading
import logging

cwd = os.getcwd()
path_to_save_result = os.path.join(cwd, "src", "superjob", "results")

log_path = os.path.join(path_to_save_result, "logfile_resume_links_parser.log")
file_handler = logging.FileHandler(log_path, encoding='utf-8')
file_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logging.getLogger().addHandler(file_handler)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

USE_PROXY = True
MAX_WORKERS = 5
MAX_RETRIES = 5


result_file = os.path.join(path_to_save_result, "resume_links.csv")
progress_file = os.path.join(path_to_save_result, "progress.csv")
df_proxies = pd.read_csv(os.path.join(cwd, "src", "proxylist.csv"), sep=";", encoding="cp1251")
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
    return webdriver.Chrome(service=Service(driver_path), options=options)

def try_process_link(link, proxy):
    global LINK_COUNT
    all_links = set()
    try:
        driver = create_driver(proxy)
        driver.get(link)
        pagination_links = driver.find_elements(By.CSS_SELECTOR, "div._2rcpb.gATod a[title]")
        numbers = [int(a.get_attribute('title')) for a in pagination_links if a.get_attribute('title') and a.get_attribute('title').isdigit()]
        max_page = max(numbers) if numbers else 1
        driver.quit()
    except:
        return False

    for page in range(1, max_page + 1):
        success = False
        retries = 0
        while not success and retries < MAX_RETRIES:
            try:
                driver = create_driver(proxy)
                driver.get(f"{link}/?page={page}")
                elements = driver.find_elements(By.CSS_SELECTOR, 'a[href*="/resume/"]')
                hrefs = [el.get_attribute("href") for el in elements if el.get_attribute("href")]
                filtered = [h for h in set(hrefs) if re.search(r"\d+\.html$", h)]
                all_links.update(filtered)
                with link_count_lock:
                    LINK_COUNT += len(filtered)
                driver.quit()
                success = True
            except:
                driver.quit()
                retries += 1
                proxy = next(proxy_cycle)

    logging.info(f'LINK_COUNT = {LINK_COUNT}')

    start_ts = int(link.split('datepub_from=')[1].split('&')[0])
    end_ts = int(link.split('datepub_to=')[1])
    append_links_to_csv(list(all_links), datetime.fromtimestamp(start_ts), datetime.fromtimestamp(end_ts))
    return True

def read_existing_links():
    if os.path.exists(result_file):
        df = pd.read_csv(result_file)
        return set(df["resume_links"].dropna().tolist())
    return set()

def append_links_to_csv(links, start_date, end_date):
    existing = read_existing_links()
    unique = list(set(links) - existing)
    if unique:
        df = pd.DataFrame(unique, columns=["resume_links"])
        df["start_date"] = start_date
        df["end_date"] = end_date
        df.to_csv(result_file, mode="a", header=not os.path.exists(result_file), index=False)

def init_result_dir():
    os.makedirs(path_to_save_result, exist_ok=True)

def read_progress():
    if os.path.exists(progress_file):
        df = pd.read_csv(progress_file)
        if not df.empty:
            return pd.to_datetime(df.iloc[-1]["end_date"])
    return None

def save_progress(start, end):
    df = pd.DataFrame([[start, end]], columns=["start_date", "end_date"])
    df.to_csv(progress_file, mode="a", header=not os.path.exists(progress_file), index=False)

def generate_date_ranges(end_date, current_date, delta_hours=24):
    result = []
    while current_date > end_date:
        next_date = current_date - timedelta(hours=delta_hours)
        result.append([int(next_date.timestamp()), int(current_date.timestamp())])
        current_date = next_date
    return result

def process_date_range(dates):
    link = f"https://russia.superjob.ru/resume/search_resume.html?datepub_from={dates[0]}&datepub_to={dates[1]}"
    for _ in range(MAX_RETRIES):
        proxy = next(proxy_cycle)
        success = try_process_link(link, proxy)
        if success:
            return

def main():
    init_result_dir()
    end_date = datetime(1999, 12, 31)
    current_date = datetime.now().replace(minute=0, second=0, microsecond=0)
    last = read_progress()
    if last:
        current_date = last
    ranges = generate_date_ranges(end_date, current_date)
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_date_range, dates): dates for dates in ranges}
        for future in tqdm(as_completed(futures), total=len(futures)):
            dates = futures[future]
            start = datetime.fromtimestamp(dates[0])
            end = datetime.fromtimestamp(dates[1])
            save_progress(start, end)
    logging.info(f"Total links: {LINK_COUNT}")

if __name__ == "__main__":
    main()
