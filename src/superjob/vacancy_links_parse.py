
import os, re, time, uuid, shutil, tempfile, logging
import pandas as pd
from tqdm import tqdm
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from concurrent.futures import ThreadPoolExecutor, as_completed
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import requests
from vacancy_content_parser import parse_job_info
import threading
from itertools import cycle

MAX_WORKERS = 10
USE_PROXY = True
PROXY_UPDATE_INTERVAL = 300
TIMEOUT_PER_LINK = 120
MAX_RETRIES = 3
RETRY_BACKOFF = 2
MAX_PAGE_RETRIES = 3

cwd = os.getcwd()
result_path = os.path.join(cwd, "src", "superjob", "results")
log_path = os.path.join(cwd, "src", "superjob", "logs")
os.makedirs(result_path, exist_ok=True)
os.makedirs(log_path, exist_ok=True)

vacancy_path = os.path.join(result_path, "vacancy.csv")
result_file = os.path.join(result_path, "vacancy_links.csv")
progress_file = os.path.join(result_path, "progress_links.csv")

logger = logging.getLogger('vacancy_parser')
logger.setLevel(logging.INFO)
fh = logging.FileHandler(os.path.join(log_path, "vacancy_parser.log"), encoding='utf-8')
fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.handlers = [fh]

proxy_pool, last_proxy_update = [], 0
proxy_cycle = None
proxy_lock = threading.Lock()

def update_proxies():
    global proxy_pool, proxy_cycle, last_proxy_update
    print("[INFO] Обновление списка прокси...")
    try:
        key = os.getenv("KEY")
        r = requests.get(f"https://api.best-proxies.ru/proxylist.csv?key={key}&type=https&limit=1000", timeout=10)
        if r.ok:
            df = pd.read_csv(pd.compat.StringIO(r.text), sep=";", encoding="cp1251")
            proxies = [f"{ip}:{port}" for ip, port in zip(df["ip"], df["port"])]
            print(f"[INFO] Получено {len(proxies)} прокси, валидация...")
            valid = [p for p in proxies[:100] if validate_proxy(p)]
            print(f"[INFO] Пройдено валидацию: {len(valid)}")
            with proxy_lock:
                proxy_pool = valid or [None]
                proxy_cycle = cycle(proxy_pool)
                last_proxy_update = time.time()
    except Exception as e:
        print(f"[ERROR] Ошибка при обновлении прокси: {e}")
        proxy_pool = [None]
        proxy_cycle = cycle(proxy_pool)

def validate_proxy(proxy):
    try:
        r = requests.get("https://www.google.com", proxies={"https": f"http://{proxy}"}, timeout=5)
        return r.ok
    except:
        return False

def get_proxy():
    print("[INFO] Получение прокси...")
    global proxy_cycle, last_proxy_update
    with proxy_lock:
        if not proxy_pool or time.time() - last_proxy_update > PROXY_UPDATE_INTERVAL:
            update_proxies()
        proxy = next(proxy_cycle)
        print(f"[INFO] Используется прокси: {proxy}")
        return proxy

driver_store = threading.local()

def create_driver(proxy=None):
    print(f"[INFO] Создание драйвера (proxy={proxy})")
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    prefs = {"profile.managed_default_content_settings.images": 2}
    options.add_experimental_option("prefs", prefs)
    if proxy:
        options.add_argument(f'--proxy-server=http://{proxy}')
    user_dir = os.path.join(tempfile.gettempdir(), f"chrome_{uuid.uuid4()}")
    options.add_argument(f"--user-data-dir={user_dir}")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    driver.implicitly_wait(1)
    return driver, user_dir

def get_driver(proxy):
    if not hasattr(driver_store, 'driver') or driver_store.proxy != proxy:
        print("[INFO] Новый драйвер создаётся...")
        if hasattr(driver_store, 'driver'):
            try:
                driver_store.driver.quit()
                shutil.rmtree(driver_store.user_data_dir, ignore_errors=True)
            except:
                pass
        driver_store.driver, driver_store.user_data_dir = create_driver(proxy)
        driver_store.proxy = proxy
    return driver_store.driver

vacancy_lock = threading.Lock()

def save_vacancy(data, link):
    if not data or not isinstance(data, dict) or not any(data.values()):
        return
    print(f"[INFO] Сохраняется вакансия: {link}")
    row = pd.DataFrame([{**data, "link": link}])
    with vacancy_lock:
        row.to_csv(vacancy_path, mode='a', header=not os.path.exists(vacancy_path), index=False)

def get_max_page(driver, link):
    print(f"[INFO] Получение количества страниц для: {link}")
    for _ in range(MAX_PAGE_RETRIES):
        try:
            driver.get(link)
            WebDriverWait(driver, 60).until(EC.presence_of_element_located((By.XPATH, "//*[contains(text(), 'Дальше')]")))
            elems = driver.find_elements(By.XPATH, "//a[@title and ancestor::div[contains(.,'Дальше')]]")
            return max([int(e.get_attribute("title")) for e in elems if e.get_attribute("title").isdigit()] or [1])
        except:
            time.sleep(1)
    return 1

def try_process_link(link, proxy):
    print(f"[INFO] Обработка страницы: {link}")
    driver = get_driver(proxy)
    links = set()
    try:
        max_page = get_max_page(driver, link)
        print(f"[INFO] Найдено {max_page} страниц")
        for page in range(1, max_page + 1):
            page_url = f"{link}?page={page}"
            print(f"[INFO] Загрузка страницы {page_url}")
            for _ in range(MAX_RETRIES):
                try:
                    driver.get(page_url)
                    WebDriverWait(driver, 60).until(EC.presence_of_element_located((By.CSS_SELECTOR, 'a[href*="/vakansii/"]')))
                    a_tags = driver.find_elements(By.CSS_SELECTOR, 'a[href*="/vakansii/"]')
                    urls = [a.get_attribute('href') for a in a_tags if a.get_attribute('href')]
                    for url in set(filter(lambda u: re.search(r'\d+\.html$', u), urls)):
                        try:
                            print(f"[INFO] Парсинг вакансии: {url}")
                            data = parse_job_info(driver, url)
                            save_vacancy(data, url)
                            links.add(url)
                        except Exception as e:
                            print(f"[WARNING] Ошибка при парсинге вакансии {url}: {e}")
                    break
                except:
                    time.sleep(1)
    except Exception as e:
        print(f"[ERROR] Ошибка при обработке {link}: {e}")
    return list(links)

def read_csv_set(path, col):
    print(f"[INFO] Чтение CSV: {path} колонка: {col}")
    if os.path.exists(path):
        return set(pd.read_csv(path)[col].dropna().tolist())
    return set()

def append_links(links):
    print(f"[INFO] Добавление новых ссылок...")
    existing = read_csv_set(result_file, 'vacancy_links')
    new_links = list(set(links) - existing)
    print(f"[INFO] Новых ссылок: {len(new_links)}")
    if new_links:
        pd.DataFrame(new_links, columns=['vacancy_links']).to_csv(result_file, mode='a', header=not os.path.exists(result_file), index=False)

def process_link(link):
    print(f"[INFO] Запуск обработки: {link}")
    proxy = get_proxy()
    return link, try_process_link(link, proxy)

def main():
    print("[INFO] Запуск главного процесса...")
    update_proxies()
    df = pd.read_csv(os.path.join(result_path, "level_0_links.csv"))
    print("[INFO] Загружены начальные ссылки")
    links = df["level_0_link"].dropna().tolist()
    processed = read_csv_set(progress_file, 'link')
    to_process = [l for l in links if l not in processed]
    print(f"[INFO] К обработке: {len(to_process)} ссылок")
    all_links, done = [], []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(process_link, link): link for link in to_process}
        for f in tqdm(as_completed(futures), total=len(futures)):
            link, res = f.result()
            all_links.extend(res)
            done.append(link)
            pd.DataFrame(done, columns=['link']).to_csv(progress_file, index=False)
    append_links(all_links)
    print("[INFO] Завершено")

if __name__ == '__main__':
    main()
