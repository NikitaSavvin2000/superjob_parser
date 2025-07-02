import os
import logging
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from itertools import cycle
from requests.exceptions import ReadTimeout, ConnectionError
import socket
import io
import requests
from bs4 import BeautifulSoup
import time
import re
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


MAX_WORKERS = 10
USE_PROXY = True
PROXY_UPDATE_INTERVAL = 100
TIMEOUT_PER_LINK = 120
MAX_RETRIES = 15
RETRY_BACKOFF = 5
MAX_PAGE_RETRIES = 15

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


def validate_proxy(proxy):
    try:
        r = requests.get("https://www.google.com", proxies={"https": f"http://{proxy}"}, timeout=3)
        return r.ok
    except:
        return False

def update_proxies_loop():
    logger.info("update_proxies_loop is working")

    global last_proxy_update
    while True:
        update_proxies()
        last_proxy_update = time.time()
        time.sleep(PROXY_UPDATE_INTERVAL)


def update_proxies():
    logger.info("update_proxies is working")

    global proxy_pool, proxy_cycle
    try:
        key = os.getenv("KEY")
        link = f"http://api.best-proxies.ru/proxylist.csv?key={key}&type=https&limit=1000"
        r = requests.get(link, timeout=30)
        if r.ok:
            df = pd.read_csv(io.StringIO(r.text), sep=";", encoding="cp1251")
            proxies = [f"{ip}:{port}" for ip, port in zip(df["ip"], df["port"])]
            valid = [p for p in proxies[:150] if validate_proxy(p)]
            if valid:
                with proxy_lock:
                    proxy_pool = valid
                    proxy_cycle = cycle(proxy_pool)
            else:
                logger.warning("Нет валидных прокси после проверки")
    except Exception as e:
        logger.error(f"Ошибка при обновлении прокси списка: {e}")

def get_proxy():
    logger.info("get_proxy is working")
    with proxy_lock:
        if proxy_pool:
            return next(proxy_cycle)
        else:
            logger.warning("Прокси пул пуст")
            return None

threading.Thread(target=update_proxies_loop, daemon=True).start()

vacancy_lock = threading.Lock()

def save_vacancy(data, link):
    if not data or not isinstance(data, dict) or not any(data.values()):
        return
    logger.info(f"Сохраняется вакансия: {link}")
    row = pd.DataFrame([{**data, "link": link}])
    with vacancy_lock:
        row.to_csv(vacancy_path, mode='a', header=not os.path.exists(vacancy_path), index=False)



def get_max_page(link):
    print(f"Получение количества страниц для: {link}")
    for _ in range(MAX_PAGE_RETRIES):
        try:
            headers = {
                "User-Agent": "Mozilla/5.0"
            }
            response = requests.get(link, headers=headers, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            elems = soup.select("a[title]")
            pages = [int(e['title']) for e in elems if e['title'].isdigit()]
            return max(pages) if pages else 1
        except Exception:
            time.sleep(1)
    return 1


def get_urls_from_page(page_url, proxy=None):
    proxies = {
        'http': f'http://{proxy}',
        'https': f'http://{proxy}'
    } if proxy else None

    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.7151.122 Safari/537.36'
    }

    response = requests.get(page_url, headers=headers, proxies=proxies, timeout=65)
    soup = BeautifulSoup(response.text, 'html.parser')
    return [a['href'] for a in soup.select('a[href*="/vakansii/"]') if a.get('href')]

def append_urls_to_csv(urls):
    file_path = os.path.join(cwd, "src", "superjob", "results", "urls_vacancy.csv")
    new_df = pd.DataFrame(urls, columns=['url'])
    if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
        existing_df = pd.read_csv(file_path)
        combined_df = pd.concat([existing_df, new_df]).drop_duplicates(subset=['url'], keep='first')
    else:
        combined_df = new_df
    combined_df.to_csv(file_path, index=False)

def try_process_link(link, proxy):
    logger.info(f"Обработка страницы: {link}")
    attempts = 0
    while attempts < MAX_RETRIES:
        try:
            links = set()
            max_page = get_max_page(link)
            logger.info(f"Найдено {max_page} страниц")
            for page in range(1, max_page + 1):
                page_url = f"{link}?page={page}"
                logger.info(f"Загрузка страницы {page_url}")
                for _ in range(MAX_RETRIES):
                    try:
                        urls = get_urls_from_page(page_url=page_url, proxy=proxy)
                        if len(urls) == 0:
                            raise ValueError(f"Страница {page_url} не содержит ссылок")
                        base_url = "https://russia.superjob.ru"
                        full_urls = [base_url + url for url in urls if re.search(r"\d+\.html$", url)]
                        logger.info(f"На {page_url} - {len(full_urls)} вакансий")
                        append_urls_to_csv(urls=full_urls)
                        break
                    except Exception as e_inner:
                        proxy = get_proxy()
                        logger.warning(f"Ошибка загрузки страницы {page_url}: {e_inner}")
                        logger.warning(f"Используем новый прокси {proxy} попытка {_}")

            return list(links)
        except (ReadTimeout, socket.timeout, ConnectionError) as e:
            logger.warning(f"Timeout или ошибка соединения при обработке {link}: {e}. Меняем прокси и повторяем...")
            proxy = get_proxy()
            attempts += 1
            time.sleep(RETRY_BACKOFF * attempts)
        except Exception as e:
            logger.error(f"Ошибка при обработке {link}: {e}")
            break
    return []

def read_csv_set(path, col):
    logger.info(f"Чтение CSV: {path} колонка: {col}")
    if os.path.exists(path):
        try:
            return set(pd.read_csv(path)[col].dropna().tolist())
        except Exception as e:
            logger.warning(f"Ошибка чтения {path}: {e}")
            return set()
    return set()

def append_links(links):
    logger.info("Добавление новых ссылок...")
    existing = read_csv_set(result_file, 'vacancy_links')
    new_links = list(set(links) - existing)
    logger.info(f"Новых ссылок: {len(new_links)}")
    if new_links:
        pd.DataFrame(new_links, columns=['vacancy_links']).to_csv(result_file, mode='a', header=not os.path.exists(result_file), index=False)

def process_link(link):
    logger.info(f"Запуск обработки: {link}")
    proxy = get_proxy() if USE_PROXY else None
    return link, try_process_link(link, proxy)

def main():
    logger.info("Запуск главного процесса...")
    if USE_PROXY:
        update_proxies()
    df = pd.read_csv(os.path.join(result_path, "level_0_links.csv"))
    logger.info("Загружены начальные ссылки")
    links = df["level_0_link"].dropna().tolist()
    processed = read_csv_set(progress_file, 'link')
    to_process = [l for l in links if l not in processed]
    logger.info(f"К обработке: {len(to_process)} ссылок")
    all_links, done = [], []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(process_link, link): link for link in to_process}
        for f in tqdm(as_completed(futures), total=len(futures)):
            link, res = f.result()
            all_links.extend(res)
            done.append(link)
            pd.DataFrame(done, columns=['link']).to_csv(progress_file, index=False)
    append_links(all_links)
    logger.info("Завершено")

if __name__ == '__main__':
    main()
