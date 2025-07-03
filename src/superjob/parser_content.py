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
import time
from vacancy_content_parser import parse_job_info
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


MAX_WORKERS = 2
USE_PROXY = True
PROXY_UPDATE_INTERVAL = 100
TIMEOUT_PER_LINK = 120
MAX_RETRIES = 15
RETRY_BACKOFF = 5
MAX_PAGE_RETRIES = 15

cwd = os.getcwd()
result_path = os.path.join(cwd, "src", "superjob", "content")
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



def append_content_to_csv(content):
    os.makedirs(result_path, exist_ok=True)
    max_size = 10 * 1024 * 1024  # 10 MB

    def get_csv_path(idx):
        return os.path.join(result_path, f"data_{idx}.csv")

    idx = 1
    while True:
        path = get_csv_path(idx)
        if not os.path.exists(path):
            df = pd.DataFrame(columns=["title", "salary", "experience", "busyness", "location", "description"])
            break
        if os.path.getsize(path) < max_size:
            break
        idx += 1

    df = pd.read_csv(path) if os.path.exists(path) else df

    new_row = {
        "title": content["title"],
        "salary": content["salary"],
        "experience": content["experience"],
        "busyness": content["busyness"],
        "location": content["location"],
        "description": content["description"],
    }

    df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
    df.to_csv(path, index=False)
    logger.info(f">>> Дозаписали данные. Всего данных = {len(df)}")


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
        # link = f"http://api.best-proxies.ru/proxylist.csv?key={key}&type=https&limit=1000"
        link = f"http://api.best-proxies.ru/proxylist.csv?key={key}&country=ru&type=http&limit=1000"

        # "https://api.best-proxies.ru/proxylist.txt?key=76fab1758feb6a4c3dfe3496d8e12521&country=ru&limit=0"
        r = requests.get(link, timeout=30)
        if r.ok:
            df = pd.read_csv(io.StringIO(r.text), sep=";", encoding="cp1251")
            proxies = [f"{ip}:{port}" for ip, port in zip(df["ip"], df["port"])]
            valid = [p for p in proxies[:15] if validate_proxy(p)]
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


def save_vacancy(data, link):
    if not data or not isinstance(data, dict) or not any(data.values()):
        return
    logger.info(f"Сохраняется вакансия: {link}")
    row = pd.DataFrame([{**data, "link": link}])

    if os.path.exists(vacancy_path):
        old_df = pd.read_csv(vacancy_path)
        new_df = pd.concat([row, old_df], ignore_index=True)
        new_df.to_csv(vacancy_path, index=False)
    else:
        row.to_csv(vacancy_path, index=False)


def append_urls_to_csv(urls):
    logger.info(f">>>>>>>>>>>>> append_urls_to_csv is working")

    file_path = os.path.join(cwd, "src", "superjob", "results", "urls_vacancy.csv")
    new_df = pd.DataFrame(urls, columns=['url'])

    file_exists = os.path.exists(file_path)
    new_df.to_csv(file_path, mode='a', header=not file_exists, index=False)


def try_process_link(link, proxy):
    logger.info(f"Обработка страницы: {link}")
    attempts = 0
    while attempts < MAX_RETRIES:
        try:
            content = parse_job_info(link=link, proxy=proxy)

            append_content_to_csv(content=content)

            # title = content["content"]
            # salary = content["salary"]
            # experience = content["experience"]
            # busyness = content["busyness"]
            # location = content["location"]
            # description = content["description"]

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
    # df = pd.read_csv(os.path.join(result_path, "level_0_links.csv"))

    df = pd.read_csv(f'{result_path}/to_parse.csv')
    logger.info("Загружены начальные ссылки")
    links = df["url"].dropna().tolist()
    logger.info(f"К обработке: {len(links)} ссылок")
    links = list(set(links))
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
