import os
import re
import time
import uuid
import shutil
import tempfile
import logging
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
from requests.exceptions import ReadTimeout, ConnectionError
import socket
import io

cwd = os.getcwd()

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





path_to_city = os.path.join(cwd, "src", "superjob", "data", "city.csv")

df_city = pd.read_csv(path_to_city)

subset = df_city[df_city['city'].isna()]['address']
result = subset.str.extract(r'г\s*([^,]+)')[0].dropna().unique().tolist()

city_list = df_city["city"].to_list()
city_list.extend(result)

city_list = list(map(str, city_list))


city_pattern = re.compile(rf'\b(?:{"|".join(map(re.escape, city_list))})\b', re.I)

driver_path = ChromeDriverManager().install()


driver = create_driver(proxy=None)

MAX_PAGE_RETRIES = 3

# def get_max_page(driver, link):
#     print(f"Получение количества страниц для: {link}")
#     for _ in range(MAX_PAGE_RETRIES):
#         try:
#             driver.get(link)
#             WebDriverWait(driver, 45).until(EC.presence_of_element_located((By.XPATH, "//*[contains(text(), 'Дальше')]")))
#             elems = driver.find_elements(By.XPATH, "//a[@title and ancestor::div[contains(.,'Дальше')]]")
#             pages = [int(e.get_attribute("title")) for e in elems if e.get_attribute("title") and e.get_attribute("title").isdigit()]
#             return max(pages) if pages else 1
#         except Exception:
#             time.sleep(1)
#     return 1

import requests
from bs4 import BeautifulSoup
import time

MAX_PAGE_RETRIES = 3

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


link = "https://russia.superjob.ru/vakansii/specialist-po-obucheniyu.html"
# max_page = get_max_page(driver=driver, link=link)
max_page = get_max_page(link=link)

print(f"max_page = {max_page}")