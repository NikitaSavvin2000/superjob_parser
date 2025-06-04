
import time

import tempfile
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import re

import os

cwd = os.getcwd()

city_pattern = None
driver_path = ChromeDriverManager().install()


def init_city_pattern():
    path_to_city = os.path.join(cwd, "src", "superjob", "data", "city.csv")
    df_city = pd.read_csv(path_to_city)

    subset = df_city[df_city['city'].isna()]['address']
    result = subset.str.extract(r'г\s*([^,]+)')[0].dropna().unique().tolist()

    city_list = df_city["city"].to_list()
    city_list.extend(result)

    city_list = list(map(str, city_list))

    global city_pattern
    city_pattern = re.compile(rf'\b(?:{"|".join(map(re.escape, city_list))})\b', re.I)

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


def parse_job_info(driver, link):
    try:
        driver.get(link)
        time.sleep(0.1)
        elem = driver.find_element(By.XPATH, "//button[.//span[text()='Откликнуться']]")
        parent_div = elem.find_element(By.XPATH, "./ancestor::div[7]")

        text = parent_div.text
        lines = text.split('\n')
        lines = lines[6:-5]

        perhaps_salary_position = lines[0:7]
        salary = next((x for x in perhaps_salary_position if re.search(r'\d.*\d', x) and not re.search(r'\bгод[а]?\b', x.lower())), None)
        if salary is None:
            salary = next((s for s in perhaps_salary_position if re.search(r'договор', s, re.I)), None)

        perhaps_title_index = perhaps_salary_position.index(salary) if salary else None
        title = perhaps_salary_position[perhaps_title_index-1] if perhaps_title_index and perhaps_title_index > 0 else None

        perhaps_location_position = lines[2:9]
        perhaps_experience_busyness_position = lines[2:20]

        experience_pattern = re.compile(r'.*опыт.*', re.IGNORECASE)
        busyness_pattern = re.compile(r'занятость', re.I)

        busyness = next((s for s in perhaps_experience_busyness_position if busyness_pattern.search(s)), None)
        if busyness is not None and ',' in busyness:
            busyness_list = busyness.split(',')
            busyness = next((s for s in busyness_list if re.search(r'занят', s, re.I)), None)


        experience_busyness = [s for s in perhaps_experience_busyness_position if experience_pattern.match(s)]
        experience_busyness_parts = experience_busyness[0].split(',') if experience_busyness else []
        experience_busyness_parts = [part.strip() for part in experience_busyness_parts]

        exp = next((s for s in experience_busyness_parts if re.search(r'опыт', s, re.I)), None)
        if busyness is None:
            busyness = next((s for s in experience_busyness_parts if re.search(r'занят|занятость|работа', s, re.I)), None)

        location = next((s for s in perhaps_location_position if city_pattern and city_pattern.search(s)), None)

        description = ' '.join(lines)

        return {
            'title': title,
            'salary': salary,
            'experience': exp,
            'busyness': busyness,
            'location': location,
            'description': description
        }
    except Exception as e:
        raise e


# Пример вызова
# driver = create_driver(proxy=None)
# link = "https://russia.superjob.ru/vakansii/menedzher-po-vnedreniyu-iskusstvennogo-intelekta-50704748.html"
# content = parse_job_info(driver=driver, link=link)
# print(content)

