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

link = "https://russia.superjob.ru/vakansii/uborschica-posudomojschica-50584872.html"
driver.get(link)
time.sleep(0.1)

page_text = driver.find_element("tag name", "body").text

elem = driver.find_element(By.XPATH, "//button[.//span[text()='Откликнуться']]")
parent_div = elem.find_element(By.XPATH, "./ancestor::div[7]")

text = parent_div.text
lines = text.split('\n')
lines = lines[6:-5]
# print(lines)
perhaps_salary_position = lines[1:7]
salary = [x for x in perhaps_salary_position if re.search(r'\d', x)][0]

perhaps_title_index = perhaps_salary_position.index(salary)
title = perhaps_salary_position[perhaps_title_index-1]

perhaps_location_position = lines[2:9]
perhaps_experience_busyness_position = lines[2:20]
pattern = re.compile(r'.*опыт.*', re.IGNORECASE)

busyness_pattern = re.compile(r'занятость', re.I)

print(perhaps_experience_busyness_position)
busyness_pattern = re.compile(r'занятость', re.I)
busyness = next((s for s in perhaps_experience_busyness_position if busyness_pattern.search(s)), None)
if busyness is not None:
    if ',' in busyness:
        busyness = busyness.split(',')[1].strip()
    else:
        busyness = busyness.strip()


experience_busyness = [s for s in perhaps_experience_busyness_position if pattern.match(s)]
experience_busyness_parts = [part.strip() for part in experience_busyness[0].split(',')] if ',' in experience_busyness[0] else [experience_busyness[0]]
exp = next((s for s in experience_busyness_parts if re.search(r'опыт', s, re.I)), None)

if busyness is None:
    busyness = next((s for s in experience_busyness_parts if re.search(r'занят|занятость|работа', s, re.I)), None)

location = next((s for s in perhaps_location_position if city_pattern.search(s)), None)

description = ' '.join(lines)

print(title)
print(salary)
print(exp)
print(busyness)
print(location)
print(description)




