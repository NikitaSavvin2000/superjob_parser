import requests
import re
from bs4 import BeautifulSoup
import pandas as pd
import os
import time
import random
cwd = os.getcwd()

def init_city_pattern():
    path_to_city = os.path.join(cwd, "src", "superjob", "data", "city.csv")
    df_city = pd.read_csv(path_to_city)
    subset = df_city[df_city['city'].isna()]['address']
    result = subset.str.extract(r'г\s*([^,]+)')[0].dropna().unique().tolist()
    city_list = df_city["city"].to_list()
    city_list.extend(result)
    city_list = list(map(str, city_list))
    return re.compile(rf'\b(?:{"|".join(map(re.escape, city_list))})\b', re.I)

city_pattern = init_city_pattern()


def parse_job_info(link, proxy=None):
    proxies = None
    if proxy:
        proxies = {
            'http': proxy,
            'https': proxy
        }
        print(f'proxies = {proxies}')
    # proxies = None
    time.sleep(random.uniform(1, 3))
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'ru,en;q=0.9',
            'Referer': 'https://russia.superjob.ru/',
            'Connection': 'keep-alive',
        }

        response = requests.get(link, headers=headers, proxies=proxies, verify=False)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        title_tag = soup.find('h1')
        title = title_tag.get_text(strip=True) if title_tag else None

        text = soup.get_text(separator='\n', strip=True)
        lines = text.split('\n')

        index = lines.index('Похожие вакансии') if 'Похожие вакансии' in lines else None
        if index is not None:
            lines = lines[:index]

        month_index = next((i for i, s in enumerate(lines) if re.search(r'месяц', s, re.I)), None)

        if month_index is None:
            salary = next((s for s in lines[:50] if re.search(r'договорен', s, re.I)), None)
        else:
            possible_zp_list = lines[month_index-5:month_index]
            numbers = [int(re.sub(r'\D', '', s)) for s in possible_zp_list if re.search(r'\d', s)]
            if len(numbers) == 1:
                salary = numbers[0]
            else:
                salary = f"{numbers[0]}-{numbers[1]}"

        possible_loc_position = lines[:50]
        location = next((s for s in possible_loc_position if city_pattern and city_pattern.search(s)), None)

        experience_busyness_parts = lines[20:50]
        exp = next((s for s in experience_busyness_parts if re.search(r'опыт', s, re.I)), None)
        busyness = next((s for s in experience_busyness_parts if re.search(r'занят|занятость|работа', s, re.I)), None)

        if location and location in lines:
            loc_index = lines.index(location)
            lines = lines[loc_index + 2:]

        index_bottom = next((i for i, s in enumerate(lines) if re.search(r'ткликнуться', s, re.I)), None)
        if index_bottom is not None:
            lines = lines[:index_bottom]

        description = ' '.join(lines)

        return {
            'title': title,
            'salary': salary,
            'experience': exp,
            'busyness': busyness,
            'location': location,
            'description': description
        }
    except Exception:
        raise

