import os
import re
import time
import pandas as pd
from tqdm import tqdm
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from concurrent.futures import ThreadPoolExecutor, as_completed


cwd = os.getcwd()

path_to_save_result = os.path.join(cwd, "src", "superjob", "results")

# link = "https://russia.superjob.ru/vakansii/katalog/"
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
#
# rusult_all_links_level_0 = []
#
# driver.get(link)
# time.sleep(0.1)
# parent_elements = driver.find_elements(By.CSS_SELECTOR, "div._1_fPU._30ND4._13P8-._3UgmH._1zXvU")
#
# level_0_links = []
# for element in parent_elements:
#     vacancy_links = element.find_elements(By.CSS_SELECTOR, 'a[href*="/vakansii/"]')
#     level_0_links.extend([link.get_attribute('href') for link in vacancy_links])
#
# divider = 'https://russia.superjob.ru/vakansii/katalog/a/'
# try:
#     divider_index = level_0_links.index(divider)
# except ValueError:
#     divider_index = -1
#
# if divider_index >= 0:
#     list_before = level_0_links[:divider_index][:-1]
#     list_after = level_0_links[divider_index:]
# else:
#     list_before = level_0_links
#     list_after = []
#
# rusult_all_links_level_0.append(list_before)
#
# for link in tqdm(list_after):
#     driver.get(link)
#     time.sleep(0.1)
#     parent_elements = driver.find_elements(By.CSS_SELECTOR, "div._1_fPU._30ND4._13P8-._3UgmH._1zXvU")
#
#     level_1_links = []
#     for element in parent_elements:
#         vacancy_links = element.find_elements(By.CSS_SELECTOR, 'a[href*="/vakansii/"]')
#         level_1_links.extend([link.get_attribute('href') for link in vacancy_links])
#     rusult_all_links_level_0.append(level_1_links)
#
# flat_list = [item for sublist in rusult_all_links_level_0 for item in sublist]
# flat_list = list(set(flat_list))
# filtered_links = [link for link in flat_list if re.search(r'\d+\.html$', link)]
# df = pd.DataFrame(flat_list, columns=['level_0_link'])
#


# df.to_csv(f'{path_to_save_result}/level_0_links.csv', index=False)

# links_df = pd.read_csv(f'{path_to_save_result}/level_0_links.csv')
#
# level_0_links = list(links_df["level_0_link"])
# level_0_links = level_0_links[:1]
#
# end_level_links = []
#
# for level_0_link in tqdm(level_0_links):
#
#     level_0_link = "https://russia.superjob.ru/vakansii/ekonomist-na-proizvodstvo.html"
#     driver.get(level_0_link)
#
#     pagination_links = driver.find_elements(By.CSS_SELECTOR, "div._1_fPU.gATod._92V-a._3OVv9.wkMAY._3P1xW._1FmBk a[title]")
#     numbers = []
#     for link in pagination_links:
#         title = link.get_attribute('title')
#         if title.isdigit():
#             numbers.append(int(title))
#     if numbers:
#         max_page = max(numbers)
#     else:
#         max_page = 1
#
#     pages_num = range(1, max_page+1)
#
#     for page in pages_num:
#         cur_link = f"{level_0_link}/?page={page}"
#         driver.get(cur_link)
#         # print(cur_link)
#         time.sleep(0.1)
#         vacancy_links = driver.find_elements(By.CSS_SELECTOR, 'a[href*="/vakansii/"]')
#         links = [link.get_attribute('href') for link in vacancy_links]
#         unique_links = list(set(links))
#         filtered_links = [link for link in unique_links if re.search(r'\d+\.html$', link)]
#         end_level_links.append(filtered_links)
#
# flat_list = [item for sublist in end_level_links for item in sublist]
# flat_list = list(set(flat_list))
# filtered_links = [link for link in flat_list if re.search(r'\d+\.html$', link)]
# df = pd.DataFrame(flat_list, columns=['vacancy_links'])
# df.to_csv(f'{path_to_save_result}/vacancy_links.csv', index=False)
# print(f'>>> Сохранили результаты. Всего {len(filtered_links)} ссылок')
#
# import os
# import pandas as pd
# import re
# import time
# from tqdm import tqdm
# from selenium import webdriver
# from selenium.webdriver.common.by import By
#
#
# result_file = f'{path_to_save_result}/vacancy_links.csv'
#
# proxy_list = [
#     "201.150.119.170:999",
#     "201.150.119.170:999"
# ]
#
# def read_existing_links():
#     if os.path.exists(result_file):
#         df = pd.read_csv(result_file)
#         return set(df['vacancy_links'].dropna().tolist())
#     return set()
#
# def append_links_to_csv(new_links):
#     existing_links = read_existing_links()
#     unique_links = set(new_links) - existing_links
#     if unique_links:
#         df = pd.DataFrame(list(unique_links), columns=['vacancy_links'])
#         df.to_csv(result_file, mode='a', header=not os.path.exists(result_file), index=False)
#
# def create_driver_with_proxy(proxy):
#     options = webdriver.ChromeOptions()
#     options.add_argument('--headless')
#     options.add_argument('--no-sandbox')
#     options.add_argument('--disable-dev-shm-usage')
#     options.add_argument(f'--proxy-server=http://{proxy}')
#     return webdriver.Chrome(options=options)
#
# def process_level_0_link(driver, level_0_link):
#     try:
#         driver.get(level_0_link)
#         pagination_links = driver.find_elements(By.CSS_SELECTOR, "div._1_fPU.gATod._92V-a._3OVv9.wkMAY._3P1xW._1FmBk a[title]")
#         numbers = [int(link.get_attribute('title')) for link in pagination_links if link.get_attribute('title').isdigit()]
#         max_page = max(numbers) if numbers else 1
#
#         print(level_0_link)
#         print(f"Страниц: {max_page}")
#
#         for page in tqdm(range(1, max_page + 1), desc=f'Парсинг {level_0_link}', leave=False):
#             cur_link = f"{level_0_link}/?page={page}"
#             driver.get(cur_link)
#             time.sleep(0.1)
#             vacancy_links = driver.find_elements(By.CSS_SELECTOR, 'a[href*="/vakansii/"]')
#             links = [link.get_attribute('href') for link in vacancy_links]
#             filtered_links = [link for link in set(links) if re.search(r'\d+\.html$', link)]
#             append_links_to_csv(filtered_links)
#         return None
#     except Exception as e:
#         print(f'Ошибка при обработке {level_0_link}: {e}')
#         return level_0_link
#
# def main():
#     if not os.path.exists(path_to_save_result):
#         os.makedirs(path_to_save_result)
#
#     links_df = pd.read_csv(f'{path_to_save_result}/level_0_links.csv')
#     level_0_links = list(links_df["level_0_link"])
#
#     failed_links = []
#
#     for i, level_0_link in enumerate(tqdm(level_0_links, desc='Обработка всех ссылок')):
#         proxy = proxy_list[i % len(proxy_list)]
#         driver = create_driver_with_proxy(proxy)
#         try:
#             failed = process_level_0_link(driver, level_0_link)
#             if failed:
#                 failed_links.append(failed)
#         finally:
#             driver.quit()
#
#     if failed_links:
#         print(f'Не удалось обработать следующие ссылки: {failed_links}')
#
# if __name__ == '__main__':
#     main()
#

cwd = os.getcwd()
path_to_save_result = os.path.join(cwd, "src", "superjob", "results")
result_file = f'{path_to_save_result}/vacancy_links.csv'

def read_existing_links():
    if os.path.exists(result_file):
        df = pd.read_csv(result_file)
        return set(df['vacancy_links'].dropna().tolist())
    return set()

def append_links_to_csv(new_links):
    existing_links = read_existing_links()
    unique_links = set(new_links) - existing_links
    if unique_links:
        df = pd.DataFrame(list(unique_links), columns=['vacancy_links'])
        df.to_csv(result_file, mode='a', header=not os.path.exists(result_file), index=False)

def create_driver():
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

def process_level_0_link(level_0_link):
    driver = create_driver()
    try:
        driver.get(level_0_link)
        pagination_links = driver.find_elements(By.CSS_SELECTOR, "div._1_fPU.gATod._92V-a._3OVv9.wkMAY._3P1xW._1FmBk a[title]")
        numbers = [int(link.get_attribute('title')) for link in pagination_links if link.get_attribute('title').isdigit()]
        max_page = max(numbers) if numbers else 1

        for page in range(1, max_page + 1):
            cur_link = f"{level_0_link}/?page={page}"
            driver.get(cur_link)
            time.sleep(0.1)
            vacancy_links = driver.find_elements(By.CSS_SELECTOR, 'a[href*="/vakansii/"]')
            links = [link.get_attribute('href') for link in vacancy_links]
            filtered_links = [link for link in set(links) if re.search(r'\d+\.html$', link)]
            append_links_to_csv(filtered_links)
    except Exception as e:
        return level_0_link
    finally:
        driver.quit()
    return None

def main():
    if not os.path.exists(path_to_save_result):
        os.makedirs(path_to_save_result)

    links_df = pd.read_csv(f'{path_to_save_result}/level_0_links.csv')
    level_0_links = list(links_df["level_0_link"])

    failed_links = []

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(process_level_0_link, link) for link in level_0_links]
        for future in tqdm(as_completed(futures), total=len(futures), desc="Обработка ссылок"):
            result = future.result()
            if result:
                failed_links.append(result)

    if failed_links:
        print(f'Не удалось обработать следующие ссылки: {failed_links}')

if __name__ == '__main__':
    main()
