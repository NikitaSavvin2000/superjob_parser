import threading
from queue import Queue
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
import time
import re
import os


cwd = os.getcwd()

path_to_save_result = os.path.join(cwd, "src", "superjob", "results")

def worker(driver, queue, results):
    while not queue.empty():
        page, link = queue.get()
        print(f'>>> Processing page {page}, link: {link}')

        try:
            driver.get(link)
            time.sleep(0.1)
            card_block = driver.find_element(By.CSS_SELECTOR, "div._1_fPU.f-test-vacancy-base-info.MDSt4._30DTa._2V8dt._3puh1._1Fj7Y._13P8-._1zXvU")

            location_block = driver.find_element(By.CSS_SELECTOR, "div._2rcpb._1g5wo._21gYL._2FiNT._33WVt._3Jhm2")


            job_name = card_block.find_element(By.CSS_SELECTOR, "h1._2liZK._30DTa._1t3-x._3puh1._25q4J._1jFZf._2BcMA._2mXDe").text

            try:
                experience = card_block.find_element(By.CSS_SELECTOR, "span._2vInL._-4Tqq.f-test-badge._1iGoC._1roDn._3ZLjD").text
            except:
                experience = None

            try:
                short_info = card_block.find_element(By.CSS_SELECTOR, "div._1_fPU._2tMz-._30DTa._2V8dt._3puh1").text
            except:
                short_info = None

            try:
                salary = card_block.find_element(By.CSS_SELECTOR, "span.aFXJ6.GfOgl").text
            except:
                try:
                    salary = card_block.find_element(By.CSS_SELECTOR, "span.kk-+S._25q4J._1jFZf._3d8Ma").text
                except:
                    salary = None

            try:
                location = location_block.find_element(By.CSS_SELECTOR, "span._2YGgq._3PuGp._1fNUj._2Wgs5").text
            except:
                location = None

            description = card_block.find_element(By.CSS_SELECTOR, "span.mrLsm._295-0._1jFZf._5rADX._3d8Ma._2mXDe").text
            description = ' '.join(description.split())

            results.append({
                'Vacancy Link': link,
                'Job Name': job_name,
                'Experience': experience,
                'Short Info': short_info,
                'Salary': salary,
                'Location': location,
                'Description': description
            })

        except Exception as e:
            print(f"Error processing {link}: {e}")

        queue.task_done()

def parse_superjob_selenium_multithreaded(page_count=1, num_threads=4):
    list_pages = list(range(1, page_count+1))
    queue = Queue()
    results = []

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    vacancy_links_all = []

    for page in list_pages:
        print(f'>>> Collecting links from page {page} from {page_count}')
        try:
            cur_link = f"https://russia.superjob.ru/vakansii/?page={page}"
            driver.get(cur_link)
            print(cur_link)
            time.sleep(0.1)
            vacancy_links = driver.find_elements(By.CSS_SELECTOR, 'a[href*="/vakansii/"]')
            links = [link.get_attribute('href') for link in vacancy_links]
            unique_links = list(set(links))
            filtered_links = [link for link in unique_links if re.search(r'\d+\.html$', link)]
            # filtered_links = filtered_links[:5]

            for link in filtered_links:
                queue.put((page, link))
                vacancy_links_all.append(link)

        except Exception as e:
            print(e)
            print(f"Error on {cur_link}")

    driver.quit()

    threads = []
    for i in range(num_threads):
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
        t = threading.Thread(target=worker, args=(driver, queue, results))
        t.start()
        threads.append((t, driver))

    queue.join()

    for t, driver in threads:
        driver.quit()
        t.join()

    df = pd.DataFrame(results)
    df.to_csv(f'{path_to_save_result}/superjob_vacancies.csv', index=False, encoding='utf-8-sig')
    df.to_parquet(f'{path_to_save_result}/superjob_vacancies.parquet', engine='pyarrow')

    return df

parse_superjob_selenium_multithreaded(page_count=50, num_threads=10)