import threading
from queue import Queue
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
import time
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
            elements = driver.find_elements(By.CSS_SELECTOR, "div._1_fPU._3Kyh0._360-q._2JGQx._3X9mq._3bLK7._1zXvU")
            first_block = elements[0] if elements else None

            job_name = first_block.find_element(By.CSS_SELECTOR, "h1.VB8-V._1OQvn._1jFZf._1pU0a").text

            try:
                salary = first_block.find_element(By.CSS_SELECTOR, "span._1OQvn._1jFZf._2BcMA").text
            except:
                salary = None

            mini_info_block = first_block.find_elements(By.CSS_SELECTOR, "div.MokF1.Q-vpS.y3ALy.JtuJ\\+.\\_3kzNE.wvZWN")

            city = mini_info_block[0].find_element(By.CSS_SELECTOR, "span._1GMF7._1jFZf._5rADX._2BcMA").text
            busyness = mini_info_block[1].find_element(By.CSS_SELECTOR, "span._1GMF7._1jFZf._5rADX._2BcMA").text
            citizenship = mini_info_block[2].find_element(By.CSS_SELECTOR, "span._1GMF7._1jFZf._5rADX._2BcMA").text

            skills_block = driver.find_element(By.CSS_SELECTOR, "ul._8jaXR._216Hm._24p6f._3rAp7._2mD1_")
            skill_elements = skills_block.find_elements(By.CSS_SELECTOR, "li._3jypR span._1GMF7._1jFZf._5rADX._3d8Ma._15Qoi")
            skills_list = [skill.text for skill in skill_elements if skill.text.strip() != "Показать еще"]

            experience_block = driver.find_element(By.CSS_SELECTOR, "div.f-test-block-assignment")

            experience = experience_block.find_element(By.CSS_SELECTOR, "h2.j66yb._25q4J._1jFZf._5rADX._3d8Ma").text

            experience_items = experience_block.find_elements(By.CSS_SELECTOR, "ul.cG_fG._99T5B._2mD1_ > li._3jypR")

            experience_data = []

            for item in experience_items:
                period = item.find_element(
                    By.CSS_SELECTOR, "ul.xmw1g li._3jypR:first-child span._1GMF7"
                ).text.strip()

                duration = item.find_element(
                    By.CSS_SELECTOR, "ul.xmw1g li._3jypR:nth-child(2) span._1GMF7"
                ).text.strip()

                position = item.find_element(
                    By.CSS_SELECTOR, "h3._9gypz"
                ).text.strip()

                company = item.find_element(
                    By.CSS_SELECTOR, "div._1_fPU._2sk9U span._1GMF7:first-child"
                ).text.strip()

                all_texts = [item_bloc.text for item_bloc in item.find_elements(By.CSS_SELECTOR, "div._3jypR")]

                description = ''
                for i in all_texts:
                    description = description + ' ' + i
                description = ' '.join(description.split())

                experience_data.append({
                    "period": period,
                    "duration": duration,
                    "position": position,
                    "company": company,
                    "description": description
                })

            education_block = driver.find_element(By.CSS_SELECTOR, "div.f-test-block-bank")

            university = education_block.find_element(
                By.CSS_SELECTOR, "h3._9gypz a._26cdC"
            ).text.strip()

            faculty = education_block.find_element(
                By.XPATH, ".//span[contains(., 'Факультет:')]//a"
            ).text.strip()

            university_specialty = education_block.find_element(
                By.XPATH, ".//span[contains(., 'Специальность:')]//a"
            ).text.strip()

            about_blocks = driver.find_elements(By.CSS_SELECTOR, "div.f-test-block-feedback")
            about_blocks_text = " ".join([block.text for block in about_blocks])
            about_blocks_text = ' '.join(about_blocks_text.split())


            foreign_languages = driver.find_elements(By.CSS_SELECTOR, "div.f-test-block-sort_by_alpha")
            foreign_languages_text = [block.text for block in foreign_languages]
            foreign_languages_text = foreign_languages_text[0].split('\n')[1:]

            results.append({
                "resume_title": job_name,
                "salary": salary,
                "city": city,
                "busyness": busyness,
                "citizenship": citizenship,
                "skills_list": skills_list,
                "experience": experience,
                "experience_data": experience_data,
                "university": university,
                "faculty": faculty,
                "university_specialty": university_specialty,
                "about_employee": about_blocks_text,
                "foreign_languages": foreign_languages_text
            })

        except Exception as e:
            print(f"Error processing {link}: {e}")

        queue.task_done()

    print(results)

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
            # filtered_links = [link for link in unique_links if re.search(r'\d+\.html$', link)]
            filtered_links = ["https://www.superjob.ru/resume/nachalnik-otk-veduschij-glavnyj-inzhener-1724157.html"]

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
    df.to_csv(f'{path_to_save_result}/superjob_resume.csv', index=False, encoding='utf-8-sig')
    # df.to_parquet(f'{path_to_save_result}/superjob_vacancies.parquet', engine='pyarrow')

    return df

parse_superjob_selenium_multithreaded(page_count=1, num_threads=20)

print(f'>>> Сохранили результаты в {path_to_save_result}/superjob_resume.csv ')