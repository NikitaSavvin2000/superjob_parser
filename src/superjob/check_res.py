import os
import pandas as pd

cwd = os.getcwd()

path_to_save_result = os.path.join(cwd, "src", "superjob", "results")

links_df = pd.read_csv(f'{path_to_save_result}/vacancy_links.csv')

print(f" Link counts = {len(links_df)}")