import pandas as pd
import pandas as pd
import re


df = pd.read_csv('/Users/nikitasavvin/Downloads/vacancy.csv')


def clean_description(text):
    if pd.isna(text):
        return text
    text = re.sub(r'^.*?[0-9]{2} сотрудников ', '', text)
    text = text.replace('Выберите сообщение', '')
    text = text.replace('Перезвоните мне, пожалуйста', '')
    text = text.replace('Хочу пообщаться по вакансии в чате', '')
    text = text.replace('Откликнуться', '')
    text = re.sub(r'\s+', ' ', text).strip()
    return text

# df = df.loc[:500]

df['description'] = df['description'].apply(clean_description)

# df = df.loc[:500]

# print(df["description"])

df.to_csv("/Users/nikitasavvin/Desktop/HSE_work/superjob_parser/src/scripts/clean_vacancy.csv")

