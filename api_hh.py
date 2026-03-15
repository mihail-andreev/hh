import requests
import pandas as pd
from bs4 import BeautifulSoup
import random
import time



# per_page = 100 # Количество вакансий на странице
# search_queries = ['Аналитик', 'Инженер данных']  # Список текстов для поиска
# area = 1 # Регион (1 - Москва)
# period = 1 № Период (Количество дней, в пределах которых производится поиск по вакансиям)
# pages_to_parse = 15 # Количество страницдля парсинга
# field # Поле, в котором выбираем где упоминается вакансия:
    #name
    #company_name
    #descriptoion
    #all
#skill_search = True нужны ли скилы в вакансии


def find_proxy():
    url = "https://free-proxy-list.net"
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
    except Exception as e:
        print(f"Ошибка получения списка прокси: {e}")
        return []

    soup = BeautifulSoup(response.text, 'html.parser')
    table = soup.find('table', {'class': 'table table-striped table-bordered'})
    if not table:
        return []

    proxies = []
    for row in table.find_all('tr')[1:]:
        cols = row.find_all('td')
        if len(cols) >= 2:
            ip = cols[0].text.strip()
            port = cols[1].text.strip()
            proxies.append(f"http://{ip}:{port}")
    return proxies

def retry_request(url, params=None, retries=5, delay=2):
    proxies_list = find_proxy()
    if not proxies_list:
        proxies_list = [None]  

    for attempt in range(retries):
        proxy = {'http': random.choice(proxies_list)} if proxies_list[0] else None
        try:
            response = requests.get(url, params=params, proxies=proxy, timeout=10)
            response.raise_for_status()
            return response
        except Exception as e:
            print(f"Попытка {attempt+1}/{retries} с прокси {proxy} не удалась: {e}")
            if attempt < retries - 1:
                time.sleep(delay * (2 ** attempt))  # экспоненциальная задержка
    return None

def query(per_page, search_queries, area, period, pages_to_parse, search_field, skills_search):
    frames = []  # инициализация
    for query_text in search_queries:
        print(f"\nОбрабатываю запрос: '{query_text}'")
        for page in range(pages_to_parse):
            print(f"Страница {page+1}/{pages_to_parse}")
            url = 'https://api.hh.ru/vacancies'
            params = {
                'page': page,
                'per_page': per_page,
                'text': query_text,
                'area': area,
                'period': period,
                'search_field': search_field  # исправлено
            }

            response = retry_request(url, params=params)
            if response is None:
                print("Не удалось получить данные страницы, пропускаем.")
                continue

            data = response.json()
            if not data.get('items'):
                print("Вакансии не найдены.")
                continue

            for item in data['items']:
                if skills_search:
                    # запрос деталей вакансии для получения ключевых навыков
                    vacancy_url = f"https://api.hh.ru/vacancies/{item['id']}"
                    vac_response = retry_request(vacancy_url)
                    if vac_response:
                        vac_data = vac_response.json()
                        skills = [skill['name'] for skill in vac_data.get('key_skills', [])]
                        item['key_skills'] = ', '.join(skills)
                    else:
                        item['key_skills'] = None
                else:
                    item['key_skills'] = None  # или не добавлять поле

                item['search_query'] = query_text
                frames.append(item)
                time.sleep(0.5)  # пауза между запросами (2 запроса в секунду)

    return frames

import datetime
import pandas as pd

def df_main(frames):
    if not frames:
        print("Нет данных для сохранения.")
        return pd.DataFrame()

    df = pd.DataFrame(frames)

    # Нормализация известных столбцов-словарей
    dict_columns = ['employer', 'area', 'salary', 'type', 'schedule', 'experience', 'employment']
    for col in dict_columns:
        if col in df.columns:
            # Извлекаем данные из словарей, заполняем пропуски
            norm = pd.json_normalize(df[col].dropna().tolist())
            if not norm.empty:
                norm.columns = [f"{col}_{subcol}" for subcol in norm.columns]
                df = df.drop(columns=[col]).join(norm, how='left')

    # Обработка professional_roles (список словарей)
    if 'professional_roles' in df.columns:
        df['professional_roles_id'] = df['professional_roles'].apply(
            lambda x: x[0]['id'] if x and isinstance(x, list) and len(x) > 0 else None
        )
        df['professional_roles_name'] = df['professional_roles'].apply(
            lambda x: x[0]['name'] if x and isinstance(x, list) and len(x) > 0 else None
        )
        df = df.drop(columns=['professional_roles'])

    # Удаляем оставшиеся столбцы-списки/словари, если они не были нормализованы
    for col in df.columns:
        if df[col].dropna().apply(lambda x: isinstance(x, (dict, list))).any():
            df = df.drop(columns=[col])
            print(f"Столбец {col} удалён (содержит неразвернутые структуры).")

    # Сохранение
    filename = f"vacancies_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    df.to_csv(filename, index=False)
    print(f"✅ Данные сохранены в {filename}")
    return df

    # per_page = 100#100  # Количество вакансий на странице
    # search_queries = ['Аналитик','Юрист']  # Список текстов для поиска
    # area = 1  # Регион (1 - Москва)
    # period = 1  # Период (количество дней)
    # pages_to_parse = 15 #15 # Количество страниц для парсинга
    # field = Используйте параметр field со значением name (или company_name для поиска по названию компании).Другие варианты уточнения поиска:
        # name	В названии вакансии 
        # company_name В названии компании 
        # description	В описании вакансии (по умолчанию)
        # all	Во всех полях
    #skills_search = True нужны ли skills в вакансии?

per_page = 50
search_queries = ['Аналитик']
area = 1
period = 1
pages_to_parse = 1
search_field = 'name'          
skills_search = False

result = query(per_page, search_queries, area, period, pages_to_parse, search_field, skills_search)
df = df_main(result)






