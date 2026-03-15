import requests
import pandas as pd
from bs4 import BeautifulSoup
import random
import time
import datetime




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



def df_main(frames, desired_columns=None):
    if not frames:
        print("Нет данных для сохранения.")
        return pd.DataFrame()

    df = pd.DataFrame(frames)

    
    dict_columns = ['employer', 'area', 'salary', 'type', 'schedule', 'experience', 'employment']
    for col in dict_columns:
        if col in df.columns:
            
            norm = pd.json_normalize(df[col].dropna().tolist())
            if not norm.empty:
                norm.columns = [f"{col}_{subcol}" for subcol in norm.columns]
                df = df.drop(columns=[col]).join(norm, how='left')

    
    if 'professional_roles' in df.columns:
        df['professional_roles_id'] = df['professional_roles'].apply(
            lambda x: x[0]['id'] if x and isinstance(x, list) and len(x) > 0 else None
        )
        df['professional_roles_name'] = df['professional_roles'].apply(
            lambda x: x[0]['name'] if x and isinstance(x, list) and len(x) > 0 else None
        )
        df = df.drop(columns=['professional_roles'])

    if desired_columns:
        existing = [col for col in desired_columns if col in df.columns]
        df = df[existing]
        print(f"Оставленные колонки: {existing}")

    filename = f"vacancies_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    df.to_csv(filename, index=False)
    print(f"✅ Данные сохранены в {filename}")
    return df


per_page = 50
search_queries = ['Аналитик']
area = 1
period = 1
pages_to_parse = 1
search_field = 'name'          
skills_search = True

all_possible_columns = [
    'id', 'premium', 'name', 'department', 'has_test', 'response_letter_required', 
    'salary_range', 'address', 'response_url', 'sort_point_distance', 'published_at', 
    'created_at', 'archived', 'apply_alternate_url', 'branding', 'show_logo_in_search', 
    'show_contacts', 'insider_interview', 'url', 'alternate_url', 'relations', 
    'snippet', 'contacts', 'working_days', 'working_time_intervals', 'working_time_modes', 
    'accept_temporary', 'fly_in_fly_out_duration', 'work_format', 'working_hours', 
    'work_schedule_by_days', 'accept_labor_contract', 'civil_law_contracts', 'night_shifts', 
    'accept_incomplete_resumes', 'employment_form', 'internship', 'adv_response_url', 
    'is_adv_vacancy', 'adv_context', 'key_skills', 'search_query', 'employer_id', 
    'employer_name', 'employer_url', 'employer_alternate_url', 'employer_vacancies_url', 
    'employer_country_id', 'employer_accredited_it_employer', 'employer_trusted', 
    'employer_logo_urls.original', 'employer_logo_urls.90', 'employer_logo_urls.240', 
    'employer_logo_urls', 'area_id', 'area_name', 'area_url', 'salary_from', 'salary_to', 
    'salary_currency', 'salary_gross', 'type_id', 'type_name', 'schedule_id', 'schedule_name', 
    'experience_id', 'experience_name', 'employment_id', 'employment_name', 
    'professional_roles_id', 'professional_roles_name'
]


desired_columns = [
    'id',                          # уникальный идентификатор вакансии
    'name',                        # название вакансии
    'published_at',                # дата публикации
    'created_at',                  # дата создания
    'alternate_url',               # ссылка на вакансию
    'employer_id',                 # id работодателя
    'employer_name',               # название компании
    'employer_trusted',            # доверенный работодатель
    'area_name',                   # название региона
    'salary_from',                 # зарплата от
    'salary_to',                   # зарплата до
    'salary_currency',             # валюта
    'salary_gross',                # до вычета налогов или после
    'experience_id',               # id требуемого опыта
    'experience_name',             # требуемый опыт
    'employment_id',               # id типа занятости
    'employment_name',             # тип занятости
    'schedule_id',                 # id графика работы
    'schedule_name',               # график работы
    'professional_roles_name',     # проф. роль
    'key_skills',                  # ключевые навыки
    'snippet',                     # требования/обязанности (кратко)
]
result = query(per_page, search_queries, area, period, pages_to_parse, search_field, skills_search)
df = df_main(result, desired_columns)






