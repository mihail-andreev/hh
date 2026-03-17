import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import random
import time
import datetime
import psycopg2
import logging
from config import DB_CONFIG


# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def find_proxy():
    """Получение списка прокси с таймаутом и обработкой ошибок"""
    url = "https://free-proxy-list.net"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    
    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
    except requests.exceptions.Timeout:
        logging.warning("Таймаут при получении списка прокси")
        return []
    except Exception as e:
        logging.warning(f"Ошибка получения списка прокси: {e}")
        return []

    try:
        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.find('table', {'class': 'table table-striped table-bordered'})
        if not table:
            return []

        proxies = []
        for row in table.find_all('tr')[1:20]:  # Ограничим первые 20 прокси
            cols = row.find_all('td')
            if len(cols) >= 7 and cols[6].text.strip() == 'yes':  # Только HTTPS прокси
                ip = cols[0].text.strip()
                port = cols[1].text.strip()
                proxies.append(f"http://{ip}:{port}")
        
        logging.info(f"Найдено {len(proxies)} прокси")
        return proxies
    except Exception as e:
        logging.warning(f"Ошибка парсинга прокси: {e}")
        return []

def retry_request(url, params=None, retries=3, delay=1):
    """Выполнение запроса с повторными попытками и прокси"""
    
    # Пробуем сначала без прокси
    for attempt in range(retries):
        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            return response
        except Exception as e:
            logging.debug(f"Попытка {attempt+1}/{retries} без прокси не удалась: {e}")
            if attempt < retries - 1:
                time.sleep(delay * (attempt + 1))
    
    # Если не получилось без прокси, пробуем с прокси
    proxies_list = find_proxy()
    if proxies_list:
        for attempt in range(retries):
            proxy = random.choice(proxies_list)
            proxy_dict = {'http': proxy, 'https': proxy}
            try:
                response = requests.get(url, params=params, proxies=proxy_dict, timeout=15)
                response.raise_for_status()
                return response
            except Exception as e:
                logging.debug(f"Попытка {attempt+1}/{retries} с прокси {proxy} не удалась: {e}")
                if attempt < retries - 1:
                    time.sleep(delay * (2 ** attempt))
    
    logging.error(f"Не удалось получить данные после всех попыток: {url}")
    return None

def query(per_page, search_queries, area, period, pages_to_parse, search_field, skills_search):
    frames = []
    
    for query_text in search_queries:
        logging.info(f"Обрабатываю запрос: '{query_text}'")
        
        for page in range(pages_to_parse):
            logging.info(f"Страница {page+1}/{pages_to_parse}")
            
            url = 'https://api.hh.ru/vacancies'
            params = {
                'page': page,
                'per_page': per_page,
                'text': query_text,
                'area': area,
                'period': period,
                'search_field': search_field
            }

            response = retry_request(url, params=params)
            if response is None:
                logging.warning("Не удалось получить данные страницы, пропускаем.")
                continue

            try:
                data = response.json()
                if not data.get('items'):
                    logging.info("Вакансии не найдены.")
                    continue

                for item in data['items']:
                    if skills_search and item.get('id'):
                        # Запрос деталей вакансии для получения ключевых навыков
                        vacancy_url = f"https://api.hh.ru/vacancies/{item['id']}"
                        vac_response = retry_request(vacancy_url)
                        
                        if vac_response:
                            try:
                                vac_data = vac_response.json()
                                skills = [skill['name'] for skill in vac_data.get('key_skills', [])]
                                item['key_skills'] = ', '.join(skills) if skills else None
                            except:
                                item['key_skills'] = None
                        else:
                            item['key_skills'] = None
                    else:
                        item['key_skills'] = None

                    item['search_query'] = query_text
                    frames.append(item)
                    
                    # Пауза между запросами (соблюдаем лимиты API)
                    time.sleep(0.5)

            except Exception as e:
                logging.error(f"Ошибка обработки данных: {e}")
                continue

    return frames

def load_to_db(df):
    DB_HOST = 'localhost'
    DB_PORT = '5432'
    DB_NAME = 'hh_api_data'
    DB_USER = 'postgres'
    DB_PASSWORD = 'simplepassword'
    
    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            dbname=DB_CONFIG['database'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password']
        )
        
        # Создание курсора
        cur = conn.cursor()
        
        # Создание таблицы
        cur.execute("""
            CREATE TABLE IF NOT EXISTS vacancies (
                id INTEGER PRIMARY KEY,
                name TEXT,
                published_at TIMESTAMP,
                alternate_url TEXT,
                employer_name TEXT,
                area_name TEXT,
                salary_from NUMERIC,
                salary_to NUMERIC,
                salary_currency VARCHAR(3),
                salary_gross BOOLEAN,
                experience_name TEXT,
                employment_name TEXT,
                schedule_name TEXT,
                key_skills TEXT,
                snippet TEXT
            )
        """)
        
        # Вставка данных построчно
        successful = 0
        failed = 0
        
        for _, row in df.iterrows():
            try:
                # Замена NaN на None
                row_clean = row.replace({np.nan: None})
                
                cur.execute("""
                    INSERT INTO vacancies (
                        id, name, published_at, alternate_url, employer_name, 
                        area_name, salary_from, salary_to, salary_currency, 
                        salary_gross, experience_name, employment_name, 
                        schedule_name, key_skills, snippet
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (id) DO NOTHING
                """, (
                    row_clean.get('id'),
                    row_clean.get('name'),
                    row_clean.get('published_at'),
                    row_clean.get('alternate_url'),
                    row_clean.get('employer_name'),
                    row_clean.get('area_name'),
                    row_clean.get('salary_from'),
                    row_clean.get('salary_to'),
                    row_clean.get('salary_currency'),
                    row_clean.get('salary_gross'),
                    row_clean.get('experience_name'),
                    row_clean.get('employment_name'),
                    row_clean.get('schedule_name'),
                    row_clean.get('key_skills'),
                    row_clean.get('snippet')
                ))
                successful += 1
                
            except psycopg2.Error as e:
                failed += 1
                logging.warning(f"Ошибка при вставке записи {row.get('id')}: {e}")
        
        # Фиксация изменений
        conn.commit()
        
        logging.info(f"Загружено: {successful} записей, пропущено: {failed}")
        
        # Закрытие курсора
        cur.close()
        
    except psycopg2.Error as e:
        logging.error(f"Ошибка PostgreSQL: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()

def df_main(frames, desired_columns=None):
    if not frames:
        logging.warning("Нет данных для сохранения.")
        return pd.DataFrame()

    df = pd.DataFrame(frames)
    logging.info(f"Получено {len(df)} вакансий")

    # Нормализация вложенных словарей
    dict_columns = ['employer', 'area', 'salary', 'type', 'schedule', 'experience', 'employment']
    
    for col in dict_columns:
        if col in df.columns:
            try:
                # Убираем пустые значения для нормализации
                valid_data = df[df[col].notna()][col].tolist()
                if valid_data:
                    norm = pd.json_normalize(valid_data)
                    if not norm.empty:
                        norm.columns = [f"{col}_{subcol}" for subcol in norm.columns]
                        df = df.drop(columns=[col]).join(norm, how='left')
            except Exception as e:
                logging.warning(f"Ошибка при нормализации колонки {col}: {e}")

    # Обработка профессиональных ролей
    if 'professional_roles' in df.columns:
        try:
            df['professional_roles_id'] = df['professional_roles'].apply(
                lambda x: x[0]['id'] if x and isinstance(x, list) and len(x) > 0 else None
            )
            df['professional_roles_name'] = df['professional_roles'].apply(
                lambda x: x[0]['name'] if x and isinstance(x, list) and len(x) > 0 else None
            )
            df = df.drop(columns=['professional_roles'])
        except Exception as e:
            logging.warning(f"Ошибка при обработке professional_roles: {e}")

    # Обработка сниппета - ИСПРАВЛЕНО!
    if 'snippet' in df.columns:
        def process_snippet(x):
            if pd.isna(x) or x is None:
                return None
            if isinstance(x, dict):
                requirement = x.get('requirement', '')
                responsibility = x.get('responsibility', '')
                if requirement is None:
                    requirement = ''
                if responsibility is None:
                    responsibility = ''
                return f"{requirement} {responsibility}".strip()
            return str(x) if x is not None else None
        
        df['snippet'] = df['snippet'].apply(process_snippet)

    # Выбор нужных колонок
    if desired_columns:
        # Добавляем search_query в желаемые колонки, если его нет
        if 'search_query' not in desired_columns:
            desired_columns.append('search_query')
            
        existing = [col for col in desired_columns if col in df.columns]
        df = df[existing]
        logging.info(f"Оставленные колонки: {existing}")

    # Загрузка в БД
    load_to_db(df)
    
    return df

# Параметры запроса
per_page = 100
search_queries = ['Аналитик']
area = 1  # Москва
period = 1  # за последний день
pages_to_parse = 1
search_field = 'name'  # поиск по названию
skills_search = True

# Желаемые колонки
desired_columns = [
    'id',
    'name',
    'published_at',
    'alternate_url',
    'employer_name',
    'area_name',
    'salary_from',
    'salary_to',
    'salary_currency',
    'salary_gross',
    'experience_name',
    'employment_name',
    'schedule_name',
    'key_skills',
    'snippet',
    'search_query'
]

# Основной запуск
if __name__ == "__main__":
    logging.info("Начало сбора данных")
    result = query(per_page, search_queries, area, period, pages_to_parse, search_field, skills_search)
    
    if result:
        df = df_main(result, desired_columns)
        logging.info(f"Готово! Загружено {len(df)} вакансий")
    else:
        logging.error("Не удалось получить данные")