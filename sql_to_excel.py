import psycopg2
import pandas as pd
from datetime import datetime
import os
from config import DB_CONFIG

def export_new_vacancies_to_excel():
    try:
        # Подключение к БД
        conn = psycopg2.connect(
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            dbname=DB_CONFIG['database'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password']
        )
        
        # Имя файла для экспорта (можно задать фиксированное имя)
        filename = 'exports/vacancies_export.xlsx'
        os.makedirs('exports', exist_ok=True)
        
        # Получаем все вакансии из БД
        query = "SELECT * FROM vacancies ORDER BY id"
        df_new = pd.read_sql_query(query, conn)
        conn.close()
        
        # Проверяем, существует ли уже файл
        if os.path.exists(filename):
            # Загружаем существующие данные
            df_existing = pd.read_excel(filename, engine='openpyxl')
            
            # Находим новые вакансии (по ID, если он есть)
            if 'id' in df_existing.columns and 'id' in df_new.columns:
                existing_ids = set(df_existing['id'])
                df_new_vacancies = df_new[~df_new['id'].isin(existing_ids)]
                
                if len(df_new_vacancies) > 0:
                    # Добавляем новые вакансии к существующим
                    df_combined = pd.concat([df_existing, df_new_vacancies], ignore_index=True)
                    df_combined.to_excel(filename, index=False, engine='openpyxl')
                    print(f"✅ Добавлено {len(df_new_vacancies)} новых вакансий")
                else:
                    print("ℹ️ Новых вакансий не найдено")
                    df_combined = df_existing
            else:
                print("⚠️ В данных нет колонки 'id' для сравнения")
                return None
        else:
            # Если файла нет, создаем новый
            df_new.to_excel(filename, index=False, engine='openpyxl')
            print(f"✅ Создан новый файл с {len(df_new)} вакансиями")
            df_combined = df_new
        
        print(f"📊 Всего записей в файле: {len(df_combined)}")
        print(f"📁 Файл: {filename}")
        
        return filename
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return None

if __name__ == "__main__":
    export_new_vacancies_to_excel()