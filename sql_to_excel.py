import psycopg2
import pandas as pd
from datetime import datetime
import os
from config import DB_CONFIG  # Импортируем из конфига

def export_to_excel():
    try:
        # Подключение использует данные из config.py
        conn = psycopg2.connect(
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            dbname=DB_CONFIG['database'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password']
        )
        
        query = "SELECT * FROM vacancies"
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        os.makedirs('exports', exist_ok=True)
        filename = f'exports/vacancies_export_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx'
        
        df.to_excel(filename, index=False, engine='openpyxl')
        
        print(f"✅ Данные экспортированы в {filename}")
        print(f"📊 Всего записей: {len(df)}")
        
        return filename
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return None

if __name__ == "__main__":
    export_to_excel()
    
