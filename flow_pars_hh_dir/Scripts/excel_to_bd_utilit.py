import pandas as pd

from utilits.connect_database import put_data
from utilits.logger_utilit import logger

# Переменные для настройки
file_path = '../Зарплаты.xlsx'  # путь к вашему файлу Excel
sheet_name = 'Лист1'  # имя листа в файле Excel
table_name = 'salaries_in_it'  # название таблицы
schema_name = ('core')  # схема базы данных, по умолчанию 'public'

# Шаг 1: Загрузка данных из файла Excel
excel_df = pd.read_excel(file_path, sheet_name=sheet_name)

# Шаг 2: Настройка названий колонок и их типов
excel_df.columns = ['quarter', 'level', 'specialization', 'salary']  # установка названий колонок

# Преобразование типов данных
#excel_df['quarter'] = pd.to_datetime(excel_df['quarter'])  # преобразование колонки к типу datetime
excel_df['quarter'] = excel_df['quarter'].astype(int)  # преобразование колонки к типу int
excel_df['level'] = excel_df['level'].astype(str)  # преобразование колонки к типу string
excel_df['specialization'] = excel_df['specialization'].astype(str)
excel_df['salary'] = (excel_df['salary'] * 1000).astype(int)


print(excel_df)

# Шаг 3: Загрузка данных в базу данных PostgreSQL
put_data(excel_df, table_name, schema_name, if_exists='replace')
logger.info("Данные успешно загружены в базу данных.")


