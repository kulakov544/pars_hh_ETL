import pandas as pd
import os
from sqlalchemy import create_engine, Date, Boolean, Integer, String
from typing import Literal

from dotenv import load_dotenv


"""
Создание календаря. Скрипт создает в базе данных таблицу из 4 столбцов: Дата, рабочие дни, праздники, выход на работу.
"""
load_dotenv()
conn_string = os.getenv("conn_string")
engine = create_engine(conn_string)

def put_data(df: pd.DataFrame,
             table_name: str,
             schema: str,
             dtype: dict,
             if_exists: Literal["fail", "replace", "append"] = "append") -> bool:
    """Функция, которая загружает дата фрейм в БД, возвращает bool
        :param schema: схема
        :param table_name: название таблицы
        :param if_exists: метод загрузки
        :param df: дата фрейм для записи
        :param dtype: типы данных столбцов
    """
    try:
        conn = engine.connect()
    except Exception as e:
        raise Exception(f'Невозможно установить соединение с сервером: {str(e)}')
    else:
        try:
            df.to_sql(name=table_name, con=conn, schema=schema, if_exists=if_exists, index=False, dtype=dtype)
            return True
        except Exception as e:
            raise e
        finally:
            conn.close()
            engine.dispose()


# Генерируем все даты 2024 года
dates_2024 = pd.date_range(start='2024-01-01', end='2024-12-31')

# Создаем DataFrame
df_calendar = pd.DataFrame(dates_2024, columns=['date'])

# Определяем рабочие дни (понедельник-пятница)
df_calendar['work_day'] = df_calendar['date'].dt.dayofweek < 5

# Выходные дни по производственному календарю
holidays = [
    '2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05',
    '2024-01-06', '2024-01-07', '2024-01-08', '2024-02-23', '2024-03-08',
    '2024-04-29', '2024-04-30', '2024-05-01', '2024-05-09',
    '2024-05-10', '2024-06-12', '2024-11-04', '2024-12-30', '2024-12-31',
    ]
holidays_transfer = [
    '2024-04-27', '2024-11-02', '2024-12-28'
]

# Преобразуем строки в даты
holidays = pd.to_datetime(holidays)
holidays_transfer = pd.to_datetime(holidays_transfer)

# Добавляем информацию о праздниках в DataFrame
df_calendar['holidays'] = df_calendar['date'].isin(holidays)
df_calendar['holidays_transfer'] = df_calendar['date'].isin(holidays_transfer)

# Определяем когда нужно выходить на работу (исключающие ИЛИ)
df_calendar['to_work'] = (df_calendar['work_day'] & ~df_calendar['holidays']) | df_calendar['holidays_transfer']

# Добавляем новые столбцы
df_calendar['week_number'] = df_calendar['date'].dt.isocalendar().week
df_calendar['month_number'] = df_calendar['date'].dt.month
df_calendar['quarter'] = df_calendar['date'].dt.quarter
df_calendar['month_name_en'] = df_calendar['date'].dt.strftime('%B')
df_calendar['month_abbr_en'] = df_calendar['date'].dt.strftime('%b')

# Русские названия месяцев
month_names_ru = {
    1: 'Январь', 2: 'Февраль', 3: 'Март', 4: 'Апрель', 5: 'Май', 6: 'Июнь',
    7: 'Июль', 8: 'Август', 9: 'Сентябрь', 10: 'Октябрь', 11: 'Ноябрь', 12: 'Декабрь'
}
month_abbr_ru = {
    1: 'Янв', 2: 'Фев', 3: 'Мар', 4: 'Апр', 5: 'Май', 6: 'Июн',
    7: 'Июл', 8: 'Авг', 9: 'Сен', 10: 'Окт', 11: 'Ноя', 12: 'Дек'
}

# Добавляем названия месяцев на русском
df_calendar['month_name'] = df_calendar['month_number'].apply(lambda x: month_names_ru[x])
df_calendar['month_abbr'] = df_calendar['month_number'].apply(lambda x: month_abbr_ru[x])


# Определяем типы данных столбцов для загрузки в БД
dtype = {
    'date': Date(),
    'work_day': Boolean(),
    'holidays': Boolean(),
    'holidays_transfer': Boolean(),
    'to_work': Boolean(),
    'week_number': Integer(),
    'month_number': Integer(),
    'quarter': Integer(),
    'month_name_en': String(),
    'month_abbr_en': String(),
    'month_name': String(),
    'month_abbr': String()
}

# Загрузка данных в БД (закомментирована для примера)
put_data(df_calendar, table_name='calendar', schema="core", if_exists="replace", dtype=dtype)

# Вывод DataFrame
#print(df_calendar)
