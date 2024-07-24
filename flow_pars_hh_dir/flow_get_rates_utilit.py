import requests
import pandas as pd
import xml.etree.ElementTree as ET
from pandas import DataFrame
import time
from prefect import flow, task

from flow_pars_hh_dir.utilits.connect_database import get_data, put_data

# URL для получения курсов валют
url = 'https://www.cbr.ru/scripts/XML_daily.asp'


@task(log_prints=True)
def get_rates_cb(max_retries=10, wait_time=10) -> DataFrame:
    """Функция получает курсы валют с сайта ЦБ.
    :param max_retries: Максимальное количество попыток запроса
    :param wait_time: Время ожидания между попытками (в секундах)
    :return: df_rates: курсы валют. USD, EUR
    """
    attempt = 0
    while attempt < max_retries:
        try:
            # Получение данных с сайта ЦБ
            response = requests.get(url, timeout=10)
            response.raise_for_status()

            # Парсинг XML
            root = ET.fromstring(response.content)

            # Инициализация списков для хранения данных
            dates = []
            currencies = []
            rates = []

            # Дата публикации курса (из атрибута 'Date' корневого элемента)
            date_published = root.attrib['Date']
            date_published = pd.to_datetime(date_published, format='%d.%m.%Y').strftime('%Y-%m-%d')

            # Список валют для получения курса
            target_currencies = {'USD', 'EUR'}

            # Парсинг данных
            for child in root.findall('Valute'):
                currency = child.find('CharCode').text
                if currency in target_currencies:
                    rate = float(child.find('Value').text.replace(',', '.')) / float(child.find('Nominal').text)
                    dates.append(date_published)
                    currencies.append(currency)
                    rates.append(rate)

            # Создание DataFrame
            df_rates = pd.DataFrame({
                'date': dates,
                'currency': currencies,
                'rate': rates
            })

            # Установка типов данных
            df_rates['date'] = pd.to_datetime(df_rates['date'])
            df_rates['currency'] = df_rates['currency'].astype('string')
            df_rates['rate'] = df_rates['rate'].astype('float')

            return df_rates

        except requests.RequestException as e:
            print("Ошибка при запросе данных: {}. Попытка {}/{}", e, attempt + 1, max_retries)
        except ET.ParseError as e:
            print("Ошибка при парсинге XML: {}. Попытка {}/{}", e, attempt + 1, max_retries)
        except Exception as e:
            print("Произошла ошибка: {}. Попытка {}/{}", e, attempt + 1, max_retries)

        attempt += 1
        time.sleep(wait_time)


@flow(name='get_rates', log_prints=True)
def flow_get_and_put_rates():
    """
    Функция проверяет что новых данных нет в базе и загружает их.
    :return:
    """
    # Загрузка курсов валют если опубликованы новые
    # Получение данных которые уже есть в таблице
    query = 'select "date" from core.rates'
    existing_rates = get_data(query)  # Даты записей присутствующих в базе

    new_rates = get_rates_cb()  # Записи полученные от Цб
    print(new_rates)

    # Фильтрация записей, которые уже присутствуют в базе
    result_rates = new_rates[~new_rates['date'].isin(existing_rates['date'])]

    # Загружаем ку
    if not result_rates.empty:
        put_data(result_rates, table_name='rates', schema='core', if_exists='append')
