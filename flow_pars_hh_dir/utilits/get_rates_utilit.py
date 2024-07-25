import requests
import pandas as pd
import xml.etree.ElementTree as ET
from pandas import DataFrame


# URL для получения курсов валют
url = 'https://www.cbr.ru/scripts/XML_daily.asp'


def get_rates() -> DataFrame:
    """

    :return:  df_rates: курсы валют. USD, EUR
    """
    try:
        # Получение данных с сайта ЦБ
        response = requests.get(url)
        response.raise_for_status()

        # Парсинг XML
        root = ET.fromstring(response.content)

        # Инициализация списков для хранения данных
        dates = []
        currencies = []
        rates = []

        # Дата публикации курса (из атрибута 'Date' корневого элемента)
        date_published = root.attrib['Date']

        # Преобразование даты в формат ГГГГ-ММ-ДД
        date_published = pd.to_datetime(date_published, format='%d.%m.%Y').strftime('%Y-%m-%d')

        # Список валют для получения курса
        target_currencies = {'USD', 'EUR', 'AZN', 'BYR', 'GEL', 'KGS', 'KZT', 'UZS'}

        # Парсинг данных
        for child in root.findall('Valute'):
            currency = child.find('CharCode').text
            if currency in target_currencies:
                rate = float(child.find('Value').text.replace(',', '.')) / float(child.find('Nominal').text)

                dates.append(date_published)
                currencies.append(currency)
                rates.append(rate)

        # Создание DataFrame
        data = {
            'date': dates,
            'currency': currencies,
            'rate': rates
        }
        df_rates = pd.DataFrame(data)

        # Установка типов данных
        df_rates['date'] = pd.to_datetime(df_rates['date'], format='%Y-%m-%d')
        df_rates['currency'] = df_rates['currency'].astype('string')
        df_rates['rate'] = df_rates['rate'].astype('float')

        # Вывод DataFrame
        return df_rates

    except requests.RequestException as e:
        print(f"Ошибка при запросе данных: {e}")
    except ET.ParseError as e:
        print(f"Ошибка при парсинге XML: {e}")
    except Exception as e:
        print(f"Произошла ошибка: {e}")
