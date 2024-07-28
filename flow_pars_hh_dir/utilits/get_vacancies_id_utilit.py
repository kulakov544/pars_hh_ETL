import pandas as pd
import requests
from pandas import DataFrame
import time
import random


def get_vacancies_id(all_params: list) -> DataFrame:
    """
    Функция составляет список id вакансий.
    :param all_params: Список параметров для запроса.
    :return: DataFrame с id вакансий
    """
    url = "https://api.hh.ru/vacancies"
    all_vacancies_id_df = pd.DataFrame()
    count_param = 0
    for params in all_params:
        count_param += 1
        print(f'Обработано наборов параметров: {count_param}/{len(all_params)}')
        print(f"Собрано {len(all_vacancies_id_df)} id вакансий")
        while True:
            try:
                response = requests.get(url, params=params)
                response.raise_for_status()  # Возбуждает исключение для ошибок HTTP
                data = response.json()

            except requests.RequestException as e:
                print(f"Ошибка запроса: {e}")
                break
            except ValueError as e:
                print(f"Ошибка декодирования JSON: {e}")
                break

            if "items" not in data:
                print("В ответе нет 'items'.")
                break

            vacancies_data = []
            for v in data.get("items", []):
                try:
                    vacancy = {
                        'vacancy_id': int(v.get('id'))
                    }
                    vacancies_data.append(vacancy)
                except Exception as e:
                    print(f"Ошибка обработки данных id вакансии: {e}")

            if not vacancies_data:
                print("Нет данных о id для добавления.")
                break

            vacancies_df = pd.DataFrame(vacancies_data)
            all_vacancies_id_df = pd.concat([all_vacancies_id_df, vacancies_df], ignore_index=True)

            if params.get("page", 0) >= data.get("pages", 0) - 1:
                break

            params["page"] = params.get("page", 0) + 1
            time.sleep(0.1)  # Задержка между страницами

        time.sleep(0.3)  # Задержка между наборами параметров

    # Удаление дубликатов
    all_vacancies_id_df.drop_duplicates(subset=['vacancy_id'], inplace=True)

    return all_vacancies_id_df
