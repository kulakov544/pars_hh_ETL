import pandas as pd
import requests
from pandas import DataFrame
import time
from prefect import task

from flow_pars_hh_dir.utilits.add_hash_to_df_utilit import add_hash_to_df


@task(log_prints=True)
def get_vacancies(all_params: list) -> DataFrame:
    """
    Функция получает JSON с данными о вакансиях.
    :param all_params: Список параметров для запроса.
    :return: DataFrame с данными о вакансиях.
    """
    url = "https://api.hh.ru/vacancies"
    all_vacancies_df = pd.DataFrame()

    for params in all_params:
        while True:
            try:
                response = requests.get(url, params=params)
                response.raise_for_status()  # Возбуждает исключение для ошибок HTTP
                data = response.json()
                print(f"Собрано {len(data.get("items", []))} вакансий")
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
                        'vacancy_id': int(v.get('id')),
                        'premium': bool(v.get('premium')),
                        'name': str(v.get('name')),
                        'department': str(v.get('department')["name"] if v.get('department') is not None else None),
                        'has_test': bool(v.get('has_test')),
                        'response_letter_required': bool(v.get('response_letter_required')),
                        'area': str(v.get('area')['name']),
                        'salary_from': int(
                            v.get('salary')['from'] if v.get('salary') and v.get('salary')['from'] is not None else 0),
                        'salary_to': int(
                            v.get('salary')['to'] if v.get('salary') and v.get('salary')['to'] is not None else 0),
                        'salary_currency': str(v.get('salary')["currency"] if v.get('salary') and v.get('salary')[
                            'currency'] is not None else ''),
                        'type': str(v.get('type')['name']),
                        'response_url': str(v.get('response_url')),
                        'sort_point_distance': str(v.get('sort_point_distance')),
                        'published_at': str(v.get('published_at')),
                        'created_at': str(v.get('created_at')),
                        'archived': bool(v.get('archived')),
                        'apply_alternate_url': str(v.get('apply_alternate_url')),
                        'show_logo_in_search': bool(v.get('show_logo_in_search')),
                        'url': str(v.get('url')),
                        'alternate_url': str(v.get('alternate_url')),
                        'employer_name': str(v.get('employer')["name"]),
                        'employer_url': str(v.get('employer', {}).get("alternate_url", '')),
                        'snippet_requirement': str(v.get('snippet')["requirement"]),
                        'snippet_responsibility': str(v.get('snippet')["responsibility"]),
                        'contacts': str(v.get('contacts')),
                        'schedule': str(v.get('schedule')["name"]),
                        "working_days": str(v.get('working_days')[0]["name"] if v.get('working_days') and len(
                            v.get('working_days')) > 0 else None),
                        "working_time_intervals": str(v.get('working_time_intervals')[0]["name"] if v.get(
                            'working_time_intervals') and len(
                            v.get('working_time_intervals')) > 0 else None),
                        "working_time_modes": str(
                            v.get('working_time_modes')[0]["name"] if v.get('working_time_modes') and len(
                                v.get('working_time_modes')) > 0 else None),
                        'accept_temporary': bool(v.get('accept_temporary')),
                        'professional_roles': str(v.get('professional_roles')[0]["name"]),
                        'accept_incomplete_resumes': bool(v.get('accept_incomplete_resumes')),
                        'experience': str(v.get('experience')["name"]),
                        'employment': str(v.get('employment')["name"]),
                    }
                    vacancies_data.append(vacancy)
                except Exception as e:
                    print(f"Ошибка обработки данных вакансии: {e}")

            if not vacancies_data:
                print("Нет данных о вакансиях для добавления.")
                break

            vacancies_df = pd.DataFrame(vacancies_data)
            all_vacancies_df = pd.concat([all_vacancies_df, vacancies_df], ignore_index=True)

            if params.get("page", 0) >= data.get("pages", 0) - 1:
                break

            params["page"] = params.get("page", 0) + 1
            time.sleep(0.1)  # Задержка между страницами

        time.sleep(0.3)  # Задержка между наборами параметров

    # Удаление дубликатов
    all_vacancies_df.drop_duplicates(subset=['vacancy_id'], inplace=True)

    # Конвертация даты
    all_vacancies_df['published_at'] = pd.to_datetime(all_vacancies_df['published_at'], format='%Y-%m-%dT%H:%M:%S%z',
                                                      errors='coerce')
    all_vacancies_df['created_at'] = pd.to_datetime(all_vacancies_df['created_at'], format='%Y-%m-%dT%H:%M:%S%z',
                                                    errors='coerce')

    # Добавление хеша
    all_vacancies_df = add_hash_to_df(all_vacancies_df)

    return all_vacancies_df
