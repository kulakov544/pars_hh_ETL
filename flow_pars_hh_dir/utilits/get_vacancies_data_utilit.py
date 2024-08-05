import json
import requests
import pandas as pd
import time
from pandas import DataFrame
from datetime import datetime
import numpy as np
from prefect import task

from flow_pars_hh_dir.utilits.add_hash_to_df_utilit import add_hash_to_df
from flow_pars_hh_dir.config import access_token


def get_vacancy_details(vacancy_id: str) -> json:
    """
    Функция передает на сервер номер вакансии и получает информацию по ней
    :param vacancy_id: Номер вакансии
    :return: Словарь с данными о вакансии.
    """
    url = f'https://api.hh.ru/vacancies/{vacancy_id}'
    headers = {
        'Authorization': f'Bearer {access_token}'
    }

    response = requests.get(url, headers=headers)
    if response.status_code == 429:
        print(f"Rate limit exceeded for vacancy_id {vacancy_id}. Retrying after delay...")
        time.sleep(60)  # Увеличьте время задержки при получении ошибки 429
        response = requests.get(url, headers=headers)

    if response.status_code != 200:
        if response.status_code == 403 and response.json().get('value') == 'vacancy_draft_limit_exceeded':
            print(f"Draft limit exceeded for vacancy_id {vacancy_id}. Skipping...")
            return None
        print(f"Error: Received status code {response.status_code} for vacancy_id {vacancy_id}")
        return None
    return response.json()

@task
def get_vacancies_data(vacancies_id: DataFrame):
    """
    Функция Формирует датафрейм с данными о вакансии
    :param vacancies_id: Список номеров вакансий
    :return: DataFrame с данными о вакансиях и DataFrame с данными о навыках.
    """
    print('Получение данных по вакансиям')

    all_vacancies_data = []
    all_vacancies_skill = []

    count = 0
    for vacancy_id in vacancies_id['vacancy_id']:
        print(f'Получено вакансий: {count}/{len(vacancies_id)}')
        count += 1
        vacancy_data = get_vacancy_details(vacancy_id)
        time.sleep(0.1)
        format_date = '%Y-%m-%dT%H:%M:%S%z'
        if vacancy_data:
            # Извлечение нужных данных из ответа
            vacancy_info = {
                'id': str(vacancy_data.get('id')),
                'name': vacancy_data.get('name', ''),
                'area': vacancy_data.get('area', {}).get('name', ''),
                'alternate_url': vacancy_data.get('alternate_url', ''),
                'approved': vacancy_data.get('approved', False),
                'archived': vacancy_data.get('archived', False),
                'description': vacancy_data.get('description', ''),
                'employer_name': vacancy_data.get('employer', {}).get('name', '') if vacancy_data.get('employer') else '',
                'employer_url': vacancy_data.get('employer', {}).get('alternate_url', '') if vacancy_data.get('employer') else '',
                'employment': vacancy_data.get('employment', {}).get('name', '') if vacancy_data.get('employment') else '',
                'experience': vacancy_data.get('experience', {}).get('name', '') if vacancy_data.get('experience') else '',
                'has_test': vacancy_data.get('has_test', False),
                'initial_created_at': datetime.strptime(vacancy_data.get('initial_created_at', ''), format_date),
                'premium': vacancy_data.get('premium', False),
                'published_at': datetime.strptime(vacancy_data.get('published_at', ''), format_date),
                'created_at': datetime.strptime(vacancy_data.get('created_at', ''), format_date),
                'professional_roles': vacancy_data.get('professional_roles', [{}])[0].get('name', '') if vacancy_data.get('professional_roles') else '',
                'working_days': vacancy_data.get('working_days', [{}])[0].get('name', '') if vacancy_data.get('working_days') else '',
                'working_time_intervals': vacancy_data.get('working_time_intervals', [{}])[0].get('name', '') if vacancy_data.get('working_time_intervals') else '',
                'working_time_modes': vacancy_data.get('working_time_modes', [{}])[0].get('name', '') if vacancy_data.get('working_time_modes') else '',
                'salary_from': vacancy_data.get('salary').get('from', np.nan) if vacancy_data.get('salary') else 0,
                'salary_to': vacancy_data.get('salary').get('to', np.nan) if vacancy_data.get('salary') else 0,
                'salary_currency': vacancy_data.get('salary', {}).get('currency', '') if vacancy_data.get('salary') else '',
                'schedule': vacancy_data.get('schedule', {}).get('name', '') if vacancy_data.get('schedule') else '',
                'address_city': vacancy_data.get('address', {}).get('city', '') if vacancy_data.get('address') else '',
                'address_street': vacancy_data.get('address', {}).get('street', '') if vacancy_data.get('address') else '',
                'address_lat': vacancy_data.get('address').get('lat', np.nan) if vacancy_data.get('address') else 0,
                'address_lng': vacancy_data.get('address').get('lng', np.nan) if vacancy_data.get('address') else 0
            }

            for skill in vacancy_data.get('key_skills', []):
                vacancies_skill = {
                    'id': vacancy_data.get('id'),
                    'skill': skill['name']
                }
                all_vacancies_skill.append(vacancies_skill)

            all_vacancies_data.append(vacancy_info)

    vacancies_df = pd.DataFrame(all_vacancies_data)
    vacancies_df.drop_duplicates(subset=['id'], inplace=True)
    add_hash_to_df(vacancies_df, 'vacancy_hash', 'utf-8')

    vacancies_skill_df = pd.DataFrame(all_vacancies_skill)

    # Преобразование типов
    # Преобразование типов и удаление информации о временных зонах
    vacancies_df['initial_created_at'] = pd.to_datetime(vacancies_df['initial_created_at']).dt.tz_localize(None)
    vacancies_df['published_at'] = pd.to_datetime(vacancies_df['published_at']).dt.tz_localize(None)
    vacancies_df['created_at'] = pd.to_datetime(vacancies_df['created_at']).dt.tz_localize(None)

    vacancies_df = vacancies_df.astype({
        'id': 'int',
        'name': 'str',
        'area': 'str',
        'alternate_url': 'str',
        'approved': 'bool',
        'archived': 'bool',
        'description': 'str',
        'employer_name': 'str',
        'employer_url': 'str',
        'employment': 'str',
        'experience': 'str',
        'has_test': 'bool',
        'initial_created_at': 'datetime64[ns]',
        'premium': 'bool',
        'published_at': 'datetime64[ns]',
        'created_at': 'datetime64[ns]',
        'professional_roles': 'str',
        'working_days': 'str',
        'working_time_intervals': 'str',
        'working_time_modes': 'str',
        'salary_from': 'float',
        'salary_to': 'float',
        'salary_currency': 'str',
        'schedule': 'str',
        'address_city': 'str',
        'address_street': 'str',
        'address_lat': 'float',
        'address_lng': 'float'
    })

    vacancies_skill_df = vacancies_skill_df.astype({
        'id': 'int',
        'skill': 'str'
    })

    return vacancies_df, vacancies_skill_df
