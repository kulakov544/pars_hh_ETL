import json
import random
import requests
import pandas as pd
import time

from pandas import DataFrame


def get_vacancy_details(vacancy_id: DataFrame) -> json:
    """
    Функция передает на сервер номер вакансии и получает информацию по ней
    :param vacancy_id: Список номеров вакансий
    :return: DataFrame с данными о вакансиях.
    """
    url = f'https://api.hh.ru/vacancies/{vacancy_id}'
    headers = {
        'Authorization': 'Bearer APPLSIK3FUVU3482R9NKFS8A8KD4R6P4AD911U6TGIV2Q91ROD0M49PR7AIRR56E'
    }

    response = requests.get(url, headers=headers)
    data = response.json()
    if response.status_code == 429:
        print(f"Rate limit exceeded for vacancy_id {vacancy_id}. Retrying after delay...")
        time.sleep(60)  # Увеличьте время задержки до 60 секунд при получении ошибки 429
        response = requests.get(url, headers=headers)

    if response.status_code != 200:
        if response.status_code == 403 and response.json().get('value') == 'vacancy_draft_limit_exceeded':
            print(f"Draft limit exceeded for vacancy_id {vacancy_id}. Skipping...")
            return None
        print(f"Error: Received status code {response.status_code} for vacancy_id {vacancy_id}")
        return None
    return data


def get_vacancies_data(vacancies_id: DataFrame):
    """
    Функция Формирует датафрейм с данными о вакансии
    :param vacancies_id: Список номеров вакансий
    :return: DataFrame с данными о вакансиях.
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
        if vacancy_data:
            # Извлечение нужных данных из ответа
            vacancy_info = {
                'id': vacancy_data.get('id'),
                'name': vacancy_data.get('name'),
                'area': vacancy_data.get('area', {}).get('name'),
                'alternate_url': vacancy_data.get('alternate_url'),
                'approved': vacancy_data.get('approved'),
                'archived': vacancy_data.get('archived'),
                'description': vacancy_data.get('description'),
                'employer_name': vacancy_data.get('employer', {}).get('name'),
                'employer_url': vacancy_data.get('employer', {}).get('alternate_url'),
                'employment': vacancy_data.get('employment', {}).get('name'),
                'experience': vacancy_data.get('experience', {}).get('name'),
                'has_test': vacancy_data.get('has_test'),
                'initial_created_at': vacancy_data.get('initial_created_at'),
                'premium': vacancy_data.get('premium'),
                'published_at': vacancy_data.get('published_at'),
                'salary_from': vacancy_data.get('salary', {}).get('from') if vacancy_data.get('salary') is not None else 0,
                'salary_to': vacancy_data.get('salary', {}).get('to') if vacancy_data.get('salary') is not None else 0,
                'salary_currency': vacancy_data.get('salary', {}).get('currency') if vacancy_data.get('salary') is not None else 0,
                'schedule': vacancy_data.get('schedule', {}).get('name'),
                'address_city': vacancy_data.get('address', {}).get('city') if vacancy_data.get('address') is not None else 0,
                'address_street': vacancy_data.get('address', {}).get('street') if vacancy_data.get('address') is not None else 0,
                'address_lat': vacancy_data.get('address', {}).get('lat') if vacancy_data.get('address') is not None else 0,
                'address_lng': vacancy_data.get('address', {}).get('lng') if vacancy_data.get('address') is not None else 0
            }

            for skill in vacancy_data.get('key_skills', []):  # Ensure 'key_skills' is not None
                vacancies_skill = {
                    'id': vacancy_data.get('id'),
                    'skill': skill['name']
                }
                all_vacancies_skill.append(vacancies_skill)

            all_vacancies_data.append(vacancy_info)

    vacancies_df = pd.DataFrame(all_vacancies_data)
    vacancies_skill_df = pd.DataFrame(all_vacancies_skill)

    return vacancies_df, vacancies_skill_df

    # vacancies_df.to_csv('vacancies_data.csv', index=False)  # Сохранение DataFrame в CSV файл
    # print("DataFrame saved to vacancies_data.csv")
    #
    # vacancies_skill_df.to_csv('vacancies_skill.csv', index=False)  # Сохранение DataFrame в CSV файл
    # print("DataFrame saved to vacancies_skill.csv")
