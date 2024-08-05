import requests

# Укажите ваш токен доступа
access_token = 'USERT355KS4AEJROVH3QNE6ML3VT9AQQLFIVGQDK015JM94AD5SJ7LUTODEM2AK1'

# URL для запроса списка черновиков вакансий
vacancies_url = 'https://api.hh.ru/vacancies'

# Заголовки запроса с токеном доступа
headers = {
    'Authorization': f'Bearer {access_token}'
}

# Параметры запроса для получения черновиков
params = {
    'status': 'draft',  # Фильтр для получения черновиков
    'per_page': 100,  # Количество результатов на страницу
    'page': 0  # Номер страницы
}

# Получение списка черновиков вакансий
response = requests.get(vacancies_url, headers=headers, params=params)

if response.status_code == 200:
    vacancies_data = response.json()
    drafts = vacancies_data.get('items', [])

    # Удаление черновиков
    for vacancy in drafts:
        vacancy_id = vacancy['id']
        delete_url = f'https://api.hh.ru/vacancies/{vacancy_id}'

        delete_response = requests.delete(delete_url, headers=headers)

        if delete_response.status_code == 204:
            print(f'Vacancy {vacancy_id} deleted successfully.')
        else:
            print(f'Failed to delete vacancy {vacancy_id}:', delete_response.status_code, delete_response.text)
else:
    print('Failed to fetch vacancies:', response.status_code, response.text)
