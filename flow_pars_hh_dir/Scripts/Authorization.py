import requests
from requests.auth import HTTPBasicAuth

# Ваши учетные данные
client_id = 'VBJFATMNOEP5RRR2HBJDGALDB88644RS201DHH8JC6E998O21398MN9HFK93O7CM'
client_secret = 'TO1AFO94P8RM0MIR607ONH9M8Q1RT7P5SJFJV98KFU7TNLJ46R4PE5D9MC0K72M3'
redirect_uri = 'http://localhost:8000/'
authorization_code = 'J2EFJNC103NT16KM9C1OI0M02RP4PE2V3MI3IM4MLF5LI7TG39IIQA5C0KFA93PD'

# Получение токена доступа
token_url = 'https://hh.ru/oauth/token'
token_data = {
    'grant_type': 'client_credentials',
    'client_id': client_id,
    'client_secret': client_secret
}

response = requests.post(token_url, data=token_data)

if response.status_code == 200:
    tokens = response.json()
    access_token = tokens['access_token']
    refresh_token = tokens['refresh_token']
    print('Access Token:', access_token)
    print('Refresh Token:', refresh_token)
else:
    print('Failed to obtain token:', response.status_code, response.text)

# Пример использования access_token для выполнения запроса к API
api_url = 'https://api.hh.ru/me'
headers = {
    'Authorization': f'Bearer {access_token}'
}

response = requests.get(api_url, headers=headers)

if response.status_code == 200:
    user_data = response.json()
    print('User Data:', user_data)
else:
    print('Failed to fetch user data:', response.status_code, response.text)
