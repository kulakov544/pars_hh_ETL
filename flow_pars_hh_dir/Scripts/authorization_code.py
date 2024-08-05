import webbrowser

client_id = 'VBJFATMNOEP5RRR2HBJDGALDB88644RS201DHH8JC6E998O21398MN9HFK93O7CM'
redirect_uri = 'https://kulakovav.ru/pars_hh'
auth_url = f'https://hh.ru/oauth/authorize?response_type=code&client_id={client_id}&redirect_uri={redirect_uri}'

# Откройте браузер для авторизации
webbrowser.open(auth_url)
