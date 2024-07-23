import pandas as pd
from prefect import task

from flow_pars_hh_dir.utilits.connect_database import execute_stmt, put_data, get_data
from flow_pars_hh_dir.utilits.get_rates_utilit import get_rates


@task(log_prints=True)
def update_core():
    # Обновление справочников
    sqlt_stmt = "SELECT core.update_core_ref();"
    execute_stmt(sqlt_stmt)

    # Обновление fact_vacancy
    sqlt_stmt = "SELECT core.update_fact_vacancy();"
    execute_stmt(sqlt_stmt)

    # Загрузка курсов валют если опубликованы новые
    query = 'select "date", currency, rate from core.rates'
    existing_rates = get_data(query)
    new_rates = get_rates()

    # Объединение DataFrame'ов с индикатором
    merged = pd.merge(new_rates, existing_rates, on=['date', 'currency', 'rate'], how='left', indicator=True)

    # Фильтрация записей, которые присутствуют только в rates
    result_rates = merged[merged['_merge'] == 'left_only'].drop(columns=['_merge'])

    # Загружаем курсов валют если они получены.
    if not result_rates.empty:
        put_data(result_rates, table_name='rates', schema='core', if_exists='append')
