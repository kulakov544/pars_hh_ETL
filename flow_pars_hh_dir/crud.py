from pandas import DataFrame
from prefect import task

from flow_pars_hh_dir.connect_database import execute_stmt, put_data


@task(log_prints=True)
def update_core():
    # Обновление справочников
    sqlt_stmt = "SELECT core.update_core_ref();"
    execute_stmt(sqlt_stmt)
    print('Обновление справочников завершено')

    # Обновление fact_vacancy
    sqlt_stmt = "SELECT core.update_fact_vacancy();"
    execute_stmt(sqlt_stmt)
    print('Обновление core завершено')


@task(log_prints=True)
def insert_stage(vacancies_df: DataFrame, table_name: str, schema: str, if_exists):
    put_data(vacancies_df, table_name, schema, if_exists)
    print('Данные загружены в базу')
