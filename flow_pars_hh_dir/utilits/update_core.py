import pandas as pd

from flow_pars_hh_dir.utilits.connect_database import execute_stmt, put_data, get_data


def update_core():
    # Обновление столбца статуса
    sqlt_stmt = "select core.add_status();"
    execute_stmt(sqlt_stmt)

    # Обновление справочников
    sqlt_stmt = "select core.update_core_ref();"
    execute_stmt(sqlt_stmt)

    # Добавление новых записей
    sqlt_stmt = "SELECT core.add_fact_vacancy_0();"
    execute_stmt(sqlt_stmt)

    # Перенос записей в историю
    sqlt_stmt = "SELECT core.update_fact_history();"
    execute_stmt(sqlt_stmt)

    # Обновление записей
    sqlt_stmt = "SELECT core.add_fact_vacancy_2();"
    execute_stmt(sqlt_stmt)

    # Обновление записей
    sqlt_stmt = "select core.add_skill();"
    execute_stmt(sqlt_stmt)


