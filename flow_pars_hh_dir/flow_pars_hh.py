from prefect import flow

from flow_pars_hh_dir.crud import insert_stage, update_core
from flow_pars_hh_dir.get_vacancies_utilit import get_vacancies


@flow(name='pars_hh', log_prints=True)
def flow_pars_hh():
    try:
        # ID больших городов России
        big_cities_ids = [1, 2, 1202, 3, 66, 88, 104, 78, 99, 76, 68, 54, 26, 72, 24, 53, 45, 34, 22, 29, 15, 67, 77, 90,
                      91, 92, 93, 94, 95, 96, 97, 98, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112,
                      113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131,
                      132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150,
                      151, 152, 153, 154, 155, 156, 157, 158, 159, 160, 161, 162, 163, 164, 165, 166, 167, 168, 53, 113]
        text_search = ['python', 'программист', 'аналитик', 'программист python']

    # Список параметров поиска
        search_params_list = []

        text_search_length = len(text_search)

        for i, city_id in enumerate(big_cities_ids):
            search_text = text_search[i % text_search_length]
            search_params_list.append(
                {"text": search_text, "area": city_id, "per_page": 100, "page": 0}
            )

        # Создание датафрейма
        print('Начало сбора вакансий')

        vacancies_df = get_vacancies(search_params_list)
        print(f"Всего собрано {len(vacancies_df)} вакансий")
    except Exception as e:
        print(e)
    else:
        try:
            # Название таблицы
            table_name = "stage_pars_hh"
            schema = "stage"

            # Загрузка данных в stage
            print('Загрузка данных в stage')
            insert_stage(vacancies_df, table_name, schema, if_exists='replace')
        except Exception as e:
            print(e)
        else:
            try:
                # Обновление core
                print("Перенос данных в core")
                update_core()

                print(f"Собрано {len(vacancies_df)} вакансий и сохранено в базу данных")
            except Exception as e:
                print(e)
            else:
                print('Скрипт завершен.')
