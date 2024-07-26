from prefect import flow
import pandas as pd

from flow_pars_hh_dir.utilits.update_core import update_core
from flow_pars_hh_dir.utilits.connect_database import put_data
from flow_pars_hh_dir.utilits.get_vacancies_utilit import get_vacancies


def chunk_list(lst, chunk_size):
    """Разбить список на подсписки фиксированного размера."""
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]


@flow(name='pars_hh', log_prints=True)
def flow_pars_hh():
    try:
        # ID больших городов России
        big_cities_ids = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 113]
        text_search = [
            'python',
            'программист',
            'аналитик',
            'программист python',
            'веб-разработчик',
            'разработчик мобильных приложений',
            'системный администратор',
            'аналитик данных',
            'инженер по тестированию',
            'devops инженер',
            'сетевой инженер',
            'архитектор программного обеспечения',
            'системный аналитик',
            'менеджер проектов в IT',
            'инженер по информационной безопасности',
            'специалист по технической поддержке',
            'администратор баз данных',
            'разработчик игр',
            'data scientist',
            'машинное обучение инженер',
            'bi разработчик',
            'frontend разработчик',
            'backend разработчик',
            'fullstack разработчик',
            'scrum-мастер',
            'product owner',
            'руководитель разработки',
            'инженер по автоматизации тестирования',
            'специалист по кибербезопасности',
            'сетевой администратор',
            'it-консультант',
            'системный архитектор',
            'бизнес-аналитик в IT',
            'инженер по облачным технологиям',
            'инженер по big data',
            'разработчик api',
            'it-аудитор',
            'инженер по devsecops',
            'разработчик микросервисов',
            'инженер по поддержке пользователей',
            'DevOps',
            'DevSecOps',
            'android',
            'application security',
            'c#',
            'cross platform',
            'data endineer',
            'data quality',
            'data scientist',
            'data аналитик',
            'dba',
            'dwh',
            'etl',
            'frontend',
            'forensic',
            'go',
            'ios',
            'java',
            'machine learning',
            'penetration testing',
            'php',
            'qa-менеджер',
            'ruby',
            'sql',
            'sre',
            'ручное тестирование',
            'сетевой инженер',
            'втоматизированное тестирование',
            'архитектор ИБ',
            'бизнес аналитик',
            'бумажная безопасность',
            'нагрузочное тестирование',
            'системный аналитик'
        ]
        specialization = [1, 2, 3, 4, 5]

        # Список параметров поиска
        search_params_list = []

        for city_id in big_cities_ids:
            for search_text in text_search:
                for specializ in specialization:
                    search_params_list.append(
                        {"text": search_text, "area": city_id, "per_page": 100, "page": 0, 'specialization': specializ}
                    )

        # Создание датафрейма
        print('Начало сбора вакансий')

        # Обработка параметров поиска пакетами по 500 штук
        for search_params_chunk in chunk_list(search_params_list, 500):
            try:
                vacancies_df = get_vacancies(search_params_chunk)
                print(f"Собрано {len(vacancies_df)} вакансий в текущем пакете")

                if not vacancies_df.empty:
                    try:
                        # Название таблицы
                        table_name = "stage_pars_hh"
                        schema = "stage"

                        # Загрузка данных в stage
                        print('Загрузка данных в stage')
                        put_data(vacancies_df, table_name, schema, 'replace')
                    except Exception as e:
                        print(f"Ошибка при загрузке данных в stage: {e}")
                    else:
                        try:
                            # Обновление core
                            print("Перенос данных в core")
                            update_core()

                            print(f"Собрано {len(vacancies_df)} вакансий и сохранено в базу данных")
                        except Exception as e:
                            print(f"Ошибка при обновлении core: {e}")
                else:
                    print("В текущем пакете нет вакансий")

            except Exception as e:
                print(f"Ошибка при сборе вакансий: {e}")

    except Exception as e:
        print(f"Ошибка в основном потоке: {e}")
    else:
        print('Скрипт завершен.')
