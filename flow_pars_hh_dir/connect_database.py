from typing import Literal
from sqlalchemy import create_engine, text
import pandas as pd
import os

from dotenv import load_dotenv

load_dotenv()
conn_string = os.getenv("conn_string")
engine = create_engine(conn_string)


def execute_stmt(sqlt_stmt: str) -> bool:
    """Функция, которая отправляет sql запрос базе данных и возвращает True или ошибку
        :param sqlt_stmt: запрос к базе
    """
    try:
        conn = engine.connect()
    except Exception as e:
        raise Exception(f'Невозможно установить соединение с сервером: {str(e)}')
    else:
        try:
            conn.execute(text(sqlt_stmt))
            conn.commit()
            return True
        except Exception as e:
            raise e
        finally:
            conn.close()
            engine.dispose()


def get_data(query: str) -> pd.DataFrame:
    """Функция, которая отправляет sql запрос базе данных БД, возвращает результат select запроса
        :param query: Запрос к базе
    """
    try:
        conn = engine.connect()
    except Exception as e:
        raise Exception(f'Невозможно установить соединение с сервером: {str(e)}')
    else:
        try:
            df = pd.read_sql(query, conn)
            return df
        except Exception as e:
            raise e
        finally:
            conn.close()
            engine.dispose()


def put_data(df: pd.DataFrame,
             table_name: str,
             schema: str,
             if_exists: Literal["fail", "replace", "append"] = "append") -> bool:
    """Функция, которая загружает дата фрейм в БД, возвращает bool
        :param schema: схема
        :param table_name: название таблицы
        :param if_exists: метод загрузки
        :param df: дата фрейм для записи
    """
    try:
        conn = engine.connect()
    except Exception as e:
        raise Exception(f'Невозможно установить соединение с сервером: {str(e)}')
    else:
        try:
            df.to_sql(name=table_name, con=conn, schema=schema, if_exists=if_exists, index=False)
            return True
        except Exception as e:
            raise e
        finally:
            conn.close()
            engine.dispose()
