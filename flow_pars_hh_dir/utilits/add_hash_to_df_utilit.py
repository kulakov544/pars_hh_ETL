import hashlib

from pandas import DataFrame


def add_hash_to_df(df: DataFrame,
                   hash_column_name: str = 'vacancy_hash',
                   encoding: str = 'utf-8') -> DataFrame:
    """
    Функция генерирует хеш по вакансиям.
    :param df: Дата фрейм
    :param hash_column_name: имя нового столбца с хешем
    :param encoding: кодировка
    :return: дата фрейм со столбцом хешей
    """

    def row_to_hash(row):
        # Преобразуем строку в строковое представление и кодируем в байты
        row_str = str(row.to_dict())
        row_bytes = row_str.encode(encoding)
        # Генерируем хеш
        hash_object = hashlib.sha256(row_bytes)
        return hash_object.hexdigest()

    df[hash_column_name] = df.apply(row_to_hash, axis=1)
    return df
