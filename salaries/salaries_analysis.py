from kaggle import KaggleApi
import os
from pandas import DataFrame, read_sql
from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import Engine, Inspector

kaggle_dataset = 'kaggle/sf-salaries'


def download_data(dataset: str, path: str = None) -> None:
    kg: KaggleApi = KaggleApi()
    kg.authenticate()
    kg.dataset_download_files(
        dataset=dataset,
        path=path,
        unzip=True
    )


def get_sqlite_engine(file: str) -> DataFrame:
    return create_engine(f'sqlite:///{file}')


def get_table_name(engine: Engine):
    ins: Inspector = inspect(engine)
    return ins.get_table_names()[0]


def load_sqlite_data(path: str) -> DataFrame:
    file: str
    df: DataFrame = None
    for file in os.listdir(path):
        if file.endswith('.sqlite'):
            engine = get_sqlite_engine(os.path.join(path, file))
            table_name = get_table_name(engine)
            df = read_sql(
                sql=f'''
                select *
                from {table_name}
                ''',
                con=engine
            )
    return df


if __name__ == '__main__':
    download_data(kaggle_dataset, 'data')
    sal: DataFrame = load_sqlite_data('data')