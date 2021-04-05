import gender_guesser.detector as gender
from kaggle import KaggleApi
import os
import pandas as pd
from pandas import DataFrame, read_sql, Series
import plotly.graph_objs as go
import re
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


def clean_data(df: DataFrame) -> DataFrame:
    df = df.copy()
    df = (
        df.assign(
            BasePay=pd.to_numeric(df.BasePay, errors='coerce'),
            OvertimePay=pd.to_numeric(df.OvertimePay, errors='coerce'),
            OtherPay=pd.to_numeric(df.OtherPay, errors='coerce'),
            Benefits=pd.to_numeric(df.Benefits, errors='coerce')
        ).drop(
            labels=['Agency', 'Notes'],
            axis=1
        )
    )
    df = get_first_name(df)
    df = get_gender(df)
    # Agency and Notes do not bring any value and therefore are dropped
    return df


def get_first_name(df: DataFrame):
    def support_function(fullname: str):
        names = re.findall(r'[\w]+', fullname)
        for name in names:
            if len(name) > 2:
                return name
        return names[0]
    df = df.copy()
    df['FirstName'] = df.EmployeeName.apply(support_function)
    return df


def get_gender(df: DataFrame) -> DataFrame:
    df = df.copy()
    g = gender.Detector(False)
    df['Gender'] = df.FirstName.apply(g.get_gender)
    return df


def print_gender_distribution(df: DataFrame, field: str):
    def define_histogram(data: Series, text: str, color: str):
        return go.Histogram(
            x=data,
            histnorm='probability density',
            name=text,
            text=text,
            xbins={'start': 0, 'end': data.max(), 'size': 5_000},
            opacity=0.5,
            marker={'color': color}
        )

    male: Series = df[df.Gender == 'male'][field]
    female: Series = df[df.Gender == 'female'][field]
    (go.Figure(
        data=[
            define_histogram(male, 'male', 'blue'),
            define_histogram(female, 'female', 'red'),
        ],
        layout=go.Layout(
            title=go.layout.Title(text=f'{field} distribution'),
            barmode='overlay'
        )
    ).show()
    )


if __name__ == '__main__':
    download_data(kaggle_dataset, 'data')
    sal: DataFrame = load_sqlite_data('data')
    sal = clean_data(sal)
    print_gender_distribution(sal, 'TotalPay')
    print()