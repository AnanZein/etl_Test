from prefect import flow, task
import pandas as pd


@task(retries=3)
def read_csv():
    df = pd.read_csv('India_Menu.csv')
    return df


####################################################################


@task(retries=3)
def transform_data(df):
    df['Menu Items'] = df['Menu Items'].str.lower()
    return df


####################################################################


@task(retries=3)
def load(df):
    df.to_csv('lowerCaseMenu.csv')
    print(df)

####################################################################


@flow
def indian_MAC_ETL():
    df = read_csv()
    transform_data(df)
    load(df)


####################################################################


# call the flow!
indian_MAC_ETL()
