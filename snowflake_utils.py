import json, os, pickle, datetime
import pandas as pd
import csv

import sqlalchemy
from sqlalchemy import create_engine
from snowflake_utils.sqlalchemy import URL


# TODO: stop using engine for everything and share connection. Maybe put whole thing into a class and handle context and connection closing/failure that way
# TODO: consistent way to deal with whether db/schema is in the engine/connection or fully qualified table names are expected

def df_to_snowflake(data_frame,
                        engine,
                        table_name,
                        reserved_words_list,
                        # stage_name,
                        truncate=True,
                        create=False,
                        drop=False,
                        reserved_words=None):
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S%MS%f")
    file_name = f"temp/{table_name}_{timestamp}.csv"
    file_path = os.path.abspath(file_name)
    data_frame.to_csv(file_path, index=False, header=False, quoting=csv.QUOTE_ALL)

    with engine.connect() as con:

        if drop:
            con.execute(f"DROP TABLE IF EXISTS {table_name}")

        if create:
            reserved_words_used = [column for column in data_frame.columns if column.upper() in reserved_words_list]
            if len(reserved_words_used) > 0:
                print(reserved_words_used)
                new_columns = ['_' + column for column in reserved_words_used]
                rename_dict = dict(zip(reserved_words_used, new_columns))
                data_frame = data_frame.rename(columns=rename_dict)

            new_cols = data_frame.columns.copy()
            new_cols = new_cols.str.replace(' ', '-')
            new_cols = new_cols.str.lower()
            new_cols = new_cols.str.replace('-', '_')
            new_cols = new_cols.str.replace('[^\w]', '')
            data_frame = data_frame.rename(columns=dict(zip(data_frame.columns, new_cols)))
            schema = pd.io.sql.get_schema(frame=data_frame, name=table_name)
            schema = schema.replace(f'CREATE TABLE "{table_name}"', f'CREATE TABLE IF NOT EXISTS {table_name}')
            schema = schema.replace('"', '')

            con.execute(schema)

        if truncate:
            con.execute(f"TRUNCATE TABLE {table_name}")

        con.execute(f"PUT file://{file_path}* @%{table_name}")
        con.execute(f"""COPY INTO {table_name} FROM @%{table_name}	FILE_FORMAT = (
					TYPE = 'CSV'
					FIELD_DELIMITER = ','
					SKIP_HEADER = 0,				
					FIELD_OPTIONALLY_ENCLOSED_BY = '"',
                    EMPTY_FIELD_AS_NULL = TRUE,
                    NULL_IF = ('', ' ')
				)""")


def table_to_df(engine, table_name):
    with engine.connect() as con:
        df = pd.read_sql(con=con, sql=f'select * from {table_name}')
    return df


def sql_to_df(engine, sql):
    with engine.connect() as con:
        df = pd.read_sql(con=con, sql=sql)
    return df


def create_sf_engine(engine_config):
    url = URL(**engine_config)
    engine = create_engine(url)
    return engine


def get_column_names(engine, table):
    with engine.connect() as con:
        columns = pd.read_sql(f'show columns in {table}')['column_name']
    return columns


def add_checksum(engine, table, columns=None):
    if not columns:
        columns = get_column_names(engine, table)
    columns = [f"coalesce({column}, '_')" for column in columns]
    concat_columns = '||'.join(columns)
    query = f'select MD5(f{concat_columns}) from f{table}'
    with engine.connect() as con:
        con.execute(f'alter table {table} add checksum varchar(64)')
        con.execute(f'update {table} set checksum = ({query})')
    return


def add_update_date(engine, table):
    with engine.connect() as con:
        con.execute(f'alter table {table} add last_updated timestamp')
        con.execute(f'update {table} set last_updated = current_timestamp() ')
    return
