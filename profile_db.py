import json, pickle
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy import MetaData
from eralchemy import render_er


def soft_string_to_numeric(series):
    if pd.api.types.is_string_dtype(series.dtype):
        try:
            series = pd.to_numeric(series)
            return series
        except:
            return series

    else:
        return series


def fallback_datetime(value):
    if value > 10 ** 9 and value < 10 ** 10:
        return pd.to_datetime(value, unit='s')
    if value >= 10 ** 10 and value < 10 ** 13:
        return pd.to_datetime(value, unit='ms')
    if value >= 10 ** 13 and value < 10 ** 16:
        return pd.to_datetime(value, unit='us')
    if value >= 10 ** 16 and value < 10 ** 19:
        return pd.to_datetime(value, unit='ns')


def convert_numeric_to_date(series):
    if series.name != 'id' and series.name.find('_id') == -1 and pd.api.types.is_numeric_dtype(
            series.dtype) and series.min() > 10 ** 9:
        try:
            series = pd.to_datetime(series, unit='s')
        except pd.errors.OutOfBoundsDatetime as e:
            print(f"error in field: {series.name}. Error: {e}. /n Attempting fallback value by value conversion")
            series = series.apply(fallback_datetime)
    return series

def load_db_to_memory(db_url=None, use_cache=False, cache_path=None):
    if use_cache:
        with open(cache_path, 'rb') as f:
            db_data = pickle.load(f)
    else:
        engine = create_engine(db_url)

        with  engine.connect() as conn:
            table_names = pd.read_sql(con=conn, sql='show tables')[:,0].values

            table_descriptions = []
            for table in table_names:
                table_desc = pd.read_sql(con=conn, sql=f'describe {table}')
                table_desc['table'] = table
                table_descriptions.append(table_desc)

            table_dictionary = {}
            for table in table_names:
                table_dictionary[table] = pd.read_sql(con=conn, sql=f'select * from {table}')

        db_data = dict(table_names=table_names, table_descriptions=table_descriptions,
                       table_dictionary=table_dictionary)
        with open(cache_path, 'wb') as f:
            pickle.dump(db_data, f)
    return db_data


def clean_tables(db_data):
    cleaned_table_dictionary = {}
    for k, v in db_data['table_dictionary'].items():
        print(k)
        v = v.convert_dtypes()
        v = v.apply(soft_string_to_numeric)
        v = v.apply(convert_numeric_to_date)
        cleaned_table_dictionary[k] = v
    return cleaned_table_dictionary

def create_pandas_data_dictionary(cleaned_table_dictionary):
    df_desc_dict = {}
    df_desc_list = []
    for k, v in cleaned_table_dictionary.items():
        dtype_df = v.dtypes.rename('pandas_dtype').to_frame()
        desc_df = v.describe(include='all').T
        desc_df = dtype_df.join(desc_df)
        desc_df['table_name'] = k
        desc_df = desc_df.reset_index().rename(columns={'index': 'field'}).set_index(['table_name', 'field'])
        df_desc_dict[k] = desc_df
        df_desc_list.append(desc_df)

    pandas_data_dictionary = pd.concat(df_desc_list)
    return pandas_data_dictionary

def create_sql_data_dictionary(db_data)
    mysql_data_dictionary = pd.concat(db_data['table_descriptions']).rename(
        columns={'table': 'table_name', 'Field': 'field'}).set_index(['table_name', 'field'])
    return mysql_data_dictionary

def create_erd_from_db_metadata(db_url, save_path):
    engine = create_engine(db_url)
    conn = engine.connect()
    metadata = MetaData()
    metadata.reflect(bind=engine)
    render_er(metadata, save_path)

def infer_erd_relationships_from_field_names(mysql_data_dictionary, erd_path, manual_string=None):
    mysql_descriptions = mysql_data_dictionary.reset_index()
    probable_fk = mysql_descriptions[mysql_descriptions['field'].str.contains('_id')]['field'].values

    # see if the prefix for that _id exists in table names, check those tables have id but not _id fields
    possible_pk_table_to_fk_relationships = {}
    for fk_name in probable_fk:
        fk_pattern = fk_name.replace('_id', '')
        possible_pk_tables = \
        mysql_descriptions[mysql_descriptions['table_name'].str.contains(f'(?<![a-z]){fk_pattern}')][
            'table_name'].unique()  # TODO use list of table names instead of df of fields
        possible_pk_table_df = mysql_descriptions[mysql_descriptions['table_name'].isin(possible_pk_tables)].copy()
        possible_pk_table_df['is_id_field'] = possible_pk_table_df['field'] == 'id'
        possible_pk_table_df['is_fk_id_field'] = possible_pk_table_df['field'] != fk_name
        possible_pk_table_agg = possible_pk_table_df.groupby('table_name').aggregate(
            has_id_field=('is_id_field', np.any), has_fk_id_field=('is_fk_id_field', np.all))
        possible_pk_table_agg = possible_pk_table_agg[
            np.logical_and(possible_pk_table_agg.has_id_field, possible_pk_table_agg.has_fk_id_field)]
        pk_table_names = possible_pk_table_agg.index.values
        if len(pk_table_names) > 0:
            possible_pk_table_to_fk_relationships[fk_name] = pk_table_names

    # limit to one possible table
    pk_table_to_fk_relationships = {}
    for k, v in possible_pk_table_to_fk_relationships.items():
        if len(v) == 1:
            pk_table_to_fk_relationships[k] = v[0]

    confirmed_fk = [k for k in pk_table_to_fk_relationships.keys()]

    tables_using_fk = {}
    for fk in confirmed_fk:
        tables_using_fk[fk] = mysql_descriptions[mysql_descriptions['field'] == fk]['table_name'].values

    table_relationships = {}
    for fk, pk_table in pk_table_to_fk_relationships.items():
        table_relationships[pk_table] = tables_using_fk[fk]

    # convert to markdown to add to erd markdown document
    relationship_string = '\n'
    for parent, children in table_relationships.items():
        for child in children:
            relationship_string += f'{parent} 1--* {child}\n'

    # Manually add relationships
    if manual_string:
        relationship_string += manual_string

    with open('erd_from_sqla.er', 'a') as f:
        f.write(relationship_string)

if __name__ == '__main__':
    config_path = 'config.json'
    with open(config_path, 'r') as f:
        config = json.load(f)
    db_data = load_db_to_memory(db_url=config['db_url'], cache_path='db_data.pkl')
    cleaned_table_dictionary = clean_tables(db_data)
    pandas_data_dictionary = create_pandas_data_dictionary(cleaned_table_dictionary)
    mysql_data_dictionary = create_sql_data_dictionary(db_data)

    data_dictionary = pandas_data_dictionary.join(mysql_data_dictionary)
    data_dictionary.to_excel('data_dictionary')
    create_erd_from_db_metadata(config['db_url'],'erd_from_sqla.er')
    infer_erd_relationships_from_field_names(mysql_data_dictionary,'erd_from_sqla.er')
    with open('cleaned_table_dictionary.pkl', 'wb') as f:
        pickle.dump(cleaned_table_dictionary, f)