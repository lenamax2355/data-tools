from google.cloud import storage
import pandas as pd
import io, re, json
from snowflake_utils import df_to_snowflake, create_sf_engine
from collections import defaultdict
import gcs_utils

if __name__ == '__main__':

    config_path = 'gcs_config.json'
    with open(config_path, 'r') as f:
        config = json.load(f)

    org_prefix = config['org_prefix']
    client = storage.Client()
    result = client.list_blobs(config['base_url'])

    blobs = [blob for blob in result if '.csv' in blob.name]
    blob_names = [blob.name for blob in blobs]

    # TODO extend existing blob?
    report_names = set()
    blob_dict = {}
    table_names = set()
    table_groups = defaultdict(list)
    for blob in blobs:
        blob.name = blob.name
        if not '.csv' in blob.name:
            continue
        if report_name := re.search(f'([^\/]+)(?=.{org_prefix})', blob.name):
            report_name = report_name.group(0)
            report_names.add(report_name)
            # blob_dict[blob.name] = {}
            if dimension_name := re.search('(?<=\d{6}_)\w+', blob.name):
                blob.report_dimension = dimension_name.group(0)
            blob.report_date = re.search('\d{6}', blob.name).group(0)
            blob.other_dimension = re.search('\w+(?=_\d{6})', blob.name).group(0)
            table_name = re.search('(?<=\/)[^\/]+(?=\.csv)', blob.name).group(0)
            table_name = re.sub('_\d{6}', '', table_name)
            table_name = re.sub('\.', '_', table_name)
            blob.report_dest_raw_table = table_name
            table_names.add(table_name)
            table_groups[table_name].append(blob)


    # concatenates all csvs feeding a table first to better detect dtypes. Only issue is this currently leads to loss of source file info. Could either manually add or do a roundtrip to pandas for each. Might wan
    # might want to concatentate to get types, then load and append source for each file?
    for table_group, blobs in table_groups.items():
        csv_string = None
        for blob in blobs:
            blob_string = gcs_utils.string_from_gcs_blob(blob)
            if blob_string and not csv_string:
                csv_string = blob_string
            elif blob_string:
                csv_string += blob_string.split('\n', 1)[1]
        if csv_string:
            concat_df = gcs_utils.df_from_gcs_string(csv_string)
            engine = create_sf_engine(config['sf_engine_config'])
            df_to_snowflake(data_frame=concat_df,
                                engine=engine,
                                table_name=table_group,
                                reserved_words_list=config['reserved_words'],
                                truncate=False,
                                create=True,
                                drop=True)


