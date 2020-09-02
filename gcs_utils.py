from google.cloud import storage
from google.api_core.exceptions import Forbidden
import pandas as pd
import io, re

def df_from_gcs_file(bucket_name, file_path):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.get_blob(file_path)
    encoding = re.search('(?<=charset=)\S+', blob.content_type).group(0)
    blob_string = blob.download_as_string().decode(encoding=encoding)
    headers = pd.read_csv(io.StringIO(blob_string), nrows=0).columns
    date_words = ['date', 'timestamp']
    date_headers = [header for header in headers if
                    any(re.search(f'(?<![a-z]){date_word}', header.lower()) for date_word in date_words)]
    df = pd.read_csv(io.StringIO(blob_string), parse_dates=date_headers)

    return df


def df_from_gcs_blob(blob):
    blob_string = string_from_gcs_blob(blob)
    df = df_from_gcs_string(blob_string)
    return df


def string_from_gcs_blob(blob):
    try:
        encoding = re.search('(?<=charset=)\S*', blob.content_type).group(0)
        blob_string = blob.download_as_string().decode(encoding=encoding)
    except Forbidden:
        print(f'cannot access {blob.name}')
        return
    except KeyError:
        print(f'not a text file: {blob.name}')
        return
    return blob_string


def df_from_gcs_string(string):
    headers = pd.read_csv(io.StringIO(string), nrows=0).columns
    date_words = ['date', 'timestamp']
    date_headers = [header for header in headers if
                    any(re.search(f'(?<![a-z]){date_word}', header.lower()) for date_word in date_words)]
    df = pd.read_csv(io.StringIO(string), parse_dates=date_headers)
    return df
