import zipfile
import pandas as pd
import tqdm
import json


def unzip(filepath, out_dir):
    with zipfile.ZipFile(filepath, 'r') as zip_ref:
        zip_ref.extractall(out_dir)


def glimpse(path):
    df = pd.read_csv(path, nrows=5)
    display(df.head(), pd.DataFrame(list(df.columns), columns=['columns']))


def event_data_to_json(input_file, output_file, chunksize=1000000):
    with open(output_file, 'w') as f:
        for item in tqdm.tqdm_notebook(pd.read_csv(input_file, chunksize=chunksize, usecols=['event_data'])):
            for line in item['event_data']:
                f.write(line)
                f.write('\n')


def get_event_data_keys(json_data):
    for k, v in json_data.items():
        if isinstance(v, dict):
            for kk in get_event_data_keys(v):
                yield '{}.{}'.format(k, kk)
        else:
            yield k


def make_json_keys_set(path, result_set, chunksize=500000):
    chunks = pd.read_csv(path, usecols=['event_data'], converters={'event_data': json.loads}, chunksize=chunksize)
    for chunk in tqdm.tqdm_notebook(chunks):
        series_of_chunk_cols = map(lambda x: set(get_event_data_keys(x)), chunk['event_data'].values)
        chunk_cols = set.union(*list(series_of_chunk_cols))
        result_set = set.union(result_set, chunk_cols)
    return result_set
