import pandas as pd
import requests
import datetime
import json
import tqdm
import io


domains_filename = 'domains.csv'
token = '1267284902:AAE7rfaQfyZnSs59V1xJVlvkw66r-smwQ6Q'
username = 'bot_4440450_bot'
chat_id = '126555500'
url_tlg = f"https://api.telegram.org/bot{token}/sendMessage"
existed_domains = 'existed.csv'


def get_rkn(url):
    print('get data from github')
    t = datetime.datetime.now()
    resp = requests.get(url)
    assert resp.status_code == 200, f'invalid status code: {resp.status_code}'
    bytes_ = resp.content
    bytes_io = io.BytesIO()
    bytes_io.write(bytes_)
    bytes_io.seek(0)
    print(f'execution (seconds): {(datetime.datetime.now() - t).total_seconds()}')
    return bytes_io


def read_csv(stream):
    print('read data')
    t = datetime.datetime.now()
    df = pd.read_csv(stream, sep=';', encoding='WINDOWS-1251', index_col=None)
    print(f'execution (seconds): {(datetime.datetime.now() - t).total_seconds()}')
    return df


def read_domains(fname):
    print('read domains')
    t = datetime.datetime.now()
    with open(fname, 'r') as fd:
        domains = set(fd.read().split('\n'))
    print(f'execution (seconds): {(datetime.datetime.now() - t).total_seconds()}')
    return domains


def transform_raw(df):
    print('prepare data from raw to DataFrame')
    t = datetime.datetime.now()
    values = list(df.index)
    df_ = pd.DataFrame(values)
    df_[5] = df.values
    df = df_.copy(deep=True)
    del df_
    print(f'execution (seconds): {(datetime.datetime.now() - t).total_seconds()}')
    return df


def find_domains_exist(df, domains):
    print('find domains')
    t = datetime.datetime.now()
    df_ = df[df.apply(lambda el: el[1] in domains, axis=1)]
    print(f'execution (seconds): {(datetime.datetime.now() - t).total_seconds()}')
    return df_


def find_domains_not_exist(df, domains):
    print('find domains')
    t = datetime.datetime.now()
    df_ = df[df.apply(lambda el: el[1] not in domains, axis=1)]
    print(f'execution (seconds): {(datetime.datetime.now() - t).total_seconds()}')
    return df_


def unique_domains(df):
    print('filter domains')
    t = datetime.datetime.now()
    df_ = df.groupby(1).first().reset_index()
    print(f'execution (seconds): {(datetime.datetime.now() - t).total_seconds()}')
    return df_


def send_to_tlg(url, chat_id, values):
    for item in tqdm.tqdm(values, desc='pass messages'):
        data = {
            'text': json.dumps(';'.join(item), ensure_ascii=False),
            'chat_id': chat_id,
        }
        resp = requests.get(url, data=data)
        assert resp.status_code == 200, f'invalid status code: {resp.status_code}'


def dump_domains(fname, df, existed):
    print('dump domains')
    t = datetime.datetime.now()
    df_existed = df.copy(deep=True)
    df_existed = df_existed[~df_existed[1].isnull()]
    domains = '\n'.join(list(set(df_existed[1].values.tolist()).union(existed)))
    with open(fname, 'w') as fd:
        fd.write(domains)
    print(f'execution (seconds): {(datetime.datetime.now() - t).total_seconds()}')
    return df

stream = get_rkn('https://raw.githubusercontent.com/zapret-info/z-i/master/dump.csv')
df = read_csv(stream)
domains = read_domains(domains_filename)
existed = read_domains(existed_domains)
df = transform_raw(df)
dump_domains(existed_domains, df, existed)
df = unique_domains(df)
df = find_domains_not_exist(df, existed)
df = find_domains_exist(df, domains)
values = df[[1,3,5]].values.tolist()
send_to_tlg(url_tlg, chat_id, values)