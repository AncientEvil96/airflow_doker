import requests
from requests.auth import HTTPBasicAuth

with open('серт.txt') as f:
    ff = f.read()

list_ = ff.split('\n')
auth = HTTPBasicAuth('admin', '5l6WV8cwRSF9')

for i in list_:
    url = f'http://192.168.0.165/utwork/hs/unloading/PodSert'
    rs = requests.get(url, params={'nomerPS': i}, auth=auth)
    try:
        print(rs.json()['SHTRIH'])
    except Exception as err:
        print(rs.text, i)
