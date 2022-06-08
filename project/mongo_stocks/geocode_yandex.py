import requests
import xmltodict
import json


def get_yandex_xml(longitude, latitude):
    url = f"https://geocode-maps.yandex.ru/1.x/?apikey=7a20bc1c-362b-46eb-af93-6cbc7006a79e&geocode={longitude},{latitude}"

    payload = {}
    headers = {
        'Authorization': 'Bearer 7a20bc1c-362b-46eb-af93-6cbc7006a79e'
    }

    response = requests.request("GET", url, headers=headers, data=payload)
    sad = json.dumps(xmltodict.parse(response.text), indent=4)
    das = json.loads(sad)

    try:
        result = {x['kind']: x['name'] for x in
                  das['ymaps']['GeoObjectCollection']['featureMember'][0]['GeoObject']['metaDataProperty'][
                      'GeocoderMetaData']['Address']['Component']}
    except Exception as err:
        print(err)
        result = None

    return result
