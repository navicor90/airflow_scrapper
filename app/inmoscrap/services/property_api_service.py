import requests
from requests.auth import HTTPBasicAuth
from app.inmoscrap.models import PropertyType
import time
import os

HOST = os.environ['PROPERTIES_HOST']
USER = os.environ['PROPERTIES_USER']
PASSWORD = os.environ['PROPERTIES_PASSWORD']

server_property_map = {PropertyType.LAND: "FI",
                       PropertyType.HOUSE: "HO",
                       PropertyType.APARTMENT: "AP"}

MAIN_PROPERTY_INFO = ['ref_id', 'district', 'province', 'currency', 'amount', 'price', 'source_web',
                          'scrapped_date', 'description', 'property_type']


def to_server_property_type(property_type: PropertyType):
    try:
        return server_property_map[property_type]
    except:
        raise Exception(f"Property type {property_type} not supported yet")       


def create_property_data(property_dict):
    prop = property_dict.copy()
    prop['property_type'] = to_server_property_type(prop['property_type'])
    if prop['amount'] != prop['amount']:# is nan
        prop['amount'] = 0

    if prop['currency'] != prop['currency']: # is nan
        prop['currency'] = 0

    extra_info = {}
    for k in prop.keys():
        # Avoid other nans
        if prop[k] != prop[k]:
            prop[k] = "null"
        # Create extra info dict
        if k not in MAIN_PROPERTY_INFO:
            extra_info[k] = prop[k]

    property_data = { 
        "ref_id": prop['ref_id'],
        "district": prop['district'],
        "province": prop['province'],
        "currency": prop['currency'],
        "amount": prop['amount'],
        "price": prop['price'],
        "url": prop['url'],
        "source_web": prop['source_web'],
        "scrapped_date": prop['scrapped_date'],
        "description": prop['description'],
        "extra_json_info": str(extra_info),
        "property_type": prop['property_type']}
    return property_data


def post_property(property_dict):
    time.sleep(5) # Avoid heroku rate limit
    print(property_dict['ref_id'])
    property_data = create_property_data(property_dict)
    headers = {'content-type': 'application/json'}
    r = requests.post(url=HOST+"properties/", 
                      json=property_data,
                      auth=HTTPBasicAuth(USER, PASSWORD), 
                      headers=headers,
                      verify=False,
                      timeout=60)
    return r


def post_properties_batch(properties_batch):
    time.sleep(5) # Avoid heroku rate limit
    data_batch = []
    print("BATCH to post:")
    for p in properties_batch:
        pd = create_property_data(p)
        print(pd['ref_id'])
        data_batch.append(pd)
    headers = {'content-type': 'application/json'}
    r = requests.post(url=HOST+"properties_batch/", 
                      json=data_batch,
                      auth=HTTPBasicAuth(USER, PASSWORD), 
                      headers=headers,
                      verify=False,
                      timeout=60)
    return r