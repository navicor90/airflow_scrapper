import requests
from requests.auth import HTTPBasicAuth
from controllers.inmoclick import PropertyType

API_ENDPOINT = "http://127.0.0.1:8000/properties/"
USER = ""
PASSWORD = ""

def server_property_type(property_type:PropertyType):
    serv_type = ""
    if property_dict['property_type'] == PropertyType.LAND:
       serv_type = "LA"
    elif property_dict['property_type'] == PropertyType.HOUSE:
       serv_type = "HO"
    else:
       raise Exception(f"Property type {property_type} not supported yet")
    return serv_type

def properties_post(property_dict):
    property_data = { 
        "ref_id": property_dict['ref_id'],
        "district": property_dict['district'],
        "province": property_dict['province'],
        "currency": property_dict['currency'],
        "amount": property_dict['amount'],
        "price": property_dict['price'],
        "url": property_dict['url'],
        "source_web": property_dict['source_web'],
        "scrapped_date": property_dict['scrapped_date'],
        "description": property_dict['description'],
        "extra_json_info": property_dict,
        "property_type": server_property_type(property_dict['property_type'])}
    r = requests.post(url=API_ENDPOINT, data=data, auth=HTTPBasicAuth(USER, PASSWORD))
    return r.text()