import requests
from requests.auth import HTTPBasicAuth
from app.models import PropertyType

API_ENDPOINT = "http://127.0.0.1:8000/properties/"
USER = ""
PASSWORD = ""

server_property_map = {PropertyType.LAND:"LA",
                       PropertyType.HOUSE:"HO"}

def to_server_property_type(property_type:PropertyType):
    try:
        return server_property_map[property_type]
    except:
        raise Exception(f"Property type {property_type} not supported yet")       


def post_property(property_dict):
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
        "property_type": to_server_property_type(property_dict['property_type'])}
    headers = {'content-type': 'application/json'}
    r = requests.post(url=API_ENDPOINT, 
                      data=property_data, 
                      auth=HTTPBasicAuth(USER, PASSWORD), 
                      headers=headers)
    return r.text() 