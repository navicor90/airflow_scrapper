"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from airflow import DAG
#from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from airflow.operators.selenium_plugin import SeleniumOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from controllers.inmoclick import InmoclickSearchPage, PropertyType
import logging as log
import time
import os
import pandas as pd
import requests
from requests.auth import HTTPBasicAuth

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2015, 6, 1),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG("inmoclick_scrapper", default_args=default_args, schedule_interval=timedelta(60*24))


def soup_from_url(driver, url):
    driver.get(url)
    time.sleep(5)
    content = driver.page_source
    return BeautifulSoup(content, features="lxml")

def save(filename, soup):
    filename = "soups/" + filename + ".html"
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, "w") as file:
        file.write(str(soup))

def read_soup(filename):
    filename = "soups/"+filename+".html"
    with open(filename, "r") as f:
        contents = f.read()
        soup = BeautifulSoup(contents, 'lxml')
        return soup

def get_found_pages(driver):
    base_url = 'https://www.inmoclick.com.ar'
    houses_list = "/inmuebles/venta/lotes-y-terrenos/mendoza"
    # houses_list = "/inmuebles/venta/casas/mendoza"
    params = "?favoritos=0&limit=48&prevEstadoMap=&provincias=21&precio%5Bmin%5D=&precio%5Bmax%5D=&moneda=1&sup_cubierta%5Bmin%5D=&sup_cubierta%5Bmax%5D=&sup_total%5Bmin%5D=&sup_total%5Bmax%5D=&page="
    p = 1
    url = base_url + houses_list + (params + str(p))
    soup = soup_from_url(driver, url)
    save(str(p), soup)

    pages_range = range(2, isp.max_page_number())
    for p in pages_range:
        url = base_url+houses_list+(params + str(p))
        log.info(url)

        #soup = soup_content_from_url(driver, url)
        # save(str(p), soup)

    #log.info(isp.search_items()[0].to_dict())


def csv_data_from_search_pages():
    found_items = []
    files = listdir('soups')
    for f in files:
        soup = read_soup(f)
        isp = InmoclickSearchPage(soup, PropertyType.LAND)
        for fi in isp.found_items():
            found_items.append(fi.to_dict())
    df = pd.DataFrame(found_items)
    df.to_csv('dataframe.csv',index=False)

def push_properties():
    API_ENDPOINT = "http://127.0.0.1:8000/properties/"
    df = pd.read_csv('dataframe.csv')
    for i,row in df.iterrows():
        property_data = { 
            "ref_id": row['ref_id'],
             "district": row['district'],
             "province": row['province'],
             "currency": row['currency'],
             "amount": row['amount'],
             "price": row['price'],
             "url": row['url'],
             "source_web": row['source_web'],
             "scrapped_date": row['scrapped_date'],
             "description": row['description'],
             "extra_json_info": row.to_dict(),
             "property_type": "HO"}
        r = requests.post(url = API_ENDPOINT, data = data, auth=HTTPBasicAuth('user', 'pass'))

start = DummyOperator(
    task_id='start',
    dag=dag)

t_get_found_pages = SeleniumOperator(
    script=get_found_pages,
    script_args=[],
    task_id='get_found_pages',
    dag=dag)

t_csv_data_from_search_pages = PythonOperator(
    python_callable=csv_data_from_search_pages,
    task_id='csv_data_from_search_pages',
    dag=dag)

end = DummyOperator(
    task_id='end',
    dag=dag)

start >> t_get_found_pages
t_get_found_pages >> t_csv_data_from_search_pages
t_csv_data_from_search_pages >> end
