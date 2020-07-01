"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from airflow import DAG
#from airflow.operators.bash_operator import BashOperator
from airflow.operators.selenium_plugin import SeleniumOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from app.utils import soup_from_url, save, read_soup
from app.services.inmoclick_service import InmoclickSearchPage, search_url
from app.services.property_service import post_property, post_properties_batch
from app.models import PropertyType
import logging as log
import pandas as pd
from datetime import datetime,timedelta
from os import listdir


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


def get_search_pages(driver, property_type:PropertyType):
    """ """
    p = 1
    url = search_url(property_type=property_type, page=p)
    soup = soup_from_url(driver, url)
    save(filename=str(p), soup=soup)

    isp = InmoclickSearchPage(soup, PropertyType.LAND)
    pages_range = range(2, isp.max_page_number())
    for p in pages_range:
        url = search_url(property_type=PropertyType.LAND, page=p)
        log.info(url)

        soup = soup_from_url(driver, url)
        save(str(p), soup)



def csv_data_from_search_pages(property_type:PropertyType):
    """ """
    found_items = []
    files = listdir('soups')
    for f in files:
        soup = read_soup(filename=f)
        isp = InmoclickSearchPage(soup=soup, property_type=property_type)
        for fi in isp.search_items():
            found_items.append(fi.to_dict())
    df = pd.DataFrame(found_items)
    df.to_csv('dataframe.csv', index=False)


def push_properties(property_type:PropertyType):
    """ """
    df = pd.read_csv('dataframe.csv')
    df['property_type'] = property_type
    properties_list = list(df.T.to_dict().values())
    print(properties_list)
    delta = 20
    for i in range(0,len(properties_list), delta):
        limit = i+delta
        batch = properties_list[i:limit]
        response = post_properties_batch(batch)
        if response.status_code==409:
            print(batch)
            print(f"Duplicated properties, code:{response.status_code} response:{response.text}")
        elif not (200 <= response.status_code < 300):
            print(batch)
            raise Exception(f"Server error, code:{response.status_code} response:{response.text}")

start = DummyOperator(
    task_id='start',
    dag=dag)

t_get_search_pages = SeleniumOperator(
    script=get_search_pages,
    script_args=[PropertyType.LAND],
    task_id='get_search_pages',
    dag=dag)

t_csv_data_from_search_pages = PythonOperator(
    python_callable=csv_data_from_search_pages,
    op_kwargs={"property_type":PropertyType.LAND},
    task_id='csv_data_from_search_pages',
    dag=dag)

t_push_properties = PythonOperator(
    python_callable=push_properties,
    op_kwargs={"property_type":PropertyType.LAND},
    task_id='push_properties',
    dag=dag)

end = DummyOperator(
    task_id='end',
    dag=dag)


start >> t_get_search_pages
t_get_search_pages >> t_csv_data_from_search_pages
t_csv_data_from_search_pages >> t_push_properties
t_push_properties >> end
