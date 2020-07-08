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
from app.services.property_api_service import post_properties_batch
from app.models import PropertyType
from app.services.file_service import S3CloudFileService
from app.services.property_file_persistence import PropertyFilePersistence
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
file_service = S3CloudFileService()


def get_search_pages(driver, property_type: PropertyType):
    """ """
    p = 1
    url = search_url(property_type=property_type, page=p)
    soup = soup_from_url(driver, url)
    save(filename=str(p), soup=soup)

    isp = InmoclickSearchPage(soup, property_type)
    pages_range = range(2, isp.max_page_number())
    for p in pages_range:
        url = search_url(property_type=property_type, page=p)
        log.info(url)

        soup = soup_from_url(driver, url)
        save(str(p), soup)


def save_htmls_in_cloud(property_type: PropertyType):
    property_persistence = PropertyFilePersistence(file_service)
    property_persistence.save_search_pages(property_type)


def csv_data_from_search_pages(property_type: PropertyType):
    """ """
    found_items = []
    files = listdir('soups')
    for f in files:
        soup = read_soup(filename=f)
        isp = InmoclickSearchPage(soup=soup, property_type=property_type)
        for fi in isp.search_items():
            found_items.append(fi.to_dict())
    df = pd.DataFrame(found_items)
    df.to_csv(f'dataframe_{property_type.value}.csv', index=False)


def push_properties(property_type: PropertyType):
    """ """
    df = pd.read_csv(f'dataframe_{property_type.value}.csv')
    df['property_type'] = property_type
    properties_list = list(df.T.to_dict().values())
    print(properties_list)
    delta = 20
    for i in range(0, len(properties_list), delta):
        limit = i+delta
        batch = properties_list[i:limit]
        response = post_properties_batch(batch)
        if response.status_code == 409:
            print(f"Duplicated properties, code:{response.status_code} response:{response.text}")
        elif not (200 <= response.status_code < 300):
            print(batch)
            raise Exception(f"Server error, code:{response.status_code} response:{response.text}")


start = DummyOperator(
    task_id='start',
    dag=dag)

end = DummyOperator(
        task_id='end',
        dag=dag)

for p in [PropertyType.LAND, PropertyType.HOUSE, PropertyType.APARTMENT]:
    t_start = DummyOperator(
        task_id=f'start_{str(p.value)}',
        dag=dag)

    start >> t_start

    t_get_search_pages = SeleniumOperator(
        script=get_search_pages,
        script_args=[p],
        task_id=f'get_search_pages_{str(p.value)}',
        dag=dag)

    t_start >> t_get_search_pages

    t_save_htmls_in_cloud = PythonOperator(
        python_callable=save_htmls_in_cloud,
        op_kwargs={"property_type": p},
        task_id=f'save_htmls_in_cloud_{str(p.value)}',
        dag=dag)

    t_csv_data_from_search_pages = PythonOperator(
        python_callable=csv_data_from_search_pages,
        op_kwargs={"property_type": p},
        task_id=f'csv_data_from_search_pages_{str(p.value)}',
        dag=dag)

    t_get_search_pages >> t_csv_data_from_search_pages
    t_get_search_pages >> t_save_htmls_in_cloud

    t_push_properties = PythonOperator(
        python_callable=push_properties,
        op_kwargs={"property_type": p},
        task_id=f'push_properties_{str(p.value)}',
        dag=dag)

    t_csv_data_from_search_pages >> t_push_properties

    t_end = DummyOperator(
        task_id=f'end_{str(p.value)}',
        dag=dag)

    t_push_properties >> t_end
    t_save_htmls_in_cloud >> t_end

    t_end >> end
