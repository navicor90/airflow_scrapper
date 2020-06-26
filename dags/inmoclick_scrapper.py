"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from airflow import DAG
#from airflow.operators.bash_operator import BashOperator
from airflow.operators.selenium_plugin import SeleniumOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from controllers.inmoclick import InmoclickSearchPage, PropertyType
from controllers.utils import soup_from_url, save, read_soup
from services.property_service import properties_post
import logging as log
import pandas as pd


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


def get_found_pages(driver):
    """ """
    p = 1
    url = search_url(property_type=PropertyType.LAND, page=p)
    soup = soup_from_url(driver, url)
    save(filename=str(p), soup=soup)

    isp = InmoclickSearchPage(soup, PropertyType.LAND)
    pages_range = range(2, isp.max_page_number())
    for p in pages_range:
        url = search_url(property_type=PropertyType.LAND, page=p)
        log.info(url)

        #soup = soup_content_from_url(driver, url)
        # save(str(p), soup)

    #log.info(isp.search_items()[0].to_dict())


def csv_data_from_found_pages():
    """ """
    found_items = []
    files = listdir('soups')
    for f in files:
        soup = read_soup(f)
        isp = InmoclickSearchPage(soup, PropertyType.LAND)
        for fi in isp.found_items():
            found_items.append(fi.to_dict())
    df = pd.DataFrame(found_items)
    df.to_csv('dataframe.csv', index=False)

def push_properties():
    """ """
    df = pd.read_csv('dataframe.csv')
    for i,row in df.iterrows():
        property_dict = row.to_dict()
        property_dict['property_type'] = PropertyType.LAND
        properties_post(property_dict)

start = DummyOperator(
    task_id='start',
    dag=dag)

t_get_found_pages = SeleniumOperator(
    script=get_found_pages,
    script_args=[],
    task_id='get_found_pages',
    dag=dag)

t_csv_data_from_found_pages = PythonOperator(
    python_callable=csv_data_from_found_pages,
    task_id='csv_data_from_found_pages',
    dag=dag)

t_push_properties = PythonOperator(
    python_callable=push_properties,
    task_id='push_properties',
    dag=dag)

end = DummyOperator(
    task_id='end',
    dag=dag)

start >> t_get_found_pages
t_get_found_pages >> t_csv_data_from_search_pages
t_csv_data_from_search_pages >> push_properties
push_properties >> end
