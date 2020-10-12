from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.selenium_plugin import SgtTurnStatusWebOperator

TurnOffices = SgtTurnStatusWebOperator.TurnOffices
TurnTypes = SgtTurnStatusWebOperator.TurnTypes
TurnCountries = SgtTurnStatusWebOperator.TurnCountries

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2020, 10, 10),
    "email": ["miltonivanterreno@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 5,
    "retry_delay": timedelta(minutes=2),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}


dag = DAG("car_license_turn_alerter", default_args=default_args, schedule_interval=timedelta(120*24))

get_turn_status = SgtTurnStatusWebOperator(
        office=TurnOffices.BARCELONA,
        ttype=TurnTypes.SWITCH_LICENSE,
        country=TurnCountries.ARGENTINA,
        task_id=f'get_turn_status',
        dag=dag,
        retries=3)
