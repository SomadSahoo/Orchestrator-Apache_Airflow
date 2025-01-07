from airflow import DAG
from airflow.api.client.local_client import Client
from airflow.models import TaskInstance
from airflow import AirflowException

import time
from datetime import datetime, timedelta

from modules.handlers.rest_handler import RestHandler
# from modules.interfaces.minio_interface import MinIOInterface
# from modules.interfaces.influx_interface import InfluxInterface

from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

import logging
import requests
import json
import sys
import os

#from conf import default
default_params = {}

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2015, 6, 1),
    "retries": 0,
    "retry_delay": timedelta(seconds=5)
}


# Initialization Space for the DAG
def subroutine_initialize(*args, **kwargs):

    logging.info('Initializing DAG with ' + str(kwargs['dag_run'].conf))

    task_instance = kwargs['task_instance']
    task_id = kwargs['task'].task_id.split('.')[-1]

    # Set base path XCOM for workflow
    metadata = kwargs['dag_run'].conf['metadata']

    # Check if all models are available in task list TODO: optimise conf calls
    registry = kwargs['dag_run'].conf['modules']['model_registry']

    try:
        r = requests.get(registry)
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(e.response.text)

    # Collect unique models in our workflow
    model_inventory = set()
    task_list = kwargs['dag_run'].conf['tasks']

    for task in task_list:
        if task_list[task]['type'] == 'computation':
            model_inventory.add(task_list[task]['api_id'])

    logging.info("Model Inventory: " + str(model_inventory))

    # Check for available models
    data = r.json()
    for item in data:

        if item['name'] in model_inventory:
            model_inventory.remove(item['name'])

    # Check if we have any unavailable models left in the inventory
    if model_inventory:
        logging.info("ERROR: Not all models available in " + task_id + " , missing: " + str(model_inventory))

    return None


# Generic Model Computation Handler Task Specification
def subroutine_computation(**kwargs):

    task_instance = kwargs['task_instance']
    task_id = kwargs['task'].task_id.split('.')[-1]
    model_runtime = 0

    logging.info('Run Instance: ' + str(task_instance))
    logging.info('Previous Task: ' + str(next(iter(kwargs['task'].upstream_task_ids))))
    logging.info('Task Started: ' + str(task_id))

    api_id = kwargs['dag_run'].conf['tasks'][task_id]['api_id']

    # Get API addr through registry request
    registry = kwargs['dag_run'].conf['modules']['model_registry']
    model_request = {'name': api_id}

    try:
        r = requests.post(str(registry) + 'search', json=model_request)
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(e.response.text)

    data = r.json()
    api_addr = None

    for item in data:
        if item['status'] == "READY":
            api_addr = str(item['uri'])
            break
    if api_addr is None:
        raise AirflowException("ERROR: No model available in " + task_id)

    logging.info('Using model ' + str(api_id) + ' located at ' + str(api_addr))

    # Get experiment metadata
    metadata = kwargs['dag_run'].conf['metadata']

    # Get experiment model config
    config = kwargs['dag_run'].conf['tasks'][task_id]['model_config']

    # Initialize Model Handler
    rest_handler = RestHandler(api_addr=api_addr,
                               task_instance=task_instance,
                               metadata=metadata,
                               config=config,
                               logger=logging,
                               timeout=0) # disable timeout, as some essim simulations take longer than 60s

    # Connect Handler with Adapter
    rest_handler.get_adapter_status()
    response = rest_handler.request_model_instance()
    model_run_id = response['model_run_id']

    # Execute Model
    rest_handler.init_model_instance(model_run_id)
    response = rest_handler.run_model_instance(model_run_id)

    # Wait for Success Response
    while response['state'] != 'SUCCEEDED':
        # response = rest_handler.get_model_status(model_run_id)
        response = rest_handler.get_model_results(model_run_id)

        if response['state'] == 'ERROR':
            raise AirflowException("ERROR: Model in task " + task_id + " returned an error state")

        time.sleep(rest_handler.interval)
        model_runtime += rest_handler.interval
        if 0 < rest_handler.timeout < model_runtime:
            raise AirflowException("TIMEOUT: Task" + task_id + ": Model with address" + str(api_addr) + "and config" +
                                   str(config) + "timed out (" + str(rest_handler.timeout) + "s)")

    # Collect Model Results
    # response = rest_handler.get_model_results(model_run_id)
    logging.info("RESP:" + str(response))
    if response['result'] is not None:
        model_result = response['result']['path']
        task_instance.xcom_push(key=task_id + '_result', value=model_result)
    else:
        logging.info("ERROR: invalid model result response " + str(response))
    rest_handler.remove_model_instance(model_run_id)

    logging.info('Task Completed: ' + str(task_id))

    return None


# Terminate DAG if value passes threshold
def subroutine_finalize(**kwargs):
    return None

#with open('default_config.json') as json_file:
#    default_params = json.load(json_file)

# DAG Specification
with DAG('Macro_UseCase_workflow',
          default_args=default_args,
          params=default_params,
          schedule_interval=None,
          tags=["EWOUD", "MMvIB","MACRO", "ESDL","ETM","MOTER", "OPERA", "REGIONALISATION"]) as dag:

    # Task Specification
    Initialize = PythonOperator(dag=dag,
                        task_id='Initialize',
                        python_callable=subroutine_initialize)

    ETM_Range = PythonOperator(dag=dag,
                        task_id='ETM_Range',
                        python_callable=subroutine_computation)

    Opera = PythonOperator(dag=dag,
                        task_id='Opera',
                        python_callable=subroutine_computation)
    
    # ETM_Import = PythonOperator(dag=dag,
    #                     task_id='ETM_Import',
    #                     python_callable=subroutine_computation)
    ETM_Import = EmptyOperator(dag=dag, task_id='ETM_Import')

    # ETM_profiles = PythonOperator(dag=dag,
    #                     task_id='ETM_Profiles',
    #                     python_callable=subroutine_computation)
    ETM_Profiles = EmptyOperator(dag=dag, task_id='ETM_Profiles')

    Regionalization = PythonOperator(dag=dag,
                         task_id='Regionalization',
                         python_callable=subroutine_computation)
    #Regionalization = EmptyOperator(dag=dag, task_id='Regionalizer')

    Connect_Infra = PythonOperator(dag=dag,
                         task_id='Connect_Infra',
                         python_callable=subroutine_computation)
    #Connect_Infra = EmptyOperator(dag=dag, task_id='Connect_Infra')

    Moter = PythonOperator(dag=dag,
                         task_id='Moter',
                         python_callable=subroutine_computation)
    #Moter = EmptyOperator(dag=dag, task_id='Moter')

    Finalize = PythonOperator(dag=dag,
                        task_id='Finalize',
                        python_callable=subroutine_finalize)

    Initialize >> ETM_Range >> Opera >> ETM_Import >> ETM_Profiles >> \
        Regionalization >> Connect_Infra >> Moter >> Finalize