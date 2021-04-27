from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from operators.CatFactsToAirflowLogs import CatFactsToAirflowLogs

dag = DAG('cat_facts_dag_multiprocessing', schedule_interval=timedelta(1), start_date=datetime(2021, 4, 27),
          catchup=False)


def pull_function(**kwargs):
    ti = kwargs['ti']
    print('kwargs', kwargs['ti'])
    pulled_fact = ti.xcom_pull(key='return_value', task_ids='get_facts')
    print(f"Most frequent fact about cats: {pulled_fact}")


t1 = CatFactsToAirflowLogs(
        task_id='get_facts',
        number_of_facts=10,
        animal_type='cat',
        dag=dag
    )

t2 = PythonOperator(
    task_id='pull_task',
    python_callable=pull_function,
    provide_context=True,
    dag=dag)

t1 >> t2