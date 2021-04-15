from datetime import timedelta, datetime
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import sys
import os



default_args = {
    'owner': 'bill',
    'depends_on_past': False,  # if set False, and the task failed yesterday, the dags will still run today
    'start_date': airflow.utils.dates.days_ago(1),
    # 'end_date': datetime()
    'email': ['22465652@qq.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),  # if fail, retry after 5 minutes
}

dag = DAG(
    'Implant_prediction',
    default_args=default_args,
    description='Predict potential implant customers by hybrid model',
    schedule_interval=timedelta(days=1)  # run the dags per day # '@daily'/'0 0 * * *'
)


#dependency = BashOperator(
#	task_id = 'pip_package',
#	bash_command = 'pip --default-timeout=100000 install -r /usr/local/airflow/requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple',
#	dag=dag
#)	

#def pred():

 #   try:
  #      workflow = os.path.join(os.getcwd(), "app_folder", "modules","app.py")
   #     os.system("python3 {}".format(workflow))

    #except Exception as error:
	#    print("ATTENTION: /n",error)	
	 #   raise Exception()


#prediction = PythonOperator(
 #   task_id='pred',
  #  provide_context=False,
   # python_callable=pred,
    #dag=dag)

prediction = BashOperator(
	task_id = 'pred',
	bash_command = 'python /usr/local/airflow/app_folder/modules/app.py',
	dag=dag
)

prediction


