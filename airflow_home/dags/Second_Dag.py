from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import random as rng


def Hello_World():
    print("Hello World!")


def Random_Number():
    print("This is my random number between 1 and 10: " + str(rng.randint(1, 10)))


def Random_Number2():
    print("This is my random number between 10 and 20: " + str(rng.randint(10, 20)))


with DAG(
    "Second_Dag",
    start_date=datetime(2022, 9, 14, 7, 30),
    schedule_interval="*/5 * * * *",
    catchup=False,
) as dag:
    Hello = PythonOperator(task_id="Hello_World", python_callable=Hello_World)
    Random_1_10 = PythonOperator(
        task_id="Random_Number_1_10", python_callable=Random_Number
    )
    Random_10_20 = PythonOperator(
        task_id="Random_Number_10_20", python_callable=Random_Number2
    )

Hello >> Random_1_10 >> Random_10_20
