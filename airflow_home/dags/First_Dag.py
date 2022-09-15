from array import array
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import random as rng


def RandomNumberGeneratorA():
    TempA = rng.randint(0, 10)
    print("Number A is: " + str(TempA))
    return TempA


def RandomNumberGeneratorB():
    TempB = rng.randint(0, 10)
    print("Number B is: " + str(TempB))
    return TempB


def RandomNumberGeneratorC():
    TempC = rng.randint(0, 10)
    print("Number A is: " + str(TempC))
    return TempC


def compare_and_print(ti):
    Array = ti.xcom_pull(
        task_ids=[
            "RandomNumberGeneratorA",
            "RandomNumberGeneratorB",
            "RandomNumberGeneratorC",
        ]
    )
    Total = 0
    for i in [0, 1, 2]:
        Array[i] = Array[i] * (Array[i] % 2)
        Total += Array[i]

    print("The sum of odd numbers is : " + str(Total))


with DAG(
    "First-Dag",
    start_date=datetime(2022, 9, 14, 8, 30),
    schedule_interval="*/5 * * * *",
    catchup=False,
) as dag:
    RandomA = PythonOperator(
        task_id="RandomNumberGeneratorA", python_callable=RandomNumberGeneratorA
    )

    RandomB = PythonOperator(
        task_id="RandomNumberGeneratorB", python_callable=RandomNumberGeneratorB
    )

    RandomC = PythonOperator(
        task_id="RandomNumberGeneratorC", python_callable=RandomNumberGeneratorC
    )
    Compare_and_print = PythonOperator(
        task_id="Compare_and_Print", python_callable=compare_and_print
    )

    ([RandomA, RandomB, RandomC] >> Compare_and_print)
