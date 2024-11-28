from airflow import DAG
from datetime import datetime
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule as tr
from airflow.utils.state import State
import random
import time
from airflow.utils.dates import days_ago

# Функція для примусового встановлення статусу DAG як успішного
def mark_dag_success(ti, **kwargs):
    dag_run = kwargs['dag_run']
    dag_run.set_state(State.SUCCESS)

# Функція для вибору випадкового значення медалі
def choose_medal():
    return random.choice(['Bronze', 'Silver', 'Gold'])

# Функція для затримки виконання
def sleep_task():
    time.sleep(35)

# Аргументи за замовчуванням для DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

# Назва з'єднання з базою даних MySQL
connection_name = "my_db_connect"

# Визначення DAG
with DAG(
        'artur_mykhailiuk_count_medals',
        default_args=default_args,
        schedule_interval=None,  # DAG не має запланованого інтервалу виконання
        catchup=False,  # Вимкнути запуск пропущених задач
        tags=["artur_mykhailiuk"]
) as dag:

    # Завдання 1: Створення таблиці
    create_table = MySqlOperator(
        task_id='create_table',
        mysql_conn_id=connection_name,
        sql="""
        CREATE TABLE IF NOT EXISTS neo_data.artur_mykhailiuk_medal_counts (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(10),
            count INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """,
    )

    # Завдання 2: Випадковий вибір медалі
    choose_medal_task = PythonOperator(
        task_id='choose_medal',
        python_callable=choose_medal,
    )

    # Завдання 3: Розгалуження
    def branch_task(**kwargs):
        ti = kwargs['ti']
        chosen_medal = ti.xcom_pull(task_ids='choose_medal')
        if chosen_medal == 'Bronze':
            return 'count_bronze'
        elif chosen_medal == 'Silver':
            return 'count_silver'
        else:
            return 'count_gold'

    branch_task = BranchPythonOperator(
        task_id='branch_task',
        python_callable=branch_task,
        provide_context=True,
    )

    # Завдання 4: Підрахунок медалей
    count_bronze = MySqlOperator(
        task_id='count_bronze',
        mysql_conn_id=connection_name,
        sql="""
        INSERT INTO neo_data.artur_mykhailiuk_medal_counts (medal_type, count)
        SELECT 'Bronze', COUNT(*)
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Bronze';
        """,
    )

    count_silver = MySqlOperator(
        task_id='count_silver',
        mysql_conn_id=connection_name,
        sql="""
        INSERT INTO neo_data.artur_mykhailiuk_medal_counts (medal_type, count)
        SELECT 'Silver', COUNT(*)
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Silver';
        """,
    )

    count_gold = MySqlOperator(
        task_id='count_gold',
        mysql_conn_id=connection_name,
        sql="""
        INSERT INTO neo_data.artur_mykhailiuk_medal_counts (medal_type, count)
        SELECT 'Gold', COUNT(*)
        FROM olympic_dataset.athlete_event_results
        WHERE medal = 'Gold';
        """,
    )

    # Завдання 5: Затримка виконання
    sleep_task = PythonOperator(
        task_id='sleep_task',
        python_callable=sleep_task,
        trigger_rule=tr.ONE_SUCCESS,  # Виконати, якщо хоча б одне з попередніх завдань успішне
   
    )

    # Завдання 6: Перевірка за допомогою сенсора
    check_recent_record = SqlSensor(
        task_id='check_recent_record',
        conn_id=connection_name,
        sql="""
            WITH count_in_medals AS (
                select COUNT(*) as nrows FROM neo_data.artur_mykhailiuk_medal_counts
                WHERE created_at >= NOW() - INTERVAL 30 SECOND
                )
            SELECT nrows > 0 from count_in_medals; 
        """,
        mode='poke',  # Режим перевірки: періодична перевірка умови
        poke_interval=5,  # Перевірка кожні 5 секунд
        timeout=6,  # Тайм-аут після 6 секунд (1 повторна перевірка)

    )

    # Визначення залежностей
    create_table >> choose_medal_task >> branch_task
    branch_task >> [count_bronze, count_silver, count_gold] >> sleep_task 
    sleep_task>> check_recent_record