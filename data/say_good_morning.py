from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator 
import requests as req
import psycopg2
#import psycopg2-binary as ps2


#расположим функцию реквест модуля
pg_hostname = 'localhost'  # Имя вашего контейнера
pg_port = '5430'    # Порт, на котором слушает PostgreSQL
pg_username = 'postgres'  # Имя пользователя
pg_pass = 'password'      # Пароль
pg_db = 'test'    


default_args = {
    'owner': 'julia',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2023, 10, 19),
    'template_searchpath': '/tmp'
}

def fetch_exchange_rate():
    rate_base = 'GBR'
    rate_target = 'USD'
    api_key = '70a6700beb16e5edbe55e49dc12369e9'

    hist_date = 'latest'
    url_base = "https://api.exchangerate.host/convert"
    url = url_base + hist_date
    response = req.get(url, params={'from': rate_base, 'access_key': api_key})
    
    # Получаем текущую дату
    
    url = f'http://api.exchangerate.host/live'
    response = req.get(url, params={'access_key': api_key, 
                                    'source': 'BTC',
                                    'currencies': 'USD',
                                    'format': 10})
    data = response.json()
  
    conn = psycopg2.connect(host=pg_hostname, port=pg_port, user=pg_username, password=pg_pass, dbname=pg_db)

    cursor = conn.cursor()
    cursor.execute("""CREATE TABLE IF NOT EXISTS test_table (
               id serial primary key,
               date date,
               source varchar(255),
               currencies varchar(255),
               rate double precision);""")
    conn.commit()

    insert_query = """
    INSERT INTO test_table (date, source, currencies, rate) VALUES 
    {data['date'], data['source'], 'USD', data['quotes']['BTCUSD']}
    """
    cursor.execute(insert_query, (data['date'], data['source'], 'USD', data['quotes']['BTCUSD']))
    conn.commit()

    cursor.close()
    conn.close()

with DAG(dag_id='say_good_morning',
         default_args=default_args,
         description="say_good_morning example DAG",
         schedule_interval=timedelta(minutes=10),
         tags=["bash", "Julia"],
         catchup=False) as dag:
    
    start = DummyOperator(task_id='start')
    
    good_morning = BashOperator(
        task_id='good_morning',
        bash_command=f"echo 'Good morning!' && mkdir -p /tmp/test")
     
    fetch_data_task = PythonOperator(
        task_id="fetch_exchange_rate",
        python_callable=fetch_exchange_rate
    )


    end = DummyOperator(task_id='end')

    start >> good_morning >> fetch_data_task >> end
