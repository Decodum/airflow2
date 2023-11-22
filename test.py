import requests as req
import psycopg2 as ps2
from datetime import datetime, timedelta, date

pg_hostname = 'localhost'
pg_port = '5430'
pg_username = 'postgres'
pg_pass = 'password'
pg_db = 'test'

def fetch_exchange_rate():
    rate_base = 'GBR'
    rate_target = 'USD'
    api_key = '70a6700beb16e5edbe55e49dc12369e9'

    hist_date = 'latest'
    url_base = "https://api.exchangerate.host/convert"
    url = url_base + hist_date
    response = req.get(url, params={'from': rate_base, 'access_key': api_key})
    
    # Получаем текущую дату
    current_date = datetime.now().strftime('%Y-%m-%d')
    url = f'http://api.exchangerate.host/live'
    response = req.get(url, params={'access_key': api_key, 
                                    'source': 'BTC',
                                    'currencies': 'USD',
                                    'format': 10})
    data = response.json()
    return data
    

data = fetch_exchange_rate()
#print(data)

def insert_table(data):
    
    conn = ps2.connect(host=pg_hostname, port=pg_port, user=pg_username, password=pg_pass, database=pg_db)
    cursor = conn.cursor()
    cursor.execute("""CREATE TABLE IF NOT EXISTS test_table (
               id serial primary key,
               date date,
               source varchar(255),
               currencies varchar(255),
               rate double precision);""")
    conn.commit()

    insert_query = f"""
    INSERT INTO test_table (date, source, currencies, rate) VALUES 
    {data['date'], data['source'], 'USD', data['quotes']['BTCUSD']}
            """
    cursor.execute(insert_query)
    conn.commit()

    cursor.close()
    conn.close()

