from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta
from docker.types import Mount

# Folder di lokal untuk menyimpan file CSV sementara sebagai media transfer antar task
HOST_PATH = "/c/tmp/airflow_data" 
DOCKER_NETWORK = "airflowdocker_default" 

DB_ENV_VARS = {
    "DB_HOST": "postgres_db_2",
    "DB_NAME": "latihan_scrapping",
    "DB_USER": "postgres",
    "DB_PASS": "password123",
    "DB_PORT": "5432"
}

default_args = {
    "owner": "l0224025_theodosius",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="wired_api_to_postgres_docker",
    start_date=datetime(2026, 4, 18),
    schedule=None, 
    catchup=False,
    default_args=default_args,
    tags=["api", "docker", "pipeline", "wired"],
) as dag:

    # =========================================================
    # TASK 1: MENGAMBIL DATA DARI API LOKAL
    # =========================================================
    fetch_api = DockerOperator(
        task_id="fetch_api_data",
        image="python:3.10-slim",
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode=DOCKER_NETWORK,
        extra_hosts={'host.docker.internal': 'host-gateway'},
        mount_tmp_dir=False,
        mounts=[Mount(source=HOST_PATH, target="/tmp", type="bind")],
        environment={"PYTHONUNBUFFERED": "1"}, 
        command="""
        bash -c "
        pip install -q requests pandas &&
        python - <<EOF
import requests
import pandas as pd
import sys

print('Menghubungkan ke API penyedia data scraping...')
url = 'http://host.docker.internal:8000/articles' 

try:
    resp = requests.get(url, timeout=15)
    if resp.status_code != 200:
        print(f'Gagal mengakses API. Status: {resp.status_code}')
        sys.exit(1)

    data = resp.json()
    
    # Mengambil array 'articles' dari struktur JSON
    articles_list = data.get('articles', [])
    if not articles_list:
        print('Data artikel kosong.')
        sys.exit(1)
        
    results = []
    for item in articles_list:
        results.append({
            'title': item.get('title', ''),
            'url': item.get('url', ''),
            'description': item.get('description', ''),
            'author': item.get('author', ''),
            'scraped_at': item.get('scraped_at', '')
        })

    df = pd.DataFrame(results)
    df.to_csv('/tmp/raw_wired_data.csv', index=False)
    print(f'Berhasil mengambil {len(df)} baris data artikel.')

except Exception as e:
    print(f'Error saat mengambil data: {e}')
    sys.exit(1)
EOF
        "
        """
    )

    # =========================================================
    # TASK 2: TRANSFORMASI FORMAT TANGGAL
    # =========================================================
    preprocess = DockerOperator(
        task_id="transform_data",
        image="python:3.10-slim",
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode=DOCKER_NETWORK,
        mount_tmp_dir=False,
        mounts=[Mount(source=HOST_PATH, target="/tmp", type="bind")],
        command="""
        bash -c "
        pip install -q pandas &&
        python - <<EOF
import pandas as pd
from datetime import datetime
import os, sys

if not os.path.exists('/tmp/raw_wired_data.csv'):
    print('File raw data tidak ditemukan.')
    sys.exit(1)

df = pd.read_csv('/tmp/raw_wired_data.csv')

def format_date_string(date_str):
    try:
        dt_obj = datetime.fromisoformat(str(date_str))
        return dt_obj.strftime('%Y-%m-%d %H:%M:%S')
    except ValueError:
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

df['scraped_at'] = df['scraped_at'].apply(format_date_string)
df.to_csv('/tmp/clean_wired_data.csv', index=False)
print('Transformasi tanggal selesai.')
EOF
        "
        """
    )

    # =========================================================
    # TASK 3: MENYIMPAN KE DATABASE POSTGRESQL
    # =========================================================
    insert_db = DockerOperator(
        task_id="insert_to_postgres",
        image="python:3.10-slim",
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        network_mode=DOCKER_NETWORK,
        mount_tmp_dir=False,
        mounts=[Mount(source=HOST_PATH, target="/tmp", type="bind")],
        environment=DB_ENV_VARS,
        command="""
        bash -c "
        pip install -q pandas psycopg2-binary &&
        python - <<EOF
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
import os, sys

if not os.path.exists('/tmp/clean_wired_data.csv'):
    sys.exit(1)

df = pd.read_csv('/tmp/clean_wired_data.csv')

try:
    conn = psycopg2.connect(
        host=os.environ.get('DB_HOST'), 
        database=os.environ.get('DB_NAME'),
        user=os.environ.get('DB_USER'), 
        password=os.environ.get('DB_PASS'), 
        port=os.environ.get('DB_PORT')
    )
    cur = conn.cursor()

    cur.execute('''
        CREATE TABLE IF NOT EXISTS wired_articles (
            id SERIAL PRIMARY KEY,
            title TEXT,
            url TEXT,
            description TEXT,
            author VARCHAR(255),
            scraped_at TIMESTAMP
        )
    ''')

    records = df[['title', 'url', 'description', 'author', 'scraped_at']].values.tolist()
    
    execute_batch(cur, '''
        INSERT INTO wired_articles (title, url, description, author, scraped_at)
        VALUES (%s, %s, %s, %s, %s)
    ''', records)

    conn.commit()
    print('Data Wired.com berhasil dimuat ke PostgreSQL.')
except Exception as e:
    print(f'Terjadi kegagalan pada database: {e}')
    sys.exit(1)
finally:
    if 'conn' in locals(): 
        conn.close()
EOF
        "
        """
    )

    fetch_api >> preprocess >> insert_db