from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
import json
from bs4 import BeautifulSoup
from google.cloud import vision
from google.oauth2 import service_account

google_ocr_api_key_str = Variable.get("GOOGLE_OCR_API_KEY")

google_credentials_info = json.loads(google_ocr_api_key_str)


def read_urls_from_file(file_path):
    urls = []
    try:
        with open(file_path, 'r') as file:
            for line in file:
                print(line)
                url = line.strip()
                if url:
                    urls.append(url)
    except IOError as e:
        print(f"Error reading file: {e}")
        return None
    return urls

def extract_image_urls_from_multiple_sites(urls):
    all_image_urls = []
    for url in urls:
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')
        image_urls = [img['src'] for img in soup.find_all('img') if 'src' in img.attrs]
        all_image_urls.extend(image_urls)
    return all_image_urls


def call_openai_api(prompt, engine="gpt-3.5-turbo"):

    openai_api_key = Variable.get("OPENAI_API_KEY")

    headers = {
        "Authorization": f"Bearer {openai_api_key}",
        "Content-Type": "application/json"
    }

    data = {
        "model": engine,
        "messages": [{"role": "user", "content": prompt}]
    }

    response = requests.post(f"https://api.openai.com/v1/chat/completions",
                             headers=headers,
                             data=json.dumps(data))

    return response.json()

def process_image_url(image_url):
    domain_name = None
    additional_info = None
    credentials = service_account.Credentials.from_service_account_info(google_credentials_info)
    client = vision.ImageAnnotatorClient(credentials=credentials)

    image = vision.Image()
    image.source.image_uri = image_url
    response_text_detection = client.text_detection(image=image)

    if response_text_detection.text_annotations:
        text_description = response_text_detection.text_annotations[0].description
        prompt = "Extract from this text ONLY domain names. If there is no domain return None " + text_description
        response_domain_name = call_openai_api(prompt)

        if response_domain_name and "choices" in response_domain_name and response_domain_name["choices"]:
            domain_name = response_domain_name["choices"][0]["message"]["content"].strip()
            if domain_name and domain_name.lower() != 'none':  # Check if the response is not 'None'
                prompt_additional_info = f"Provide a brief overview or additional information about this domain: {domain_name}"
                response_additional_info = call_openai_api(prompt_additional_info)

                if response_additional_info and "choices" in response_additional_info and response_additional_info[
                    "choices"]:
                    additional_info = response_additional_info["choices"][0]["message"]["content"].strip()

    return {
        'image_url': image_url,
        'domain_name': domain_name,
        'additional_info': additional_info
    }

def extract_and_process_data():
    url_file_path = '/opt/airflow/urls.txt'
    image_urls = extract_image_urls_from_multiple_sites(read_urls_from_file(url_file_path))
    return [process_image_url(url) for url in image_urls]


def insert_data_into_db(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='extract_and_process_data_task')

    pg_hook = PostgresHook(postgres_conn_id='postgres_localhost')

    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    check_sql = """
            SELECT dt
            FROM marketing_domains_db
            WHERE domain_name = %s
        """

    insert_sql = """
            INSERT INTO marketing_domains_db (dt, image_url, domain_name, additional_info)
            VALUES (%s, %s, %s, %s)
        """

    update_sql = """
            UPDATE marketing_domains_db
            SET image_url = %s, additional_info = %s
            WHERE dt = %s
        """

    for record in data:
        dt_value = record.get('dt') or datetime.utcnow()

        cursor.execute(check_sql, [record['domain_name']])
        existing_record = cursor.fetchone()

        if existing_record:
            cursor.execute(update_sql, (record['image_url'], record['additional_info'], existing_record[0]))
        else:
            cursor.execute(insert_sql,
                           (dt_value, record['image_url'], record['domain_name'], record['additional_info']))

    conn.commit()
    cursor.close()
    conn.close()


default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

start_date = datetime(2023, 12, 12)

with DAG(dag_id='dag_response', default_args=default_args, start_date=start_date, schedule_interval=None, catchup=False) as dag:
    create_table = PostgresOperator(
        task_id='create_postgres_table',
        postgres_conn_id='postgres_localhost',
        sql="""
              CREATE TABLE IF NOT EXISTS marketing_domains_db (
                dt TIMESTAMP NOT NULL,
                image_url TEXT,
                domain_name TEXT,
                additional_info TEXT,
                PRIMARY KEY (dt)
        )
               """
    )

    extract_and_process_data_task = PythonOperator(
        task_id='extract_and_process_data_task',
        python_callable=extract_and_process_data,
        dag=dag
    )

    insert_data_task = PythonOperator(
        task_id='insert_data_task',
        python_callable=insert_data_into_db,
        provide_context=True,
        dag=dag
    )

    create_table >> extract_and_process_data_task >> insert_data_task
