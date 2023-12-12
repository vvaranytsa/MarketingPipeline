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

google_credentials_info = {
    "type": "service_account",
    "project_id": "homework-cv",
    "private_key_id": "f6f0637839a0abab4cdc385c3f2b3d0cb0571937",
    "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCifgDe9b55X05M\nXNZ33gcz/yIXGMFBg3NLdbC0BSk79wLImgL1FZP2Yt/BQ86LTbTksfNlkUTQ4b2x\ngqbZFV+80gIIUiB3/swzBmOFuSn6Ped9Fnjq8UdHKDXw03WHuCPamdtms4SgdJcS\nitDoEPTz1XNSI9XCqwnSEOR+1MONnnzL5FNJRV9X/Erhuzio4hO/HXn22/i/OMvx\nPGZRvLUPmILji/puhC4Iq5jwyYSiSqUrO55xn+tVgxR7DGYadI4IbQVS4Lsw+seR\n6pV5ruuB3ijRXWRU/hzV2ftmvOOCcjzM7j6Ya1cW2ubauDyX9NHpTj8tjK/MWZR3\n8zoATq0FAgMBAAECggEAFNm5T+LVtapSmD7jcE8nunbXD4KoLblp3nl0Gq183Ip2\nK3qDvaIusdN/JcQHKV3nw97HvjU1UN5eonwC3+E7vzVteFbdfrhNyJBdzed0KP2v\niiALlQ4v2MQio8vmjWtGAoNyoUuQzawYRJNWeijO3cj6esRJZijfYjqzr0iq7S2A\nOsHway8tqPN4axz+lxpCY3IJ4W94VRYiq+1WpDO9VG60JVy9y5ZJFd4p9Be8KeYI\nj9hFjHmqPziCpDoQsm5OaZHo0dmJQmnzbQB6smgWhwmVauMVpKmqTax6htSbjyTY\nqf95tqljAr5lsmL/B5/FwmNQ5FxmjmKfbxQuxD8kYQKBgQDNv/zu+ZB3mfVuBYaB\nPMxRUURAbLCMJ3U4PjCVpqtNU42rWU2eextfZt5ReG16ZdMUHaurjiC5k+sHNLVS\nT/P7SuCiBMSAJ9OBc331+Gm6i0jhY1WvnZo7GkkV3DyVLalxpoZuRcbb2RGm9QZG\nkl/qsKlR9tiBawZ9fPogV04kUQKBgQDKLW4q2UkGPbISONfFh20gq6mqMLSxc0u0\nZFazI3QclXTWePiocONMr8wohK7F3CSwq6LfH9s6b4Pfh19ny5FOQ/RGFp6MaHmz\nKaUd7t3+5d4BebwiIbqkTlzhe5xwAeEL1/AIwfcIwtmRdRHqfvV/m5ut1Bv5kfrz\nfo3UAvXUdQKBgCo9w2EQza8wZaoL6lx4Lf5378pGRkzQAQnmArWGvV6ny8slD4F9\nBtakWPpi/h8F7rsPiGI1UNSo4LTfBmb60T2DVvEMh4dEJfFK085/DL3mwLS9Xycp\n9TMzJ1QcnjeGY2ZY6PqUysnyG+SqI3qzrIuTb3/LbRHM9k0nLncbVYixAoGAV5QI\nZe1L0bVF/ti6tykr33wc+ckxbLDZ+WGBoQXZlUw0mXc7l5OXErAQSvj20GTFS/Z9\njhO5nn2R7XExpb+ryPBszzObKI1VMoP2r5m6dmFSoub91PcxZ10+pLosEOTvfHbk\n1pZXEWd+YaEJyr+GTum0LT91gs12nKWUgGNr48UCgYA8LwW+OQT9spX80ebI0AQb\na1tYq7/TCgELMblJN+AV6zilJey4aqKe0q0xpjNFU9WjyAEpICW3al4ieIYzpMlW\niCzVxeGgDtO+KL3rC+MJvFKGBfnOJXhgrPwU727rPzzDz8mwxg4xU+mwtwIeWSZE\nS+fM0iP/+UjumoIwInh65A==\n-----END PRIVATE KEY-----\n",
    "client_email": "cv-homework-varanytsia@homework-cv.iam.gserviceaccount.com",
    "client_id": "117275442528205407633",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/cv-homework-varanytsia%40homework-cv.iam.gserviceaccount.com",
    "universe_domain": "googleapis.com"}


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
