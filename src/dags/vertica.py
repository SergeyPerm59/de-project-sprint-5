from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator

DAG_ID='vertica'

with DAG(
        dag_id= DAG_ID,
        start_date=datetime(2022, 9, 24),
        schedule_interval=timedelta(minutes=15),
        catchup=False,
) as dag:
    def download():
        import boto3
        AWS_ACCESS_KEY_ID = "YCAJEWXOyY8Bmyk2eJL-hlt2K"
        AWS_SECRET_ACCESS_KEY = "YCPs52ajb2jNXxOUsL4-pFDL1HnV2BCPd928_ZoA"
        session = boto3.session.Session()
        s3_client = session.client(
            service_name='s3',
            endpoint_url='https://storage.yandexcloud.net',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        )
        s3_client.download_file(
            Bucket='sprint6',
            Key='group_log.csv',
            Filename='/data/group_log.csv'
        )

    def insert():
        import vertica_python
        from vertica_python import connect
        conn_info = {'host': '51.250.75.20',
                    'port': '5433',
                    'user': 'SERGEYPERM89YANDEXRU',
                    'password': '19LGI6wzy3zbfGS',
                    'database': 'dwh'}
        connect2= vertica_python.connect(**conn_info)
        cur=connect2.cursor()
        cur.execute('truncate table SERGEYPERM89YANDEXRU__STAGING.group_log')
        with open('/data/group_log.csv') as fs:
            my_file = fs.read()
            cur.copy("COPY SERGEYPERM89YANDEXRU__STAGING.group_log FROM STDIN PARSER FDELIMITEDPARSER (delimiter=',')", my_file)
            cur.close
download_task=PythonOperator(
    task_id='download',
    python_callable=download,
    dag=dag)

insert_task=PythonOperator(
    task_id='insert',
    python_callable=insert,
    dag=dag)

download_task >> insert_task