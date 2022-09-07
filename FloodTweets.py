

from datetime import timedelta, datetime


from airflow import DAG 
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator



GOOGLE_CONN_ID = "google_cloud_default"
PROJECT_ID="final-project-vinazol" # copy from google cloud console
BUCKET_NAME = 'final-project-bucket-vinazol' # bucket name from GCS
GS_PATH = "kerala-floods/" # folder inside bucket
STAGING_DATASET = "staging_tweets_dataset" # create in bigquery
DATASET = "tweets_dataset" # create in bigquery
LOCATION = "us-central1" # make sure the same location is selected everywhere

default_args = {
    'owner': 'Veena Solomon',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'start_date':  days_ago(2),
    'retry_delay': timedelta(minutes=5),
}

with DAG('FloodTweets', schedule_interval=timedelta(days=1), default_args=default_args) as dag:
    start_pipeline = DummyOperator(
        task_id = 'start_pipeline',
        dag = dag
        )

    load_staging_dataset = DummyOperator(
        task_id = 'load_staging_dataset',
        dag = dag
        )    
    
    load_flood_tweets = GCSToBigQueryOperator(
        task_id = 'load_flood_tweets',
        bucket = BUCKET_NAME,
        source_objects = ['kerala-floods/keralafloods.csv'],
        destination_project_dataset_table = f'{PROJECT_ID}:{STAGING_DATASET}.dataset_flood',
        write_disposition='WRITE_TRUNCATE',
        source_format = 'csv',
        allow_quoted_newlines = 'true',
        skip_leading_rows = 1,
        schema_fields=[
        {'name': 'id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'username', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'tweet_text', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'tweet_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'retweets', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'favs', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'followers', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'follows', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'location', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'bio', 'type': 'STRING', 'mode': 'NULLABLE'},
            ]
        )
    
    check_dataset_flood = BigQueryCheckOperator(
        task_id = 'check_dataset_flood',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.{STAGING_DATASET}.dataset_flood`'
        )

    create_District_Table = DummyOperator(
        task_id = 'Create_District_Table',
        dag = dag
        )

    create_D_dataset_trivandrum = BigQueryOperator(
        task_id = 'create_D_dataset_trivandrum',
        use_legacy_sql = False,
        location = LOCATION,
        sql = './sql/D_dataset_trivandrum.sql'
        )

    create_D_dataset_kollam = BigQueryOperator(
        task_id = 'create_D_dataset_kollam',
        use_legacy_sql = False,
        location = LOCATION,
        sql = './sql/D_dataset_kollam.sql'
        ) 

    create_D_dataset_alappuzha = BigQueryOperator(
        task_id = 'create_D_dataset_alappuzha',
        use_legacy_sql = False,
        location = LOCATION,
        sql = './sql/D_dataset_alappuzha.sql'
        ) 

    create_D_dataset_pathanamthitta = BigQueryOperator(
        task_id = 'create_D_dataset_pathanamthitta',
        use_legacy_sql = False,
        location = LOCATION,
        sql = './sql/D_dataset_pathanamthitta.sql'
        ) 

    create_D_dataset_kottayam = BigQueryOperator(
        task_id = 'create_D_dataset_kottayam',
        use_legacy_sql = False,
        location = LOCATION,
        sql = './sql/D_dataset_kottayam.sql'
        )         

    create_D_dataset_idukki = BigQueryOperator(
        task_id = 'create_D_dataset_idukki',
        use_legacy_sql = False,
        location = LOCATION,
        sql = './sql/D_dataset_idukki.sql'
        )

    create_D_dataset_ernakulam = BigQueryOperator(
        task_id = 'create_D_dataset_ernakulam',
        use_legacy_sql = False,
        location = LOCATION,
        sql = './sql/D_dataset_ernakulam.sql'
        )

    create_D_dataset_thrissur = BigQueryOperator(
        task_id = 'create_D_dataset_thrissur',
        use_legacy_sql = False,
        location = LOCATION,
        sql = './sql/D_dataset_thrissur.sql'
        )

    create_D_dataset_palakkad = BigQueryOperator(
        task_id = 'create_D_dataset_palakkad',
        use_legacy_sql = False,
        location = LOCATION,
        sql = './sql/D_dataset_palakkad.sql'
        )
    
    create_D_dataset_kozhikode = BigQueryOperator(
        task_id = 'create_D_dataset_kozhikode',
        use_legacy_sql = False,
        location = LOCATION,
        sql = './sql/D_dataset_kozhikode.sql'
        )
    
    create_D_dataset_malappuram = BigQueryOperator(
        task_id = 'create_D_dataset_malappuram',
        use_legacy_sql = False,
        location = LOCATION,
        sql = './sql/D_dataset_malappuram.sql'
        )
    
    create_D_dataset_wayanad = BigQueryOperator(
        task_id = 'create_D_dataset_wayanad',
        use_legacy_sql = False,
        location = LOCATION,
        sql = './sql/D_dataset_wayanad.sql'
        )
    
    create_D_dataset_kannur = BigQueryOperator(
        task_id = 'create_D_dataset_kannur',
        use_legacy_sql = False,
        location = LOCATION,
        sql = './sql/D_dataset_kannur.sql'
        )
    
    create_D_dataset_kasargod = BigQueryOperator(
        task_id = 'create_D_dataset_kasargod',
        use_legacy_sql = False,
        location = LOCATION,
        sql = './sql/D_dataset_kasargod.sql'
        )

    create_F_dataset_flood = BigQueryOperator(
        task_id = 'create_F_dataset_flood',
        use_legacy_sql = False,
        location = LOCATION,
        sql = './sql/F_dataset_flood.sql'
        )

    check_F_dataset_flood = BigQueryCheckOperator(
        task_id = 'check_F_dataset_flood',
        use_legacy_sql=False,
        location = LOCATION,
        sql = f'SELECT count(*) FROM `{PROJECT_ID}.{DATASET}.F_dataset_flood`'
        )

    finish_pipeline = DummyOperator(
        task_id = 'finish_pipeline',
        dag = dag
        ) 
start_pipeline >> load_staging_dataset >> load_flood_tweets >> check_dataset_flood >> create_District_Table >> [create_D_dataset_trivandrum, create_D_dataset_kollam, create_D_dataset_alappuzha, create_D_dataset_pathanamthitta, create_D_dataset_kottayam, create_D_dataset_idukki, create_D_dataset_ernakulam, create_D_dataset_thrissur, create_D_dataset_palakkad, create_D_dataset_malappuram, create_D_dataset_kozhikode, create_D_dataset_wayanad, create_D_dataset_kannur, create_D_dataset_kasargod] >> create_F_dataset_flood >> check_F_dataset_flood >> finish_pipeline
