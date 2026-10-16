from datetime import timedelta

import pendulum
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator

from operators import (StageToRedshiftOperator, LoadFactOperator,
                       LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# S3 location of the project data (copied from s3://udacity-dend into the
# same region as the Redshift Serverless workgroup)
S3_BUCKET = 'sparkify-airflow-data-229160085404'
S3_REGION = 'us-east-1'
LOG_DATA_KEY = 'log-data'
LOG_JSONPATH = 's3://{}/log_json_path.json'.format(S3_BUCKET)
SONG_DATA_KEY = 'song-data/A/A/'

REDSHIFT_CONN_ID = 'redshift'
AWS_CONN_ID = 'aws_credentials'

default_args = {
    'owner': 'sparkify',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2018, 11, 1, tz='UTC'),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
}


@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule='0 * * * *',
    catchup=False,
    max_active_runs=1,
)
def final_project():

    start_operator = EmptyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id=REDSHIFT_CONN_ID,
        aws_credentials_id=AWS_CONN_ID,
        table='staging_events',
        s3_bucket=S3_BUCKET,
        s3_key=LOG_DATA_KEY,
        json_path=LOG_JSONPATH,
        region=S3_REGION,
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id=REDSHIFT_CONN_ID,
        aws_credentials_id=AWS_CONN_ID,
        table='staging_songs',
        s3_bucket=S3_BUCKET,
        s3_key=SONG_DATA_KEY,
        json_path='auto',
        region=S3_REGION,
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id=REDSHIFT_CONN_ID,
        table='songplays',
        sql=SqlQueries.songplay_table_insert,
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id=REDSHIFT_CONN_ID,
        table='users',
        sql=SqlQueries.user_table_insert,
        mode='truncate-insert',
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id=REDSHIFT_CONN_ID,
        table='songs',
        sql=SqlQueries.song_table_insert,
        mode='truncate-insert',
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id=REDSHIFT_CONN_ID,
        table='artists',
        sql=SqlQueries.artist_table_insert,
        mode='truncate-insert',
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id=REDSHIFT_CONN_ID,
        table='time',
        sql=SqlQueries.time_table_insert,
        mode='truncate-insert',
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id=REDSHIFT_CONN_ID,
        dq_checks=[
            {'check_sql': 'SELECT COUNT(*) FROM songplays', 'expected_result': 0, 'comparison': '>'},
            {'check_sql': 'SELECT COUNT(*) FROM songplays WHERE playid IS NULL', 'expected_result': 0},
            {'check_sql': 'SELECT COUNT(*) FROM users', 'expected_result': 0, 'comparison': '>'},
            {'check_sql': 'SELECT COUNT(*) FROM users WHERE userid IS NULL', 'expected_result': 0},
            {'check_sql': 'SELECT COUNT(*) FROM songs WHERE songid IS NULL', 'expected_result': 0},
            {'check_sql': 'SELECT COUNT(*) FROM artists WHERE artistid IS NULL', 'expected_result': 0},
            {'check_sql': 'SELECT COUNT(*) FROM "time" WHERE start_time IS NULL', 'expected_result': 0},
        ],
    )

    end_operator = EmptyOperator(task_id='Stop_execution')

    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table

    load_songplays_table >> [
        load_user_dimension_table,
        load_song_dimension_table,
        load_artist_dimension_table,
        load_time_dimension_table,
    ] >> run_quality_checks

    run_quality_checks >> end_operator


final_project_dag = final_project()
