from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2023, 1, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False
}

dag = DAG('udac_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          catchup=False)

# Begin execution
start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

# Stage events from S3 to Redshift
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket="lucas-furlan",
    s3_key="log-data",  # Adjust path or use templated field if necessary
    json_path="s3://lucas-furlan/log_json_path.json"
)

# Stage songs from S3 to Redshift
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket="lucas-furlan",
    s3_key="song-data",
    json_path="auto"
)

# Load songplays fact table
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_query=SqlQueries.songplay_table_insert,
    table="songplays"
)

# Load user dimension table
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_query=SqlQueries.user_table_insert,
    table="users",
    truncate_before_insert=True
)

# Load song dimension table
load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_query=SqlQueries.song_table_insert,
    table="songs",
    truncate_before_insert=True
)

# Load artist dimension table
load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_query=SqlQueries.artist_table_insert,
    table="artists",
    truncate_before_insert=True
)

# Load time dimension table
load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql_query=SqlQueries.time_table_insert,
    table="time",
    truncate_before_insert=True
)

# Run data quality checks
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tests=[
        {'sql': "SELECT COUNT(*) FROM songplays WHERE playid IS NULL", 'expected_result': 0},
        {'sql': "SELECT COUNT(*) FROM users", 'expected_result': 0}
        # Add additional tests as needed
    ]
)

# End of the DAG
end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

# Set task dependencies
start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]

[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table

load_songplays_table >> [load_user_dimension_table,
                         load_song_dimension_table,
                         load_artist_dimension_table,
                         load_time_dimension_table]

[load_user_dimension_table,
 load_song_dimension_table,
 load_artist_dimension_table,
 load_time_dimension_table] >> run_quality_checks

run_quality_checks >> end_operator
