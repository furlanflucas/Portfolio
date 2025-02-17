stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket="sean-murdock",
    s3_key="log-data",  # You can even use templating here: e.g., "log-data/{{ ds }}/" if needed.
    json_path="s3://sean-murdock/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket="sean-murdock",
    s3_key="song-data",
    json_path="auto"  # For song data, often using 'auto' works.
)
