run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tests=[
        {'sql': "SELECT COUNT(*) FROM songplays WHERE playid IS NULL", 'expected_result': 0},
        {'sql': "SELECT COUNT(*) FROM users", 'expected_result': 0}  # You can add more tests.
    ]
)
