# Use a scheduling tool like Apache Airflow or cron to automate the pipeline. 
# Monitor the migration process using the data_migration_log table.

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

def transform_data():
    import pandas as pd
    df = pd.read_csv("/tmp/shipments_data.csv")
    df["shipment_date"] = pd.to_datetime(df["shipment_date"]).dt.strftime("%Y-%m-%d %H:%M:%S")
    df.to_csv("/tmp/transformed_shipments_data.csv", index=False)

default_args = {
    "owner": "pepsa_team",
    "depends_on_past": False,
    "start_date": datetime(2023, 10, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "mysql_to_postgres_migration",
    default_args=default_args,
    description="Migrate shipment data from MySQL to PostgreSQL",
    schedule_interval=timedelta(days=1),
)

extract_task = BashOperator(
    task_id="extract_data",
    bash_command="mysql -u mysql_user -p -D mysql_db -e 'SELECT * FROM shipments LIMIT 100000 INTO OUTFILE \'/tmp/shipments_data.csv\' FIELDS TERMINATED BY \',\' ENCLOSED BY \'\"\' LINES TERMINATED BY \'\n\';'",
    dag=dag,
)

transform_task = PythonOperator(
    task_id="transform_data",
    python_callable=transform_data,
    dag=dag,
)

load_task = BashOperator(
    task_id="load_data",
    bash_command="psql -U postgres_user -d postgres_db -c \"COPY shipments FROM '/tmp/transformed_shipments_data.csv' WITH CSV HEADER;\"",
    dag=dag,
)

extract_task >> transform_task >> load_task