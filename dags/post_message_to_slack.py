from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from airflow import DAG
from airflow.models import Variable, XCom
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd
import logging
from datetime import datetime

logger = logging.getLogger("airflow.task")

with DAG(
    "post_message_to_slack",
    start_date=datetime(2024, 12, 13),
    schedule="*/5 * * * *",  # Every 5 minutes
    catchup=False,
) as dag:

    wait_for_s3_upload_task = ExternalTaskSensor(
        task_id="wait_for_s3_upload_task",
        external_dag_id="scrape_headlines",  # the DAG that contains the scraping task
        external_task_id="upload_csv_to_s3",  # the task within the other DAG
        poke_interval=60,  # Check every 60 seconds
        timeout=300,  # Stop poking after 5 minutes
        mode="poke",  # 'poke' mode keeps checking the task until it succeeds or times out
    )

    @task(provide_context=True)
    def post_message_to_slack(ti):
        SLACK_BOT_TOKEN = Variable.get("SLACK_BOT_TOKEN")

        s3_key = XCom.get_one(
            execution_date=ti.execution_date,
            task_id="generate_s3_key",
            dag_id="scrape_headlines",
        )

        s3_hook = S3Hook(aws_conn_id="aws-s3-bucket-conn")
        value = s3_hook.read_key(s3_key, bucket_name="airflow-s3-bucket-upload")

        # Check if the value is None (data not found)
        if value is None:
            logger.error("No data found in XCom.")
            return

        # Check if the value is a string and attempt to convert it to DataFrame
        if isinstance(value, str):
            # Check if it's a CSV string
            try:
                logger.info("Attempting to convert value from CSV string to DataFrame.")
                from io import StringIO

                df = pd.read_csv(StringIO(value))  # Use StringIO to simulate a file
            except Exception as e:
                logger.error(f"Error converting CSV string to DataFrame: {e}")
                return

        slack_message = ""

        for index, row in df.iterrows():
            slack_message += f"*Headline: {row['Title']}*\n"
            slack_message += f"\t✏️ Date: {row['Date']}\n"
            slack_message += f"\t📎 Source: {row['Source']}\n\n"

        logger.info(f"Formatted Data: {slack_message}")

        try:
            client = WebClient(token=SLACK_BOT_TOKEN)

            response = client.chat_postMessage(
                channel="C085PUH3R89",
                text=f"*✅ Data successfully loaded to s3! Headlines for today:*\n\n\n {slack_message}",
            )

            logger.info(f"Slack Response: {response}")
        except SlackApiError as e:
            logger.error(f"Slack API Error: {e.response['error']}")
        except Exception as e:
            logger.error(f"An error occurred: {str(e)}")

    wait_for_s3_upload_task >> post_message_to_slack()
