from datetime import datetime, timedelta
import logging
import os

import requests
import pandas as pd
from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.transfers.local_to_s3 import (
    LocalFilesystemToS3Operator,
)
from bs4 import BeautifulSoup

logger = logging.getLogger("airflow.task")


# Utility Functions
def save_csv(headlines):
    """Save headlines to a CSV file."""
    os.makedirs("/opt/airflow/csv_files/headlines", exist_ok=True)
    filename = f'/opt/airflow/csv_files/headlines/{datetime.now().strftime("%B").lower()}_{datetime.now().strftime("%d_%Y_%H_%M")}_headlines.csv'
    df = pd.DataFrame(headlines)
    df.to_csv(filename, index=False)
    logger.info(f"CSV saved to {filename}")
    return filename


def extract_headlines(soup, container_id, headline_class=None):
    """Extract headlines from a BeautifulSoup object."""
    container = soup.find(id=container_id)
    if not container:
        return []

    headlines = []
    for headline in (
        container.find_all("div", class_=headline_class)
        if headline_class
        else [container]
    ):
        title = headline.find("a") or headline.find(["h1", "h2"])
        date = headline.find("h3")
        if title and date:
            headlines.append(
                {
                    "Title": title.text.strip(),
                    "Date": date.text.strip(),
                    "Source": "Inquirer" if "cmr" in container_id else "Philstar",
                }
            )
    logger.info(f"Extracted {len(headlines)} headlines from {container_id}")
    return headlines


def scrape_philstar(url, headers):
    """Scrape headlines from Philstar."""
    page = requests.get(url, headers=headers)
    soup = BeautifulSoup(page.content, "html.parser")
    headlines = []

    carousel = soup.find(id="carousel_light")
    if carousel:
        items = carousel.find_all("div", class_="carousel__item")
        for item in items:
            title = item.find("div", class_="carousel__item__title")
            time = item.find("div", class_="carousel__item__time")
            headlines.append(
                {
                    "Title": title.text.strip() if title else None,
                    "Date": time.text.strip() if time else None,
                    "Source": "Philstar",
                }
            )

    logger.info(f"Scraped {len(headlines)} headlines from Philstar")
    return headlines


def scrape_inquirer(url, headers):
    """Scrape headlines from Inquirer."""
    page = requests.get(url, headers=headers)
    soup = BeautifulSoup(page.content, "html.parser")
    all_headlines = []
    all_headlines.extend(extract_headlines(soup, "cmr-left"))
    all_headlines.extend(extract_headlines(soup, "cmr-right", "cmr-bgi"))
    logger.info(f"Scraped {len(all_headlines)} headlines from Inquirer")
    return all_headlines


# DAG Definition
with DAG(
    "scrape_headlines",
    start_date=datetime(2024, 12, 13),
    schedule_interval="*/5 * * * *",  # Every 5 minutes
    catchup=False,
) as dag:

    @task()
    def extract_headlines_task():
        """Extract headlines and save them to a CSV file."""
        philstar_url = "https://www.philstar.com/headlines"
        inquirer_url = "https://newsinfo.inquirer.net/"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
        }
        philstar_headlines = scrape_philstar(philstar_url, headers)
        inquirer_headlines = scrape_inquirer(inquirer_url, headers)
        all_headlines = philstar_headlines + inquirer_headlines

        logger.info(f"Scraped {len(all_headlines)} headlines")

        return save_csv(all_headlines)  # Save CSV and return the file path

    @task()
    def transform_data_task(csv_file_path: str):
        """Transform phrases such as "3 hours ago" into a date in the CSV file."""
        df = pd.read_csv(csv_file_path)
        for idx, row in df.iterrows():
            if "ago" in row["Date"]:
                hours_ago = int(row["Date"].split()[0])
                transformed_date = datetime.now() - timedelta(hours=hours_ago)
                df.at[idx, "Date"] = transformed_date.strftime("%B %d, %Y")

        # Save the transformed data back to a new CSV
        transformed_csv_path = csv_file_path
        df.to_csv(transformed_csv_path, index=False)
        logger.info(f"Transformed data saved to {transformed_csv_path}")
        return transformed_csv_path

    @task()
    def generate_s3_key():
        """Generate the S3 key for the CSV file."""
        s3_key = f"headlines/{datetime.now().strftime('%B').lower()}_{datetime.now().strftime('%d_%Y_%H_%M')}_headlines.csv"
        return s3_key

    upload_to_s3 = LocalFilesystemToS3Operator(
        filename="{{ task_instance.xcom_pull(task_ids='transform_data_task') }}",
        task_id="upload_csv_to_s3",
        dest_key="{{ ti.xcom_pull(task_ids='generate_s3_key') }}",
        dest_bucket="airflow-s3-bucket-upload",
        aws_conn_id="aws-s3-bucket-conn",
    )

    # Task Dependencies
    extracted_file = extract_headlines_task()
    transformed_file = transform_data_task(extracted_file)

    transformed_file >> generate_s3_key() >> upload_to_s3
