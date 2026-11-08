"""Daily ETL: Frankfurter exchange rates -> transform -> MongoDB.

Tasks hand data to each other only through named XComs:
    extract_exchange_rates    pushes  raw_exchange_rates
    transform_exchange_rates  pulls   raw_exchange_rates
                              pushes  transformed_exchange_rates
    load_to_mongodb           pulls   transformed_exchange_rates
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone

import requests
from airflow.sdk import dag, task
from pymongo import MongoClient, UpdateOne

log = logging.getLogger(__name__)

API_URL = os.getenv("FRANKFURTER_API_URL", "https://api.frankfurter.dev/v2/rates")
BASE_CURRENCY = "EUR"
TARGET_CURRENCIES = ("USD", "GBP", "JPY")
SOURCE_NAME = "Frankfurter API"

MONGO_DATABASE = "currency_db"
MONGO_COLLECTION = "exchange_rates"

RAW_XCOM_KEY = "raw_exchange_rates"
TRANSFORMED_XCOM_KEY = "transformed_exchange_rates"


def fetch_rates() -> dict:
    """Call the API and return its rows with the time they were retrieved."""
    response = requests.get(
        API_URL,
        params={"base": BASE_CURRENCY, "quotes": ",".join(TARGET_CURRENCIES)},
        timeout=(5, 30),
    )
    response.raise_for_status()
    rows = response.json()

    if not isinstance(rows, list) or not rows:
        raise ValueError(f"Unexpected API response: {rows!r}")

    retrieved_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    return {"retrieved_at": retrieved_at, "rates": rows}


def transform_rates(raw: dict) -> list[dict]:
    """Convert raw API rows into the documents stored in MongoDB."""
    documents = []
    for row in raw["rates"]:
        rate = float(row["rate"])
        if rate <= 0:
            raise ValueError(f"Invalid exchange rate in row: {row!r}")

        documents.append(
            {
                "base_currency": row["base"].upper(),
                "target_currency": row["quote"].upper(),
                "exchange_rate": rate,
                "rate_date": row["date"],
                "source": SOURCE_NAME,
                "retrieved_at": raw["retrieved_at"],
            }
        )
    return documents


def upsert_rates(documents: list[dict], mongo_uri: str) -> dict:
    """Upsert one document per currency pair per day, so reruns add no duplicates."""
    key_fields = ("base_currency", "target_currency", "rate_date")
    operations = [
        UpdateOne(
            {field: document[field] for field in key_fields},
            {"$set": document},
            upsert=True,
        )
        for document in documents
    ]

    with MongoClient(mongo_uri, serverSelectionTimeoutMS=10_000) as client:
        collection = client[MONGO_DATABASE][MONGO_COLLECTION]
        collection.create_index(
            [(field, 1) for field in key_fields],
            unique=True,
            name="unique_pair_per_day",
        )
        result = collection.bulk_write(operations, ordered=False)

    return {
        "inserted": result.upserted_count,
        "updated": result.modified_count,
        "unchanged": result.matched_count - result.modified_count,
    }


@dag(
    dag_id="currency_exchange_etl",
    description="Load daily currency exchange rates into MongoDB",
    schedule="@daily",
    start_date=datetime(2026, 11, 1, tzinfo=timezone.utc),
    catchup=False,
    is_paused_upon_creation=False,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=2)},
    tags=["etl", "currency", "mongodb"],
)
def currency_exchange_etl():
    @task
    def extract_exchange_rates(**context) -> None:
        raw = fetch_rates()
        log.info("Extracted %d rates at %s", len(raw["rates"]), raw["retrieved_at"])
        context["ti"].xcom_push(key=RAW_XCOM_KEY, value=raw)

    @task
    def transform_exchange_rates(**context) -> None:
        ti = context["ti"]
        raw = ti.xcom_pull(task_ids="extract_exchange_rates", key=RAW_XCOM_KEY)
        documents = transform_rates(raw)
        log.info("Transformed documents: %s", documents)
        ti.xcom_push(key=TRANSFORMED_XCOM_KEY, value=documents)

    @task
    def load_to_mongodb(**context) -> None:
        documents = context["ti"].xcom_pull(
            task_ids="transform_exchange_rates", key=TRANSFORMED_XCOM_KEY
        )
        summary = upsert_rates(documents, os.environ["MONGO_URI"])
        log.info(
            "Loaded into %s.%s: %s", MONGO_DATABASE, MONGO_COLLECTION, summary
        )

    extract_exchange_rates() >> transform_exchange_rates() >> load_to_mongodb()


currency_exchange_etl_dag = currency_exchange_etl()
