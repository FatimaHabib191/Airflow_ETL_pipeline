"""Run with: astro dev pytest"""

import importlib.util
from pathlib import Path

import pytest

DAG_FILE = Path(__file__).parents[2] / "dags" / "currency_exchange_etl.py"
spec = importlib.util.spec_from_file_location("currency_exchange_etl", DAG_FILE)
etl = importlib.util.module_from_spec(spec)
spec.loader.exec_module(etl)


def test_dag_has_three_tasks_in_etl_order():
    dag = etl.currency_exchange_etl_dag
    assert set(dag.task_ids) == {
        "extract_exchange_rates",
        "transform_exchange_rates",
        "load_to_mongodb",
    }
    assert dag.get_task("extract_exchange_rates").downstream_task_ids == {
        "transform_exchange_rates"
    }
    assert dag.get_task("transform_exchange_rates").downstream_task_ids == {
        "load_to_mongodb"
    }


def test_transform_matches_required_schema():
    raw = {
        "retrieved_at": "2026-11-05T07:15:00",
        "rates": [
            {"date": "2026-11-04", "base": "EUR", "quote": "USD", "rate": 1.15}
        ],
    }
    assert etl.transform_rates(raw) == [
        {
            "base_currency": "EUR",
            "target_currency": "USD",
            "exchange_rate": 1.15,
            "rate_date": "2026-11-04",
            "source": "Frankfurter API",
            "retrieved_at": "2026-11-05T07:15:00",
        }
    ]


def test_transform_rejects_non_positive_rate():
    raw = {
        "retrieved_at": "2026-11-05T07:15:00",
        "rates": [{"date": "2026-11-04", "base": "EUR", "quote": "USD", "rate": 0}],
    }
    with pytest.raises(ValueError):
        etl.transform_rates(raw)
