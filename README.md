# Currency Exchange ETL with Airflow (Astro CLI) and MongoDB

Daily Airflow pipeline that pulls exchange rates from the
[Frankfurter API](https://frankfurter.dev/), reshapes them, and stores them in MongoDB.
Mongo Express provides the web interface for the database.

```
Frankfurter API -> extract_exchange_rates -> transform_exchange_rates -> load_to_mongodb -> MongoDB <- Mongo Express
                        XCom: raw_exchange_rates   XCom: transformed_exchange_rates
```

## Requirements

- Docker Desktop, running
- [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli)

## Run

```bash
astro dev start
astro dev run dags trigger currency_exchange_etl
```

| Service       | URL                     | Login              |
|---------------|-------------------------|--------------------|
| Airflow       | http://localhost:8080   | none needed        |
| Mongo Express | http://localhost:8081   | admin / admin123   |
| MongoDB       | localhost:27018         | admin / admin123   |

In Mongo Express, open `currency_db` and then `exchange_rates`.

## Pipeline

| Task                       | Reads XCom                   | Writes XCom                  | Job |
|----------------------------|------------------------------|------------------------------|-----|
| `extract_exchange_rates`   | none                         | `raw_exchange_rates`         | Calls `/v2/rates` for EUR to USD, GBP, JPY |
| `transform_exchange_rates` | `raw_exchange_rates`         | `transformed_exchange_rates` | Builds the target documents |
| `load_to_mongodb`          | `transformed_exchange_rates` | none                         | Upserts into `currency_db.exchange_rates` |

Stored document:

```json
{
  "base_currency": "EUR",
  "target_currency": "USD",
  "exchange_rate": 1.15,
  "rate_date": "2026-11-04",
  "source": "Frankfurter API",
  "retrieved_at": "2026-11-05T07:15:00"
}
```

`retrieved_at` is in UTC. A unique index on currency pair and `rate_date` means a rerun
updates the day's documents and never duplicates them.

## Docker networking and storage

`docker-compose.override.yml` adds `mongodb` and `mongo-express` to Astro's stack.

- **Networking:** both services join Astro's `airflow` bridge network, so Airflow and
  Mongo Express reach the database by name at `mongodb:27017`.
- **Persistent storage:** the named volumes `mongo_data` and `mongo_config` keep the data
  across `astro dev stop` and `astro dev restart`. `astro dev kill` deletes them.

## Useful commands

```bash
astro dev pytest                                   # unit and DAG structure tests
astro dev logs -s                                  # scheduler logs
docker exec currency_mongodb mongosh -u admin -p admin123 --authenticationDatabase admin --quiet \
  --eval 'db.getSiblingDB("currency_db").exchange_rates.find({},{_id:0}).toArray()'
astro dev stop
```

If `astro dev start` reports that port 5432 or 8080 is taken, run
`astro config set postgres.port 5435` or `astro config set api-server.port 8090`.
