# Project1-StockMarket 

## Repo Structure

Project1-StockMarket/
├─ docker-compose.yml          # Orchestrates API, Kafka, Spark, Postgres, pgAdmin, Kafka UI
├─ .env                        # versions, passwords, ports
│
├─ kafka/                      # custom Kafka build
│  ├─ Dockerfile
│  ├─ prebuildfs/
│  └─ rootfs/
│
├─ api/                        # API service (publishes to Kafka)
│  ├─ Dockerfile
│  ├─ app.py                   # Example FastAPI producer
│  ├─ requirements.txt
│
├─ spark/                      # Spark jobs consuming Kafka and writing to Postgres
│  ├─ job.py                   # Structured Streaming job (Kafka → Postgres)
│
├─ postgres/
│  ├─ init.sql                 # Initializes DB (events table etc.)
│
├─ scripts/ (chmod +x scripts/*.sh)
│  ├─ send_test_event.sh       # quick curl to API
│  └─ wait-for-url.sh          # tiny helper used by healthchecks
│
├─ notebooks/                  # For ad-hoc exploration
│  └─ exploration.ipynb
│
└─ README.md                   # Setup + run instructions

## Project Tech Stack and Flow

- Kafka UI → inspect topics/messages.
- API → produces JSON events into Kafka.
- Spark → consumes from Kafka, writes to Postgres.
- Postgres → stores results for analytics.
- pgAdmin → manage Postgres visually.
- Power BI → external (connects to Postgres at localhost:5432).

## Run Container

docker compose down
docker compose build
docker compose up -d -- start up container

-- health check
    curl -fsS http://localhost:8088/actuator/health && echo " (Kafka UI OK)"
    curl -fsS http://localhost:8000/health && echo " (API OK)"

-- send data:
    ./scripts/send_test_event.sh
-- Send a test event and verify rows land in Postgres:
    curl -X POST http://localhost:8000/event \
    -H "content-type: application/json" \
    -d '{"source":"api","value":123,"category":"spark"}'

## Output

- Kafka UI → http://localhost:8088  (topic events appears after first message)
- API → http://localhost:8000/health
- Postgres → localhost:5433 (db eventsdb; table public.events_stream)
- pgAdmin → http://localhost:5050  (add server host postgres, user/pass app/app). add a server with:
        Host: postgres | DB: eventsdb | User/Pass: app / app
- Spark driver UI shows at http://localhost:4040  while the job is running

## check opened ports
    docker compose ps
    docker compose port postgres 5433
        netstat -ano | findstr :5433

## Tail logs
    docker compose logs -f api
    docker compose logs -f kafka
    docker compose logs -f spark

## Restart a single service after edits
    docker compose up -d --build api
    docker compose restart spark

## Validate compose file: docker compose config

## Confirm Spark has the Kafka & Postgres jars: docker compose exec spark bash -lc 'ls /opt/bitnami/spark/jars | egrep "kafka|postgresql"'

## API says queued but no Kafka messages: Check KAFKA_BOOTSTRAP is kafka:9092 inside the API container and that topic is events


## Power BI
    Connect to PostgreSQL:
        Server: localhost
        Database: eventsdb
        Credentials: user app, password app
        Optional SQL:
            SELECT source, category, value, to_timestamp(ingested_at) AS ingested_at_ts, processed_ts
                    FROM public.events_stream;