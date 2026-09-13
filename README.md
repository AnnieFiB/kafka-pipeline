# Project1-StockMarket

## 📁 Project Structure

```text
Project1-StockMarket/
│
├── docker-compose.yml        # Orchestrates all services
├── .env                      # Environment variables, versions, passwords, and ports
│
├── kafka/                    # Kafka configuration and custom image
│   ├── Dockerfile
│   ├── prebuildfs/
│   └── rootfs/
│
├── api/                      # FastAPI service that publishes events to Kafka
│   ├── Dockerfile
│   ├── app.py
│   └── requirements.txt
│
├── spark/                    # Spark streaming processing
│   └── job.py                # Kafka → Spark → PostgreSQL pipeline
│
├── postgres/
│   └── init.sql              # Database and table initialization
│
├── scripts/                  # Utility and testing scripts
│   ├── send_test_event.sh
│   └── wait-for-url.sh
│
├── notebooks/                # Data exploration and analysis
│   └── exploration.ipynb
│
└── README.md                 # Project documentation
```

> **Note:** Make shell scripts executable before running:
>
> ```bash
> chmod +x scripts/*.sh
> ```


## Project Tech Stack and Flow

The pipeline processes stock market events through the following services:

```text
API
 ↓
Kafka
 ↓
Spark
 ↓
PostgreSQL
 ↓
Power BI
```

- **API** → Produces JSON events to Kafka.
- **Kafka** → Handles real-time event streaming.
- **Kafka UI** → Monitors Kafka topics and messages.
- **Spark** → Consumes Kafka events and processes streaming data.
- **PostgreSQL** → Stores processed data for analytics.
- **pgAdmin** → Provides a graphical interface for PostgreSQL.
- **Power BI** → Connects to PostgreSQL for reporting and visualization.

---

## Run the Containers

Stop existing containers:

```bash
docker compose down
```

Build the services:

```bash
docker compose build
```

Start the containers:

```bash
docker compose up -d
```

Check container status:

```bash
docker compose ps
```

---

## Health Checks

Check Kafka UI:

```bash
curl -fsS http://localhost:8088/actuator/health && echo " (Kafka UI OK)"
```

Check API:

```bash
curl -fsS http://localhost:8000/health && echo " (API OK)"
```

---

## Send Test Data

Using the test script:

```bash
./scripts/send_test_event.sh
```

Or send an event directly to the API:

```bash
curl -X POST http://localhost:8000/event \
  -H "content-type: application/json" \
  -d '{"source":"api","value":123,"category":"spark"}'
```

The event should flow through:

```text
API → Kafka → Spark → PostgreSQL
```

---

## Service Access

| Service    | Address                          | Details                                         |
| ---------- | -------------------------------- | ----------------------------------------------- |
| Kafka UI   | `http://localhost:8088`        | Topic`events` appears after the first message |
| API        | `http://localhost:8000/health` | API health endpoint                             |
| PostgreSQL | `localhost:5433`               | Database:`eventsdb`                           |
| pgAdmin    | `http://localhost:5050`        | PostgreSQL administration                       |
| Spark UI   | `http://localhost:4040`        | Available while Spark job is running            |

### pgAdmin Connection

Add a new server using:

```text
Host:     postgres
Database: eventsdb
Username: app
Password: app
```

The streaming data is stored in:

```text
public.events_stream
```

---

## Check Open Ports

```bash
docker compose ps
```

Check the PostgreSQL port mapping:

```bash
docker compose port postgres 5432
```

---

## View Container Logs

```bash
docker compose logs -f api
docker compose logs -f kafka
docker compose logs -f spark
```

---

## Restart Individual Services

Rebuild and restart the API after code changes:

```bash
docker compose up -d --build api
```

Restart Spark:

```bash
docker compose restart spark
```

---

## Validate Docker Compose

```bash
docker compose config
```

---

## Verify Spark Dependencies

Confirm that the Kafka and PostgreSQL JARs are available:

```bash
docker compose exec spark bash -lc \
'ls /opt/bitnami/spark/jars | egrep "kafka|postgresql"'
```

---

## Troubleshooting

If the API reports that an event was queued but no Kafka message appears, confirm:

```text
KAFKA_BOOTSTRAP=kafka:9092
```

Also confirm that the Kafka topic is:

```text
events
```

---

## 📊 Power BI

Connect Power BI to PostgreSQL using:

```text
Server:   localhost
Port:     5433
Database: eventsdb
Username: app
Password: app
```

Optional SQL query:

```sql
SELECT
    source,
    category,
    value,
    to_timestamp(ingested_at) AS ingested_at_ts,
    processed_ts
FROM public.events_stream;
```
