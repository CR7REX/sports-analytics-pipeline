# Sports Analytics Pipeline

[![Python](https://img.shields.io/badge/Python-3.11-blue.svg)](https://www.python.org/)
[![Apache Airflow](https://img.shields.io/badge/Airflow-2.8+-017CEE.svg)](https://airflow.apache.org/)
[![dbt](https://img.shields.io/badge/dbt-1.7+-FF694B.svg)](https://www.getdbt.com/)

A data pipeline for football stats because I got tired of checking league tables manually.

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Data Source    │     │   Processing    │     │   Serving       │
│                 │     │                 │     │                 │
│ Football-Data   │────▶│  Airflow DAGs   │────▶│  Streamlit      │
│ (.co.uk CSVs)   │     │  (Daily ETL)    │     │  Dashboard      │
└─────────────────┘     └────────┬────────┘     └─────────────────┘
                                 │
                                 ▼
                        ┌─────────────────┐
                        │   PostgreSQL    │
                        │   (Raw Data)    │
                        └────────┬────────┘
                                 │
                                 ▼
                        ┌─────────────────┐
                        │      dbt        │
                        │  (Transform &   │
                        │   Data Quality) │
                        └────────┬────────┘
                                 │
                                 ▼
                        ┌─────────────────┐
                        │   PostgreSQL    │
                        │  (Modeled Data) │
                        └─────────────────┘
```

**Data Flow**: CSV Download → Airflow Orchestration → Raw Storage → dbt Modeling → Analytics-Ready Data → Streamlit Viz

## What this actually does

Grabs match data from Football-Data.co.uk every day, shoves it through Airflow, models it with dbt, and spits out a Streamlit dashboard so I can see who's actually performing vs who just got lucky.

## Why I built this

Mostly because I wanted an excuse to use Airflow in a real project. Also because arguing about football stats with incomplete data is annoying.

## The setup

Data comes from here → Airflow DAGs run daily → dbt models clean it up → Streamlit shows pretty charts

I was going to use StatsBomb but their API requires a PhD to authenticate. Football-Data.co.uk gives you CSVs directly. No API key, no rate limits, no drama.

## What's working so far

- **Daily scraping** of match results (Premier League, La Liga, Bundesliga, Serie A, Ligue 1)
- **Dynamic season calculation** - automatically switches seasons based on current date
- **Data quality checks** with dbt (not null, unique constraints, date validation)
- **dbt marts models**: league standings, top scorers, team form
- **Streamlit dashboard** with dbt-powered analytics:
  - League standings with Champions League (top 4) and relegation highlighting
  - Top scorers rankings with home/away breakdown
  - Team form analysis (last 5 matches)
  - Interactive charts and team comparison
  - **Refresh button** to clear cache and reload data

## What's not working / TODO

- [x] Actually finish the dbt marts models (league tables, top scorers)
- [x] Make the Streamlit dashboard not look like it was made in 1995
- [x] Integrate dbt into Airflow DAG
- [ ] Add xG data if I can find a free source
- [ ] Maybe some betting odds comparison if I'm feeling spicy
- [ ] Add email/Slack alerts for DAG failures

## Tech stuff

- **Airflow** for scheduling
- **dbt** for data modeling
- **PostgreSQL** for storage (running in Docker)
- **Streamlit** for visualization
- **Docker** because I don't want to install Postgres on my machine

## Quick Start

```bash
# Clone and start everything
git clone https://github.com/CR7REX/sports-analytics-pipeline.git
cd sports-analytics-pipeline
make up

# Or manually:
docker-compose up -d --build
```

Then go to:
- **Airflow**: http://localhost:8080 (admin/admin)
- **Streamlit**: http://localhost:8501

## Useful Commands

```bash
# View logs
make logs

# Run dbt manually
make dbt-run

# Run tests
make test

# Restart everything
make restart

# Clean up (removes all data!)
make clean
```

## Project Structure

```
.
├── dags/                       # Airflow DAGs
│   └── football_data_etl.py   # Main ETL pipeline
├── dbt/                        # dbt models
│   ├── models/
│   │   ├── staging/           # Staging models
│   │   └── marts/             # Marts models
│   ├── profiles.yml           # dbt profiles
│   └── dbt_project.yml        # dbt project config
├── streamlit/                  # Streamlit app
│   ├── app.py                 # Main dashboard
│   ├── Dockerfile             # Streamlit container
│   └── requirements.txt       # Python deps
├── tests/                      # Unit tests
├── docker-compose.yml          # Docker services
├── Makefile                    # Useful commands
└── README.md                   # You are here
```

## What I learned so far

- Airflow's TaskFlow API is actually pretty nice once you get used to it
- dbt tests save you from embarrassing data errors
- Football data is surprisingly messy (who formats dates like DD/MM/YY in 2025??)
- Docker networking is still black magic to me
- SQLAlchemy 2.0+ changed a lot of things (no more `conn.commit()`)

## Data source

[Football-Data.co.uk](https://www.football-data.co.uk/) - free historical data going back to the 90s. Not the most detailed but good enough for this.

## Troubleshooting

**Streamlit shows old data?**
Click the "🔄 Refresh Data" button or wait for the 1-hour cache to expire.

**DAG failed at load_to_postgres?**
Check if the table has dependent views. We use TRUNCATE+INSERT instead of REPLACE to avoid this.

**dbt command not found in Airflow?**
We install dbt at runtime. If it fails, exec into the container and run:
```bash
docker exec -u airflow sports_airflow_scheduler /home/airflow/.local/bin/pip install dbt-core dbt-postgres
```

---

*Built while arguing about whether xG is actually useful or just fancy stats for nerds.*
