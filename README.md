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

- Daily scraping of match results (Premier League, La Liga, Bundesliga, Serie A, Ligue 1)
- Data quality checks with dbt (not null, unique constraints, date validation)
- **dbt marts models**: league standings, top scorers, team form
- **Streamlit dashboard** with dbt-powered analytics:
  - League standings with Champions League (top 4) and relegation highlighting
  - Top scorers rankings with home/away breakdown
  - Team form analysis (last 5 matches)
  - Interactive charts and team comparison

## What's not working / TODO

- [x] Actually finish the dbt marts models (league tables, top scorers)
- [x] Make the Streamlit dashboard not look like it was made in 1995
- [ ] Add xG data if I can find a free source
- [ ] Maybe some betting odds comparison if I'm feeling spicy

## Tech stuff

- **Airflow** for scheduling
- **dbt** for data modeling (trying to follow best practices but probably failing)
- **BigQuery** for storage (the free tier is generous)
- **Streamlit** for visualization
- **Docker** because I don't want to install Postgres on my machine

## Running it locally

```bash
git clone https://github.com/CR7REX/sports-analytics-pipeline.git
cd sports-analytics-pipeline
docker-compose up -d
```

Then go to `localhost:8080` for Airflow, `localhost:8501` for Streamlit.

## What I learned so far

- Airflow's TaskFlow API is actually pretty nice once you get used to it
- dbt tests save you from embarrassing data errors
- Football data is surprisingly messy (who formats dates like DD/MM/YY in 2025??)
- Docker networking is still black magic to me

## Data source

[Football-Data.co.uk](https://www.football-data.co.uk/) - free historical data going back to the 90s. Not the most detailed but good enough for this.

---

*Built while arguing about whether xG is actually useful or just fancy stats for nerds.*
