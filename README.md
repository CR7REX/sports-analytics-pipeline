# Sports Analytics Pipeline ⚽

[![Python](https://img.shields.io/badge/Python-3.11-blue.svg)](https://www.python.org/)
[![Apache Airflow](https://img.shields.io/badge/Airflow-2.8+-017CEE.svg)](https://airflow.apache.org/)
[![dbt](https://img.shields.io/badge/dbt-1.7+-FF694B.svg)](https://www.getdbt.com/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

End-to-end data pipeline for football analytics, processing match data from multiple sources into actionable insights.

## 🎯 Overview

This project demonstrates modern data engineering practices by building a complete analytics pipeline for football data:

- **Data Ingestion**: Automated extraction from StatsBomb and FBref APIs
- **Orchestration**: Apache Airflow for workflow management
- **Transformation**: dbt for data modeling and testing
- **Storage**: BigQuery for data warehouse
- **Visualization**: Streamlit for interactive dashboards

## 🏗️ Architecture

```
┌─────────────────┐     ┌─────────────┐     ┌──────────────┐
│   Data Sources  │────▶│   Airflow   │────▶│   BigQuery   │
│  (StatsBomb,    │     │   (DAGs)    │     │  (Raw Data)  │
│    FBref)       │     └─────────────┘     └──────────────┘
└─────────────────┘                                  │
                                                     ▼
┌─────────────────┐     ┌─────────────┐     ┌──────────────┐
│  Streamlit App  │◀────│   dbt       │◀────│   BigQuery   │
│  (Dashboard)    │     │  (Models)   │     │  (Analytics) │
└─────────────────┘     └─────────────┘     └──────────────┘
```

## 🚀 Features

- **Automated Data Collection**: Daily ingestion of match results, player stats, and league standings
- **Data Quality**: Great Expectations for validation and anomaly detection
- **Dimensional Modeling**: Star schema optimized for analytical queries
- **Performance Metrics**: xG analysis, form tracking, and team comparisons
- **Interactive Dashboards**: Real-time visualization of key metrics

## 🛠️ Tech Stack

| Component | Technology |
|-----------|------------|
| Orchestration | Apache Airflow |
| Data Warehouse | BigQuery |
| Transformation | dbt |
| Data Quality | Great Expectations |
| Visualization | Streamlit |
| Infrastructure | Docker, Terraform |

## 📁 Project Structure

```
.
├── dags/                    # Airflow DAGs
│   ├── extract_matches.py
│   ├── extract_players.py
│   └── daily_etl.py
├── dbt/                     # dbt project
│   ├── models/
│   │   ├── staging/
│   │   ├── intermediate/
│   │   └── marts/
│   └── tests/
├── streamlit/               # Dashboard app
│   └── app.py
├── docker-compose.yml       # Local development
└── README.md
```

## 🚦 Quick Start

### Prerequisites

- Python 3.11+
- Docker & Docker Compose
- Google Cloud Platform account

### Local Development

```bash
# Clone the repository
git clone https://github.com/CR7REX/sports-analytics-pipeline.git
cd sports-analytics-pipeline

# Start Airflow and dependencies
docker-compose up -d

# Access Airflow UI
open http://localhost:8080

# Run dbt models
cd dbt
dbt run
dbt test
```

## 📊 Sample Insights

- League standings with rolling form (last 5 matches)
- Player performance radar charts
- Team comparison matrices
- Goal prediction based on xG

## 🗺️ Roadmap

- [ ] Add real-time match event streaming
- [ ] Implement ML models for match outcome prediction
- [ ] Expand to additional leagues and sports
- [ ] Add betting odds comparison module

## 📝 License

MIT License - see [LICENSE](LICENSE) for details.

---

*Built with passion for football and data* ⚽📊
