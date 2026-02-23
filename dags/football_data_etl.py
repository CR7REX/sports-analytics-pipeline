from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import requests
import pandas as pd
import json
import os

# Football-Data.co.uk API endpoints
BASE_URL = "https://www.football-data.co.uk/mmz4281"

# Data paths - use environment variable or default to local path
DATA_DIR = os.environ.get('AIRFLOW_DATA_DIR', './data')
RAW_DATA_PATH = os.path.join(DATA_DIR, 'raw', 'matches.csv')

# Top European leagues
LEAGUES = {
    'E0': 'Premier League',
    'E1': 'Championship', 
    'D1': 'Bundesliga',
    'SP1': 'La Liga',
    'I1': 'Serie A',
    'F1': 'Ligue 1'
}

def extract_match_data(**context):
    """
    Extract match data from Football-Data.co.uk
    Downloads CSV data for current season
    """
    season = "2526"  # 2025-26 season format
    all_matches = []
    
    for league_code, league_name in LEAGUES.items():
        url = f"{BASE_URL}/{season}/{league_code}.csv"
        try:
            df = pd.read_csv(url)
            df['league'] = league_name
            df['league_code'] = league_code
            df['extracted_at'] = datetime.now().isoformat()
            all_matches.append(df)
            print(f"✅ Downloaded {len(df)} matches from {league_name}")
        except Exception as e:
            print(f"❌ Failed to download {league_name}: {e}")
    
    if all_matches:
        combined = pd.concat(all_matches, ignore_index=True)
        # Ensure directory exists
        os.makedirs(os.path.dirname(RAW_DATA_PATH), exist_ok=True)
        combined.to_csv(RAW_DATA_PATH, index=False)
        print(f"💾 Saved {len(combined)} total matches to {RAW_DATA_PATH}")
        return len(combined)
    return 0

def validate_data(**context):
    """
    Basic data quality checks
    """
    if not os.path.exists(RAW_DATA_PATH):
        raise ValueError(f"Raw data file not found at {RAW_DATA_PATH}!")
    
    df = pd.read_csv(RAW_DATA_PATH)
    
    # Quality checks
    checks = {
        'row_count': len(df) > 0,
        'has_home_team': 'HomeTeam' in df.columns,
        'has_away_team': 'AwayTeam' in df.columns,
        'has_date': 'Date' in df.columns,
        'no_null_teams': df['HomeTeam'].notna().all() and df['AwayTeam'].notna().all()
    }
    
    failed = [k for k, v in checks.items() if not v]
    if failed:
        raise ValueError(f"Data quality checks failed: {failed}")
    
    print(f"✅ All {len(checks)} data quality checks passed")
    return checks

# DAG definition
with DAG(
    'football_data_etl',
    default_args={
        'owner': 'CR7REX',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Daily ETL for football match data',
    schedule_interval='0 6 * * *',  # Daily at 6 AM
    start_date=days_ago(1),
    catchup=False,
    tags=['football', 'etl'],
) as dag:

    extract_task = PythonOperator(
        task_id='extract_match_data',
        python_callable=extract_match_data,
    )

    validate_task = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data,
    )

    extract_task >> validate_task
