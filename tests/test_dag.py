"""Tests for football_data_etl DAG"""
import pytest
import pandas as pd
from datetime import datetime


def test_get_current_season():
    """Test season calculation logic"""
    # Import the function from DAG
    import sys
    sys.path.insert(0, 'dags')
    from football_data_etl import get_current_season
    
    season = get_current_season()
    assert len(season) == 4
    assert season.isdigit()


def test_column_mapping():
    """Test data column mapping"""
    sample_data = {
        'Date': ['01/01/25'],
        'HomeTeam': ['Arsenal'],
        'AwayTeam': ['Chelsea'],
        'FTHG': [2],
        'FTAG': [1],
        'FTR': ['H']
    }
    df = pd.DataFrame(sample_data)
    
    # Test lowercase conversion
    df.columns = [col.lower() for col in df.columns]
    
    assert 'hometeam' in df.columns
    assert 'date' in df.columns


def test_date_parsing():
    """Test date format handling"""
    # Test DD/MM/YY format
    date_str = "05/03/25"
    result = pd.to_datetime(date_str, format='%d/%m/%y')
    assert result.year == 2025
    
    # Test DD/MM/YYYY format
    date_str = "05/03/2025"
    result = pd.to_datetime(date_str, format='%d/%m/%Y')
    assert result.year == 2025
