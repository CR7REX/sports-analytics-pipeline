import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

st.set_page_config(
    page_title="Football Analytics Dashboard",
    page_icon="⚽",
    layout="wide"
)

st.title("⚽ Football Analytics Dashboard")

# Load data
@st.cache_data
def load_data():
    try:
        df = pd.read_csv("data/raw/matches.csv")
        # Handle DD/MM/YYYY format
        df['Date'] = pd.to_datetime(df['Date'], format='%d/%m/%Y', errors='coerce')
        return df
    except:
        return pd.DataFrame()

df = load_data()

if df.empty:
    st.warning("📭 No data available. Run the Airflow DAG to fetch match data.")
    st.info("To fetch data: `docker-compose up airflow-scheduler`")
else:
    # Sidebar filters
    st.sidebar.header("🔍 Filters")
    leagues = st.sidebar.multiselect(
        "Select Leagues",
        options=df['league'].unique() if 'league' in df.columns else [],
        default=df['league'].unique() if 'league' in df.columns else []
    )
    
    # Filter data
    if leagues and 'league' in df.columns:
        filtered_df = df[df['league'].isin(leagues)]
    else:
        filtered_df = df
    
    # KPIs
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Matches", len(filtered_df))
    with col2:
        if 'FTHG' in filtered_df.columns and 'FTAG' in filtered_df.columns:
            avg_goals = (filtered_df['FTHG'] + filtered_df['FTAG']).mean()
            st.metric("Avg Goals per Match", f"{avg_goals:.2f}")
    with col3:
        if 'league' in filtered_df.columns:
            st.metric("Leagues", filtered_df['league'].nunique())
    
    # Charts
    st.subheader("📊 Match Results Distribution")
    if 'FTR' in filtered_df.columns:
        result_counts = filtered_df['FTR'].value_counts()
        fig = px.pie(
            values=result_counts.values,
            names=['Home Win', 'Draw', 'Away Win'],
            title="Match Results"
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Raw data
    st.subheader("📋 Recent Matches")
    st.dataframe(filtered_df.head(20), use_container_width=True)

# Footer
st.markdown("---")
st.caption("Built with ❤️ using Streamlit • Data from Football-Data.co.uk")
