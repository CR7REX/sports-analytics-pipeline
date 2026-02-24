import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import psycopg2
from sqlalchemy import create_engine

st.set_page_config(
    page_title="Football Analytics Dashboard",
    page_icon="⚽",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("⚽ Football Analytics Dashboard")
st.markdown("Real-time insights from European football leagues")

# Database connection
@st.cache_resource
def get_db_engine():
    """Create SQLAlchemy engine for PostgreSQL connection"""
    try:
        # Docker-compose service name
        engine = create_engine('postgresql://airflow:airflow@postgres:5432/airflow')
        return engine
    except Exception as e:
        st.error(f"Database connection failed: {e}")
        return None

# Load data from dbt models
@st.cache_data(ttl=3600)
def load_dbt_models():
    """Load data from dbt marts models"""
    engine = get_db_engine()
    if engine is None:
        return None, None, None
    
    try:
        # Load league standings from public_marts schema
        standings = pd.read_sql("SELECT * FROM public_marts.fct_league_standings ORDER BY league, rank", engine)
        # Load top scorers from public_marts schema
        scorers = pd.read_sql("SELECT * FROM public_marts.fct_top_scorers ORDER BY league, league_rank", engine)
        # Load team form from public_marts schema
        team_form = pd.read_sql("SELECT * FROM public_marts.fct_team_form ORDER BY league, form_rank", engine)
        return standings, scorers, team_form
    except Exception as e:
        st.warning(f"Could not load dbt models: {e}")
        return None, None, None

# Load raw match data (fallback)
@st.cache_data(ttl=3600)
def load_raw_data():
    try:
        df = pd.read_csv("data/raw/matches.csv")
        df['Date'] = pd.to_datetime(df['Date'], format='%d/%m/%Y', errors='coerce')
        df['Month'] = df['Date'].dt.to_period('M').astype(str)
        df['Week'] = df['Date'].dt.to_period('W').astype(str)
        df['TotalGoals'] = df['FTHG'] + df['FTAG']
        return df
    except Exception as e:
        return pd.DataFrame()

# Try to load dbt models first, fallback to raw data
standings_df, scorers_df, team_form_df = load_dbt_models()
df = load_raw_data()

# Check if we have data
has_dbt_data = standings_df is not None and not standings_df.empty
has_raw_data = not df.empty

if not has_dbt_data and not has_raw_data:
    st.warning("📭 No data available. Run the Airflow DAG to fetch match data.")
    st.info("To fetch data: `docker-compose up airflow-scheduler`")
    st.stop()

# ============ SIDEBAR FILTERS ============
st.sidebar.header("🔍 Filters")

# Determine available leagues (exclude Championship)
if has_dbt_data:
    available_leagues = [l for l in sorted(standings_df['league'].unique()) if l != 'Championship']
else:
    available_leagues = [l for l in sorted(df['league'].unique()) if l != 'Championship'] if 'league' in df.columns else []

# Default to first 3 leagues (excluding Championship)
default_leagues = available_leagues[:3] if len(available_leagues) >= 3 else available_leagues

leagues = st.sidebar.multiselect(
    "Select Leagues",
    options=available_leagues,
    default=default_leagues
)

st.sidebar.markdown("---")
st.sidebar.header("⚔️ Team Comparison")

if has_dbt_data:
    all_teams = sorted(standings_df['team'].unique())
else:
    all_teams = set()
    if 'HomeTeam' in df.columns:
        all_teams.update(df['HomeTeam'].unique())
    if 'AwayTeam' in df.columns:
        all_teams.update(df['AwayTeam'].unique())
    all_teams = sorted(all_teams)

team1 = st.sidebar.selectbox("Team A", all_teams, index=0 if len(all_teams) > 0 else None)
team2 = st.sidebar.selectbox("Team B", all_teams, index=min(1, len(all_teams)-1) if len(all_teams) > 1 else None)

# ============ TAB LAYOUT ============
tabs = ["📊 Overview", "🏆 League Standings", "⚽ Top Scorers", "📈 Trends", "⚔️ Team Comparison", "📋 Data Tables"]
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(tabs)

# ============ TAB 1: OVERVIEW ============
with tab1:
    st.header("📈 Key Metrics")
    
    kpi_col1, kpi_col2, kpi_col3, kpi_col4 = st.columns(4)
    
    if has_raw_data:
        filtered_df = df[df['league'].isin(leagues)] if leagues and 'league' in df.columns else df
        
        with kpi_col1:
            st.metric("Total Matches", len(filtered_df))
        
        with kpi_col2:
            if 'TotalGoals' in filtered_df.columns:
                avg_goals = filtered_df['TotalGoals'].mean()
                st.metric("Avg Goals/Match", f"{avg_goals:.2f}")
        
        with kpi_col3:
            if 'FTHG' in filtered_df.columns and 'FTAG' in filtered_df.columns:
                home_goals = filtered_df['FTHG'].sum()
                away_goals = filtered_df['FTAG'].sum()
                home_advantage = (home_goals / (home_goals + away_goals) * 100) if (home_goals + away_goals) > 0 else 0
                st.metric("Home Goal %", f"{home_advantage:.1f}%")
        
        with kpi_col4:
            if 'league' in filtered_df.columns:
                st.metric("Active Leagues", filtered_df['league'].nunique())
    elif has_dbt_data:
        filtered_standings = standings_df[standings_df['league'].isin(leagues)] if leagues else standings_df
        filtered_scorers = scorers_df[scorers_df['league'].isin(leagues)] if leagues else scorers_df
        
        with kpi_col1:
            total_matches = filtered_standings['matches_played'].sum() // 2
            st.metric("Total Matches", int(total_matches))
        
        with kpi_col2:
            total_goals = filtered_scorers['total_goals'].sum()
            st.metric("Total Goals", int(total_goals))
        
        with kpi_col3:
            avg_goals = total_goals / total_matches if total_matches > 0 else 0
            st.metric("Avg Goals/Match", f"{avg_goals:.2f}")
        
        with kpi_col4:
            st.metric("Teams Tracked", len(filtered_standings))
    
    st.markdown("---")
    
    # Quick league standings preview
    if has_dbt_data and leagues:
        st.subheader("🏆 Quick League Standings Preview")
        for league in leagues[:3]:  # Show top 3 leagues
            league_data = standings_df[standings_df['league'] == league].head(5)
            if not league_data.empty:
                with st.expander(f"{league} - Top 5 Teams"):
                    display_cols = ['rank', 'team', 'points', 'wins', 'draws', 'losses', 'goals_for', 'goals_against']
                    st.dataframe(
                        league_data[display_cols],
                        use_container_width=True,
                        hide_index=True,
                        column_config={
                            'rank': st.column_config.NumberColumn("Rank", width="small"),
                            'team': st.column_config.TextColumn("Team"),
                            'points': st.column_config.NumberColumn("Points", width="small"),
                            'wins': st.column_config.NumberColumn("W", width="small"),
                            'draws': st.column_config.NumberColumn("D", width="small"),
                            'losses': st.column_config.NumberColumn("L", width="small"),
                        }
                    )

# ============ TAB 2: LEAGUE STANDINGS ============
with tab2:
    st.header("🏆 League Standings")
    
    if has_dbt_data:
        for league in leagues if leagues else available_leagues:
            league_standings = standings_df[standings_df['league'] == league].sort_values('rank')
            
            if not league_standings.empty:
                st.subheader(f"📊 {league}")
                
                # Format standings table
                display_df = league_standings[['rank', 'team', 'matches_played', 'wins', 'draws', 'losses', 
                                               'goals_for', 'goals_against', 'goal_difference', 'points']].copy()
                display_df.columns = ['Rank', 'Team', 'MP', 'W', 'D', 'L', 'GF', 'GA', 'GD', 'Pts']
                
                # Highlight top 4 (Champions League spots - dark gold) and bottom 3 (relegation - red)
                def highlight_rows(row):
                    if row['Rank'] <= 4:
                        return ['background-color: #b8860b; color: #ffffff; font-weight: bold'] * len(row)
                    elif row['Rank'] > len(display_df) - 3:
                        return ['background-color: #dc3545; color: #ffffff'] * len(row)
                    return [''] * len(row)
                
                st.dataframe(
                    display_df.style.apply(highlight_rows, axis=1),
                    use_container_width=True,
                    hide_index=True,
                    height=min(400, len(display_df) * 35 + 38)
                )
                
                # Points distribution chart
                col1, col2 = st.columns([2, 1])
                
                with col1:
                    fig = go.Figure()
                    fig.add_trace(go.Bar(
                        y=league_standings['team'],
                        x=league_standings['points'],
                        orientation='h',
                        marker_color=league_standings['points'].apply(
                            lambda x: '#28a745' if x >= league_standings['points'].quantile(0.75) else 
                                     '#dc3545' if x <= league_standings['points'].quantile(0.25) else '#6c757d'
                        ),
                        text=league_standings['points'],
                        textposition='outside'
                    ))
                    fig.update_layout(
                        title="Points Distribution",
                        xaxis_title="Points",
                        yaxis_title="Team",
                        yaxis=dict(autorange="reversed"),
                        showlegend=False,
                        height=max(300, len(league_standings) * 25)
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                with col2:
                    # Goals vs Conceded scatter
                    fig2 = px.scatter(
                        league_standings,
                        x='goals_for',
                        y='goals_against',
                        text='team',
                        size='points',
                        color='points',
                        color_continuous_scale='RdYlGn',
                        title="Goals For vs Against"
                    )
                    fig2.update_traces(textposition='top center', textfont_size=8)
                    fig2.update_layout(height=400)
                    st.plotly_chart(fig2, use_container_width=True)
                
                st.markdown("---")
    else:
        st.info("📭 League standings data not available. Run dbt models to generate standings.")

# ============ TAB 3: TOP SCORERS ============
with tab3:
    st.header("⚽ Top Scorers")
    
    if has_dbt_data and scorers_df is not None:
        for league in leagues if leagues else available_leagues:
            league_scorers = scorers_df[scorers_df['league'] == league].sort_values('league_rank').head(15)
            
            if not league_scorers.empty:
                st.subheader(f"🎯 {league} - Top Scorers")
                
                col1, col2 = st.columns([3, 2])
                
                with col1:
                    # Goals bar chart
                    fig = px.bar(
                        league_scorers,
                        y='team',
                        x='total_goals',
                        orientation='h',
                        color='goals_per_match',
                        color_continuous_scale='Reds',
                        title=f"Top 15 Scoring Teams - {league}",
                        text='total_goals'
                    )
                    fig.update_traces(textposition='outside')
                    fig.update_layout(
                        yaxis=dict(autorange="reversed"),
                        height=500,
                        xaxis_title="Total Goals",
                        yaxis_title="Team"
                    )
                    st.plotly_chart(fig, use_container_width=True)
                
                with col2:
                    # Stats table
                    display_cols = ['league_rank', 'team', 'total_goals', 'goals_per_match', 'matches_played', 'home_goals', 'away_goals']
                    display_df = league_scorers[display_cols].copy()
                    display_df.columns = ['Rank', 'Team', 'Goals', 'GPG', 'MP', 'Home', 'Away']
                    
                    st.dataframe(
                        display_df,
                        use_container_width=True,
                        hide_index=True,
                        column_config={
                            'Rank': st.column_config.NumberColumn("#", width="small"),
                            'Team': st.column_config.TextColumn("Team"),
                            'Goals': st.column_config.NumberColumn("⚽", width="small"),
                            'GPG': st.column_config.NumberColumn("GPG", format="%.2f"),
                            'MP': st.column_config.NumberColumn("MP", width="small"),
                            'Home': st.column_config.NumberColumn("H", width="small"),
                            'Away': st.column_config.NumberColumn("A", width="small"),
                        }
                    )
                    
                    # Home vs Away goals pie
                    total_home = league_scorers['home_goals'].sum()
                    total_away = league_scorers['away_goals'].sum()
                    
                    fig_pie = px.pie(
                        values=[total_home, total_away],
                        names=['Home Goals', 'Away Goals'],
                        title="Home vs Away Goals",
                        color=['Home Goals', 'Away Goals'],
                        color_discrete_map={'Home Goals': '#2ecc71', 'Away Goals': '#3498db'}
                    )
                    st.plotly_chart(fig_pie, use_container_width=True)
                
                st.markdown("---")
    else:
        st.info("📭 Top scorers data not available. Run dbt models to generate statistics.")

# ============ TAB 4: TRENDS ============
with tab4:
    if has_raw_data:
        filtered_df = df[df['league'].isin(leagues)] if leagues and 'league' in df.columns else df
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("📈 Goals Trend Over Time")
            if 'Date' in filtered_df.columns and 'TotalGoals' in filtered_df.columns:
                trend_type = st.radio("View by", ["Weekly", "Monthly"], horizontal=True, key="trend_type")
                period_col = 'Week' if trend_type == "Weekly" else 'Month'
                
                trend = filtered_df.groupby(period_col).agg({
                    'TotalGoals': ['mean', 'sum', 'count']
                }).reset_index()
                trend.columns = [period_col, 'Avg Goals', 'Total Goals', 'Matches']
                
                fig_trend = go.Figure()
                fig_trend.add_trace(go.Scatter(
                    x=trend[period_col],
                    y=trend['Avg Goals'],
                    mode='lines+markers',
                    name='Avg Goals',
                    line=dict(color='#3498db', width=3),
                    fill='tozeroy'
                ))
                fig_trend.update_layout(
                    title=f"Average Goals per Match ({trend_type})",
                    xaxis_title="Period",
                    yaxis_title="Goals",
                    hovermode='x unified'
                )
                st.plotly_chart(fig_trend, use_container_width=True)
        
        with col2:
            st.subheader("🏠 Home vs Away Performance")
            if 'league' in filtered_df.columns and 'FTHG' in filtered_df.columns:
                league_performance = filtered_df.groupby('league').agg({
                    'FTHG': 'mean',
                    'FTAG': 'mean'
                }).reset_index()
                league_performance.columns = ['League', 'Home Avg', 'Away Avg']
                
                fig_ha = go.Figure()
                fig_ha.add_trace(go.Bar(
                    name='Home Goals',
                    x=league_performance['League'],
                    y=league_performance['Home Avg'],
                    marker_color='#2ecc71'
                ))
                fig_ha.add_trace(go.Bar(
                    name='Away Goals',
                    x=league_performance['League'],
                    y=league_performance['Away Avg'],
                    marker_color='#e74c3c'
                ))
                fig_ha.update_layout(
                    barmode='group',
                    title="Home vs Away Goals by League",
                    xaxis_title="League",
                    yaxis_title="Average Goals"
                )
                st.plotly_chart(fig_ha, use_container_width=True)
    else:
        st.info("📭 Raw match data not available for trend analysis.")

# ============ TAB 5: TEAM COMPARISON ============
with tab5:
    if team1 and team2 and has_raw_data:
        st.header(f"⚔️ {team1} vs {team2}")
        
        def get_team_stats(team_name, df):
            home_matches = df[df['HomeTeam'] == team_name]
            away_matches = df[df['AwayTeam'] == team_name]
            
            total_matches = len(home_matches) + len(away_matches)
            if total_matches == 0:
                return None
            
            home_goals = home_matches['FTHG'].sum() if len(home_matches) > 0 else 0
            away_goals = away_matches['FTAG'].sum() if len(away_matches) > 0 else 0
            home_conceded = home_matches['FTAG'].sum() if len(home_matches) > 0 else 0
            away_conceded = away_matches['FTHG'].sum() if len(away_matches) > 0 else 0
            
            home_wins = len(home_matches[home_matches['FTR'] == 'H']) if len(home_matches) > 0 else 0
            away_wins = len(away_matches[away_matches['FTR'] == 'A']) if len(away_matches) > 0 else 0
            draws = len(home_matches[home_matches['FTR'] == 'D']) + len(away_matches[away_matches['FTR'] == 'D'])
            
            return {
                'matches': total_matches,
                'goals_for': home_goals + away_goals,
                'goals_against': home_conceded + away_conceded,
                'wins': home_wins + away_wins,
                'draws': draws,
                'losses': total_matches - home_wins - away_wins - draws,
                'home_goals': home_goals,
                'away_goals': away_goals
            }
        
        filtered_df = df[df['league'].isin(leagues)] if leagues and 'league' in df.columns else df
        stats1 = get_team_stats(team1, filtered_df)
        stats2 = get_team_stats(team2, filtered_df)
        
        if stats1 and stats2:
            # Radar chart
            categories = ['Goals/Match', 'Goals Against/Match', 'Win %', 'Draw %', 'Home Goals %']
            
            team1_values = [
                stats1['goals_for'] / stats1['matches'],
                stats1['goals_against'] / stats1['matches'],
                (stats1['wins'] / stats1['matches']) * 100,
                (stats1['draws'] / stats1['matches']) * 100,
                (stats1['home_goals'] / stats1['goals_for'] * 100) if stats1['goals_for'] > 0 else 0
            ]
            
            team2_values = [
                stats2['goals_for'] / stats2['matches'],
                stats2['goals_against'] / stats2['matches'],
                (stats2['wins'] / stats2['matches']) * 100,
                (stats2['draws'] / stats2['matches']) * 100,
                (stats2['home_goals'] / stats2['goals_for'] * 100) if stats2['goals_for'] > 0 else 0
            ]
            
            fig_radar = go.Figure()
            fig_radar.add_trace(go.Scatterpolar(
                r=team1_values,
                theta=categories,
                fill='toself',
                name=team1,
                line_color='#3498db'
            ))
            fig_radar.add_trace(go.Scatterpolar(
                r=team2_values,
                theta=categories,
                fill='toself',
                name=team2,
                line_color='#e74c3c'
            ))
            fig_radar.update_layout(
                polar=dict(radialaxis=dict(visible=True, range=[0, max(max(team1_values), max(team2_values)) * 1.2])),
                showlegend=True,
                title="Team Comparison Radar"
            )
            st.plotly_chart(fig_radar, use_container_width=True)
            
            # Side-by-side metrics
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader(f"🔵 {team1}")
                m1, m2, m3 = st.columns(3)
                m1.metric("Matches", stats1['matches'])
                m2.metric("Goals", stats1['goals_for'])
                m3.metric("Wins", stats1['wins'])
            
            with col2:
                st.subheader(f"🔴 {team2}")
                m1, m2, m3 = st.columns(3)
                m1.metric("Matches", stats2['matches'])
                m2.metric("Goals", stats2['goals_for'])
                m3.metric("Wins", stats2['wins'])
    else:
        st.info("📭 Select two teams from the sidebar to compare.")

# ============ TAB 6: DATA TABLES ============
with tab6:
    tab6_col1, tab6_col2 = st.tabs(["📊 League Standings Data", "📋 Raw Matches Data"])
    
    with tab6_col1:
        if has_dbt_data and standings_df is not None:
            st.subheader("Complete League Standings")
            filtered_standings = standings_df[standings_df['league'].isin(leagues)] if leagues else standings_df
            st.dataframe(filtered_standings, use_container_width=True, hide_index=True)
        else:
            st.info("📭 League standings data not available.")
    
    with tab6_col2:
        if has_raw_data:
            st.subheader("Recent Matches")
            filtered_df = df[df['league'].isin(leagues)] if leagues and 'league' in df.columns else df
            display_cols = ['Date', 'league', 'HomeTeam', 'AwayTeam', 'FTHG', 'FTAG', 'FTR', 'TotalGoals']
            available_cols = [c for c in display_cols if c in filtered_df.columns]
            st.dataframe(
                filtered_df[available_cols].sort_values('Date', ascending=False).head(100),
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("📭 Raw match data not available.")

# Footer
st.markdown("---")
footer_col1, footer_col2, footer_col3 = st.columns([1, 2, 1])
with footer_col2:
    st.caption("Built with ❤️ using Streamlit • Data from Football-Data.co.uk • dbt models integrated • Last updated: " + datetime.now().strftime("%Y-%m-%d %H:%M"))
