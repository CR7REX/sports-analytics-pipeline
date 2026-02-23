import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta

st.set_page_config(
    page_title="Football Analytics Dashboard",
    page_icon="⚽",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("⚽ Football Analytics Dashboard")
st.markdown("Real-time insights from European football leagues")

# Load data
@st.cache_data(ttl=3600)
def load_data():
    try:
        df = pd.read_csv("data/raw/matches.csv")
        # Handle DD/MM/YYYY format
        df['Date'] = pd.to_datetime(df['Date'], format='%d/%m/%Y', errors='coerce')
        df['Month'] = df['Date'].dt.to_period('M').astype(str)
        df['Week'] = df['Date'].dt.to_period('W').astype(str)
        df['TotalGoals'] = df['FTHG'] + df['FTAG']
        return df
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return pd.DataFrame()

df = load_data()

if df.empty:
    st.warning("📭 No data available. Run the Airflow DAG to fetch match data.")
    st.info("To fetch data: `docker-compose up airflow-scheduler`")
else:
    # ============ SIDEBAR FILTERS ============
    st.sidebar.header("🔍 Filters")
    
    # League filter
    leagues = st.sidebar.multiselect(
        "Select Leagues",
        options=sorted(df['league'].unique()) if 'league' in df.columns else [],
        default=df['league'].unique() if 'league' in df.columns else []
    )
    
    # Date range filter
    if 'Date' in df.columns:
        min_date = df['Date'].min()
        max_date = df['Date'].max()
        date_range = st.sidebar.date_input(
            "Date Range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date
        )
    
    # Team filter for comparison
    all_teams = set()
    if 'HomeTeam' in df.columns:
        all_teams.update(df['HomeTeam'].unique())
    if 'AwayTeam' in df.columns:
        all_teams.update(df['AwayTeam'].unique())
    
    st.sidebar.markdown("---")
    st.sidebar.header("⚔️ Team Comparison")
    team1 = st.sidebar.selectbox("Team A", sorted(all_teams), index=0)
    team2 = st.sidebar.selectbox("Team B", sorted(all_teams), index=min(1, len(all_teams)-1))
    
    # Filter data
    filtered_df = df.copy()
    if leagues and 'league' in df.columns:
        filtered_df = filtered_df[filtered_df['league'].isin(leagues)]
    if 'Date' in df.columns and len(date_range) == 2:
        filtered_df = filtered_df[
            (filtered_df['Date'] >= pd.Timestamp(date_range[0])) &
            (filtered_df['Date'] <= pd.Timestamp(date_range[1]))
        ]
    
    # ============ KPI METRICS ============
    st.header("📈 Key Metrics")
    
    kpi_col1, kpi_col2, kpi_col3, kpi_col4, kpi_col5 = st.columns(5)
    
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
    
    with kpi_col5:
        if 'TotalGoals' in filtered_df.columns:
            high_scoring = (filtered_df['TotalGoals'] >= 3).sum()
            pct = (high_scoring / len(filtered_df) * 100) if len(filtered_df) > 0 else 0
            st.metric("High Scoring %", f"{pct:.1f}%")
    
    st.markdown("---")
    
    # ============ TAB LAYOUT ============
    tab1, tab2, tab3, tab4 = st.tabs([
        "📊 Overview", 
        "📈 Trends", 
        "⚔️ Team Comparison",
        "📋 Data Tables"
    ])
    
    # ============ TAB 1: OVERVIEW ============
    with tab1:
        col1, col2 = st.columns(2)
        
        with col1:
            # Match Results Distribution
            st.subheader("Match Results Distribution")
            if 'FTR' in filtered_df.columns:
                result_counts = filtered_df['FTR'].value_counts()
                result_labels = {'H': 'Home Win', 'D': 'Draw', 'A': 'Away Win'}
                result_names = [result_labels.get(x, x) for x in result_counts.index]
                
                fig_pie = px.pie(
                    values=result_counts.values,
                    names=result_names,
                    title="Results by Venue",
                    color=result_counts.index,
                    color_discrete_map={'H': '#2ecc71', 'D': '#f39c12', 'A': '#e74c3c'}
                )
                fig_pie.update_traces(textinfo='percent+label')
                st.plotly_chart(fig_pie, use_container_width=True)
        
        with col2:
            # Goals per League
            st.subheader("Goals per League")
            if 'league' in filtered_df.columns and 'TotalGoals' in filtered_df.columns:
                league_goals = filtered_df.groupby('league')['TotalGoals'].agg(['mean', 'sum']).reset_index()
                league_goals.columns = ['League', 'Avg Goals', 'Total Goals']
                league_goals = league_goals.sort_values('Avg Goals', ascending=True)
                
                fig_bar = px.bar(
                    league_goals,
                    y='League',
                    x='Avg Goals',
                    orientation='h',
                    title="Average Goals per Match by League",
                    color='Avg Goals',
                    color_continuous_scale='RdYlGn',
                    text=league_goals['Avg Goals'].round(2)
                )
                fig_bar.update_traces(textposition='outside')
                st.plotly_chart(fig_bar, use_container_width=True)
        
        # Top Scoring Teams
        st.subheader("🏆 Top Scoring Teams")
        col3, col4 = st.columns([1, 3])
        
        with col3:
            top_n = st.slider("Number of teams", 5, 30, 15)
        
        with col4:
            if 'HomeTeam' in filtered_df.columns and 'AwayTeam' in filtered_df.columns:
                # Calculate goals for each team
                home_goals = filtered_df.groupby(['HomeTeam', 'league'])['FTHG'].sum().reset_index()
                home_goals.columns = ['Team', 'League', 'Goals']
                
                away_goals = filtered_df.groupby(['AwayTeam', 'league'])['FTAG'].sum().reset_index()
                away_goals.columns = ['Team', 'League', 'Goals']
                
                team_goals = pd.concat([home_goals, away_goals])
                team_total = team_goals.groupby('Team')['Goals'].sum().reset_index()
                team_total = team_total.merge(
                    team_goals.groupby('Team')['League'].first().reset_index(),
                    on='Team'
                )
                team_total = team_total.sort_values('Goals', ascending=False).head(top_n)
                
                fig_top = px.bar(
                    team_total,
                    x='Goals',
                    y='Team',
                    orientation='h',
                    color='League',
                    title=f"Top {top_n} Scoring Teams",
                    text='Goals'
                )
                fig_top.update_traces(textposition='outside')
                fig_top.update_layout(yaxis=dict(autorange="reversed"))
                st.plotly_chart(fig_top, use_container_width=True)
    
    # ============ TAB 2: TRENDS ============
    with tab2:
        col1, col2 = st.columns(2)
        
        with col1:
            # Goals Trend Over Time
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
            # Home vs Away Performance
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
        
        # Goals Distribution Histogram
        st.subheader("🎯 Goals Distribution")
        if 'TotalGoals' in filtered_df.columns:
            max_goals = int(filtered_df['TotalGoals'].max())
            fig_hist = px.histogram(
                filtered_df,
                x='TotalGoals',
                nbins=max_goals + 1,
                title="Distribution of Total Goals per Match",
                labels={'TotalGoals': 'Total Goals', 'count': 'Number of Matches'},
                color_discrete_sequence=['#3498db']
            )
            fig_hist.update_layout(bargap=0.1)
            st.plotly_chart(fig_hist, use_container_width=True)
    
    # ============ TAB 3: TEAM COMPARISON ============
    with tab3:
        st.header(f"⚔️ {team1} vs {team2}")
        
        # Get team stats
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
        
        stats1 = get_team_stats(team1, filtered_df)
        stats2 = get_team_stats(team2, filtered_df)
        
        if stats1 and stats2:
            # Radar chart comparison
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
                
                st.progress(stats1['wins'] / stats1['matches'], text=f"Win Rate: {(stats1['wins']/stats1['matches']*100):.1f}%")
                st.progress(stats1['draws'] / stats1['matches'], text=f"Draw Rate: {(stats1['draws']/stats1['matches']*100):.1f}%")
            
            with col2:
                st.subheader(f"🔴 {team2}")
                m1, m2, m3 = st.columns(3)
                m1.metric("Matches", stats2['matches'])
                m2.metric("Goals", stats2['goals_for'])
                m3.metric("Wins", stats2['wins'])
                
                st.progress(stats2['wins'] / stats2['matches'], text=f"Win Rate: {(stats2['wins']/stats2['matches']*100):.1f}%")
                st.progress(stats2['draws'] / stats2['matches'], text=f"Draw Rate: {(stats2['draws']/stats2['matches']*100):.1f}%")
        else:
            st.warning("Not enough data for comparison. Please select different teams.")
    
    # ============ TAB 4: DATA TABLES ============
    with tab4:
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.subheader("📋 Recent Matches")
            display_cols = ['Date', 'league', 'HomeTeam', 'AwayTeam', 'FTHG', 'FTAG', 'FTR', 'TotalGoals']
            available_cols = [c for c in display_cols if c in filtered_df.columns]
            st.dataframe(
                filtered_df[available_cols].sort_values('Date', ascending=False).head(50),
                use_container_width=True,
                hide_index=True
            )
        
        with col2:
            st.subheader("📊 League Summary")
            if 'league' in filtered_df.columns:
                summary = filtered_df.groupby('league').agg({
                    'Date': 'count',
                    'TotalGoals': ['mean', 'sum']
                }).round(2)
                summary.columns = ['Matches', 'Avg Goals', 'Total Goals']
                st.dataframe(summary, use_container_width=True)

# Footer
st.markdown("---")
footer_col1, footer_col2, footer_col3 = st.columns([1, 2, 1])
with footer_col2:
    st.caption("Built with ❤️ using Streamlit • Data from Football-Data.co.uk • Last updated: " + datetime.now().strftime("%Y-%m-%d %H:%M"))