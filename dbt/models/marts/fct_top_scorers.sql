-- Top scorers (team goals ranking) with home/away breakdown
-- This model calculates team goal statistics for scoring analysis

with team_goals as (
    -- Home team goals
    select
n        league,
        home_team as team,
        home_goals as goals,
        'home' as venue,
        match_date
    from {{ ref('stg_matches') }}
    
    union all
    
    -- Away team goals
    select
        league,
        away_team as team,
        away_goals as goals,
        'away' as venue,
        match_date
    from {{ ref('stg_matches') }}
),

goal_stats as (
    select
        league,
        team,
        count(*) as matches_played,
        sum(goals) as total_goals,
        avg(goals) as goals_per_match,
        sum(case when venue = 'home' then goals end) as home_goals,
        sum(case when venue = 'away' then goals end) as away_goals,
        max(goals) as max_goals_in_match,
        -- Count matches with 3+ goals ("big scoring" games)
        sum(case when goals >= 3 then 1 else 0 end) as high_scoring_matches,
        -- Count clean sheets (0 goals conceded logic needs join, so we skip here)
        sum(goals) as attacking_strength
    from team_goals
    group by league, team
),

goal_rankings as (
    select
        league,
        team,
        matches_played,
        total_goals,
        round(goals_per_match, 2) as goals_per_match,
        home_goals,
        away_goals,
        round(100.0 * home_goals / nullif(total_goals, 0), 1) as home_goals_pct,
        round(100.0 * away_goals / nullif(total_goals, 0), 1) as away_goals_pct,
        max_goals_in_match,
        high_scoring_matches,
        -- Rank within each league
        row_number() over (partition by league order by total_goals desc, goals_per_match desc) as league_rank,
        -- Rank by goals per match (minimum 5 games for meaningful average)
        case 
            when matches_played >= 5 then 
                row_number() over (partition by league order by goals_per_match desc)
            else null
        end as efficiency_rank,
        current_timestamp() as calculated_at
    from goal_stats
)

select *
from goal_rankings
order by league, league_rank