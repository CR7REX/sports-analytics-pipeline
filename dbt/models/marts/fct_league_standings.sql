-- League standings with points, wins, draws, losses, goal difference
-- This model calculates current league table positions

with match_results as (
    select
        league,
        home_team as team,
        case
            when result = 'H' then 3  -- Home win
            when result = 'D' then 1  -- Draw
            else 0                     -- Loss
        end as points,
        case when result = 'H' then 1 else 0 end as wins,
        case when result = 'D' then 1 else 0 end as draws,
        case when result = 'A' then 1 else 0 end as losses,
        home_goals as goals_for,
        away_goals as goals_against,
        home_goals - away_goals as goal_diff
    from {{ ref('stg_matches') }}
    
    union all
    
    select
        league,
        away_team as team,
        case
            when result = 'A' then 3  -- Away win
            when result = 'D' then 1  -- Draw
            else 0                     -- Loss
        end as points,
        case when result = 'A' then 1 else 0 end as wins,
        case when result = 'D' then 1 else 0 end as draws,
        case when result = 'H' then 1 else 0 end as losses,
        away_goals as goals_for,
        home_goals as goals_against,
        away_goals - home_goals as goal_diff
    from {{ ref('stg_matches') }}
),

standings as (
    select
        league,
        team,
        sum(points) as total_points,
        count(*) as matches_played,
        sum(wins) as wins,
        sum(draws) as draws,
        sum(losses) as losses,
        sum(goals_for) as goals_for,
        sum(goals_against) as goals_against,
        sum(goal_diff) as goal_difference
    from match_results
    group by league, team
)

select
    league,
    team,
    matches_played,
    wins,
    draws,
    losses,
    total_points as points,
    goals_for,
    goals_against,
    goal_difference,
    -- Rank within each league
    row_number() over (partition by league order by total_points desc, goal_difference desc, goals_for desc) as league_position,
    current_timestamp() as calculated_at
from standings
order by league, league_position