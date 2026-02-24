-- Team form analysis: last 5 matches for each team
-- Shows recent performance trends

with team_matches as (
    -- Home team perspective
    select
        league,
        home_team as team,
        match_date,
        result,
        case
            when result = 'H' then 'W'
            when result = 'D' then 'D'
            else 'L'
        end as form_result,
        home_goals as goals_for,
        away_goals as goals_against
    from {{ ref('stg_matches') }}
    
    union all
    
    -- Away team perspective
    select
        league,
        away_team as team,
        match_date,
        result,
        case
            when result = 'A' then 'W'
            when result = 'D' then 'D'
            else 'L'
        end as form_result,
        away_goals as goals_for,
        home_goals as goals_against
    from {{ ref('stg_matches') }}
),

recent_form as (
    select
        league,
        team,
        match_date,
        form_result,
        goals_for,
        goals_against,
        goals_for - goals_against as goal_diff,
        -- Get last 5 matches per team
        row_number() over (partition by team order by match_date desc) as match_number
    from team_matches
),

form_summary as (
    select
        league,
        team,
        -- Recent form (last 5 results as string, e.g., "WWDLW")
        string_agg(form_result, '') over (
            partition by team 
            order by match_date desc 
            rows between current row and 4 following
        ) as last_5_results,
        -- Points from last 5
        sum(case form_result when 'W' then 3 when 'D' then 1 else 0 end) over (
            partition by team
            order by match_date desc
            rows between current row and 4 following
        ) as points_last_5,
        -- Goals stats
        sum(goals_for) over (
            partition by team
            order by match_date desc
            rows between current row and 4 following
        ) as goals_for_last_5,
        sum(goals_against) over (
            partition by team
            order by match_date desc
            rows between current row and 4 following
        ) as goals_against_last_5,
        sum(goal_diff) over (
            partition by team
            order by match_date desc
            rows between current row and 4 following
        ) as goal_diff_last_5
    from recent_form
    where match_number <= 5
)

select distinct
    league,
    team,
    substr(last_5_results, 1, 5) as form_last_5,
    points_last_5,
    goals_for_last_5,
    goals_against_last_5,
    goal_diff_last_5,
    round(points_last_5 / 15.0 * 100, 1) as form_percentage,
    current_timestamp as calculated_at,
    row_number() over (partition by league order by points_last_5 desc, goal_diff_last_5 desc) as form_rank
from form_summary
where length(last_5_results) >= 5
order by league, points_last_5 desc, goal_diff_last_5 desc