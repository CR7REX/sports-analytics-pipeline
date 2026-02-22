with source as (
    select * from {{ source('raw', 'matches') }}
),

renamed as (
    select
        -- Generate unique match ID
        {{ dbt_utils.generate_surrogate_key(['Date', 'HomeTeam', 'AwayTeam']) }} as match_id,
        
        -- League info
        league,
        league_code,
        
        -- Match details
        to_date(Date, 'DD/MM/YY') as match_date,
        HomeTeam as home_team,
        AwayTeam as away_team,
        
        -- Goals
        cast(FTHG as int) as home_goals,
        cast(FTAG as int) as away_goals,
        
        -- Result
        FTR as result,  -- H = Home win, D = Draw, A = Away win
        
        -- Metadata
        extracted_at
        
    from source
)

select * from renamed
