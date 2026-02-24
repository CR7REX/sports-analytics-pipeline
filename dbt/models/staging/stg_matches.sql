with source as (
    select * from {{ source('raw', 'matches') }}
),

renamed as (
    select
        -- Generate unique match ID
        {{ dbt_utils.generate_surrogate_key(['date', 'hometeam', 'awayteam']) }} as match_id,
        
        -- League info
        league,
        league_code,
        
        -- Match details
        to_date(date, 'DD/MM/YY') as match_date,
        hometeam as home_team,
        awayteam as away_team,
        
        -- Goals
        cast(fthg as int) as home_goals,
        cast(ftag as int) as away_goals,
        
        -- Result
        ftr as result,  -- H = Home win, D = Draw, A = Away win
        
        -- Metadata
        extracted_at
        
    from source
)

select * from renamed
