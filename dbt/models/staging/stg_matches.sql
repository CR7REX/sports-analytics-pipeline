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
        
        -- Match details - handle both YY and YYYY formats
        case
            when length(date) = 8 then to_date(date, 'DD/MM/YY')  -- 05/03/25
            when length(date) = 10 then to_date(date, 'DD/MM/YYYY')  -- 05/03/2025
            else to_date(date, 'DD/MM/YY')
        end as match_date,
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
