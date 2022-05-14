WITH source AS (
    SELECT * FROM {{ ref('src_users') }}
),

source_cleaned AS (
    SELECT
        user_id,
        city,
        UPPER(country) AS country,
        memberships,
        
    -- Cleaning hometown data. Examples: 'Oslo, Norway', 'Caracas, VE' , 'Arona (Italy)'
        -- Remove text in () (inclusive)
        -- Take first element of "," delimited string split
        -- TRIM possible whitespaces (last step)
        -- NOTE: I realized full cleaning of this column requires many more steps. I will leave it here for now.
        hometown,
            TRIM(
                SPLIT(
                    REGEXP_REPLACE(hometown, "\\(([^\\)]+)\\)", "")
                ,",")[SAFE_OFFSET(0)]
            ) AS hometown_cleaned
    
    FROM source)

SELECT
    user_id,
    country,
    city,
    hometown,
    hometown_cleaned,
    memberships

FROM source_cleaned