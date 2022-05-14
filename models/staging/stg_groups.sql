WITH source AS (
    SELECT * FROM {{ ref('src_groups') }}
)

SELECT 
    group_id,
    name,
    REGEXP_REPLACE(description, "<.*?>", "") AS description,
    link,
    TIMESTAMP_MILLIS(created) AS created_at,
    city,
    lat,
    lon,
    topics

FROM source