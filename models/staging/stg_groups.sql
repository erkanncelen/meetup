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

FROM {{ ref('src_groups') }}