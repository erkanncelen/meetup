SELECT 
    group_id,
    LOWER(topics) AS topics
FROM {{ ref('stg_groups') }}, UNNEST(topics) topics