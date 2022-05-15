WITH source AS (
    SELECT * FROM {{ ref('stg_groups') }}
)

SELECT 
    group_id,
    LOWER(topics) AS topics
FROM
  source, UNNEST(topics) topics