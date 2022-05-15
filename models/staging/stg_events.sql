WITH source AS (
    SELECT * FROM {{ ref('src_events') }}
)

SELECT
    ROW_NUMBER() OVER() AS event_id,
    group_id,
    name,
    REGEXP_REPLACE(description, "<.*?>", "") AS description,
    TIMESTAMP_MILLIS(created) AS created_at,
    TIMESTAMP_MILLIS(time) AS start_time,
    duration AS duration_seconds,
    rsvp_limit,
    venue_id,
    status,
    rsvps

FROM source