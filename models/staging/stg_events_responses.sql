WITH source AS (
    SELECT * FROM {{ ref('stg_events') }}
)

SELECT
    event_id,
    rsvps.user_id AS user_id,
    TIMESTAMP_MILLIS(rsvps.when) AS responded_at,
    LOWER(rsvps.response) AS response,
    rsvps.guests AS guests,
    rsvp_limit
FROM
  source, UNNEST(rsvps) rsvps