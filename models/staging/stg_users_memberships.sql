WITH source AS (
    SELECT * FROM {{ ref('stg_users') }}
)

SELECT
    user_id,
    memberships.group_id,
    TIMESTAMP_MILLIS(memberships.joined) AS joined_at
FROM
  source, UNNEST(memberships) memberships