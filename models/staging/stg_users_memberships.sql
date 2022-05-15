SELECT
    user_id,
    memberships.group_id,
    TIMESTAMP_MILLIS(memberships.joined) AS joined_at
FROM {{ ref('stg_users') }}, UNNEST(memberships) memberships