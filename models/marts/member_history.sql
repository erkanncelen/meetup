SELECT
    sume.group_id,
    sume.user_id,
    sume.joined_at,
    ROW_NUMBER() OVER(PARTITION BY sume.group_id ORDER BY sume.joined_at) AS member_count
FROM {{ ref('stg_users_memberships')}} sume
ORDER BY 1,3