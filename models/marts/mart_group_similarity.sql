-- calculating similarity within groups based on shared member count
WITH shared_groups AS (
SELECT
    sg.group_id,
    sume.user_id,
    sumes.group_id AS other_group

FROM {{ ref('stg_groups')}} sg
LEFT JOIN {{ ref('stg_users_memberships')}} sume ON sume.group_id = sg.group_id
LEFT JOIN {{ ref('stg_users_memberships')}} sumes ON sumes.user_id = sume.user_id
),

member_counts AS (
SELECT 
    mg.group_id,
    mg.member_count
FROM {{ ref('mart_groups')}} mg
),

similiarities AS (
SELECT
    sg.group_id,
    mc.member_count,
    sg.other_group,
    COUNT(DISTINCT sg.user_id) AS shared_users,
    COUNT(DISTINCT sg.user_id)*100.00/mc.member_count AS similarity_percentage
FROM shared_groups sg
LEFT JOIN member_counts mc ON mc.group_id = sg.group_id
WHERE sg.group_id != sg.other_group
GROUP BY 1,2,3
)

SELECT
    s.group_id,
    sg.name AS group_name,
    sg.topics AS group_topics,
    s.member_count,
    s.other_group,
    sgg.name AS other_group_name,
    sgg.topics AS other_group_topics,
    s.shared_users,
    s.similarity_percentage
FROM similiarities s
LEFT JOIN {{ ref('stg_groups')}} sg ON sg.group_id = s.group_id
LEFT JOIN {{ ref('stg_groups')}} sgg ON sgg.group_id = s.other_group
ORDER BY s.group_id, s.shared_users DESC
