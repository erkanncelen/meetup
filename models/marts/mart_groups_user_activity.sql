-- calculating users' participation to the events that started after they became a member of the group
WITH user_activity AS (
SELECT
    sg.group_id,
    sume.user_id,

    SUM(CASE WHEN ser.response IS NOT NULL THEN 1 ELSE 0 END) AS response_count,
    SUM(CASE WHEN ser.response IN ('yes', 'waitlist') THEN 1 ELSE 0 END) AS positive_response_count,
    COUNT(DISTINCT se.event_id) AS event_count


FROM {{ ref('stg_groups') }} sg
LEFT JOIN {{ ref('stg_users_memberships') }} sume ON sume.group_id = sg.group_id
LEFT JOIN {{ ref('stg_events') }} se ON se.group_id = sg.group_id AND se.start_time > sume.joined_at
LEFT JOIN {{ ref('stg_events_responses') }} ser ON ser.event_id = se.event_id AND ser.user_id = sume.user_id
GROUP BY 1,2
)

SELECT
    group_id,
    user_id,
    response_count,
    positive_response_count,
    event_count,
    response_count*1.00/NULLIF(event_count,0) AS response_ratio,
    positive_response_count*1.00/NULLIF(event_count,0) AS participation_ratio

FROM user_activity