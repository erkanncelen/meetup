-- calculating group metrics
WITH group_metrics AS (
SELECT
    sg.group_id,
    LENGTH(sg.name) AS group_name_length,
    LENGTH(sg.description) AS group_description_length,
    COUNT(DISTINCT se.event_id) event_count,
    ROUND(AVG(LENGTH(se.name)),2) AS event_name_length_avg,
    ROUND(AVG(LENGTH(se.description)),2) AS event_description_length_avg,
    DATETIME_DIFF(MAX(se.start_time), MIN(se.start_time), DAY) AS group_active_time,
    COUNT(DISTINCT se.event_id)*1.00/NULLIF(DATETIME_DIFF(MAX(se.start_time), MIN(se.start_time), DAY),0)*30 AS events_per_month

FROM {{ ref('stg_groups') }} sg
LEFT JOIN {{ ref('stg_events') }} se ON se.group_id = sg.group_id
GROUP BY 1,2,3
),

-- counting members per group
group_members AS (
SELECT 
    sg.group_id,
    COUNT(DISTINCT sume.user_id) AS member_count

FROM {{ ref('stg_groups') }} sg
LEFT JOIN {{ ref('stg_users_memberships') }} sume ON sume.group_id = sg.group_id
GROUP BY 1
),

-- counting topics per group
group_topics AS (
SELECT
    sg.group_id,
    COUNT(DISTINCT sgt.topics) AS number_of_topics
FROM {{ ref('stg_groups') }} sg
LEFT JOIN {{ ref('stg_groups_topics') }} sgt ON sgt.group_id = sg.group_id
GROUP BY 1
),

-- calulating average rspvs behaviour per group (at the time of the event)
group_rsvps AS (
SELECT
    sg.group_id,
    AVG(rsvps_to_members_ratio) AS rsvps_to_members_ratio_avg,
    AVG(positive_rspvs_ratio) AS positive_rspvs_ratio_avg

FROM {{ ref('stg_groups') }} sg
LEFT JOIN {{ ref('mart_events') }} me ON me.group_id = sg.group_id
GROUP BY 1
),

-- inspecting group name and description relationship with group topics
topics_pre AS (
SELECT 
    sg.group_id,
    sgt.topics,
    CASE WHEN UPPER(sg.name) LIKE '%' || UPPER(sgt.topics) || '%'  THEN 1 ELSE 0 END AS name_contains_topic,
    CASE WHEN UPPER(sg.description) LIKE '%' || UPPER(sgt.topics) || '%'  THEN 1 ELSE 0 END AS description_contains_topic,

FROM {{ ref('stg_groups') }} sg
LEFT JOIN {{ ref('stg_groups_topics') }} sgt ON sgt.group_id = sg.group_id
),
name_description_topic AS (
SELECT 
    group_id,
    SUM(name_contains_topic) AS group_name_topic_match,
    SUM(description_contains_topic) AS group_description_topic_match,
FROM topics_pre
GROUP BY 1
),

active_users AS (
WITH user_activity AS (
SELECT
    sg.group_id,
    sume.user_id,
    sume.joined_at,
    se.event_id,
    se.start_time,
    ser.response,
    ROW_NUMBER() OVER(PARTITION BY sg.group_id, sume.user_id ORDER BY se.start_time) AS event_number

FROM {{ ref('stg_groups')}} sg
LEFT JOIN {{ ref('stg_users_memberships')}} sume ON sume.group_id = sg.group_id
LEFT JOIN {{ ref('stg_events')}} se ON se.group_id = sg.group_id AND se.start_time > sume.joined_at
LEFT JOIN {{ ref('stg_events_responses')}} ser ON ser.event_id = se.event_id AND ser.user_id = sume.user_id
),
activity_calculation AS (
SELECT
    group_id,
    user_id,
    SUM(CASE WHEN response IS NOT NULL THEN 1 ELSE 0 END) AS actively_responding,
    SUM(CASE WHEN response IN ('yes', 'waitlist') THEN 1 ELSE 0 END) AS actively_participating
FROM user_activity
WHERE event_number <=5
GROUP BY 1,2
)

SELECT
    group_id,
    SUM(CASE WHEN actively_responding > 0 THEN 1 ELSE 0 END) AS active_respondents,
    SUM(CASE WHEN actively_participating > 0 THEN 1 ELSE 0 END) AS active_participants
FROM activity_calculation
GROUP BY 1
)



SELECT
    sg.group_id,
    sg.name,
    sg.description,
    sg.city,
    sg.lat,
    sg.lon,
    
    gmet.group_name_length,
    gmet.group_description_length,
    gmet.event_count,
    gmet.event_name_length_avg,
    gmet.event_description_length_avg,
    gmet.group_active_time,
    gmet.events_per_month,
    gmem.member_count,
    au.active_respondents,
    au.active_participants,
    au.active_respondents*1.00/NULLIF(gmem.member_count,0) AS active_respondents_ratio,
    au.active_participants*1.00/NULLIF(gmem.member_count,0) AS active_participants_ratio,
    gt.number_of_topics,
    gr.rsvps_to_members_ratio_avg,
    gr.positive_rspvs_ratio_avg,

    ndt.group_name_topic_match,
    ndt.group_description_topic_match


FROM {{ ref('stg_groups') }} sg
LEFT JOIN group_metrics gmet ON gmet.group_id = sg.group_id
LEFT JOIN group_members gmem ON gmem.group_id = sg.group_id
LEFT JOIN group_topics gt ON gt.group_id = sg.group_id
LEFT JOIN group_rsvps gr ON gr.group_id = sg.group_id
LEFT JOIN name_description_topic ndt ON ndt.group_id = sg.group_id
LEFT JOIN active_users au ON au.group_id = sg.group_id
