-- calculating group metrics
WITH group_metrics AS (
SELECT
    sg.group_id,
    LENGTH(sg.name) AS group_name_length,
    LENGTH(sg.description) AS group_description_length,
    COUNT(DISTINCT se.event_id) event_count,
    ROUND(AVG(LENGTH(se.name)),2) AS event_name_length_avg,
    ROUND(AVG(LENGTH(se.description)),2) AS event_description_avg,
    DATETIME_DIFF(MAX(se.start_time), MIN(se.start_time), DAY) AS group_active_time,
    COUNT(DISTINCT se.event_id)/NULLIF(DATETIME_DIFF(MAX(se.start_time), MIN(se.start_time), DAY),0)*30 AS events_per_month

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
    gmet.event_description_avg,
    gmet.group_active_time,
    gmet.events_per_month,
    gmem.member_count,
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
