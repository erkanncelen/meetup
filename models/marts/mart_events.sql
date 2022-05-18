-- counting the group members at the time of the event
WITH group_members AS (
SELECT
    se.event_id,
    se.group_id,
    se.start_time,
    COUNT(DISTINCT sume.user_id) AS member_count

FROM {{ ref('stg_events') }} se
LEFT JOIN {{ ref('stg_users_memberships') }} sume ON sume.group_id = se.group_id
WHERE sume.joined_at < se.start_time
GROUP BY 1,2,3
),

-- calculating rspvs numbers for every event
event_rsvps AS (
SELECT
    ser.event_id,
    se.group_id,
    gm.member_count,
    se.created_at,
    se.start_time,
    ser.rsvp_limit,
    COUNT(ser.response) AS rsvps_count,
    SUM(CASE WHEN ser.response = 'yes' THEN 1 ELSE 0 END) AS rsvps_yes_count,
    SUM(CASE WHEN ser.response = 'no' THEN 1 ELSE 0 END) AS rsvps_no_count,
    SUM(CASE WHEN ser.response = 'waitlist' THEN 1 ELSE 0 END) AS rsvps_waitlist_count,
    ROUND(AVG(DATETIME_DIFF(ser.responded_at, se.created_at, HOUR)),2) AS avg_response_time_hour

FROM {{ ref('stg_events_responses') }} ser
LEFT JOIN {{ ref('stg_events') }} se ON se.event_id = ser.event_id
LEFT JOIN group_members gm ON gm.event_id = ser.event_id
GROUP BY 1,2,3,4,5,6
),

-- calculating group to venue distances per event
venue_distance AS (
SELECT
    se.event_id,
    se.venue_id,
    se.group_id,
    ROUND(ST_DISTANCE(ST_GEOGPOINT(sv.lon, sv.lat), ST_GEOGPOINT(sg.lon, sg.lat)),2) AS group_to_venue_distance_m

FROM {{ ref('stg_events') }} se
LEFT JOIN {{ ref('stg_venues') }} sv ON sv.venue_id = se.venue_id
LEFT JOIN {{ ref('stg_groups') }} sg ON sg.group_id = se.group_id
),

-- inspecting event name and description lengths (and relationship with group topics)
topics_pre AS (
SELECT 
    se.event_id,
    se.group_id,
    sgt.topics,

    se.name,
    CASE WHEN UPPER(se.name) LIKE '%' || UPPER(sgt.topics) || '%'  THEN 1 ELSE 0 END AS name_contains_topic,
    LENGTH(se.name) AS name_length,
    
    se.description,
    CASE WHEN UPPER(se.description) LIKE '%' || UPPER(sgt.topics) || '%'  THEN 1 ELSE 0 END AS description_contains_topic,
    LENGTH(se.description) AS description_length

FROM {{ ref('stg_events') }} se
LEFT JOIN {{ ref('stg_groups_topics') }} sgt ON sgt.group_id = se.group_id
),
name_description_topic AS (
SELECT 
    event_id,
    group_id,
    name_length,
    description_length,
    COUNT(DISTINCT topics) AS nr_of_group_topics,
    SUM(name_contains_topic) AS name_topic_match,
    SUM(description_contains_topic) AS description_topic_match,
FROM topics_pre
GROUP BY 1,2,3,4
),

-- calculating event specific metrics
event_metrics AS (
SELECT 
    se.event_id,
    se.name,
    se.group_id,
    se.created_at,
    se.start_time,
    se.duration_seconds/60000 AS duration_minutes,
    EXTRACT(HOUR FROM se.start_time) AS hour_of_event,
    CASE 
        WHEN EXTRACT(DAYOFWEEK FROM se.start_time) = 1 THEN 'Sunday'
        WHEN EXTRACT(DAYOFWEEK FROM se.start_time) = 2 THEN 'Monday'
        WHEN EXTRACT(DAYOFWEEK FROM se.start_time) = 3 THEN 'Tuesday'
        WHEN EXTRACT(DAYOFWEEK FROM se.start_time) = 4 THEN 'Wednesday'
        WHEN EXTRACT(DAYOFWEEK FROM se.start_time) = 5 THEN 'Thursday'
        WHEN EXTRACT(DAYOFWEEK FROM se.start_time) = 6 THEN 'Friday'
        WHEN EXTRACT(DAYOFWEEK FROM se.start_time) = 7 THEN 'Saturday'
    END AS day_of_event,
    DATETIME_DIFF(se.start_time, se.created_at, DAY) AS event_prep_time_days,
    ROW_NUMBER() OVER (PARTITION BY se.group_id ORDER BY se.created_at) AS nth_event_of_the_group

FROM {{ ref('stg_events') }} se
)

SELECT
    se.event_id,
    se.name,
    se.created_at,
    se.start_time,
    se.description,
    se.group_id,
    sg.lat AS group_lat,
    sg.lon group_lon,
    se.venue_id,
    sv.lat AS venue_lat,
    sv.lon AS venue_lon,
    se.status,
    gm.member_count,
    er.rsvp_limit,
    er.rsvps_count,
    er.rsvps_yes_count,
    er.rsvps_no_count,
    er.rsvps_waitlist_count,
    er.avg_response_time_hour,
    er.rsvps_count*1.00/er.member_count AS rsvps_to_members_ratio,
    (er.rsvps_yes_count + er.rsvps_waitlist_count)*1.00/er.rsvps_count AS positive_rspvs_ratio,
    vd.group_to_venue_distance_m,
    ndt.name_length,
    ndt.description_length,
    ndt.name_topic_match,
    ndt.description_topic_match,
    ndt.nr_of_group_topics,
    em.duration_minutes,
    em.hour_of_event,
    em.day_of_event,
    em.event_prep_time_days,
    em.nth_event_of_the_group

FROM {{ ref('stg_events') }} se
LEFT JOIN group_members gm ON gm.event_id = se.event_id
LEFT JOIN event_rsvps er ON er.event_id = se.event_id
LEFT JOIN venue_distance vd ON vd.event_id = se.event_id
LEFT JOIN name_description_topic ndt ON ndt.event_id = se.event_id
LEFT JOIN event_metrics em ON em.event_id = se.event_id
LEFT JOIN {{ ref('stg_venues') }} sv ON sv.venue_id = se.venue_id
LEFT JOIN {{ ref('stg_groups') }} sg ON sg.group_id = se.group_id

