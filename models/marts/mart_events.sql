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
LEFT JOIN {{ ref('stg_groups') }} sg ON sg.group_id = se.group_id)





-- rsvps_count*1.00/member_count AS rsvps_to_members_ratio,
-- (rsvps_yes_count + rsvps_waitlist_count)*1.00/rsvps_count AS positive_rspvs_ratio,
-- WHERE event status is SUCCESSFULL!


