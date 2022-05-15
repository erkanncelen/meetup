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
ORDER BY 2,3)

SELECT
    ser.event_id,
    se.name,
    COUNT(ser.response) AS rsvps_count,
    SUM(CASE WHEN ser.response = 'yes' THEN 1 ELSE 0 END) AS rsvps_yes_count,
    SUM(CASE WHEN ser.response = 'no' THEN 1 ELSE 0 END) AS rsvps_no_count,
    SUM(CASE WHEN ser.response = 'waitlist' THEN 1 ELSE 0 END) AS rsvps_waitlist_count,
    AVG(ser.responded_at - se.created_at) AS avg_response_time

FROM {{ ref('stg_events_responses') }} ser
LEFT JOIN {{ ref('stg_events') }} se ON se.event_id = ser.event_id
GROUP BY 1,2
ORDER BY 2


