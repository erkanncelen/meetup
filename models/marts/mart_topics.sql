-- calculating distinct counts of groups, users and events for each topic
SELECT
    st.topics,
    COUNT(DISTINCT st.group_id) AS group_count,
    COUNT(DISTINCT sume.user_id) AS users_count,
    COUNT(DISTINCT se.event_id) AS events_count


FROM {{ ref('stg_groups_topics') }} st
LEFT JOIN {{ ref('stg_users_memberships') }} sume ON sume.group_id = st.group_id
LEFT JOIN {{ ref('stg_events') }} se ON se.group_id = st.group_id
GROUP BY 1