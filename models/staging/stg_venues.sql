SELECT 
    venue_id,
    name,
    -- city column needs cleaning if needed in analysis
    city,
    UPPER(country) AS country, -- unifying country codes to UPPER case
    lat,
    lon

FROM {{ ref('src_venues') }}
-- table contains 1 NULL row, leaving it like this for now.