{{ config(
    materialized='incremental',
    unique_key=[
        'carrier_flight_number',
        'sched_dttm_local',
        'origin_airport_dk'
    ]
) }}


SELECT
    (
        f.flight_dt::text || ' ' ||
        LEFT(LPAD(f.scheduled_dep_tm, 4, '0'), 2) || ':' ||
        RIGHT(LPAD(f.scheduled_dep_tm, 4, '0'), 2))::timestamp
        AT TIME ZONE COALESCE(t.timezone, 'UTC') AS sched_dttm_local,
    f.carrier_code,
    f.carrier_flight_number,
    ao.airport_dk AS origin_airport_dk,
    ad.airport_dk AS dest_airport_dk,
    f.tail_num,
    f.distance,
    ao.iso_country AS origin_country,
    ad.iso_country AS dest_country,
    CASE
        WHEN ao.iso_country IS NULL OR ad.iso_country IS NULL THEN 'Unknown'
        WHEN ao.iso_country = ad.iso_country THEN 'Domestic'
        ELSE 'International'
    END AS flight_type,
    f.cancelled,
    f.cancellation_code,
    CURRENT_TIMESTAMP AS processed_dttm
FROM {{ source('stg', 'flights_raw') }} f
LEFT JOIN {{ source('stg', 'airports') }} ao
    ON f.origin_code = ao.iata_code
LEFT JOIN {{ source('stg', 'airports') }} ad
    ON f.dest_code = ad.iata_code
LEFT JOIN {{ source('dds', 'airports_timezones') }} t
    ON f.origin_code = t.iata_code
WHERE f.cancelled = TRUE


{% if is_incremental() %}
    AND f.flight_dt >
        (SELECT MAX(sched_dttm_local::date)
        FROM {{ this }})
{% endif %}