{{ config(materialized='table', schema='dm') }}

select
    sched_dttm_local::date as flight_date,
    extract(hour from sched_dttm_local)::int as flight_hour,
    extract(dow from sched_dttm_local)::int as flight_day_of_week,
    carrier_code,
    origin_airport_dk,
    origin_airport_type,
    cancellation_code,
    count(*) as total_cancelled_flights,
    now() as processed_dttm
from {{ ref('flights_cancelled') }}
group by 1, 2, 3, 4, 5, 6, 7
