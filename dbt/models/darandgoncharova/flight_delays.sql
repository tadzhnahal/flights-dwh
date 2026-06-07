{{ config(
    materialized='table',
    schema='team_vdga_dm'
) }}

select
    flight_dttm_local::date as flight_date,
    extract(hour from flight_dttm_local)::int as flight_hour,
    extract(dow from flight_dttm_local)::int as flight_day_of_week,
    carrier_code,
    origin_airport_dk,
    origin_airport_type,
    case when distance < 500  then 'short'
        when distance < 1500 then 'medium' else 'long' end as flight_range,
    count(*) as total_flights,
    count(*) filter (where dep_delay_min>0) as total_delayed_flights,
    round(avg(dep_delay_min)::numeric, 2) as avg_delay_mins,
    round(count(*) filter (where dep_delay_min > 0)* 100.0/ NULLIF(count(*), 0), 2) as delayed_pct,
    round(avg(case when carrier_delay_min > 0 then carrier_delay_min end)::numeric, 2)   as avg_carrier_delay_min,
    round(avg(case when weather_delay_min  > 0 then weather_delay_min  end)::numeric, 2) as avg_weather_delay_min,
    round(avg(case when nas_delay_min      > 0 then nas_delay_min      end)::numeric, 2) as avg_nas_delay_min,
    round(avg(case when security_delay_min > 0 then security_delay_min end)::numeric, 2) as avg_security_delay_min,
    round(avg(case when late_aircraft_min  > 0 then late_aircraft_min  end)::numeric, 2) as avg_late_aircraft_min,
    now() as processed_dttm
from {{ ref('flights_performed') }}
group by 1, 2, 3, 4, 5, 6, 7
