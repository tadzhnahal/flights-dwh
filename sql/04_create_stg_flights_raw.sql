create table if not exists team_vdga_stg.flights_raw (
    flight_raw_id bigint generated always as identity primary key,
    year integer,
    month integer,
    flight_dt date,
    carrier_code text,
    tail_num text,
    carrier_flight_number text,
    origin_code text,
    origin_city_name text,
    dest_code text,
    dest_city_name text,
    scheduled_dep_tm text,
    actual_dep_tm text,
    dep_delay_min integer,
    dep_delay_group_num integer,
    scheduled_arr_tm text,
    actual_arr_tm text,
    arr_delay_min integer,
    arr_delay_group_num integer,
    cancelled boolean,
    cancellation_code text,
    flights_cnt integer,
    distance numeric(12, 2),
    distance_group_num integer,
    carrier_delay_min integer,
    weather_delay_min integer,
    nas_delay_min integer,
    security_delay_min integer,
    late_aircraft_min integer,
    loaded_at timestamptz not null default now(),
    source_file text not null
);

create index if not exists flights_raw_flight_dt_idx
on team_vdga_stg.flights_raw (flight_dt);

create index if not exists flights_raw_source_file_idx
on team_vdga_stg.flights_raw (source_file);

create index if not exists flights_raw_origin_dest_idx
on team_vdga_stg.flights_raw (origin_code, dest_code);