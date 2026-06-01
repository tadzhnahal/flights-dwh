create table if not exists team_vdga_stg.flights_raw (
    flight_raw_id bigint generated always as identity primary key,
    year integer,
    month integer,
    flight_dt date,
    carrier_code text,
    tail_num text,
    carrier_flight_number text,
    origin_code text,
    dest_code text,
    distance numeric(12, 2),
    scheduled_dep_tm text,
    actual_dep_tm text,
    dep_delay_min numeric(12, 2),
    scheduled_arr_tm text,
    actual_arr_tm text,
    arr_delay_min numeric(12, 2),
    taxi_out_min numeric(12, 2),
    wheels_off_tm text,
    wheels_on_tm text,
    taxi_in_min numeric(12, 2),
    carrier_delay_min numeric(12, 2),
    weather_delay_min numeric(12, 2),
    nas_delay_min numeric(12, 2),
    security_delay_min numeric(12, 2),
    late_aircraft_min numeric(12, 2),
    cancelled boolean,
    cancellation_code text,
    loaded_at timestamptz not null default now(),
    source_file text not null
);

create index if not exists flights_raw_flight_dt_idx
on team_vdga_stg.flights_raw (flight_dt);

create index if not exists flights_raw_source_file_idx
on team_vdga_stg.flights_raw (source_file);

create index if not exists flights_raw_origin_dest_idx
on team_vdga_stg.flights_raw (origin_code, dest_code);

create index if not exists flights_raw_carrier_code_idx
on team_vdga_stg.flights_raw (carrier_code);
