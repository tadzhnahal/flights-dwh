create table if not exists team_vdga_metadata.etl_load_log (
    id bigint generated always as identity primary key,
    flow_name text not null,
    source_file text,
    flight_dt date,
    rows_loaded bigint not null default 0,
    status text not null,
    error_message text,
    loaded_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create unique index if not exists etl_load_log_success_file_idx
on team_vdga_metadata.etl_load_log (flow_name, source_file)
where status = 'success' and source_file is not null;