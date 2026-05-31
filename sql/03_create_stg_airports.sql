create table if not exists stg.airports (
    airport_dk bigint generated always as identity primary key,
    airport_id bigint,
    ident text,
    type text,
    name text,
    latitude_deg numeric(12, 8),
    longitude_deg numeric(12, 8),
    elevation_ft integer,
    continent text,
    iso_country text,
    iso_region text,
    municipality text,
    scheduled_service text,
    icao_code text,
    iata_code text,
    gps_code text,
    local_code text,
    home_link text,
    wikipedia_link text,
    keywords text,
    timezone text,
    loaded_at timestamptz not null default now()
);

create index if not exists airports_iata_code_idx
on stg.airports (iata_code);