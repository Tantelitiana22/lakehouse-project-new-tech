{{ config(materialized='table', schema='gold', file_format='delta', tags=['gold', 'dimension']) }}

with source as (
    select * from {{ source('bronze', 'communes_geo_77') }}
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by code
            order by _ingested_at desc
        ) as ingest_row_number
    from source
)

select
    cast(code as string)                              as commune_code,
    cast(nom as string)                               as commune_name,
    cast(code_departement as string)                  as department_code,
    cast(code_region as string)                       as region_code,
    cast(code_epci as string)                         as epci_code,
    cast(population as bigint)                        as population,
    cast(centre.coordinates[0] as double)             as longitude,
    cast(centre.coordinates[1] as double)             as latitude,
    _ingested_at
from deduplicated
where ingest_row_number = 1
