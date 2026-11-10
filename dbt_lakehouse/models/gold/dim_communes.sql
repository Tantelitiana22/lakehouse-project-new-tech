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
    cast(deduplicated.code as string) as commune_code,
    cast(deduplicated.nom as string) as commune_name,
    cast(deduplicated.code_departement as string) as department_code,
    cast(deduplicated.code_region as string) as region_code,
    cast(deduplicated.code_epci as string) as epci_code,
    cast(deduplicated.population as bigint) as population,
    cast(deduplicated.centre.coordinates[0] as double) as longitude,
    cast(deduplicated.centre.coordinates[1] as double) as latitude,
    deduplicated._ingested_at
from deduplicated
where deduplicated.ingest_row_number = 1
