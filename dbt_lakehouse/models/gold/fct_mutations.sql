{{ config(materialized='table', schema='gold', file_format='delta', tags=['gold', 'fact']) }}

with source as (
    select * from {{ source('bronze', 'dvf_mutations_77') }}
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by id_mutation, id_parcelle
            order by _ingested_at desc
        ) as ingest_row_number
    from source
),

cleaned as (
    select
        cast(id_mutation as string)          as mutation_id,
        cast(date_mutation as date)          as mutation_date,
        cast(nature_mutation as string)      as nature_mutation,
        cast(valeur_fonciere as double)      as valeur_fonciere,
        cast(surface_reelle_bati as double)  as surface_reelle_bati,
        cast(surface_terrain as double)      as surface_terrain,
        cast(nombre_lots as int)             as nombre_lots,
        cast(code_commune as string)         as commune_code,
        cast(nom_commune as string)          as commune_name,
        cast(code_departement as string)     as department_code,
        cast(longitude as double)            as longitude,
        cast(latitude as double)             as latitude,
        _ingested_at
    from deduplicated
    where ingest_row_number = 1
)

select
    *,
    valeur_fonciere / nullif(surface_reelle_bati, 0) as prix_m2
from cleaned
