{{ config(materialized='table', schema='gold', file_format='delta', tags=['gold', 'fact']) }}

-- Anti-fanout protection: lot rows are deduplicated first, then mutation-level
-- totals are computed with window functions over that single deduplicated CTE.
-- No raw fact table is joined here, so a mutation with N lots cannot multiply
-- the EUR/m2 numerator or denominator.
with source as (
    select * from {{ source('bronze', 'dvf_lots_batis_77') }}
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

lots as (
    select
        cast(id_mutation as string) as mutation_id,
        cast(id_parcelle as string) as parcelle_id,
        cast(date_mutation as date) as mutation_date,
        cast(nature_mutation as string) as nature_mutation,
        cast(valeur_fonciere as double) as valeur_fonciere,
        cast(surface_reelle_bati as double) as surface_reelle_bati,
        cast(surface_terrain as double) as surface_terrain,
        cast(nombre_pieces_principales as int) as nombre_pieces_principales,
        cast(code_commune as string) as commune_code,
        cast(nom_commune as string) as commune_name,
        cast(code_departement as string) as department_code,
        cast(longitude as double) as longitude,
        cast(latitude as double) as latitude,
        _ingested_at,
        concat(
            cast(id_mutation as string),
            '|',
            coalesce(cast(id_parcelle as string), 'unknown')
        ) as lot_id
    from deduplicated
    where ingest_row_number = 1
),

with_mutation_totals as (
    select
        *,
        sum(valeur_fonciere) over (
            partition by mutation_id
        ) as mutation_volume_ventes_eur,
        sum(surface_reelle_bati) over (
            partition by mutation_id
        ) as mutation_surface_vendue_m2,
        count(*) over (
            partition by mutation_id
        ) as mutation_nombre_lots
    from lots
)

select
    *,
    mutation_volume_ventes_eur / nullif(mutation_surface_vendue_m2, 0) as mutation_prix_m2
from with_mutation_totals
