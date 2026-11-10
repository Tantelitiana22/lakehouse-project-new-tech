{{ config(materialized='table', schema='gold', file_format='delta', tags=['gold', 'fact']) }}

-- Anti-fanout protection: lot rows are deduplicated first, then mutation-level
-- totals are computed with window functions over that single deduplicated CTE.
-- No raw fact table is joined here, so a mutation with N lots cannot multiply
-- the EUR/m2 numerator or denominator.
WITH SOURCE AS (
    SELECT * FROM {{ source('bronze', 'dvf_lots_batis_77') }}
),

DEDUPLICATED AS (
    SELECT
        *,
        row_number() OVER (
            PARTITION BY ID_MUTATION, ID_PARCELLE
            ORDER BY _INGESTED_AT DESC
        ) AS INGEST_ROW_NUMBER
    FROM SOURCE
),

LOTS AS (
    SELECT
        CAST(ID_MUTATION AS string) AS MUTATION_ID,
        CAST(ID_PARCELLE AS string) AS PARCELLE_ID,
        CAST(DATE_MUTATION AS date) AS MUTATION_DATE,
        CAST(NATURE_MUTATION AS string) AS NATURE_MUTATION,
        CAST(VALEUR_FONCIERE AS double) AS VALEUR_FONCIERE,
        CAST(SURFACE_REELLE_BATI AS double) AS SURFACE_REELLE_BATI,
        CAST(SURFACE_TERRAIN AS double) AS SURFACE_TERRAIN,
        CAST(NOMBRE_PIECES_PRINCIPALES AS int) AS NOMBRE_PIECES_PRINCIPALES,
        CAST(CODE_COMMUNE AS string) AS COMMUNE_CODE,
        CAST(NOM_COMMUNE AS string) AS COMMUNE_NAME,
        CAST(CODE_DEPARTEMENT AS string) AS DEPARTMENT_CODE,
        CAST(LONGITUDE AS double) AS LONGITUDE,
        CAST(LATITUDE AS double) AS LATITUDE,
        _INGESTED_AT,
        concat(
            CAST(ID_MUTATION AS string),
            '|',
            coalesce(CAST(ID_PARCELLE AS string), 'unknown')
        ) AS LOT_ID
    FROM DEDUPLICATED
    WHERE INGEST_ROW_NUMBER = 1
),

WITH_MUTATION_TOTALS AS (
    SELECT
        *,
        sum(VALEUR_FONCIERE) OVER (
            PARTITION BY MUTATION_ID
        ) AS MUTATION_VOLUME_VENTES_EUR,
        sum(SURFACE_REELLE_BATI) OVER (
            PARTITION BY MUTATION_ID
        ) AS MUTATION_SURFACE_VENDUE_M2,
        count(*) OVER (
            PARTITION BY MUTATION_ID
        ) AS MUTATION_NOMBRE_LOTS
    FROM LOTS
)

SELECT
    *,
    MUTATION_VOLUME_VENTES_EUR / nullif(MUTATION_SURFACE_VENDUE_M2, 0) AS MUTATION_PRIX_M2
FROM WITH_MUTATION_TOTALS
