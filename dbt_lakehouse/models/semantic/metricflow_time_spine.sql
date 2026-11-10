{{ config(materialized='table', schema='gold', file_format='delta', tags=['semantic']) }}

SELECT
    explode(
        sequence(
            to_date('2020-01-01'),
            to_date('2035-12-31'),
            INTERVAL 1 DAY
        )
    ) AS DATE_DAY
