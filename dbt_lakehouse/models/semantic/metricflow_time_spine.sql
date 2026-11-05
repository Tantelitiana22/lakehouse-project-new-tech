{{ config(materialized='table', schema='gold', file_format='delta', tags=['semantic']) }}

select
    explode(
        sequence(
            to_date('2020-01-01'),
            to_date('2035-12-31'),
            interval 1 day
        )
    ) as date_day
