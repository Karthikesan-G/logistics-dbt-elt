{{
    config(
        materialized = 'table',
        schema = 'dimensions'
    )
}}

with merge_date_table as (
    select distinct date from {{ ref('stg_logistics') }} where date is not null
    union
    select distinct expiry_date from {{ ref('stg_logistics') }} where expiry_date is not null
    union 
    select distinct receive_date from {{ ref('stg_logistics') }} where receive_date is not null
),
format_table as (
    select 
        row_number() over(order by date) as date_id,
        date as full_date,
        day(date) as day,
        month(date) as month,
        year(date) as year,
        quarter(date) as quarter,
        dayname(date) as weekday
    from merge_date_table
)

select * from format_table