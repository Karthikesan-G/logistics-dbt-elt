{{
    config(
        materialized = 'table',
        schema = 'dimensions'
    )
}}

select distinct
    md5(concat_ws('||', 
        coalesce(pouch_no, 'null'),
        coalesce(consignment_no, 'null'),
        coalesce(booking_code, 'null')
    )) as shipment_id,
    total_pieces,
    actual_wt,
    volumetric_wt,
    chargeable_wt,
    tariff,
    vas_charges,
    total_amount,
    current_timestamp as inserted_at
from {{ ref('stg_logistics') }}