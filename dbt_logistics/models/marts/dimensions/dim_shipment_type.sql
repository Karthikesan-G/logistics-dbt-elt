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
    pouch_no,
    consignment_no,
    booking_code,
    mode,
    mode_of_payment,
    nature_of_consignment,
    value_added_services,
    description,
    risk_surcharge,
    current_timestamp as inserted_at
from {{ ref('stg_logistics') }}