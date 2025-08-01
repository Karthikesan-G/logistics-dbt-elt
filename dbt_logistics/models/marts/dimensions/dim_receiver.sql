{{ 
    config(
        materialized = 'table',
        schema = 'dimensions'
    )
}}

select distinct
    md5(concat_ws('||', 
        coalesce(receiver_name, 'null'), 
        coalesce(receiver_address, 'null'), 
        coalesce(receiver_gstin, 'null'), 
        coalesce(receiver_phone, 'null')
    )) as receiver_id,
    receiver_name,
    receiver_address,
    receiver_city,
    receiver_state,
    receiver_pincode,
    receiver_phone,
    receiver_gstin,
    receiver_signature,
    current_timestamp as inserted_at
from {{ ref('stg_logistics') }}