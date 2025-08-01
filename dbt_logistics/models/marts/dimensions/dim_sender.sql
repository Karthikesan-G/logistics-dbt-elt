{{ 
    config(
        materialized = 'table',
        schema = 'dimensions'
    )
}}

select distinct
    md5(concat_ws('||', 
        coalesce(sender_name, 'null'), 
        coalesce(sender_address, 'null'), 
        coalesce(sender_gstin, 'null'), 
        coalesce(sender_phone, 'null')
    )) as sender_id,
    sender_name,
    sender_address,
    sender_city,
    sender_state,
    sender_pincode,
    sender_phone,
    sender_gstin,
    sender_signature,
    current_timestamp as inserted_at
from {{ ref('stg_logistics') }}