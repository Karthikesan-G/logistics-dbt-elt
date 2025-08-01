{{ 
    config(
        materialized = 'table',
        schema = 'fact'
    )
}}


select dst.shipment_id,
    ds.sender_id,
    dr.receiver_id,
    dd1.date_id as shipment_date_id,
    dd2.date_id as expiry_date_id,
    dd3.date_id as receive_date_id,
    paperwork,
    company_stamp,
    relationship
from {{ ref('stg_logistics') }} sl
left join {{ ref('dim_shipment_type') }} dst 
on coalesce(sl.pouch_no, 'null') = coalesce(dst.pouch_no, 'null')
    and coalesce(sl.consignment_no, 'null') = coalesce(dst.consignment_no, 'null')
    and coalesce(sl.booking_code, 'null') = coalesce(dst.booking_code, 'null')
left join {{ ref('dim_sender') }} ds
on coalesce(sl.sender_name, 'null') = coalesce(ds.sender_name, 'null')
    and coalesce(sl.sender_address, 'null') = coalesce(ds.sender_address, 'null')
    and coalesce(sl.sender_phone, 'null') = coalesce(ds.sender_phone, 'null')  
    and coalesce(sl.sender_gstin, 'null') = coalesce(ds.sender_gstin, 'null')
left join {{ ref('dim_receiver') }} dr
on coalesce(sl.receiver_name, 'null') = coalesce(dr.receiver_name, 'null')
    and coalesce(sl.receiver_address, 'null') = coalesce(dr.receiver_address, 'null')
    and coalesce(sl.receiver_gstin, 'null') = coalesce(dr.receiver_gstin, 'null')
    and coalesce(sl.receiver_phone, 'null') = coalesce(dr.receiver_phone, 'null')
left join {{ ref('dim_date') }} dd1
on sl.date = dd1.full_date
left join {{ ref('dim_date') }} dd2
on sl.expiry_date = dd2.full_date
left join {{ ref('dim_date') }} dd3
on sl.receive_date = dd3.full_date