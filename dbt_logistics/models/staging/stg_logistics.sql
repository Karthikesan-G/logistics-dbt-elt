{{ 
    config(
        materialized = 'incremental',
        unique_key = 'pouch_no',
        schema = 'staging'
    )
 }}

{% if is_incremental() %}
    with max_val as (
        select max(inserted_at) as max_inserted_at from {{ this }}
    ),
    stg_table as (
        select *
        from {{ source('raw', 'logistics_details') }}
        where inserted_at > (select max_inserted_at from max_val)
    ),
{% else %}
    with stg_table as (
        select * from {{ source('raw', 'logistics_details') }}
    ),
{% endif %}
cleaned_table as (
    select 
        trim(origin) as origin,
        trim(destination) as destination,
        trim(pouch_no) as pouch_no,
        trim(consignment_no) as consignment_no,
        date,
        expiry_date,
        trim(booking_code) as booking_code,
        trim(mode) as mode,
        trim(mode_of_payment) as mode_of_payment,
        trim(nature_of_consignment) as nature_of_consignment,
        trim(value_added_services) as value_added_services,
        trim(description) as description,
        trim(risk_surcharge) as risk_surcharge,
        trim(sender_name) as sender_name,
        trim(sender_phone) as sender_phone,
        trim(sender_address) as sender_address,
        trim(sender_city) as sender_city,
        {{ state_normalize('sender_state') }} as sender_state,
        sender_pincode,
        trim(sender_gstin) as sender_gstin,
        coalesce(paperwork, false) as paperwork,
        coalesce(sender_signature, false) as sender_signature,
        trim(recipient_name) as receiver_name,
        trim(recipient_phone) as receiver_phone,
        trim(recipient_address) as receiver_address,
        trim(recipient_city) as receiver_city,
        {{ state_normalize('receiver_state') }} as receiver_state,
        receiver_pincode,
        trim(recipient_gstin) as receiver_gstin,
        trim(relationship) as relationship,
        coalesce(company_stamp, false) as company_stamp,
        coalesce(receiver_signature, false) as receiver_signature,
        receive_date,
        total_pieces,
        actual_wt,
        volumetric_wt,
        chargeable_wt,
        tariff,
        vas_charges,
        total_amount, 
        inserted_at
    from stg_table
),
invalid_removed as (
    select *,
        row_number() over(partition by consignment_no order by pouch_no) as row_no
    from cleaned_table
)


select 
    origin, destination, pouch_no, consignment_no, date, expiry_date, booking_code, mode, mode_of_payment, nature_of_consignment, value_added_services, description, risk_surcharge, sender_name, sender_phone, sender_address, sender_city, sender_state, sender_pincode, sender_gstin, paperwork, sender_signature, receiver_name, receiver_phone, receiver_address, receiver_city, receiver_state, receiver_pincode, receiver_gstin, relationship, company_stamp, receiver_signature, receive_date, total_pieces, actual_wt, volumetric_wt, chargeable_wt, tariff, vas_charges, total_amount, inserted_at
from invalid_removed
where row_no = 1