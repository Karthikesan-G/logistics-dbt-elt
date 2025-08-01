use role accountadmin;
use warehouse compute_wh;

create or replace procedure load_raw_data()
returns string
language sql
as
$$
begin
    truncate table logistics_dwh.raw.logistics_details;
    copy into logistics_dwh.raw.logistics_details(
        origin,
        destination,
        pouch_no,
        date,
        sender_name,
        sender_phone,
        sender_address,
        sender_city,
        sender_state,
        sender_pincode,
        sender_gstin,
        total_pieces,
        actual_wt,
        volumetric_wt,
        chargeable_wt,
        paperwork,
        sender_signature,
        sender_date,
        recipient_name,
        recipient_phone,
        recipient_address,
        recipient_city,
        receiver_state,
        receiver_pincode,
        description,
        value_added_services,
        consignment_no,
        expiry_date,
        booking_code,
        recipient_gstin,
        receiver_name,
        relationship,
        company_stamp,
        receiver_signature,
        receive_date,
        tariff,
        vas_charges,
        total_amount,
        mode,
        risk_surcharge,
        mode_of_payment,
        nature_of_consignment
    )
    from '@logistics_dwh.raw.s3_ex_stage'
        file_format = (format_name = 'csv_file_format');
    
    commit;

end
$$