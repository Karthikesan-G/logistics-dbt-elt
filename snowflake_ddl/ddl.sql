use role accountadmin;
use warehouse compute_wh;

create or replace database logistics_dwh;
create or replace schema raw;

-- create raw table
create or replace table logistics_dwh.raw.logistics_details
(
    origin varchar,
    destination varchar,
    pouch_no varchar,
    date date,
    sender_name varchar,
    sender_phone varchar,
    sender_address varchar,
    sender_city varchar,
    sender_state varchar,
    sender_pincode int,
    sender_gstin varchar,
    total_pieces int,
    actual_wt decimal,
    volumetric_wt decimal,
    chargeable_wt decimal,
    paperwork boolean,
    sender_signature boolean,
    sender_date date,
    recipient_name varchar,
    recipient_phone varchar,
    recipient_address varchar,
    recipient_city varchar,
    receiver_state varchar,
    receiver_pincode int,
    description varchar,
    value_added_services varchar,
    consignment_no varchar,
    expiry_date date,
    booking_code varchar,
    recipient_gstin varchar,
    receiver_name varchar,
    relationship varchar,
    company_stamp boolean,
    receiver_signature boolean,
    receive_date date,
    tariff decimal,
    vas_charges decimal,
    total_amount decimal,
    mode varchar,
    risk_surcharge varchar,
    mode_of_payment varchar,
    nature_of_consignment varchar,
    inserted_at date default current_date()
);