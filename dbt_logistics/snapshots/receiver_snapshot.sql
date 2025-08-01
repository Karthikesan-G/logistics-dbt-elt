{% snapshot receiver_snapshot %}

{{
    config(
        unique_key = 'receiver_id',
        target_schema = 'snapshots',
        strategy = 'check',
        check_cols = 'all'
    )
}}

select * from {{ ref('dim_receiver') }}

{% endsnapshot %}