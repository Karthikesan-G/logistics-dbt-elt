{% snapshot sender_snapshot %}

{{
    config(
        unique_key = 'sender_id',
        target_schema = 'snapshots',
        strategy = 'check',
        check_cols = 'all'
    )
}}

select * from {{ ref('dim_sender') }}

{% endsnapshot %}