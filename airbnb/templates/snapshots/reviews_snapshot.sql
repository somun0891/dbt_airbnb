{% snapshot reviews_snapshot %}

{{
    config(
        target_schema = 'snapshots',
        strategy = 'timestamp',
        unique_key = 'LISTING_ID||REVIEW_DATE',
        updated_at = 'ingest_ts'
    )

}}

select 
*
 from {{ ref('src_reviews') }}

{% endsnapshot %}

