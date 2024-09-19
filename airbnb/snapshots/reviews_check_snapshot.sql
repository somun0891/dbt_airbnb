{% snapshot reviews_check_snapshot %}

{{
    config(
        target_schema = 'snapshots',
        strategy = 'check',
        unique_key = 'LISTING_ID||REVIEW_DATE||REVIEWER_NAME||REVIEW_SENTIMENT',
        check_cols = ["REVIEW_COMMENTS"],
        transient=false
    )

}}

select 
*
 from {{ ref('src_reviews') }}

{% endsnapshot %}

