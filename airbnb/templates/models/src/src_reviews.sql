with raw_reviews as 
(
    select * from raw.raw_reviews
)
select *
RENAME (DATE AS REVIEW_DATE , COMMENTS AS REVIEW_COMMENTS , SENTIMENT AS REVIEW_SENTIMENT  )
FROM raw_reviews


  {# {{flags.FULL_REFRESH}}  #}

{# set marketunit = ['RIA' , 'Bank' , 'Institution'] }}
{{marketunit}}
{{ generate_case_stmt(marketunit) }} #}

