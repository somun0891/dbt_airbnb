{% test avg_dollars_greater_than_one(model , column_name , group_by)%}

select 
    {{group_by}},
    avg({{column_name}}) as average_amount
from {{model}}
group by 1 
having average_amount < 1

{% endtest %}