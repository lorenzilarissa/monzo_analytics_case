with 
    current_status as (
        select *
        from {{ ref('int_account_status_history') }}
    )

    , final as (
        select
            account_id
            , last_event_at
            , current_status
            , previous_event_type
            , is_currently_open
        from current_status
    )

select *
from final
