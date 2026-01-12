with 
    account_created as (
        select
            account_id
            , user_id
            , account_type
            , created_at as event_at
            , 'OPEN' as event_type
        from {{ ref('stg_accounts_created') }}
    )

    , account_closed as (
        select
            account_id
            , closed_at as event_at
            , 'CLOSED' as event_type
        from {{ ref('stg_accounts_closed') }}
    )

    , account_reopened as (
        select
            account_id
            , reopened_at as event_at
            , 'REOPENED' as event_type
        from {{ ref('stg_accounts_reopened') }}
    )

    , unioned_events as (
        select 
            account_id
            , event_at
            , event_type
        from account_created

        union all

        select
            account_id
            , event_at
            , event_type
        from account_closed

        union all

        select
            account_id
            , event_at
            , event_type
        from account_reopened
    )

    , status_window as (
        select
            *
            , row_number() over (
                partition by account_id 
                order by event_at desc
            ) as event_rank
            , lag(event_type) over (
                partition by account_id 
                order by event_at asc
            ) as previous_event_type
        from unioned_events
    )

    , final as (
        select
            account_id
            , event_at as last_event_at
            , event_type as current_status
            , previous_event_type
            , case
                when event_type
                    in (
                        'OPEN'
                        , 'REOPENED'
                    )
                    then true
                else false
            end as is_currently_open
        from status_window
        where event_rank = 1
    )

select *
from final
