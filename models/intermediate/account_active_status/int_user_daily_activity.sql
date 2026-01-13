with
date_spine as (
    select calendar_date
    from unnest(
        generate_date_array(
            '2019-01-01'
            , current_date()
            , interval 1 day
        )
    ) as calendar_date
)

, user_list as (
    select
        user_id
        , cast(min(created_at) as date) as user_created_date
    from {{ ref('stg_accounts_created') }}
    group by 1
)

, user_grid as (
    select
        user_list.user_id
        , date_spine.calendar_date
    from user_list
    cross join date_spine
    where date_spine.calendar_date >= user_list.user_created_date
)

, user_transactions_daily as (
    select
        created.user_id
        , transactions.transaction_date
        , sum(transactions.transactions_total) as transactions_count
    from {{ ref('stg_account_transactions') }} as transactions
    inner join {{ ref('stg_accounts_created') }} as created
        on transactions.account_id = created.account_id
    group by
        created.user_id
        , transactions.transaction_date
)

, activity_rolling as (
    select
        user_grid.user_id
        , user_grid.calendar_date
        , sum(
            coalesce(
                user_transactions_daily.transactions_count
                , 0
            )
        ) over (
                partition by user_grid.user_id
                order by unix_date(user_grid.calendar_date)
                range between 6 preceding
                and current row
        ) as total_tx_7d
    from user_grid
    left join user_transactions_daily
        on user_grid.user_id = user_transactions_daily.user_id
                and user_grid.calendar_date = user_transactions_daily.transaction_date
)

, final as (
    select
        user_id
        , calendar_date as reference_date
        , total_tx_7d
        , total_tx_7d > 0 as is_7d_active
    from activity_rolling
)

select *
from final
