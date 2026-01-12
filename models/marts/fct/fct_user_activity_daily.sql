with
daily_activity as (
    select * from {{ ref('int_user_daily_activity') }}
)

, final as (
    select
        user_id
        , reference_date
        , total_tx_7d
        , is_7d_active
        , md5(
            concat(
                user_id
                , '-'
                , reference_date
            )
        ) as activity_id
    from daily_activity
)

select *
from final
