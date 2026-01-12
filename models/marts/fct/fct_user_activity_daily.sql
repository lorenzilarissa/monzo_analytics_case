with 
    daily_activity as (
        select * from {{ ref('int_user_daily_activity') }}
    )

    , final as (
        select
            md5(
                concat(
                    user_id
                    , '-'
                    , reference_date
                )
            ) as activity_id
            , user_id
            , reference_date
            , total_tx_7d
            , is_7d_active
        from daily_activity
    )

select *
from final
