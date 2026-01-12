with
source as (
    select
        account_id_hashed
        , reopened_ts
    from {{ source('monzo_raw', 'account_reopened') }}
)

, renamed as (
    select
        cast(account_id_hashed as string) as account_id
        , cast(reopened_ts as timestamp) as reopened_at
    from source
)

, dedup as (
    select
        *
        , row_number() over (
            partition by
                account_id
                , reopened_at
            order by reopened_at desc
        ) as rn
    from renamed
)

select *
from dedup
where rn = 1
