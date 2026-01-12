with 
    source as (
        select
            account_id_hashed
            , closed_ts
        from {{ source('monzo_raw', 'account_closed') }}
    )

    , renamed as (
        select
            cast(account_id_hashed as string) as account_id
            , cast(closed_ts as timestamp) as closed_at
        from source
    )

    , dedup as (
        select
            *
            , row_number() over (
                partition by
                    account_id
                    , closed_at
                order by closed_at desc
            ) as rn
        from renamed
    )

select *
from dedup
where rn = 1
