with 
    source as (
        select
            account_id_hashed
            , user_id_hashed
            , account_type
            , created_ts
        from {{ source('monzo_raw', 'account_created') }}
    )

    , renamed as (
        select
            cast(account_id_hashed as string) as account_id
            , cast(user_id_hashed as string) as user_id
            , cast(account_type as string) as account_type
            , cast(created_ts as timestamp) as created_at
        from source
    )

    , dedup as (
        select
            *
            , row_number() over (
                partition by
                    account_id
                    , created_at
                    , user_id
                order by created_at desc
            ) as rn
        from renamed
    )

select *
from dedup
where rn = 1