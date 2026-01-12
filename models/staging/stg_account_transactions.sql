with 
    source as (
        select
            account_id_hashed
            , date
            , transactions_num
        from {{ source('monzo_raw', 'account_transactions') }}
    )

    , renamed as (
        select
            cast(account_id_hashed as string) as account_id
            , cast(date as date) as transaction_date
            , cast(transactions_num as integer) as transactions_total
        from source
    )

    , dedup as (
        select
            *
            , row_number() over (
                partition by
                account_id
                , transaction_date
                , transactions_total
            order by transaction_date desc
            ) as rn
        from renamed
    )

select *
from dedup
where rn = 1
