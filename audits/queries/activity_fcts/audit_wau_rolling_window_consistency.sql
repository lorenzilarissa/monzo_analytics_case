/* Requirement: WAU must be historically consistent and deterministic.
Spot-check: Comparing Mart results against a raw manual calculation for 2020-08-10.
*/

with 
    manual_calc as (
        select count(distinct user_id) as manual_wau
        from `analytics-take-home-test.larissa_lorenzi_analytics.stg_account_transactions` as t
        inner join `analytics-take-home-test.larissa_lorenzi_analytics.stg_accounts_created` as a
            on t.account_id = a.account_id
        where t.transaction_date between date_sub('2020-08-10', interval 6 day)
            and '2020-08-10'
    )
    select 
        m.weekly_active_users as mart_wau
        , c.manual_wau
        , (m.weekly_active_users - c.manual_wau) as variance
    from (
        select count(distinct user_id) as weekly_active_users 
        from `analytics-take-home-test.larissa_lorenzi_analytics.fct_user_activity_daily`
        where reference_date = '2020-08-10'
            and is_7d_active = true
    ) as m, manual_calc as c
;
