/* Requirement: Ensure the model is complete.
This test checks for "orphan" transactions that don't match our account dimension.
*/

select count(distinct stg.account_id) as orphan_accounts
from `analytics-take-home-test.larissa_lorenzi_analytics.stg_account_transactions` as stg
left join `analytics-take-home-test.larissa_lorenzi_analytics.dim_account_status` as dim
    on stg.account_id = dim.account_id
where dim.account_id is null
;
