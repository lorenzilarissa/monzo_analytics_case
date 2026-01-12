/* Requirement: The model must be accurate and reliable.
This test ensures each account_id appears only once in the final dimension.
*/

select 
    account_id
    , count(*) as record_count
from `analytics-take-home-test.larissa_lorenzi_analytics.dim_account_status`
group by 1
having count(*) > 1
;
