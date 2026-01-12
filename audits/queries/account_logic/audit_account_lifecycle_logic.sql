/* Requirement: Reliable representation of account states.
This test ensures accounts flagged as open actually have an OPEN or REOPENED status.
*/

select 
    current_status
    , is_currently_open
    , count(*) as account_count
from `analytics-take-home-test.larissa_lorenzi_analytics.dim_account_status`
group by 1, 2
order by 2 desc
;
