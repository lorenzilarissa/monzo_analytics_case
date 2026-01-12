/* Requirement: Metric should be calculated for any given day of the year.
Checking if the daily activity table has a continuous record count for the high-volume period.
*/

select 
    reference_date
    , count(*) as daily_records
from `analytics-take-home-test.larissa_lorenzi_analytics.fct_user_activity_daily`
where reference_date between '2020-01-01'
    and '2020-08-18'
group by 1
order by 1
;
