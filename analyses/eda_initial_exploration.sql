/* ===============================================================================
Initial data discovery - Monzo Analytics Case
Goal: Exploratory queries to understand grain, volume, and data integrity
run these directly in the bigquery console.
===============================================================================
*/

-- 1. Basic volume check across all staging tables
-- Purpose: understand the scale of the dataset we are dealing with.
select 
    'accounts_created' as table_name
    , count(*) as total_rows
from `analytics-take-home-test.larissa_lorenzi_analytics.stg_accounts_created`
union all
select 
    'accounts_closed'
    , count(*)
from `analytics-take-home-test.larissa_lorenzi_analytics.stg_accounts_closed`
union all
select 
    'accounts_reopened'
    , count(*)
from `analytics-take-home-test.larissa_lorenzi_analytics.stg_accounts_reopened`
union all
select 
    'account_transactions'
    , count(*)
from `analytics-take-home-test.larissa_lorenzi_analytics.stg_account_transactions`
;


-- 2. Primary key validation (account_id)
-- Purpose: check if account_id is truly unique in the creation table.
select 
    account_id
    , count(*) as occurrences
from `analytics-take-home-test.larissa_lorenzi_analytics.stg_accounts_created`
group by 1
having occurrences > 1
;


-- 3. Account type distribution
-- Purpose: understand the business variety of accounts (retail vs pots, etc).
select 
    account_type
    , count(*) as total_accounts
    , round(count(*) * 100 / sum(count(*)) over(), 2) as pct_total
from `analytics-take-home-test.larissa_lorenzi_analytics.stg_accounts_created`
group by 1
order by 2 desc
;


-- 4. Transaction data range and sparsity
-- Purpose: check the time horizon and if we have daily data for the wau calculation.
select 
    min(transaction_date) as first_transaction
    , max(transaction_date) as last_transaction
    , count(distinct transaction_date) as days_with_data
    , date_diff(max(transaction_date), min(transaction_date), day) as calendar_days
from `analytics-take-home-test.larissa_lorenzi_analytics.stg_account_transactions`
;


-- 5. User vs account cardinality
-- Purpose: confirm if one user_id can own multiple accounts (critical for wau).
select 
    user_id
    , count(distinct account_id) as total_accounts
from `analytics-take-home-test.larissa_lorenzi_analytics.stg_accounts_created`
group by 1
order by 2 desc
limit 10
;


-- 6. Transaction volume outliers
-- Purpose: identifying the heaviest users in terms of transaction frequency.
select 
    account_id
    , sum(transactions_total) as grand_total_tx
    , avg(transactions_total) as avg_daily_tx
from `analytics-take-home-test.larissa_lorenzi_analytics.stg_account_transactions`
group by 1
order by 2 desc
limit 10
;