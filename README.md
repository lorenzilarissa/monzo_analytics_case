# Monzo Analytics Case

**A Senior Analytics Engineering project delivering a robust, scalable, and audited data infrastructure for Monzo's accounts and transactions ecosystem.**

---

## Overview

This project establishes a comprehensive analytics data warehouse for Monzo, providing reliable metrics on Weekly Active Users (WAU) and account lifecycle states. The solution combines multi-layered data modeling with extensive data quality validations, ensuring both accuracy and usability for BI analysts.

**Key Objective:** Enable data-driven decision-making through trusted, well-documented data assets.

---

## Architecture

The project follows a modular three-layer design pattern, adhering to analytics engineering best practices:

### Staging Layer
Raw data extraction and basic transformations. Maintains fidelity to source systems without applying business logic.
- `stg_account_transactions`
- `stg_accounts_created`
- `stg_accounts_closed`
- `stg_accounts_reopened`

### Intermediate Layer
Business logic implementation and complex transformations. Home to the core intelligence of the warehouse.
- **Status History Management:** `int_account_status_history` implements a state machine managing OPEN, CLOSED, and REOPENED states
- **Usage Metrics:** `int_user_daily_activity` tracks daily user engagement; `int_account_active_status` maintains current account states

### Marts Layer
Optimized dimension and fact tables designed for analytics consumption, performance, and usability.
- **Dimensions:** `dim_account_status` - provides the authoritative current state of each account
- **Facts:** `fct_user_activity_daily` - daily activity metrics aggregated at the user level

---

## Core Features

### State Machine Implementation
The account status tracking uses a sophisticated state machine that manages transitions between OPEN, CLOSED, and REOPENED states. This ensures `dim_account_status` always reflects the most current account state while maintaining historical accuracy.

### WAU Consistency Validation
Weekly Active Users are calculated using a 7-day rolling window with zero deviation from manual calculations on raw data. This demonstrates complete accuracy in aggregation logic.

### Semantic Organization
Folder and table naming follows self-documenting conventions:
- `status_history` - explicitly indicates state transition logic
- `usage_metrics` - clearly denotes activity-based metrics
- This improves maintainability and reduces onboarding time

---

## Data Quality Framework

Data integrity is the foundation of this project. Beyond native dbt tests, a comprehensive audit framework validates critical data properties:

### Audit Queries (located in `/audits/queries/`)

**Account Logic Audits**
- `audit_account_lifecycle_logic` - validates state transition consistency
- `audit_dim_account_uniqueness` - ensures one record per account

**Activity Facts Audits**
- `audit_fct_activity_date_completeness` - confirms continuous daily coverage
- `audit_fct_activity_referential_integrity` - validates relationships between fact and dimension tables
- `audit_wau_rolling_window_consistency` - confirms WAU calculation accuracy

---

## Project Structure

\\\
monzo_analytics_case/
├── models/
│   ├── staging/           # Raw data transformations
│   ├── intermediate/      # Business logic and complex calculations
│   │   ├── status_history/
│   │   └── usage_metrics/
│   └── marts/             # Final analytics tables
│       ├── dim/
│       └── fct/
├── audits/                # Data quality validation queries
├── tests/                 # dbt test configurations
├── dbt_project.yml        # dbt project configuration
└── README.md
\\\

---

## Getting Started

### Prerequisites
- Python 3.8 or later
- Google Cloud SDK (gcloud) configured for the project
- dbt installed

### Installation

1. **Clone the repository**
   \\\ash
   git clone <repository-url>
   cd monzo_analytics_case
   \\\

2. **Set up Python virtual environment**
   \\\ash
   python -m venv venv
   # On Windows
   venv\Scripts\activate
   # On macOS/Linux
   source venv/bin/activate
   \\\

3. **Install dependencies**
   \\\ash
   pip install -r requirements.txt
   \\\

4. **Configure dbt profiles** (if needed)
   \\\ash
   dbt debug
   \\\

### Running the Project

**Full data build and tests**
\\\ash
dbt build
\\\

**Run only models**
\\\ash
dbt run
\\\

**Execute tests**
\\\ash
dbt test
\\\

**Run audit queries**
\\\ash
dbt run-operation run-audits
\\\

---

## Data Models Documentation

Each model includes comprehensive YAML documentation with descriptions, column definitions, and data quality tests. Refer to the `.yml` files in each model directory for detailed specifications:
- `models/staging/src_monzo.yml`
- `models/intermediate/status_history/int_account_status_history.yml`
- `models/intermediate/usage_metrics/int_account_active_status.yml`
- `models/marts/dim/dim_account_status.yml`
- `models/marts/fct/fct_user_activity_daily.yml`

---

## Key Metrics

- **WAU (Weekly Active Users):** Unique users with at least one transaction in a 7-day rolling window
- **Account Status:** Current state of each account (OPEN, CLOSED, REOPENED)
- **Daily Activity:** User transaction counts and engagement metrics aggregated daily

---

## Testing & Validation

All models include built-in tests (uniqueness, not-null, referential integrity) as defined in the YAML documentation. The audit framework provides additional business logic validation through analytical queries.

---

## Contact & Support

For questions or contributions, please refer to the project documentation or contact the analytics engineering team.
