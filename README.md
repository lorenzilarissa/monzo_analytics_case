# Monzo Analytics Case - Senior Analytics Engineering Delivery

## Project Overview
This project establishes a robust, scalable, and audited data infrastructure for Monzo's account and transaction data. The primary objective was to provide reliable metrics on Weekly Active Users (WAU) and account lifecycle states, ensuring full data integrity for BI analysts.

---

## Data Architecture
The project follows a modular 3-layer architecture to ensure separation of concerns:

* **Staging**: Raw data cleaning and type casting without applying business logic.
* **Intermediate**: Implementation of "State Machines" for status history and usage metrics.
* **Marts**: Dimension and fact tables optimized for end-user consumption and high performance.

---

## Key Technical Features
* **Account State Machine**: Logic in `int_account_status_history` that manages OPEN, CLOSED, and REOPENED states.
* **WAU Consistency**: 7-day rolling window with zero variance compared to manual calculations on raw data.
* **Semantic Naming**: Folder structure organized by `status_history` and `usage_metrics` for better maintainability.
* **dbt Best Practices**: Strict use of `ref()`, `.yml` documentation, and uniqueness tests.

---

## Data Quality and Audit Framework
In addition to native dbt tests, a manual audit framework was implemented in `/audits`:

* **Referential Integrity**: Validation of zero orphan transactions between facts and dimensions.
* **Uniqueness Audit**: Confirmation of zero duplicates at the account grain in the Mart layer.
* **Lifecycle Logic Validation**: Audit to ensure activity flags correspond correctly to account status.

---

## Getting Started

### Prerequisites
* Python 3.8+
* Authenticated Google Cloud SDK (gcloud)
* Access to BigQuery project: `analytics-take-home-test`

---

## Installation and Setup

### 1. Clone and access the repository
```bash
git clone https://github.com/lorenzilarissa/monzo_analytics_case
cd monzo_analytics_case
```
### 2. Configure virtual environment and dependencies
```bash
python -m venv venv
source venv/Scripts/activate  # For Windows (Git Bash)
pip install -r requirements.txt
```
### 3. GCP Authentication (Application Default Credentials)
```bash
gcloud auth application-default login
```
### 4. Execute the dbt pipeline
```bash
dbt deps
dbt build
```

---

## Audit Results Summary
* **WAU Variance**: PASS
  * Objective: 0.0 variance between Mart and Raw Data

* **Account Uniqueness**: PASS
  * Objective: 100% Unique Account IDs in final dimension

* **Referential Integrity**: PASS
  * Objective: Zero orphan records detected

* **Status Distribution**: PASS
  * Objective: Correct mapping of account lifecycle statuses
