# dbt and Data Modeling Standards

> **Read `dbt_project.yml` first — before quoting any dataset/schema/folder name.** This document describes the *target* convention; the YAML is the only truth. Where they disagree, the YAML wins.

Standards and conventions for a lending data warehouse built with dbt and BigQuery. Examples use the project's synthetic consumer-lending domain: borrowers, loans, payments, delinquency events.

## Core Principles

1. **Descriptive and unambiguous** — names MUST convey meaning without context
2. **Consistent patterns** — same concept = same naming pattern everywhere
3. **Layer-appropriate** — data transforms progressively through well-defined layers
4. **Dimensional model ready** — clear fact/dimension distinction in DWH
5. **Business-aligned dates** — local (Berlin) dates for reporting, UTC for storage

## Timezone Convention

**All timestamps are UTC unless explicitly suffixed with a timezone.**

Berlin (Europe/Berlin) is the chosen business timezone for the synthetic bank — all reporting dates align to it.

- Columns ending in `_at` without a timezone suffix → UTC
- Columns ending in `_berlin` → Europe/Berlin timezone (IANA tz, includes DST: CET/CEST)
- When in doubt, it's UTC

## BigQuery Structure: Datasets and Tables

In BigQuery: **dataset = schema, table = table within dataset**. Always be explicit.

| Layer | Dataset | Table Naming | Example |
|-------|---------|--------------|---------|
| Raw | `raw` | source name as-is | `raw.loan` |
| Staging | `stg` | `{source}__{entity}` | `stg.lending__loan` |
| Intermediate | `int` | `{entity}` (flat — no suffix conventions in use) | `int.loan`, `int.borrower` |
| DWH | `dwh` | `dim_{entity}` or `fct_{entity}[_{detail}]` | `dwh.dim_borrower`, `dwh.fct_payment` |
| Marts | `mart_{domain}` | `mart_{domain}_{name}` | `mart_risk.mart_risk_roll_rates` |

Mart domains in use: `mart_risk`, `mart_finance`, `mart_ops` (verify in `dbt_project.yml`).

**Naming rules:** all lowercase snake_case · double underscore separates source from entity in staging · singular nouns (`dim_borrower` not `dim_borrowers`).

## Layer Responsibilities

| Layer | Purpose | Intermediate variants? |
|-------|---------|------------------------|
| Raw | source data as-is, untouched | No — 1:1 with source |
| Staging | source cleaning, renaming, casting | Allowed: `{source}__{entity}_cleaned` etc. (rare in practice) |
| Intermediate | transformations including dimensional prep | Flat `int_{entity}.sql`, one model per entity. A `_base/_enriched/_validated/_dim_prep/_fact_prep` suffix scheme is **not** in use — don't introduce it. **Mart-prep sub-type** (see below) is the one recognised structural variant. |
| DWH | final dimensional model: surrogate keys, SCD, persistence | No — final `dim_*` and `fct_*` only |
| Marts | domain-specific consumption views | As needed |

### Intermediate sub-type: mart-prep intermediate

A **mart-prep intermediate** is an intermediate model that aggregates DWH facts and dimensions into a domain-specific projection — typically the source for one or more marts. It is the legitimate way to express:

- Eligibility / qualification logic that joins many DWH entities (e.g. securitization-pool eligibility, investor-reporting eligibility)
- Domain-specific roll-ups that would be unwieldy inside a single mart CTE
- Logic that is shared across multiple marts in the same domain (avoid duplication)

**Mart-prep intermediates legitimately reference DWH facts and dimensions.** The typical-flow guidance "intermediate reads from staging" describes the default direction, not a hard prohibition. The mart-prep sub-type is the documented exception.

**Folder convention:** mart-prep intermediates live in `models/intermediate/{domain}/` (e.g. `models/intermediate/risk/`). The subfolder is the marker — anything at the root of `models/intermediate/` is a regular intermediate and reads staging only.

**File header convention:** the first comment block in a mart-prep intermediate SQL file should state explicitly: `-- Mart-prep intermediate. Reads DWH facts/dimensions to build {domain}-specific projection for downstream {mart_name} marts.`

**Folder context file:** add a `_context.md` next to mart-prep files documenting the pattern, so reviewers (human or automated) treat DWH refs from these files as intentional, not as layer-boundary breaches.

Example instance: `models/intermediate/risk/int_risk_pool_eligibility.sql` (securitization-pool eligibility, feeding `mart_risk_pool` and `mart_risk_roll_rates`).

### Mart Layer Naming Patterns

Mart files follow `mart_{domain}_{name}.sql` (so the BQ table ends up as `mart_{domain}.mart_{domain}_{name}` — the prefix repetition is the convention; do not strip it on existing marts). The `{name}` slot uses one of these patterns:

| Pattern | When to Use | Example |
|---------|-------------|---------|
| Entity mirror | dimension-like data filtered/shaped for a domain | `mart_risk.mart_risk_borrower` |
| Aggregation | pre-computed metrics over time periods | `mart_risk.mart_risk_roll_rates` |
| Report | output matching external requirements | `mart_finance.mart_finance_investor_portfolio` |

## Key Naming

| Key Type | Pattern | Layer Created | Example |
|----------|---------|---------------|---------|
| Natural | `{entity}_id` | Staging | `loan_id` |
| Surrogate | `{entity}_key` | DWH | `loan_key` |
| Date FK | `{event}_date_key` | DWH | `payment_date_key` |

Foreign keys match the key name of the referenced table. Example: `dwh.fct_payment.loan_key` references `dwh.dim_loan.loan_key`.

## Timestamps and Dates

- `{event}_at` — TIMESTAMP, always UTC (`created_at`, `modified_at`, `contract_signed_at`)
- `{event}_date` — DATE, local/business date (`due_date`, `payment_date`)
- `{event}_date_berlin` — DATE derived from UTC to Europe/Berlin (`contract_signed_date_berlin`)
- DWH metadata: `_loaded_at`, `_updated_at`, `_extracted_at`, `_valid_from`, `_valid_to`

### Date Keys and dim_date

**Rule: date keys in DWH are derived from DATE fields, not directly from timestamps.**

If you need to join a timestamp to `dim_date`, derive a date field in the intermediate layer first. Date keys MUST reference business-relevant dates — typically the local Berlin date — so reports align with business calendar expectations.

```sql
-- Intermediate: derive Berlin date from UTC timestamp
int.payment:
    payment_id
    paid_at                         -- TIMESTAMP (UTC)
    payment_date_berlin             -- DATE (Europe/Berlin)

-- DWH: date key from Berlin date
dwh.fct_payment:
    payment_key
    payment_date_key                -- FK to dim_date, from payment_date_berlin

-- Join in fact build:
LEFT JOIN dwh.dim_date
    ON int.payment.payment_date_berlin = dwh.dim_date.full_date
```

### Source vs DWH Timestamps

Distinguish business timestamps from DWH metadata using underscore prefix.

- **Business** (no prefix): `created_at`, `modified_at` — meaningful in source
- **Metadata** (underscore prefix): `_loaded_at`, `_updated_at`, `_extracted_at`, `_valid_from`, `_valid_to` — system bookkeeping

The underscore signals "system metadata, not business data."

## Booleans

Booleans read as yes/no questions. `WHERE is_active` reads naturally — never `WHERE is_active = TRUE`.

| Pattern | Meaning | Examples |
|---------|---------|----------|
| `is_{state}` | current state | `is_active`, `is_archived`, `is_paid`, `is_delinquent` |
| `has_{thing}` | possession | `has_direct_debit`, `has_open_balance`, `has_payment_plan` |
| `was_{action}` | past event | `was_migrated`, `was_manually_approved` |
| `passed_{check}` | validation result | `passed_credit_check`, `passed_aml_check` |

## Numeric Columns

### Monetary

Specify VAT treatment when relevant (fees can be quoted gross or net).

```sql
amount_incl_vat
amount_excl_vat
vat_amount
monthly_fee_incl_vat
monthly_fee_excl_vat

-- When VAT not relevant
principal_amount
downpayment_amount
outstanding_amount
```

DE VAT = 19%, AT VAT = 20%.

### Counts

Pattern: `{thing}_count` — `payment_count`, `missed_payment_count`, `days_overdue_count`.

### Rates and Percentages

| Form | Storage | Layer | Example |
|------|---------|-------|---------|
| `_rate` | decimal 0–1 (0.05 = 5%) | Staging through DWH | `interest_rate`, `default_rate` |
| `_pct` | value 0–100 (5 = 5%) | Marts only (presentation) | `utilization_pct` |

**Don't mix `_rate` and `_pct`.** Different scales; convert decimal to percentage only when needed for presentation. Every rate column in the warehouse is `_rate` (decimal); `_pct` exists so the first presentation-layer percentage lands correctly.

### Measurements

Include unit when not obvious: `term_months`, `grace_period_days`, `statement_cycle_days`, `duration_months`.

## Text and Categorical

```sql
-- Status / type
status, loan_status, payment_status, type, category

-- Names / descriptions
name, full_name, display_name, description, code

-- External IDs
external_id, core_banking_id, stripe_id, crm_id
```

## Fact Table Structure

Facts contain keys, measures, degenerate dimensions, and metadata. Example: `dwh.fct_payment`.

```sql
-- dwh.fct_payment
payment_key                     -- surrogate PK
loan_key                        -- FK → dwh.dim_loan
borrower_key                    -- FK → dwh.dim_borrower
payment_date_key                -- FK → dwh.dim_date (from payment_date_berlin)
due_date_key                    -- FK → dwh.dim_date (from due_date_berlin)

payment_reference               -- degenerate dimension
payment_method                  -- degenerate dimension

scheduled_amount                -- measure
paid_amount                     -- measure
principal_amount                -- measure
interest_amount                 -- measure
outstanding_amount              -- measure

is_paid                         -- boolean measure
is_overdue                      -- boolean measure

created_at                      -- source timestamp (UTC)
_loaded_at                      -- DWH metadata
_updated_at                     -- DWH metadata
```

## Dimension Table Structure

Dimensions contain surrogate key, natural key, attributes, and SCD fields. Example: `dwh.dim_borrower` (the credit-bearing customer side of a loan).

```sql
-- dwh.dim_borrower
borrower_version_key            -- surrogate PK of the row (SCD2)
borrower_key                    -- surrogate PK of the entity
borrower_id                     -- natural key (for lookups)

full_name, email, city, country, segment    -- attributes
is_active, has_direct_debit                  -- boolean attributes

created_at                      -- source timestamp (UTC)
_loaded_at, _updated_at         -- DWH metadata
_valid_from, _valid_to          -- SCD2
_is_current                     -- SCD2 flag
```

## Date Dimension

`dwh.dim_date` holds calendar attributes. Holiday/business-day logic lives in `dim_holiday` due to regional complexity.

```sql
-- dwh.dim_date
date_key                        -- surrogate (YYYYMMDD integer)
full_date                       -- DATE

day_of_week, day_name           -- 1-7, Monday/Tuesday/...
day_of_month, day_of_year       -- 1-31, 1-366
week_of_year, month_number      -- 1-53, 1-12
month_name, quarter_number      -- January/February/..., 1-4
year_number                     -- 2024, 2025...

is_weekend
fiscal_year, fiscal_quarter
```

## Holiday Dimension

Holidays vary by country and region (German states differ; Austria has no regional variation). Keep them out of `dim_date`.

```sql
-- dwh.dim_holiday
holiday_key                     -- surrogate PK
date_key                        -- FK → dwh.dim_date
country_code                    -- 'DE', 'AT'
region_code                     -- 'DE-BY' (Bavaria), 'DE-BE' (Berlin), NULL = national
holiday_name                    -- 'Christmas Day', 'German Unity Day'
is_public_holiday               -- official public holiday
is_bank_holiday                 -- banks closed
```

### Business Day Determination

```sql
-- Business days for Berlin (DE-BE)
WITH parameters AS (
    SELECT '2025-01-01' AS start_date,
           '2025-12-31' AS end_date,
           'DE' AS country,
           'DE-BE' AS region
)
SELECT
    dates.full_date,
    dates.day_name,
    NOT (dates.is_weekend OR holidays.holiday_key IS NOT NULL) AS is_business_day
FROM dwh.dim_date AS dates
CROSS JOIN parameters
LEFT JOIN dwh.dim_holiday AS holidays
    ON dates.date_key = holidays.date_key
    AND (
        (holidays.country_code = parameters.country AND holidays.region_code IS NULL)
        OR (holidays.country_code = parameters.country AND holidays.region_code = parameters.region)
    )
WHERE dates.full_date BETWEEN parameters.start_date AND parameters.end_date
```

Join logic: `country_code = X AND region_code IS NULL` matches national holidays; `country_code = X AND region_code = Y` matches regional. AT has no regional variation.

## Layer Transformation Summary

How names evolve through layers:

| Concept | Raw (`raw`) | Staging (`stg`) | Intermediate (`int`) | DWH (`dwh`) |
|---------|-------------|------------------|----------------------|-------------|
| Primary key | `_id` | `loan_id` | `loan_id` | `loan_key` |
| Foreign key | `borrower_user` | `borrower_id` | `borrower_id` | `borrower_key` |
| Source created | `Created_Date` | `created_at` | `created_at` | `created_at` |
| Source modified | `Modified_Date` | `modified_at` | `modified_at` | `modified_at` |
| DWH load time | — | — | — | `_loaded_at` |
| DWH update time | — | — | — | `_updated_at` |
| Boolean | `archived_boolean` | `is_archived` | `is_archived` | `is_archived` |
| Amount | `amount_incl_vat_number` | `amount_incl_vat` | `amount_incl_vat` | `amount_incl_vat` |
| Rate | `rate_number` | `interest_rate` | `interest_rate` | `interest_rate` |

### Date/Timestamp Flow

| Stage | Column | Type | Description |
|-------|--------|------|-------------|
| Raw | `signed_date_date` | TIMESTAMP | source field (often misnamed) |
| Staging | `contract_signed_at` | TIMESTAMP | renamed, UTC |
| Intermediate | `contract_signed_at` | TIMESTAMP | UTC |
| Intermediate | `contract_signed_date_berlin` | DATE | derived UTC → Europe/Berlin |
| DWH | `contract_signed_date_key` | INT | FK → dim_date, from Berlin date |

**Rule:** date keys always derived from DATE fields (typically Berlin), never directly from timestamps.

## Model Dependencies (DAG)

| Model Type | Can Reference | Cannot Reference |
|------------|---------------|------------------|
| Staging | Sources only | other staging, intermediate, dwh, marts |
| Intermediate | staging, other intermediate, plus `dim_date` / `dim_holiday` for date-key derivation (see exception below) | other DWH (except mart-prep sub-type, documented above), marts |
| DWH (dim/fct) | intermediate, other DWH | staging directly, marts |
| Marts | DWH only | staging, intermediate |

**Exception — `dim_date` / `dim_holiday`:** intermediate models may `ref('dim_date')` or `ref('dim_holiday')` for date-key derivation and business-day logic. These two dims are calendar primitives, not derived from any source — so the usual "no upward refs" rule doesn't add ordering safety here. (e.g., business-day-aware due-date derivation in a mart-prep intermediate.)

## Materialization by Layer

| Layer | Default | When to Change |
|-------|---------|----------------|
| Staging | `view` | never |
| Intermediate | `view` | `ephemeral` if used only once |
| DWH dimensions | `table` | `incremental` if >1M rows |
| DWH facts | `table` | `incremental` for the payment/transaction fact families that scale with loan volume; rest stay `table` |
| Marts | `table` | rarely change |

### SCD2 Temporal Join Safety

Any LEFT JOIN from an SCD2 dimension to a **current-state-only source** (seed, non-SCD2 table, external lookup) **MUST** include `AND <dim>._is_current` in the join condition. Without this, historical SCD2 rows inherit present-day values — a loan added to a seed in Q4 shows the flag as TRUE on all historical rows from before it was added.

```sql
-- WRONG — is_watchlisted leaks into historical rows
SELECT all_loans.*, seed.loan_id IS NOT NULL AS is_watchlisted
FROM all_loans
LEFT JOIN seed ON all_loans.loan_id = seed.loan_id

-- RIGHT — historical rows default to FALSE
SELECT all_loans.*, seed.loan_id IS NOT NULL AS is_watchlisted
FROM all_loans
LEFT JOIN seed
    ON all_loans.loan_id = seed.loan_id
    AND all_loans._is_current
```

### Incremental Snapshot Protection

Any incremental model that accumulates point-in-time state (daily snapshots, append-only ledgers, `insert_overwrite` on date-partitioned tables) **MUST** include `full_refresh=false` in its config block. Without this, `dbt run --full-refresh` silently destroys all accumulated historical partitions — irrecoverable without a backup.

```sql
{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    full_refresh=false,    -- REQUIRED for state-accumulating models
    partition_by={'field': 'snapshot_date_berlin', 'data_type': 'date', 'granularity': 'day'}
) }}
```

## File Naming

| Layer | Pattern | Example |
|-------|---------|---------|
| Staging | `stg_{source}__{entity}.sql` | `stg_lending__loan.sql` |
| Intermediate | `int_{entity}.sql` (flat) | `int_loan.sql`, `int_borrower.sql` |
| DWH | `dim_{entity}.sql` or `fct_{entity}[_{detail}].sql` | `dim_borrower.sql`, `fct_payment.sql`, `fct_loan_status_event.sql` |
| Marts | `mart_{domain}_{name}.sql` | `mart_risk_roll_rates.sql` |

YAML files: `_{source}__sources.yml`, `_{folder}__models.yml` (leading underscore sorts to top).

## Documentation Requirements

1. Every model MUST have a description
2. Every column MUST have a description
3. Primary keys MUST have `unique` and `not_null` tests
4. Foreign keys MUST have `relationships` tests

## Testing Requirements

| Column Type | Required Tests |
|-------------|----------------|
| Surrogate key (`{entity}_key`) | `unique`, `not_null` |
| Natural key (`{entity}_id`) | `not_null`, `unique` (if applicable) |
| Foreign key (`{entity}_key`) | `not_null` (if required), `relationships` |
| Status / type columns | `accepted_values` |
| Date columns | `not_null` (for required dates) |

Source freshness on the `raw` dataset:

```yaml
sources:
  - name: raw
    freshness:
      warn_after: {count: 12, period: hour}
      error_after: {count: 24, period: hour}
    loaded_at_field: modified_at
```

## Antipatterns

### Keys
- `id` alone — ambiguous
- `pk_loan`, `fk_borrower` — Hungarian notation
- `LoanID` — wrong casing

### Timestamps / Dates
- `created` — no suffix, unclear type
- `timestamp_created` — redundant
- `dt_signed` — abbreviation unclear
- Deriving date keys directly from UTC timestamps — use Berlin dates

### Booleans
- `active` — use `is_active`
- `deleted_flag` — use `is_deleted`
- `direct_debit` for boolean — use `has_direct_debit`

### Rates / Percentages
- Mixing `_rate` and `_pct` — different scales
- Storing percentages (0-100) in `_rate` columns — rates are decimals (0-1)

### General
- `value`, `amount` alone — what value?
- `type1`, `type2` — not descriptive
- CamelCase or PascalCase — use snake_case
- `tmp_`, `test_` in production
- Field-to-field fallbacks (`COALESCE(x, y)` to mask missing data) — surface as a data-quality or eligibility failure instead

## Quick Reference

| Category | Pattern | Example |
|----------|---------|---------|
| **Datasets** | | |
| Raw | `raw` | `raw` |
| Staging | `stg` | `stg` |
| Intermediate | `int` | `int` |
| DWH | `dwh` | `dwh` |
| Mart | `mart_{domain}` | `mart_risk`, `mart_finance`, `mart_ops` |
| **Tables** | | |
| Staging | `{source}__{entity}` | `lending__loan` |
| Intermediate | `{entity}` (flat) | `loan`, `borrower` |
| Dimension | `dim_{entity}` | `dim_borrower`, `dim_loan` |
| Fact | `fct_{entity}[_{detail}]` | `fct_payment`, `fct_delinquency_event` |
| Mart | `mart_{domain}_{name}` | `mart_risk_roll_rates` |
| **Columns** | | |
| Natural key | `{entity}_id` | `borrower_id` |
| Surrogate key | `{entity}_key` | `borrower_key` |
| Date key | `{event}_date_key` | `payment_date_key` |
| Timestamp (UTC) | `{event}_at` | `created_at` |
| Date | `{event}_date` | `due_date` |
| Berlin date | `{event}_date_berlin` | `signed_date_berlin` |
| DWH metadata | `_{field}` | `_loaded_at`, `_updated_at` |
| Boolean state | `is_{state}` | `is_active` |
| Boolean possession | `has_{thing}` | `has_direct_debit` |
| Boolean validation | `passed_{check}` | `passed_credit_check` |
| Amount with VAT | `{field}_incl_vat` | `fee_incl_vat` |
| Amount without VAT | `{field}_excl_vat` | `fee_excl_vat` |
| Count | `{thing}_count` | `payment_count` |
| Rate (decimal) | `{concept}_rate` | `interest_rate` |
| Percentage (0-100) | `{concept}_pct` | `utilization_pct` (marts only) |
