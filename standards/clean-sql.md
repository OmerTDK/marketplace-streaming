# Clean SQL Rules

## Structure and Organization

1. **Never use subqueries** - Flatten them into the main query or use CTEs when needed for complex logic
2. **Use CTEs with descriptive names** - Name them based on their purpose: `loans_filtered`, `fee_calculations`, `interest_calculations` (never `base`, `calc`, `t`)
3. **Constants CTE always first** - If using constants, it MUST be the first CTE
4. **Parameters CTE after constants** - If using toggleable parameters, it comes second

## Table Aliases and Prefixes

1. **Remove table aliases when only one table** - Don't use `l.column`, just use `column`
2. **Use descriptive aliases for multiple tables** - Use `loans` and `payment_plan`, not `l` and `pp`
3. **Always prefix columns when multiple tables exist** - If you have 2+ actual tables (JOINs), prefix ALL columns with table names. Exception: constants and parameters CTEs that are only CROSS JOINed don't count as "multiple tables" - no need to prefix those.

Example:

```sql
-- Single table + constants = no prefixes
FROM loans
CROSS JOIN constants
WHERE status = 'Active'

-- Multiple tables = always prefix
FROM loans
LEFT JOIN payment_plan ON loans.loan_id = payment_plan.loan_id
WHERE loans.status = 'Active'
  AND payment_plan.amount > 0
```

## Column Naming

1. **Use descriptive, meaningful names** - Every column alias MUST clearly indicate what it contains
2. **Boolean columns named as questions** - `is_contract_signed_date_missing`, `is_balance_inflated`, `has_payment_plan` not `Check_Is_contract_signed_date_empty` or `Balance_inflated`
3. **Remove "Check" prefix from validation columns** - Use descriptive names like `discrepancy_excl_vat`, `principal_excess`, `transaction_fee_difference`
4. **Consistent casing in output** - When source columns have inconsistent casing, use aliases to normalize (all lowercase unless it's a report)
5. **Report queries use quoted display names** - Like `Loan ID`, `Transaction Fee excl VAT` (backticks in BigQuery, no parentheses allowed in column names)

## Magic Numbers and Constants

1. **Never use magic numbers** - Extract all literal numbers to a `constants` CTE
2. **Constant names must be specific and descriptive** - Not just `threshold` but `minimum_principal_excess`, `minimum_absolute_balance_difference`, `decimal_precision`
3. **Constants MUST describe what they measure** - Include context about what is being measured or limited
4. **Use CROSS JOIN to reference constants** - `FROM table CROSS JOIN constants WHERE value > constants.threshold`

## Code Cleanliness

1. **Remove all comments** - SQL MUST be self-documenting through clear naming
2. **Never include commented-out code** - Use parameters in a CTE instead with boolean flags
3. **Remove unnecessary LIMIT clauses** - Don't hard-code Excel row limits (1048575) or other arbitrary limits unless explicitly needed as a parameter

## Syntax Preferences

1. **Boolean columns named as questions + avoid `= TRUE` syntax** - Name booleans as questions (e.g., `is_balance_inflated`, `has_payment_plan`), then use them directly without `= TRUE`:

```sql
-- Good
WHERE is_balance_inflated
  AND has_payment_plan

-- Bad
WHERE Balance_inflated = TRUE
  AND payment_plan = TRUE
```

This makes the query read naturally: "where is balance inflated and has payment plan"

2. **Remove backticks from regular identifiers** - Only use backticks for display names in SELECT clause for reports
3. **Explicit columns in `UNION` or `UNION ALL`** - Never use `SELECT *`, always list all columns explicitly
4. **Consistent formatting** - Proper indentation, one condition per line in WHERE clauses

## When to Use Parameters

1. **Use parameters CTE for toggleable options** - Like `include_inflated` boolean flag
2. **Parameters structure**:

```sql
WITH parameters AS (
  SELECT FALSE AS include_inflated
)
```

3. **Don't extract filters without context** - Keep status/country/financing filters inline unless there's clear reason to parameterize them

## Example Structure

```sql
WITH constants AS (
  SELECT
    1 AS minimum_discrepancy,
    2 AS decimal_precision
),
parameters AS (
  SELECT FALSE AS include_optional_check
),
loans_filtered AS (
  SELECT
    loan_id,
    status,
    amount_field
  FROM loans
  WHERE condition
)
SELECT
  loan_id,
  status,
  calculated_field
FROM loans_filtered
CROSS JOIN constants
WHERE ABS(calculated_field) > constants.minimum_discrepancy
ORDER BY calculated_field DESC;
```
