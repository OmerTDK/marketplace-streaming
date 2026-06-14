# Engineering Principles

Adapted from the [Linux kernel coding style](https://www.kernel.org/doc/Documentation/process/coding-style.rst). These are foundational engineering principles that apply to **all code in this portfolio** — Python, SQL, dbt, shell scripts, configuration. When other standards conflict with these, these principles take precedence.

---

## 1. Indentation Reveals Structure

Indentation exists to show control flow at a glance. If your code needs more than 3 levels of indentation, it is too complex. Refactor it.

```python
# Bad — too deeply nested
def process_loans(loans):
    for loan in loans:
        if loan.is_active:
            if loan.country == "DE":
                if loan.has_payment_plan:
                    if loan.principal_amount > 0:
                        do_something(loan)

# Good — flatten with early returns and guard clauses
def process_loans(loans):
    active_loans = [loan for loan in loans if loan.is_active]
    for loan in active_loans:
        if not is_eligible(loan):
            continue
        do_something(loan)

def is_eligible(loan):
    return (
        loan.country == "DE"
        and loan.has_payment_plan
        and loan.principal_amount > 0
    )
```

```sql
-- Bad — nested subqueries
SELECT * FROM (
  SELECT * FROM (
    SELECT * FROM loans WHERE status = 'Active'
  ) WHERE country_code = 'DE'
) WHERE principal_amount > 0

-- Good — flat CTEs
WITH active_loans AS (
  SELECT loan_id, country_code, principal_amount
  FROM loans
  WHERE status = 'Active'
)
SELECT loan_id, country_code, principal_amount
FROM active_loans
WHERE country_code = 'DE'
  AND principal_amount > 0
```

**The rule:** If you need more than 3 levels of nesting, your code needs restructuring. Deep nesting is a design problem, not a formatting problem.

---

## 2. Functions Do One Thing

A function should fit on one screen (~50 lines for Python, ~40 lines for SQL CTEs). The maximum length of a function is inversely proportional to its complexity.

- Simple functions with linear logic can be longer
- Complex functions with branching and error handling must be shorter
- If a function has more than 7 local variables, it is doing too much — split it

```python
# Bad — does three things
def process_portfolio_report(date):
    # fetch data
    client = bigquery.Client()
    query = "SELECT ..."
    rows = client.query(query).result()
    # transform
    df = pd.DataFrame(rows)
    df["amount"] = df["amount"].apply(lambda x: round(x, 2))
    df["date"] = pd.to_datetime(df["date"])
    # ... 40 more lines of transformation ...
    # export
    df.to_csv(f"report_{date}.csv")
    upload_to_drive(f"report_{date}.csv")

# Good — each function does one thing
def fetch_portfolio_data(report_date: date) -> pd.DataFrame:
    ...

def transform_portfolio_data(raw_data: pd.DataFrame) -> pd.DataFrame:
    ...

def export_portfolio_report(report: pd.DataFrame, report_date: date) -> None:
    ...
```

**The rule:** Each function does exactly one thing and does it well. If you cannot describe what a function does without using "and", split it.

---

## 3. Naming Is Design

Good naming eliminates the need for comments. A name should tell you **what** something is or **what** a function does without reading the implementation.

### Local variables: short is fine when scope is small

```python
# Fine — small scope, obvious meaning
for row in rows:
    ...

i, j = 0, 0
```

### Functions and globals: descriptive, no abbreviations

```python
# Bad
def calc_pp():  ...
def proc():     ...
cnt = 0

# Good
def calculate_payment_plan():   ...
def process_payments():         ...
loan_count = 0
```

### No type encoding (Hungarian notation)

The language and type hints handle types. Never encode the type in the name.

```python
# Bad
str_name = "Lending Platform"
lst_loans = []
df_payments = pd.DataFrame()
dict_config = {}

# Good
name = "Lending Platform"
loans = []
payments = pd.DataFrame()
config = {}
```

### Inclusive terminology

Use neutral, descriptive terms:
- `allowlist` / `denylist` (not whitelist/blacklist)
- `primary` / `replica` (not master/slave)
- `leader` / `follower` (not master/slave)

---

## 4. Comments Explain WHY, Never HOW or WHAT

If your code needs a comment to explain **what** it does, rewrite the code. Comments exist only to explain **why** something non-obvious is necessary.

```python
# Bad — explains what (the code already says this)
# Loop through loans and filter active ones
active = [loan for loan in loans if loan.is_active]

# Bad — explains how (the code already says this)
# Use string formatting to build the query
query = f"SELECT * FROM {table}"

# Good — explains why (not obvious from code)
# ECB rate API has 15-minute cache; fetch at :20 past to ensure fresh data
RATE_FETCH_MINUTE = 20

# Good — explains a business rule
# Partner API spec v3.2 requires amounts rounded to 2 decimals
amount = round(raw_amount, 2)
```

```sql
-- Bad
-- Join loans with payment plans
FROM loans
LEFT JOIN payment_plan ON loans.loan_id = payment_plan.loan_id

-- Good (no comment needed — the code is self-documenting)
FROM loans
LEFT JOIN payment_plan ON loans.loan_id = payment_plan.loan_id
```

**The rule:** If you feel the urge to write a comment, first try to restructure the code or rename things so the comment becomes unnecessary. Only add a comment when the **why** genuinely cannot be expressed in code.

---

## 5. Centralized Exit Points

Functions with multiple cleanup steps should use a single exit path. In Python, this means `try/finally` blocks or context managers. In SQL, this means structured CTEs rather than scattered logic.

```python
# Bad — cleanup scattered across multiple return paths
def process_file(path):
    file = open(path)
    data = file.read()
    if not data:
        file.close()
        return None
    result = parse(data)
    if not result:
        file.close()
        return None
    file.close()
    return result

# Good — single exit path via context manager
def process_file(path):
    with open(path) as file:
        data = file.read()
    if not data:
        return None
    return parse(data)
```

```python
# Good — single cleanup path with try/finally for resources without context managers
def upload_report(report_data, drive_folder_id):
    temp_path = None
    try:
        temp_path = write_temp_file(report_data)
        upload_to_drive(temp_path, drive_folder_id)
    finally:
        if temp_path and temp_path.exists():
            temp_path.unlink()
```

**The rule:** Every resource acquisition should have exactly one corresponding release, and it should be obvious where that release happens.

---

## 6. Do Not Overuse Abstractions

Do not create an abstraction for something that happens once. Three similar lines of code are better than a premature wrapper. Abstractions have a cost: indirection, cognitive load, and maintenance burden.

```python
# Bad — abstraction for a one-time operation
class BigQueryQueryExecutor:
    def __init__(self, project_id):
        self.client = bigquery.Client(project=project_id)

    def execute(self, query):
        return self.client.query(query).result()

executor = BigQueryQueryExecutor("lending-analytics")
result = executor.execute("SELECT 1")

# Good — just use the client directly
client = bigquery.Client(project="lending-analytics")
result = client.query("SELECT 1").result()
```

**The rule:** Don't add a layer of indirection unless it will be used in at least 3 places and provides genuine simplification. The right amount of abstraction is the minimum needed for the current task.

---

## 7. Error Handling Is Not Optional

Errors should never pass silently. Every error path must be handled explicitly. When an error occurs, the code should either recover meaningfully or fail loudly with context.

```python
# Bad — silent failure
try:
    upload_to_drive(report)
except Exception:
    pass

# Bad — generic catch with no context
try:
    upload_to_drive(report)
except Exception as e:
    logger.error(e)

# Good — specific handling with context and recovery or re-raise
try:
    upload_to_drive(report)
except GoogleAPIError as e:
    logger.error(f"Drive upload failed for {report.name}: {e}")
    raise
except FileNotFoundError as e:
    raise FileNotFoundError(
        f"Report file missing before upload: {report.path}"
    ) from e
```

**The rule:** Handle errors at the right level. Catch specific exceptions. Always include enough context to diagnose the problem. Never swallow errors silently.

---

## 8. Avoid the Inline Disease

Do not over-optimize. Do not inline everything. Do not add micro-optimizations that trade away readability. The compiler (or interpreter, or query engine) is smarter than you think.

In Python this means:
- Do not write one-liners that pack 3 operations into a single expression
- Do not use nested comprehensions when a loop is clearer
- Do not trade readability for marginal performance

```python
# Bad — clever one-liner, hard to read
result = {k: [x["amount"] for x in v if x["status"] == "settled"] for k, v in groupby(sorted(payments, key=lambda x: x["loan"]), key=lambda x: x["loan"])}

# Good — clear, step by step
payments_by_loan = defaultdict(list)
for payment in payments:
    if payment["status"] == "settled":
        payments_by_loan[payment["loan"]].append(payment["amount"])
```

In SQL this means:
- Do not nest `CASE WHEN` inside `CASE WHEN` inside `COALESCE`
- Break complex expressions into CTEs with descriptive names

**The rule:** Write for the reader, not the machine. Clear code that runs slightly slower is worth more than clever code that nobody can maintain.

---

## 9. Function Return Values Are Contracts

Be consistent about what functions return. A function's return type is a contract with its callers.

- **Action functions** return nothing (`None`) on success, raise on failure
- **Query functions** return the data or `None` if not found
- **Validation functions** return `bool`
- **Never** return mixed types (`str | None | int`) — pick one contract and stick with it

```python
# Bad — inconsistent return contract
def get_loan(loan_id: str):
    if not loan_id:
        return False          # bool?
    loan = fetch(loan_id)
    if not loan:
        return "not found"    # str?
    return loan               # dict?

# Good — consistent contract: returns dict or raises
def get_loan(loan_id: str) -> dict:
    if not loan_id:
        raise ValueError("loan_id is required")
    loan = fetch(loan_id)
    if loan is None:
        raise LookupError(f"Loan not found: {loan_id}")
    return loan
```

**The rule:** Every function has a single, clear return contract. Callers should never need to check what type came back.

---

## 10. Conditional Compilation = Conditional Logic

The kernel avoids `#ifdef` scattered through code because it destroys readability. The same applies here: do not scatter environment checks, feature flags, or conditional logic throughout business logic.

```python
# Bad — environment checks scattered everywhere
def generate_report(date):
    if os.getenv("ENV") == "dev":
        client = bigquery.Client(project="lending-analytics-dev")
    else:
        client = bigquery.Client(project="lending-analytics")
    data = client.query(QUERY).result()
    if os.getenv("ENV") == "dev":
        logger.setLevel(logging.DEBUG)
    ...

# Good — centralize configuration, business logic stays clean
def get_bigquery_client() -> bigquery.Client:
    project_id = os.getenv("BQ_PROJECT_ID", "lending-analytics")
    return bigquery.Client(project=project_id)

def generate_report(client: bigquery.Client, report_date: date) -> pd.DataFrame:
    data = client.query(QUERY).result()
    ...
```

**The rule:** Configuration and environment decisions belong at the boundaries of your system (entry points, config files, dependency injection). Business logic should be pure and environment-agnostic.

---

## 11. Do Not Reinvent the Wheel

Before writing a utility function, check if the standard library or an existing dependency already provides it. Before writing a SQL helper, check if the database has a built-in function.

```python
# Bad — hand-rolled date logic
def is_weekday(d):
    return d.weekday() < 5

def add_business_days(start, days):
    current = start
    added = 0
    while added < days:
        current += timedelta(days=1)
        if is_weekday(current):
            added += 1
    return current

# Good — use numpy (already a dependency)
import numpy as np
result = np.busday_offset(start_date, days)
```

**The rule:** Use existing tools. Check the standard library first, then existing project utilities, then third-party libraries. Only write custom code when nothing else fits.

---

## 12. Prevent Crashes, Do Not Hide Them

Never silence a problem to keep things running. A silent failure that corrupts data is infinitely worse than a loud crash that stops the pipeline.

```python
# Bad — silences the problem, data silently wrong
def calculate_vat(amount, country):
    try:
        rate = VAT_RATES[country]
    except KeyError:
        rate = 0.19  # "default" hides a real bug
    return amount * rate

# Good — fail loudly on unexpected input
VAT_RATES = {"DE": Decimal("0.19"), "AT": Decimal("0.20")}

def calculate_vat(amount: Decimal, country: str) -> Decimal:
    if country not in VAT_RATES:
        raise ValueError(
            f"Unsupported country for VAT calculation: {country!r}. "
            f"Expected one of: {', '.join(VAT_RATES)}"
        )
    return amount * VAT_RATES[country]
```

**The rule:** A pipeline that crashes on bad data is safer than a pipeline that silently produces wrong numbers. Wrong financial data sent to a funding partner is worse than no data sent at all.

---

## Summary: The Kernel Philosophy

These principles reduce to a few core truths:

1. **Simplicity is a feature.** Complex code is buggy code. The simplest solution that works is the best solution.
2. **Readability is non-negotiable.** Code is read 10x more than it is written. Optimize for the reader.
3. **Every line should earn its place.** No dead code, no speculative abstractions, no premature optimization.
4. **Fail loudly and clearly.** Silent failures corrupt data. Loud failures get fixed.
5. **Naming is the most important design decision.** Good names eliminate the need for comments, documentation, and tribal knowledge.
6. **Flat is better than nested.** If your code arrow-points to the right, refactor.

---

## References

- [Linux kernel coding style](https://www.kernel.org/doc/Documentation/process/coding-style.rst)
- *The C Programming Language*, Kernighan & Ritchie
- *The Practice of Programming*, Kernighan & Pike
- PEP 20 — The Zen of Python
