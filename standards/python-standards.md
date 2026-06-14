# Python Standards

Python coding standards for all Python code in this portfolio.

---

## Core Principles

`import this` (PEP 20) is the implicit baseline. Python code MUST be:

1. **Self-documenting** — clear naming eliminates the need for comments
2. **Explicit over implicit** — no magic, no hidden behavior
3. **Simple over clever** — readable code beats "smart" code
4. **Testable** — if it can't be tested, it can't be trusted
5. **Consistent** — follow established patterns across the codebase
6. **Fail loud** — errors never pass silently (PEP 20); never mask missing data with field-to-field fallbacks — surface it as an explicit failure instead

---

## Code Style

### Linting and Formatting: ruff

We use **ruff** for both linting and formatting, configured in `pyproject.toml`. All code must pass `ruff check` and `ruff format --check` before merging. Ruff's `I` rules replace isort, so import sorting is enforced by the same tool.

```bash
# Lint
ruff check .

# Format
ruff format .

# CI verification (no changes, just check)
ruff check . && ruff format --check .
```

#### ruff Configuration

```toml
# pyproject.toml

[tool.ruff]
line-length = 100
target-version = "py312"

[tool.ruff.lint]
select = ["E", "F", "I", "N", "UP", "B", "SIM", "ARG", "PTH", "RUF"]

[tool.pytest.ini_options]
testpaths = ["tests"]
```

What the selected rule families enforce:

| Family | Enforces |
| --- | --- |
| `E` / `F` | pycodestyle errors and pyflakes (syntax, unused imports/variables) |
| `I` | import sorting (isort-compatible) |
| `N` | PEP 8 naming conventions |
| `UP` | modern Python syntax (pyupgrade) |
| `B` | likely bugs and design problems (flake8-bugbear) |
| `SIM` | simplifiable code (flake8-simplify) |
| `ARG` | unused function arguments |
| `PTH` | `pathlib` over `os.path` |
| `RUF` | ruff-specific correctness rules |

### Type Checking

```bash
pip install mypy
mypy scripts/
```

---

## Naming Conventions

| Type | Convention | Example |
| --- | --- | --- |
| Variables | snake_case | `loan_count`, `payment_amount` |
| Functions | snake_case | `calculate_principal()`, `load_loans()` |
| Classes | PascalCase | `LoanTapeSyncer`, `BigQueryLoader` |
| Constants | UPPER_SNAKE_CASE | `MAX_RETRIES`, `DEFAULT_TIMEOUT` |
| Private | Leading underscore | `_internal_helper()`, `_cached_value` |
| Modules | snake_case | `date_utils.py`, `bigquery_helpers.py` |

### Naming Rules

1. **Descriptive names always** — `calculate_principal_outstanding()` not `calc_po()`
2. **Boolean variables as questions** — `is_active`, `has_payment_plan`, `should_retry`
3. **Functions describe actions** — `get_`, `calculate_`, `validate_`, `load_`, `extract_`
4. **No abbreviations** — `borrower` not `brwr`, `payment` not `pmt`
5. **Avoid generic names** — Never use `data`, `info`, `temp`, `result` alone

---

## Code Organization

### File Structure

```python
"""Module description - one line explaining purpose."""

# Standard library imports
import os
from datetime import date, datetime
from typing import Optional

# Third-party imports
import pandas as pd
from google.cloud import bigquery

# Local imports
from scripts.utils.date_utils import is_business_day
from scripts.utils.validation import validate_loan

# Constants
MAX_RETRIES = 3
DEFAULT_TIMEOUT = 30

# Classes and functions below...
```

### Function Structure

```python
def calculate_principal_outstanding(
    principal_amount: float,
    payments_made: int,
    monthly_principal: float,
) -> float:
    """Calculate remaining principal after payments.

    Args:
        principal_amount: Original loan principal.
        payments_made: Number of completed payments.
        monthly_principal: Principal portion per payment.

    Returns:
        Outstanding principal amount.

    Raises:
        ValueError: If payments_made is negative.
    """
    if payments_made < 0:
        raise ValueError("payments_made cannot be negative")

    return principal_amount - (payments_made * monthly_principal)
```

---

## Type Hints

All functions **must** have type hints.

```python
# Good
from typing import Any

def get_loan(loan_id: str) -> dict[str, Any]:
    ...

def load_loans(
    status: str | None = None,
    country: str = "DE",
) -> list[dict]:
    ...

# Also good - using typing module for complex types
from typing import Optional

def process_payments(
    payments: list[dict],
    filter_status: Optional[str] = None,
) -> tuple[list[dict], int]:
    ...
```

---

## Error Handling

### Fail Fast, Fail Loud

```python
# Good - explicit error with context
def load_loan(loan_id: str) -> dict:
    if not loan_id:
        raise ValueError("loan_id cannot be empty")

    loan = fetch_from_api(loan_id)

    if loan is None:
        raise LookupError(f"Loan not found: {loan_id}")

    return loan
```

### Never Silence Errors

```python
# Bad - silently swallows errors
try:
    result = risky_operation()
except Exception:
    pass

# Good - handle specifically or re-raise
try:
    result = risky_operation()
except ConnectionError as e:
    logger.warning(f"Connection failed, retrying: {e}")
    result = retry_operation()
except ValueError as e:
    raise ValueError(f"Invalid input for operation: {e}") from e
```

---

## Constants and Magic Numbers

Same principle as SQL: **no magic numbers**.

```python
# Bad
if retry_count > 3:
    raise TimeoutError()

if amount > 50000:
    require_approval = True

# Good
MAX_RETRIES = 3
APPROVAL_THRESHOLD_EUR = 50000

if retry_count > MAX_RETRIES:
    raise TimeoutError(f"Failed after {MAX_RETRIES} attempts")

if amount > APPROVAL_THRESHOLD_EUR:
    require_approval = True
```

---

## Comments and Documentation

> **Minimize comments.** Self-documenting code is the goal. If you need a comment, the code probably needs refactoring.

### When to Comment

Comments explain **why**, not **what**. Use them sparingly.

```python
# Bad - explains what (obvious from code)
# Increment counter by 1
counter += 1

# Good - explains why (not obvious)
# ECB publishes rates at 16:00 CET, add buffer for delays
rate_fetch_hour = 17
```

### Docstrings

Use Google-style docstrings for all public functions and classes.

```python
def validate_iban(iban: str, country: str = "DE") -> bool:
    """Validate IBAN format and checksum.

    Validates the IBAN according to ISO 13616 standards. Supports
    German (DE) and Austrian (AT) formats.

    Args:
        iban: The IBAN string to validate (spaces allowed).
        country: Expected country code. Defaults to "DE".

    Returns:
        True if IBAN is valid, False otherwise.

    Raises:
        ValueError: If country code is not supported.

    Example:
        >>> validate_iban("DE89 3704 0044 0532 0130 00")
        True
    """
    ...
```

---

## Testing

We use **pytest** as our testing framework.

### Quick Reference

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=scripts --cov-report=term-missing

# Run specific test file
pytest tests/test_date_utils.py
```

### Test Structure

```python
# tests/test_validation.py

import pytest
from scripts.utils.validation import calculate_principal_outstanding

def test_calculate_principal_outstanding_with_payments():
    # Arrange
    principal = 25000.00
    payments = 5
    monthly = 150.00

    # Act
    result = calculate_principal_outstanding(principal, payments, monthly)

    # Assert
    assert result == 24250.00

def test_raises_on_negative_payments():
    with pytest.raises(ValueError, match="cannot be negative"):
        calculate_principal_outstanding(25000, -1, 150)
```

---

## Project Structure

```
lending-pipeline/
├── scripts/
│   ├── __init__.py
│   ├── loan_tape_sync.py
│   ├── ecb_exchange_rates.py
│   └── utils/
│       ├── __init__.py
│       ├── date_utils.py
│       ├── bigquery_helpers.py
│       └── validation.py
├── tests/
│   ├── conftest.py
│   ├── test_loan_tape_sync.py
│   ├── test_date_utils.py
│   └── fixtures/
│       └── sample_loan.json
├── pyproject.toml
└── README.md
```

All tool configuration (ruff, pytest) lives in `pyproject.toml` — no scattered `.cfg` / `.ini` config files.

---

## Quick Checklist

- [ ] Type hints on all functions
- [ ] Descriptive names (no abbreviations)
- [ ] No magic numbers (use constants)
- [ ] `ruff check` and `ruff format --check` pass
- [ ] Tests written (80%+ coverage)
- [ ] Docstrings on public functions
- [ ] Imports sorted (standard → third-party → local; enforced by ruff `I` rules)
- [ ] Error handling is explicit (no silent failures, no field-to-field fallbacks — surface missing data as an explicit failure)

---

## Related Standards

- `clean-sql.md` — SQL coding standards
- `engineering-principles.md` — Foundational engineering principles
- `git-workflow.md` — Branching and PR process
