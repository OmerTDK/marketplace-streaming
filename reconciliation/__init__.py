"""marketplace-streaming reconciliation package.

The batch-vs-stream reconciliation: an independent pandas recompute of the
fulfillment-SLA metric, diffed against the streaming MV result, with a Dagster
asset-check that fails on divergence beyond tolerance.

Public surface:
  Pure logic (no containers, fast-lane testable):
    batch_recompute_fulfillment_sla  — recompute SLA metrics from raw events
    reconcile                        — diff streaming vs batch per window
    is_clean / max_abs_delta         — asset-check predicates
    ReconciliationRow                — one reconciled window
    floor_to_window                  — 5-minute tumble assignment

  IO adapters (plain functions over live connections):
    sync_streaming_sla_to_clickhouse, batch/audit writers, RisingWave readers

  Dagster (imported lazily — only when dagster is installed):
    reconciliation.assets.defs       — Definitions with the 3 assets + check
"""

from reconciliation.logic import (
    DEFAULT_TOLERANCE,
    RECONCILED_METRIC,
    STATUS_CONVERGED,
    STATUS_DIVERGED,
    STATUS_WITHIN_TOLERANCE,
    ReconciliationRow,
    batch_recompute_fulfillment_sla,
    diverged_keys,
    floor_to_window,
    is_clean,
    max_abs_delta,
    reconcile,
)

__all__ = [
    "DEFAULT_TOLERANCE",
    "RECONCILED_METRIC",
    "STATUS_CONVERGED",
    "STATUS_DIVERGED",
    "STATUS_WITHIN_TOLERANCE",
    "ReconciliationRow",
    "batch_recompute_fulfillment_sla",
    "diverged_keys",
    "floor_to_window",
    "is_clean",
    "max_abs_delta",
    "reconcile",
]
