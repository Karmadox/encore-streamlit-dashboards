"""Guarded historical market-performance calculations for the NDX dashboard."""

from __future__ import annotations

import math
from typing import Any


_PRICE_OBSERVATIONS_CTE = """
WITH price_pairs AS (
    SELECT
        p.trade_date,
        p.instrument_id,
        i.ticker,
        p.close_price,
        LAG(p.close_price) OVER (
            PARTITION BY p.instrument_id
            ORDER BY p.trade_date
        ) AS prior_close_price
    FROM encoredb.equity_daily_prices p
    JOIN encoredb.instruments i
      ON i.instrument_id = p.instrument_id
    WHERE p.trade_date >= DATE '2026-01-30'
),
weighted_observations AS (
    SELECT
        pp.trade_date,
        pp.instrument_id,
        pp.ticker,
        pp.close_price,
        pp.prior_close_price,
        w.ndx_weight_pct,
        CASE
            WHEN pp.close_price IS NULL THEN 'MISSING_CLOSE'
            WHEN pp.close_price::text IN ('NaN', 'Infinity', '-Infinity')
                THEN 'NON_FINITE_CLOSE'
            WHEN pp.close_price <= 0 THEN 'NON_POSITIVE_CLOSE'
            WHEN pp.prior_close_price::text IN ('NaN', 'Infinity', '-Infinity')
                THEN 'NON_FINITE_PRIOR_CLOSE'
            WHEN pp.prior_close_price <= 0 THEN 'NON_POSITIVE_PRIOR_CLOSE'
            WHEN w.ndx_weight_pct IS NULL THEN 'MISSING_WEIGHT'
            WHEN w.ndx_weight_pct::text IN ('NaN', 'Infinity', '-Infinity')
                THEN 'NON_FINITE_WEIGHT'
            ELSE NULL
        END AS issue_reason,
        CASE
            WHEN pp.close_price IS NOT NULL
             AND pp.close_price::text NOT IN ('NaN', 'Infinity', '-Infinity')
             AND pp.close_price > 0
             AND pp.prior_close_price IS NOT NULL
             AND pp.prior_close_price::text NOT IN ('NaN', 'Infinity', '-Infinity')
             AND pp.prior_close_price > 0
             AND w.ndx_weight_pct IS NOT NULL
             AND w.ndx_weight_pct::text NOT IN ('NaN', 'Infinity', '-Infinity')
            THEN LN(pp.close_price / pp.prior_close_price)
            ELSE NULL
        END AS log_return
    FROM price_pairs pp
    JOIN encoredb.ndx_weights_snapshot w
      ON w.ticker = pp.ticker
     AND w.snapshot_date = pp.trade_date
)
"""


def chain_linked_return_sql() -> str:
    """Return SQL that excludes invalid weighted observations deterministically."""

    return _PRICE_OBSERVATIONS_CTE + """
, daily_index_return AS (
    SELECT
        trade_date,
        SUM((ndx_weight_pct / 100.0) * log_return) AS index_log_return
    FROM weighted_observations
    WHERE issue_reason IS NULL
      AND log_return IS NOT NULL
    GROUP BY trade_date
)
SELECT SUM(index_log_return) AS total_log_return
FROM daily_index_return
"""


def historical_attribution_sql() -> str:
    """Return guarded SQL for semiconductor/non-semiconductor attribution."""

    return _PRICE_OBSERVATIONS_CTE + """
, classified_returns AS (
    SELECT
        CASE
            WHEN m.cohort_name = 'Semiconductors' THEN 'Semiconductors'
            ELSE 'Non-Semiconductors'
        END AS grp,
        (o.ndx_weight_pct / 100.0) * o.log_return AS weighted_log_return
    FROM weighted_observations o
    JOIN (
        SELECT DISTINCT ticker, cohort_name
        FROM encoredb.v_index_canonical_market_state_enriched
    ) m
      ON m.ticker = o.ticker
    WHERE o.issue_reason IS NULL
      AND o.log_return IS NOT NULL
)
SELECT grp, SUM(weighted_log_return) AS total_log_return
FROM classified_returns
GROUP BY grp
"""


def historical_quality_issues_sql() -> str:
    """Return bounded diagnostics for invalid observations that have index weight."""

    return _PRICE_OBSERVATIONS_CTE + """
SELECT
    trade_date,
    instrument_id,
    ticker,
    close_price,
    prior_close_price,
    ndx_weight_pct,
    issue_reason
FROM weighted_observations
WHERE issue_reason IS NOT NULL
ORDER BY trade_date DESC, ticker
LIMIT 100
"""


def compounded_return_percent(total_log_return: Any) -> float | None:
    """Convert a finite log return to percent, otherwise report unavailable."""

    try:
        value = float(total_log_return)
    except (TypeError, ValueError, OverflowError):
        return None

    if not math.isfinite(value):
        return None

    try:
        result = math.expm1(value) * 100.0
    except OverflowError:
        return None

    return result if math.isfinite(result) else None


def format_percent(value: Any, decimals: int = 1) -> str:
    """Format finite percentages without ever rendering ``nan%`` or infinity."""

    try:
        numeric = float(value)
    except (TypeError, ValueError, OverflowError):
        return "Unavailable"

    if not math.isfinite(numeric):
        return "Unavailable"

    return f"{numeric:.{decimals}f}%"
