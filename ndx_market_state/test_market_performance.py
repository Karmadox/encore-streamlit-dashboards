import math
import unittest

from market_performance import (
    chain_linked_return_sql,
    compounded_return_percent,
    format_percent,
    historical_attribution_sql,
    historical_quality_issues_sql,
)


class MarketPerformanceTests(unittest.TestCase):
    def test_formats_finite_percentage(self):
        self.assertEqual(format_percent(23.84), "23.8%")

    def test_never_formats_non_finite_percentage(self):
        for value in (None, float("nan"), float("inf"), float("-inf"), "bad"):
            with self.subTest(value=value):
                self.assertEqual(format_percent(value), "Unavailable")

    def test_compounds_finite_log_return(self):
        value = compounded_return_percent(math.log(1.238))
        self.assertIsNotNone(value)
        self.assertAlmostEqual(value, 23.8, places=10)

    def test_rejects_non_finite_log_return(self):
        for value in (None, float("nan"), float("inf"), float("-inf"), "bad"):
            with self.subTest(value=value):
                self.assertIsNone(compounded_return_percent(value))

    def test_all_historical_queries_guard_non_finite_inputs(self):
        for query in (
            chain_linked_return_sql(),
            historical_attribution_sql(),
            historical_quality_issues_sql(),
        ):
            with self.subTest(query=query[-80:]):
                self.assertIn("'NaN', 'Infinity', '-Infinity'", query)
                self.assertIn("NON_FINITE_CLOSE", query)
                self.assertIn("NON_FINITE_PRIOR_CLOSE", query)
                self.assertIn("MISSING_WEIGHT", query)
                self.assertIn("NON_FINITE_WEIGHT", query)

    def test_calculation_queries_only_use_valid_observations(self):
        self.assertIn("issue_reason IS NULL", chain_linked_return_sql())
        self.assertIn("issue_reason IS NULL", historical_attribution_sql())

    def test_quality_query_is_bounded(self):
        self.assertIn("LIMIT 100", historical_quality_issues_sql())


if __name__ == "__main__":
    unittest.main()
