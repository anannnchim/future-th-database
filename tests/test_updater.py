"""Offline regression tests for the F1-TH updater's safety boundaries."""

import importlib.util
from pathlib import Path
import unittest

import pandas as pd


MODULE_PATH = Path(__file__).parents[1] / "manual-system-f1-th-ver2.py"
SPEC = importlib.util.spec_from_file_location("f1_updater", MODULE_PATH)
updater = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(updater)


def series(rows):
    return pd.DataFrame(rows, columns=updater.DATA_COLUMNS)


class FakeWorksheet:
    title = "EUR"

    def __init__(self, frame):
        self.frame = frame.copy()

    def get_all_records(self):
        output = self.frame.copy()
        output["date"] = output["date"].dt.strftime("%Y-%m-%d")
        return output.to_dict("records")


class FakeDataSheet:
    def __init__(self, worksheet):
        self._worksheet = worksheet

    def worksheet(self, ticker):
        self.ticker = ticker
        return self._worksheet


class UpdaterTests(unittest.TestCase):
    def test_prep_df_drops_unusable_rows_and_sorts_dates(self):
        raw = pd.DataFrame([
            ["02 Jan 2026", "1,200", "-", "-", "-", "1,201", "", "", "10", "20", "EURU26"],
            ["01 Jan 2026", "1,100", "-", "-", "-", "-", "", "", "10", "20", "EURU26"],
        ], columns=updater.TFEX_COLUMNS + ["Symbol"])
        prepared = updater.prep_df(raw)
        self.assertEqual(prepared["date"].dt.strftime("%Y-%m-%d").tolist(), ["2026-01-02"])
        self.assertEqual(prepared["sp"].tolist(), [1201])

    def test_validate_series_rejects_duplicate_dates(self):
        frame = series([
            [pd.Timestamp("2026-01-01"), 1, 1, 1, 1, 1, 1, 1, "EURU26", 1],
            [pd.Timestamp("2026-01-01"), 2, 2, 2, 2, 2, 2, 2, "EURU26", 2],
        ])
        with self.assertRaisesRegex(ValueError, "duplicate"):
            updater.validate_series(frame, "EUR")

    def test_dry_run_never_calls_write(self):
        previous = series([
            [pd.Timestamp("2026-01-01"), 1, 1, 1, 1, 100, 1, 1, "EURU26", 100],
            [pd.Timestamp("2026-01-02"), 1, 1, 1, 1, 101, 1, 1, "EURU26", 101],
        ])
        scraped = previous.drop(columns=["adj_price"]).copy()
        scraped.loc[len(scraped)] = [pd.Timestamp("2026-01-03"), 1, 1, 1, 1, 102, 1, 1, "EURU26"]
        worksheet = FakeWorksheet(previous)
        original_scrape, original_write = updater.scrape_prepared, updater.write_and_verify
        updater.scrape_prepared = lambda symbol: scraped
        updater.write_and_verify = lambda *args: self.fail("dry run attempted a write")
        try:
            updater.update_symbol(FakeDataSheet(worksheet), "EURU26", dry_run=True)
        finally:
            updater.scrape_prepared, updater.write_and_verify = original_scrape, original_write
        self.assertEqual(len(worksheet.frame), 2)


if __name__ == "__main__":
    unittest.main()
