# Future-TH-Database

This repository updates the Google Sheets market database used by System F1-TH.

## Automated update

The `Update F1-TH market data` workflow:

- runs at 11:30 Asia/Bangkok, Monday through Saturday;
- can also be started manually with `workflow_dispatch`;
- reads the current contracts from the `holding_information` worksheet;
- scrapes TFEX historical trading data;
- refreshes current-contract rows and appends every missing date;
- preserves continuous-series back-adjustment when a contract rolls;
- reads each worksheet back after writing and fails if verification does not match.

The workflow is defined in `.github/workflows/main.yml` and runs
`manual-system-f1-th-ver2.py`.

## Validation

`.github/workflows/validate-sheets.yml` compares the latest stored date across
all active F1-TH tickers and reports mismatches.

## Contract rolls

Update `current_symbol` in the `holding_information` worksheet when a
contract changes. The updater detects that the stored symbol differs, obtains
the overlapping old-contract settlement, applies the back-adjustment, and adds
the new contract rows.
