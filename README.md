# Future-TH-Database

This repository updates the Google Sheets market database used by System F1-TH.

## Automated update

The `Update F1-TH market data` workflow:

- runs at 03:07 Asia/Bangkok, Monday through Saturday, with a 07:30 fallback;
- can also be started manually with `workflow_dispatch`;
- reads the current contracts from the `holding_information` worksheet;
- scrapes TFEX historical trading data;
- refreshes current-contract rows and appends every missing date;
- preserves continuous-series back-adjustment when a contract rolls;
- reads each worksheet back after writing and fails if verification does not match.

The workflow is defined in `.github/workflows/update-market-data.yml` and runs
`manual-system-f1-th-ver2.py`.

## Safe local checks

Install the pinned production dependencies and run the offline regression suite:

```bash
python -m pip install -r requirements.txt
python -m unittest discover -s tests -v
```

To validate the live inputs and TFEX data without changing any Google Sheet,
set `GOOGLE_APPLICATION_CREDENTIALS` to the service-account JSON file and run:

```bash
python manual-system-f1-th-ver2.py --dry-run
```

The dry run performs the same data-quality and contract-roll checks as a
production run, but never calls the worksheet write operation.

## Change policy

Do not make direct edits to the continuous-series worksheets. Change active
contracts only through `market-input`, review updater changes through a pull
request, and use the dry run before merging changes that affect data logic.

## Validation

`.github/workflows/validate-sheets.yml` compares the latest stored date across
all active F1-TH tickers and reports mismatches.

## Contract rolls

Update `current_symbol` in the `holding_information` worksheet when a
contract changes. The updater detects that the stored symbol differs, obtains
the overlapping old-contract settlement, applies the back-adjustment, and adds
the new contract rows.
