# Future-TH-Database

This repository updates the Google Sheets market database used by System F1-TH.

## Production workflow

`Update F1-TH market data` runs twice Monday-Saturday:

- primary: 03:07 Asia/Bangkok;
- fallback: 07:30 Asia/Bangkok.

It can also be run manually. The fallback is safe because the update is
idempotent.

A successful workflow means the complete chain is current:

1. determine the latest completed TFEX trading date;
2. scrape and validate all active contracts before writing;
3. update and read back all market-data tabs;
4. wait for Automated F1 to recalculate;
5. verify every Automated F1 instrument tab and `B-Monitoring`.

The fallback run opens or updates a GitHub issue if the chain remains stale.
A later successful run closes the issue automatically.

## Trading calendar

Thailand public holidays are supplied by the Python `holidays` package.
Add exchange-only closure dates to `config/tfex_holidays.txt`, one ISO date per
line, and review the file against the official TFEX/SET calendar annually.

## Manual validation

Run the `Validate F1-TH freshness` workflow to check both workbooks without
updating data. Its report is available in the job summary and as an artifact.

## Contract rolls

Update `current_symbol` in the `holding_information` worksheet when a contract
changes. The updater detects the new symbol, uses the overlapping old-contract
settlement for the back-adjustment, and appends the new contract rows.
