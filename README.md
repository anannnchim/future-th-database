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

The same check is available in GitHub Actions: select **Run workflow**, set
**Dry run** to true, and review the job log. This uses the existing repository
credential without needing to place a service-account file on a local machine.

## Change policy

Do not make direct edits to the continuous-series worksheets. Change active
contracts only through `market-input`, review updater changes through a pull
request, and use the dry run before merging changes that affect data logic.

## Validation

`.github/workflows/validate-sheets.yml` compares the latest stored date across
all active F1-TH tickers and reports mismatches.

## Execution-confirmation staging

`scripts/execution_reconciliation.py` stages a broker Daily Derivatives
Confirmation Note for review. It is deliberately read-only: it never writes to
the `Execution` or `Equity` worksheets.

Use a locally extracted text file while validating a new broker PDF layout:

```bash
python scripts/execution_reconciliation.py \
  --text-file confirmation.txt \
  --trade-date 2026-10-07 \
  --equity-previous 3346451.954 \
  --equity-current 3365712.387
```

For an encrypted PDF, set `F1_TH_PDF_PASSWORD` outside the repository and pass
`--pdf path/to/confirmation.pdf`. Install the requirements with the `crypto`
extra (already included in `requirements.txt`) so AES-encrypted broker PDFs can
be read. The report masks financial values unless
`--include-values` is explicitly requested. The parser rejects duplicate daily
instrument prices, unknown instruments, malformed statements, and failed
equity checks for manual review.

## Gmail confirmation intake

`scripts/execution_confirmation_intake.py` reads checked rows in the
`order-confirmation` tab of Automated System F1. It only searches and parses
the broker PDF, then updates that control row to `STAGED`,
`MISSING_CONFIRMATION`, or `REVIEW_REQUIRED`. It never writes `Execution` or
`Equity`.

The **Stage F1-TH execution confirmations** workflow runs around 06:15 and
07:15 Asia/Bangkok, with a manual option for a specific trade date. GitHub
cron timing is best-effort, so use manual dispatch when timing is critical.
Before enabling it, add these repository secrets:

- `GMAIL_OAUTH_TOKEN`: OAuth authorized-user JSON with Gmail read-only access
  and a refresh token;
- `F1_TH_PDF_PASSWORD`: the broker PDF password.

It reuses the existing `GOOGLE_APPLICATION_CREDENTIALS` and SMTP secrets.

## Contract rolls

Update `current_symbol` in the `holding_information` worksheet when a
contract changes. The updater detects that the stored symbol differs, obtains
the overlapping old-contract settlement, applies the back-adjustment, and adds
the new contract rows.
