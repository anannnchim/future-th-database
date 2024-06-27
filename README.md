# Future-TH-Database

This repository is responsible for automating the database updates for System F1 - TH. It provides both automated and manual scripts to handle database updates related to financial data processing.

## Repository Structure

Here are the key components and files in the repository:

- `.github/workflows/main.yml`: Contains the GitHub Actions workflow that automates the database update process.
- `automated-system-f1-th.py`: This script updates the database automatically every day at 05:00 AM.
- `manual-system-f1-th.py`: Script to manually update the database on contract roll dates.
- `manual-system-f1-th-ver2.py`: Alternative version of the manual update script for contract roll dates.
- `test_automation/`: Directory containing scripts and logs related to automated test processes.
- `notebook/start_log1`: Notebook used for generating initial logs and test data.

## Automated Workflow

The automated workflow is managed by GitHub Actions as defined in `.github/workflows/main.yml`. This workflow handles:
- Daily updates at 05:00 AM.
- Dependency installations.
- Execution of `automated-system-f1-th.py`.

## Manual Update Scripts

For manual updates on contract roll dates, use:
- `manual-system-f1-th.py`: For the standard manual update process.
- `manual-system-f1-th-ver2.py`: For an alternative approach to the manual update process.

## Setup and Execution

To set up and run the scripts locally, you need Python 3.x and the necessary libraries installed. Install dependencies via:

```bash
pip install -r requirements.txt
