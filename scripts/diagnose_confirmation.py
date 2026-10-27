"""Read-only diagnosis; prints checks, never financial values or identifiers."""
from datetime import date
from decimal import Decimal
import os
from pathlib import Path

from apply_execution_confirmation import date_row, previous_date_row, decimal_or_none
from check_gsheets_dates import auth_client, AUTOMATED_F1_URL, parse_sheet_date
from execution_confirmation_intake import gmail_service, parse_single_confirmation
from execution_reconciliation import EQUITY_OFFSET, TOLERANCE, INSTRUMENTS
from trading_calendar import expected_trading_date


def number(value):
    return decimal_or_none(value) or Decimal(0)


def main():
    target = expected_trading_date()
    parsed = parse_single_confirmation(gmail_service(Path(os.environ['GMAIL_OAUTH_TOKEN_FILE'])), target, os.environ['F1_TH_PDF_PASSWORD'])
    if not parsed:
        print('DIAG broker_pdf=NOT_FOUND')
        return
    _, confirmation = parsed
    print('DIAG broker_pdf=PARSED statement_date=' + target.isoformat())
    workbook = auth_client().open_by_url(AUTOMATED_F1_URL)
    equity, execution, readme = [workbook.worksheet(x) for x in ('Equity', 'Execution', 'README')]
    dates = equity.col_values(1)
    row, previous_row = date_row(dates, target), previous_date_row(dates, target)
    exrow = date_row(execution.col_values(1), target)
    previous = number(equity.acell(f'N{previous_row}').value)
    prior_stored_broker_end = number(equity.acell(f'O{previous_row}').value)
    print('DIAG prior_calculated_matches_stored_broker=' + str(abs(previous-prior_stored_broker_end-EQUITY_OFFSET) <= TOLERANCE))
    print('DIAG broker_begin_matches_prior_stored_broker=' + str(abs(confirmation.begin_equity-prior_stored_broker_end) <= TOLERANCE))
    begin_ok = abs(previous - confirmation.begin_equity - EQUITY_OFFSET) <= TOLERANCE
    print('DIAG begin_equity=' + ('PASS' if begin_ok else 'FAIL'))
    print('DIAG previous_equity_date=' + str(parse_sheet_date(dates[previous_row-1])))
    print('DIAG adjustment_date_matches=' + str(parse_sheet_date(readme.acell('P18').value) == target))
    inputs = readme.get('O24:U29')
    trades = {t.instrument: t for t in confirmation.trades}
    adjustment = Decimal(0)
    position_changes = {}
    for instrument, values in zip(INSTRUMENTS, inputs):
        values = list(values) + [''] * 7
        multiplier, old_settlement = number(values[1]), number(values[2])
        trade = trades.get(instrument)
        change = Decimal(trade.quantity) if trade else Decimal(0)
        position_changes[instrument] = change
        print('DIAG ' + instrument + '_quantity_matches_position_change=' + str(change == (trade.quantity if trade else 0)))
        if change:
            if trade is None:
                print('DIAG projected_equity=UNAVAILABLE missing_instrument_confirmation')
                return
            adjustment -= (trade.price - old_settlement) * change * multiplier
    rates = execution.get(f'P{exrow}:U{exrow}')[0]
    fee = sum((abs(trades[i].quantity) * number(rate) if i in trades else Decimal(0)) for i, rate in zip(INSTRUMENTS, rates))
    fee += number(execution.acell(f'AG{exrow}').value)
    pnl = number(equity.acell(f'I{row}').value)
    per_contract_pnl = equity.get(f'B{row}:G{row}')[0]
    pnl += sum(number(per_contract_pnl[index]) * trade.quantity for index, instrument in enumerate(INSTRUMENTS) if (trade := trades.get(instrument)) is not None)
    projected = previous + pnl - fee + adjustment
    print('DIAG projected_end_equity=' + ('PASS' if abs(projected-confirmation.end_equity-EQUITY_OFFSET) <= TOLERANCE else 'FAIL'))
    projected_from_broker = prior_stored_broker_end + EQUITY_OFFSET + pnl - fee + adjustment
    print('DIAG projected_end_using_prior_broker=' + ('PASS' if abs(projected_from_broker-confirmation.end_equity-EQUITY_OFFSET) <= TOLERANCE else 'FAIL'))
    print('DIAG projected_end_using_prior_broker_within_rounding=' + str(abs(projected_from_broker-confirmation.end_equity-EQUITY_OFFSET) <= Decimal('0.10')))
    print('DIAG production_writes=NONE')


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        print('DIAG ERROR_TYPE=' + type(exc).__name__)
        raise SystemExit(1)
