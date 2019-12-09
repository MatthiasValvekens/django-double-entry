import datetime
import re
from dataclasses import dataclass
from typing import Optional

from django.utils import timezone
from django.utils.translation import ugettext_lazy as _
from djmoney.money import Money

from double_entry.utils import (
    _dt_fallback, parse_amount, NegativeAmountError,
    OGM_REGEX, parse_ogm, ogm_from_prefix, CIDictReader,
)
from double_entry import models

class FinancialCSVParser:
    delimiter = ','
    amount_column_name = 'bedrag'
    date_column_name= 'datum'
    dt_fallback_with_max = True

    @dataclass
    class TransactionInfo:
        line_no: int
        amount: Money
        timestamp: datetime.datetime
        account_lookup_str: str

        @property
        def ledger_entry(self) -> models.DoubleBookModel:
            ledger_entry: Optional[models.DoubleBookModel]
            try:
                return self._ledger_entry
            except AttributeError:
                raise ValueError('Ledger entry not initialised yet')

        @ledger_entry.setter
        def ledger_entry(self, ledger_entry: models.DoubleBookModel):
            self._ledger_entry = ledger_entry

    def __init__(self, csv_file):
        self.csv_file = csv_file
        self._file_read = False
        self._errors = []
        self._objects = []

    def error(self, line_no, msg):
        self._errors.insert(0, (line_no, msg))

    def parse_row(self, line_no, row):
        kwargs = self.parse_row_to_dict(line_no, row)
        if kwargs is None:
            return None
        return self.__class__.TransactionInfo(**kwargs)

    def parse_row_to_dict(self, line_no, row):
        amount = self._parse_amount(
            line_no, row[self.amount_column_name]
        )
        date_str = row.get(self.date_column_name, None)
        if not date_str:
            timestamp = timezone.now()
        else:
            timestamp = self._parse_date(line_no, date_str)

        if timestamp is None or amount is None:
            return None

        timestamp = _dt_fallback(timestamp, self.dt_fallback_with_max)
        return {
            'amount': amount,
            'timestamp': timestamp,
            'line_no': line_no
        }

    @property
    def errors(self):
        if not self._file_read:
            self._read()
        return self._errors

    @property
    def parsed_data(self):
        if not self._file_read:
            self._read()
        return self._objects

    def _read(self):
        if self.csv_file is None:
            self._file_read = True
            return

        csv = CIDictReader(self.csv_file, delimiter=self.delimiter)

        def gen():
            for line_no, row in enumerate(csv):
                # +1 to offset zero-indexing, and +1 to skip the header
                t = self.parse_row(line_no + 2, row)
                if t is not None:
                    yield t

        try:
            self._objects = list(gen())
        except KeyError as e:
            from django.utils.translation import ugettext as _
            self.error(
                0, _('Missing column: %(col)s. No data processed.') % {
                    'col': e.args[0]
                }
            )
        self._file_read = True

    def _parse_amount(self, line_no, amount_str):
        try:
            return parse_amount(amount_str)
        except NegativeAmountError:
            # don't bother flagging this, just silently ignore
            # (negative amounts can occur in payment imports, but those
            # transactions simply aren't relevant to us)
            return None
        except ValueError:
            self.error(
                line_no,
                _('Invalid amount %(amt)s') % {
                    'amt': amount_str
                },
            )
            return None

    def _parse_date(self, line_no, date_str):
        try:
            return datetime.datetime.strptime(
                date_str, '%d/%m/%Y'
            ).date()
        except ValueError:
            self.error(
                line_no,
                _('Invalid date %(date)s, please use dd/mm/YYYY.') % {
                    'date': date_str
                },
            )
            return None


class AccountColumnTransactionParser(FinancialCSVParser):
    account_column_name = 'account'

    def parse_row_to_dict(self, line_no, row):
        parsed = super().parse_row_to_dict(line_no, row)
        if parsed is None:
            return None
        parsed['account_lookup_str'] = row[self.account_column_name]
        return parsed

class BankCSVParser(FinancialCSVParser):

    verbose_name = None

    def get_ogm(self, line_no, row):
        raise NotImplementedError

    def parse_row_to_dict(self, line_no, row):
        parsed = super().parse_row_to_dict(line_no, row)
        if parsed is None:
            return None
        ogm = self.get_ogm(line_no, row)
        if ogm is None:
            return None
        parsed['account_lookup_str'] = ogm
        return parsed


FORTIS_FIND_OGM = r'MEDEDELING\s*:\s+' + OGM_REGEX
FORTIS_SEARCH_PATTERN = re.compile(FORTIS_FIND_OGM)


class FortisCSVParser(BankCSVParser):
    delimiter = ';'

    # TODO: force all relevant columns to be present here
    amount_column_name = 'Bedrag'
    date_column_name = 'Uitvoeringsdatum'
    verbose_name = _('Fortis .csv parser')

    def get_ogm(self, line_no, row):
        m = FORTIS_SEARCH_PATTERN.search(row['Details'])
        if m is None:
            return None
        ogm_str = m.group(0)
        try:
            prefix, modulus = parse_ogm(ogm_str, match=m)
        except (ValueError, TypeError):
            self.error(
                line_no,
                _('Illegal OGM string %(ogm)s.') % {
                    'ogm': ogm_str
                }
            )
            return None

        ogm_canonical = ogm_from_prefix(prefix)
        return ogm_canonical


class KBCCSVParser(BankCSVParser):
    # The inconsistent capitalisation in column names
    # is *not* a typo on my part.
    # (although it shouldn't be necessary any longer given the fact that
    # csv headers are now parsed case-insensitively)
    delimiter = ';'
    verbose_name = _('KBC .csv parser')

    # we're using this for incoming transactions, so this is fine
    amount_column_name = 'credit'
    date_column_name = 'Datum'

    def get_ogm(self, line_no, row):
        ogm_str = row['gestructureerde mededeling'].strip()
        if not ogm_str:
            # this is a fallback option, so we don't require this column
            # to be present
            ogm_str = row.get('Vrije mededeling', '').strip()
            heuristic_ogm = True
        else:
            heuristic_ogm = False
        try:
            prefix, modulus = parse_ogm(ogm_str)
        except (ValueError, TypeError):
            # not much point in generating an error if the candidate OGM was
            # nicked from an unstructured field
            if not heuristic_ogm:
                self.error(
                    line_no,
                    _('Illegal OGM string %(ogm)s.') % {
                        'ogm': ogm_str
                    }
                )
            return None

        ogm_canonical = ogm_from_prefix(prefix)
        return ogm_canonical


BANK_TRANSFER_PARSER_REGISTRY = [FortisCSVParser, KBCCSVParser]