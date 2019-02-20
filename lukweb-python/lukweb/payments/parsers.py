import datetime
import re
from _pydecimal import Decimal, DecimalException
from csv import DictReader

from django.conf import settings
from django.utils import timezone
from django.utils.text import slugify
from django.utils.translation import ugettext_lazy as _
from djmoney.money import Money


from .utils import (
    PAYMENT_NATURE_CASH, PAYMENT_NATURE_TRANSFER, OGM_REGEX,
    parse_ogm, ogm_from_prefix
)
from ..utils import _dt_fallback

__all__ = [
    'FinancialCSVParser', 'PaymentCSVParser', 'KBCCSVParser', 'FortisCSVParser',
    'MemberTransactionParser', 'MiscDebtPaymentCSVParser', 'DebtCSVParser',
    'BANK_TRANSFER_PARSER_REGISTRY'
]

class FinancialCSVParser:
    delimiter = ','
    amount_column_name = 'bedrag'
    date_column_name= 'datum'

    class TransactionInfo:
        def __init__(self, *, line_no, amount, timestamp):
            self.ledger_entry = None
            self.line_no = line_no
            self.amount = amount
            self.timestamp = _dt_fallback(timestamp)

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
        return self.__class__.TransactionInfo(
            **kwargs
        )

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

        csv = DictReader(self.csv_file, delimiter=self.delimiter)

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

        # ugly, but Decimal doesn't really support formatting parameters
        # (unless we involve the locale module)
        amt_str = amount_str.replace(',', '.')
        # even though currency may be available in the input row
        # we still force EUR, since the data model can't handle
        # anything else
        currency = settings.BOOKKEEPING_CURRENCY

        try:
            rd = Decimal(amt_str).quantize(Decimal('.01'))
            if rd <= 0:
                self.error(line_no, _('Amount must be greater than zero.'))
                return None
            return Money(rd, currency)
        except (ValueError, IndexError, DecimalException):
            self.error(
                line_no,
                _('Invalid amount %(amt)s') % {
                    'amt': amt_str
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


class MemberTransactionParser(FinancialCSVParser):
    member_column_name = 'lid'

    class TransactionInfo(FinancialCSVParser.TransactionInfo):
        def __init__(self, *, member_str, **kwargs):
            super().__init__(**kwargs)
            self.member_str = member_str

    def parse_row_to_dict(self, line_no, row):
        parsed = super().parse_row_to_dict(line_no, row)
        if parsed is None:
            return None
        parsed['member_str'] = row[self.member_column_name]
        return parsed


class PaymentCSVParser(FinancialCSVParser):

    class TransactionInfo(FinancialCSVParser.TransactionInfo):
        def __init__(self, *, nature, **kwargs):
            super().__init__(**kwargs)
            self.nature = int(nature)

    def get_nature(self, line_no, row):
        raise NotImplementedError

    def parse_row_to_dict(self, line_no, row):
        parsed = super().parse_row_to_dict(line_no, row)
        if parsed is None:
            return None
        nature = self.get_nature(line_no, row)
        if nature is None:
            return None
        parsed['nature'] = nature
        return parsed


class DebtCSVParser(MemberTransactionParser):
    delimiter = ';'

    comment_column_name = 'mededeling'
    gnucash_column_name = 'gnucash'
    activity_column_name = 'activiteit'

    class TransactionInfo(MemberTransactionParser.TransactionInfo):
        def __init__(self, *, comment, gnucash, filter_slug, activity_id=None,
                     **kwargs):
            super().__init__(**kwargs)
            self.comment = comment
            self.gnucash = gnucash
            self.filter_slug = filter_slug
            self.activity_id = activity_id

    def parse_row_to_dict(self, line_no, row):
        parsed = super().parse_row_to_dict(line_no, row)
        if parsed is None:
            return None
        parsed['comment'] = row[self.comment_column_name]
        parsed['gnucash'] = row[self.gnucash_column_name]
        # coerce falsy values
        parsed['filter_slug'] = slugify(row.get('filter', '')) or None
        activity_id = row.get(self.activity_column_name, None)
        if activity_id:
            try:
                parsed['activity_id'] = int(activity_id)
            except ValueError:
                self.error(
                    line_no, _(
                        '\'%(colname)s\' value should be an integer, not '
                        '%(val)s.'
                    ) % {
                        'colname': self.activity_column_name,
                        'val': activity_id
                    }
                )
        return parsed


class MiscDebtPaymentCSVParser(PaymentCSVParser, MemberTransactionParser):
    delimiter = ';'
    nature_column_name = 'aard'
    filter_column_name = 'filter'
    filters_present = False

    class TransactionInfo(PaymentCSVParser.TransactionInfo,
                          MemberTransactionParser.TransactionInfo):
        def __init__(self, *, debt_filter=None, **kwargs):
            super().__init__(**kwargs)
            self.debt_filter = debt_filter

    def get_nature(self, line_no, row):
        nature = row.get(self.nature_column_name, PAYMENT_NATURE_CASH)
        if nature in ('bank', 'overschrijving'):
            nature = PAYMENT_NATURE_TRANSFER
        else:
            nature = PAYMENT_NATURE_CASH
        return nature

    # required columns: lid, bedrag
    # optional columns: datum, aard, filter
    # filter column requires a value if supplied!
    def parse_row_to_dict(self, line_no, row):
        parsed = super().parse_row_to_dict(line_no, row)
        if parsed is None:
            return None
        try:
            debt_filter = slugify(row[self.filter_column_name])
            if not debt_filter:
                self.error(
                    line_no, _(
                        'You must supply a filter value for all payments in '
                        '\'Misc. internal debt payments\', or omit the '
                        '\'%(colname)s\' column entirely. '
                        'Skipped processing.'
                    ) % {'colname': self.filter_column_name}
                )
                return None
            else:
                parsed['debt_filter'] = debt_filter
                self.filters_present = True
        except KeyError:
            # proceed as normal
            pass

        return parsed


class BankCSVParser(PaymentCSVParser):

    verbose_name = None

    class TransactionInfo(PaymentCSVParser.TransactionInfo):
        def __init__(self, *, ogm, **kwargs):
            super().__init__(**kwargs)
            self.ogm = ogm

    def get_nature(self, line_no, row):
        return PAYMENT_NATURE_TRANSFER

    def get_ogm(self, line_no, row):
        raise NotImplementedError

    def parse_row_to_dict(self, line_no, row):
        parsed = super().parse_row_to_dict(line_no, row)
        if parsed is None:
            return None
        ogm = self.get_ogm(line_no, row)
        if ogm is None:
            return None
        parsed['ogm'] = ogm
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
    delimiter = ';'
    verbose_name = _('KBC .csv parser')

    # we're using this for incoming transactions, so this is fine
    amount_column_name = 'credit'
    date_column_name = 'Datum'

    def get_ogm(self, line_no, row):
        ogm_str = row['gestructureerde mededeling'].strip()
        if not ogm_str:
            # Always assume that there will be simpletons who don't know the
            # this is a fallback option, so we don't require this column
            # to be present
            ogm_str = row.get('Vrije mededeling', '').strip()
        try:
            prefix, modulus = parse_ogm(ogm_str)
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


BANK_TRANSFER_PARSER_REGISTRY = [FortisCSVParser, KBCCSVParser]