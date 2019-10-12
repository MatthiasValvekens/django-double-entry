from django.utils.text import slugify
from django.utils.translation import ugettext_lazy as _
from double_entry.forms.csv import AccountColumnTransactionParser

from .utils import (
    PAYMENT_NATURE_CASH, PAYMENT_NATURE_TRANSFER,
)

__all__ = [
    'MemberTransactionParser', 'MiscDebtPaymentCSVParser', 'DebtCSVParser'
]


class MemberTransactionParser(AccountColumnTransactionParser):
    account_column_name = 'lid'


class DebtCSVParser(MemberTransactionParser):
    delimiter = ';'

    comment_column_name = 'mededeling'
    gnucash_column_name = 'gnucash'
    activity_column_name = 'activiteit'

    class TransactionInfo(MemberTransactionParser.TransactionInfo):
        dt_fallback_with_max = False

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
        column_required_msg = _('You must supply a value for \'%(colname)s\'.')

        parsed['comment'] = comment = row[self.comment_column_name]
        if not comment:
            self.error(
                line_no, column_required_msg % {
                    'colname': self.comment_column_name,
                }
            )
            return None

        parsed['gnucash'] = gnucash = row[self.gnucash_column_name]
        if not gnucash:
            self.error(
                line_no, column_required_msg % {
                    'colname': self.gnucash_column_name,
                }
            )
            return None

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


class MiscDebtPaymentCSVParser(MemberTransactionParser):
    delimiter = ';'
    nature_column_name = 'aard'
    filter_column_name = 'filter'
    filters_present = False

    class TransactionInfo(MemberTransactionParser.TransactionInfo):
        def __init__(self, *, nature, debt_filter=None, **kwargs):
            super().__init__(**kwargs)
            self.nature = int(nature)
            self.debt_filter = debt_filter

    def get_nature(self, line_no, row):
        nature = row.get(self.nature_column_name, PAYMENT_NATURE_CASH)
        if nature in ('bank', 'overschrijving'):
            nature = PAYMENT_NATURE_TRANSFER
        elif nature in ('cash', 'contant', ''):
            nature = PAYMENT_NATURE_CASH
        else:
            self.error(
                line_no, _(
                    'Unknown payment nature %(nature)s, assuming cash.'
                ) % {'nature': str(nature)}
            )
            nature = PAYMENT_NATURE_CASH
        return nature

    # required columns: lid, bedrag
    # optional columns: datum, aard, filter
    # filter column requires a value if supplied!
    def parse_row_to_dict(self, line_no, row):
        parsed = super().parse_row_to_dict(line_no, row)
        if parsed is None:
            return None
        nature = self.get_nature(line_no, row)
        if nature is None:
            return None
        parsed['nature'] = nature
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


