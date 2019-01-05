import datetime
from csv import DictReader
from decimal import Decimal, DecimalException
from collections import defaultdict

from django.conf import settings
from django.utils import timezone
from django.utils.functional import cached_property
from django.utils.translation import (
    ugettext_lazy as _,
)
from djmoney.money import Money

from ...utils import _dt_fallback
from ..utils import ParserErrorMixin

"""
Utilities for processing & displaying accounting data
originating from .csv files
"""
# TODO (wbc): user-configurable column names, or at least
# translatable ones
# TODO: unified method to mark required/optional columns

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
        nature = self.get_nature(line_no, row)
        if nature is None:
            return None
        parsed['nature'] = nature
        return parsed


class LedgerEntryPreparator(ParserErrorMixin):
    model = None 
    formset_class = None
    formset_prefix = None
    _valid_transactions = None

    def __init__(self, parser):
        super().__init__(parser)
        if parser is not None:
            self.transactions = parser.parsed_data
        else:
            self.transactions = []

    def error_at_line(self, line_no, msg, params=None):
        self.error_at_lines([line_no], msg, params)

    def error_at_lines(self, line_nos, msg, params=None):
        fmtd_msg = msg % (params or {})
        self._errors.insert(0, (sorted(line_nos), fmtd_msg))

    # The prepare/review methods are called before
    # and after transaction validation, respectively.
    # The review method can access and use
    # self.valid_transactions.
    def prepare(self):
        return

    def review(self):
        return

    def model_kwargs_for_transaction(self, transaction):
        # validate and build model kwargs for transaction
        if transaction.amount.amount < 0:
            self.error_at_line(
                transaction.line_no,
                _('Payment amount %(amount)s is negative.'),
                params={'amount': transaction.amount}
            )
            return None

        return {
            'total_amount': transaction.amount,
            'timestamp': transaction.timestamp
        } 

    def validate_global(self, valid_transactions):
        # this method can assume that all transactions have the
        # ledger_entry property set to something meaningful
        return valid_transactions

    @property
    def valid_transactions(self): 
        if self._valid_transactions is None:
            self.prepare()
            def valid(t):
                kwargs = self.model_kwargs_for_transaction(transaction)
                if kwargs is not None:
                    t.ledger_entry = self.model(**kwargs)
                    return True
                else:
                    return False
            indiv_transactions = [
                t for transaction in self.transactions if valid(t)
            ]
            self._valid_transactions = validate_global(indiv_transactions)
            self.review()
        return self._valid_transactions

    def form_kwargs_for_transaction(self, transaction):
        return {
            'total_amount': transaction.amount,
            'timestamp': transaction.timestamp
        }

    @cached_property
    def formset(self):
        initial_data = [
            self.form_kwargs_for_transaction(t)
            for t in self.valid_transactions
        ]
        fs = self.formset_class(
            queryset=self.model._default_manager.none(),
            initial=initial_data,
            prefix=self.formset_prefix
        )
        fs.extra = len(initial_data)
        return fs


class FetchMembersMixin(LedgerEntryPreparator):

    # TODO: is this still necessary to keep around?
    transactions_by_member_id = None
    members_by_str = None

    def unknown_member(self, member_str, line_nos):
        msg = _(
            '%(member_str)s does not designate a registered member.'
        )

        self.error_at_lines(
            line_nos, msg, params={'member_str': member_str}
        )

    def prepare(self): 
        super().prepare()
        # split the transaction list into email and name indices
        email_index, name_index = defaultdict(list), defaultdict(list)
        for info in self.transactions:
            use_email = '@' in info.member_str
            targ = email_index if use_email else name_index
            key = info.member_str if use_email else info.member_str.casefold()
            targ[key].append(info)

        member_email_qs, unseen = models.ChoirMember.objects \
            .select_related('user').with_debt_annotations().by_emails(
                email_index.keys(), validate_unseen=True
            )

        for email in unseen:
            ts = email_index[email]
            self.unknown_member(email, [t.line_no for t in ts])

        # TODO Restrict to active members only, maybe?
        member_name_qs, unseen, duplicates = models.ChoirMember.objects \
            .select_related('user').with_debt_annotations().by_full_names(
                name_index.keys(), validate_unseen=True, validate_nodups=True
            )

        for name in unseen:
            ts = name_index[name.casefold()]
            self.unknown_member(name, [t.line_no for t in ts])

        for name in duplicates:
            ts = name_index[name.casefold()]
            msg = _(
                '%(member_str)s designates multiple registered members. '
                'Skipped processing.',
            )
            self.error_at_lines(
                [t.line_no for t in ts], msg, params={'member_str': name},
            )

        transactions_by_member_id = defaultdict(list)
        members_by_str = dict()

        for m in member_email_qs:
            member_str = m.user.email
            members_by_str[member_str] = m
            transactions_by_member_id[m.pk] = email_index[member_str]

        for m in member_name_qs:
            member_str = m.full_name
            imember_str = member_str.casefold()
            if imember_str not in duplicates:
                members_by_str[member_str] = m
                transactions_by_member_id[m.pk] = name_index[imember_str]

    def form_kwargs_for_transaction(self, transaction):
        kwargs = super().form_kwargs_for_transaction(transaction)
        member = transaction.ledger_entry.member
        kwargs['member_id'] = member.pk
        kwargs['name'] = member.full_name
        kwargs['email'] = member.user.email
        return kwargs
    
    def model_kwargs_for_transaction(self, transaction):
        kwargs = super().model_kwargs_for_transaction(transaction)
        try:
            member = self.members_by_str[transaction.member_str]
            kwargs['member'] = member
            return kwargs
        except KeyError: 
            # member search errors have already been logged
            # in the preparation step, so we don't care
            return kwargs


class DuplicationProtectedPreparator(LedgerEntryPreparator):
    single_dup_message = None
    multiple_dup_message = None

    def validate_global(self, valid_transactions):
        valid_transactions = super().validate_global(valid_transactions)
        dates = [
            timezone.localdate(t.timestamp) for t in valid_transactions
        ]

        historical_buckets = self.model._default_manager.dupcheck_buckets(
            date_bounds=(min(dates), max(dates))
        )

        import_buckets = defaultdict(list)
        for transaction in valid_transactions:
            sig = transaction.ledger_entry.dupcheck_signature
            import_buckets[sig].append(transaction)

        def strip_duplicates():
            for sig, transactions in import_buckets.items():
                occ_in_import = len(transactions)
                occ_in_hist = historical_buckets[sig]
                dupcount = min(occ_in_hist, occ_in_import)
                if occ_in_hist:
                    # signal duplicate with an error message
                    params = dup_error_params(sig)
                    params['hist'] = occ_in_hist
                    params['import'] = occ_in_import
                    params['dupcount'] = dupcount

                    # special case, this is the most likely one to occur
                    # so deserves special wording
                    if occ_in_hist == occ_in_import == 1:
                        msg_fmt_str = self.single_dup_message
                    else:
                        msg_fmt_str = self.multiple_dup_message
                    # report duplicate error
                    self.error_at_lines(
                        [t.line_no for t in transactions],
                        msg_fmt_str,
                        params=params
                    )
                # skip the first dupcount entries, we treat those as the 
                # duplicate ones. The others will be entered into the db
                # as usual
                yield from transactions[dupcount:]

        return list(strip_duplicates())
                

    def dup_error_params(self, signature_used):
        return {
            'date': signature_used[0],
            'amount': signature_used[1],
        }


class CreditApportionmentMixin(LedgerEntryPreparator):
    split_model = None
    overpayment_fmt_string = None

    # optional, can be derived through reflection
    payment_fk_name = None
    debt_fk_name = None
    _debt_buckets = None

    def debt_buckets(self):
        raise NotImplementedError

    def transactions_for(self, debt_key):
        raise NotImplementedError

    def make_splits(self, payments, debts):
        from ...payments import make_payment_splits
        return make_payment_splits(
            payments, debts, self.split_model, 
            payment_fk_name=self.payment_fk_name,
            debt_fk_name=self.debt_fk_name
        )

    def review(self):
        super().review()
        # compute the total credit used vs the total
        # credit established, and notify the treasurer
        self._debt_buckets = self.debt_buckets()
        for key, debts in self._debt_buckets.items():
            transactions = self.transactions_for(debt_key) 
            payments = [
                t.ledger_entry for t in transactions
            ]
            splits = self.make_splits(payments, debts)
            total_used = sum(
                (s.amount for s in splits),
                Money(0, settings.BOOKKEEPING_CURRENCY)
            )
            total_credit = sum(
                (p.total_amount for p in payments),
                Money(0, settings.BOOKKEEPING_CURRENCY)
            )
            if total_used < total_credit:
                self.error(
                    [t.line_no for t in transactions],
                    self.overpayment_fmt_string,
                    params={
                        'key': key,
                        'total_used': total_used,
                        'total_credit': total_credit
                    }
                )
                # TODO: figure out how to deal with refunds here
                # I'm convinced again that having a to_refund account in gnucash
                # is the best way to deal with this, but we need to
                # have a qif_excluded field on payments in case the treasurer
                # wants to deal with these things manually
