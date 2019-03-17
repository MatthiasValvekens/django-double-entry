import logging
from decimal import Decimal
from collections import defaultdict, deque
from typing import (
    TypeVar, Sequence, Generator, Type, Tuple,
    Iterator, Optional, Iterable,
    cast,
    List,
)

from django import forms
from django.conf import settings
from django.core.exceptions import SuspiciousOperation
from django.db import transaction as db_transaction
from django.utils import timezone
from django.utils.functional import cached_property
from django.utils.translation import (
    ugettext_lazy as _,
    ugettext,
)
from djmoney.money import Money

from ...models.accounting.base import (
    TransactionPartyMixin, BaseDebtPaymentSplit
)
from ... import models
from ...models.accounting import base as accounting_base
from ...payments import decimal_to_money
from ..utils import ParserErrorMixin, CSVUploadForm

logger = logging.getLogger(__name__)

"""
Utilities for processing & displaying accounting data
originating from .csv files
"""
# TODO (wbc): user-configurable column names, or at least
# translatable ones
# TODO: unified method to mark required/optional columns


class LedgerEntryPreparator(ParserErrorMixin):
    model: accounting_base.DoubleBookModel = None
    formset_class = None
    formset_prefix = None
    _valid_transactions = None
    _formset = None

    def __init__(self, parser):
        super().__init__(parser)
        if parser is not None:
            self.transactions = parser.parsed_data
        else:
            self.transactions = []

    def error_at_line(self, line_no, msg, params=None):
        self.error_at_lines([line_no], msg, params)

    def error_at_lines(self, line_nos, msg, params):
        fmtd_msg = msg % params
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
                kwargs = self.model_kwargs_for_transaction(t)
                if kwargs is not None:
                    t.ledger_entry = self.model(**kwargs)
                    return True
                else:
                    return False
            indiv_transactions = [
                t for t in self.transactions if valid(t)
            ]
            self._valid_transactions = self.validate_global(indiv_transactions)
            self.review()

        return self._valid_transactions

    def form_kwargs_for_transaction(self, transaction):
        if self._valid_transactions is None:
            raise ValueError(
                'Ledger entries are not ready.'
            )
        return {
            'total_amount': transaction.amount,
            'timestamp': transaction.timestamp
        }

    def formset_kwargs(self):
        return {}

    def run(self):
        initial_data = [
            self.form_kwargs_for_transaction(t)
            for t in self.valid_transactions
        ]
        fs = self.formset_class(
            queryset=self.model._default_manager.none(),
            initial=initial_data,
            prefix=self.formset_prefix,
            **self.formset_kwargs()
        )
        fs.extra = len(self.valid_transactions)
        self._formset = fs

    @property
    def formset(self):
        if self._formset is None:
            self.run()
        return self._formset


class TransactionPartyIndexBuilder:
    def __init__(self, ledger_preparator):
        self.transaction_index = defaultdict(list)
        self.ledger_preparator: FetchTransactionAccountsMixin = ledger_preparator

    @classmethod
    def lookup_key_for_account(cls, account):
        raise NotImplementedError

    def append(self, tinfo):
        raise NotImplementedError

    def execute_query(self) -> Iterable[TransactionPartyMixin]:
        raise NotImplementedError

    def populate_indexes(self):
        for p in self.execute_query():
            self.ledger_preparator._by_id[p.pk] = p
            self.ledger_preparator._by_lookup_str[
                self.__class__.lookup_key_for_account(p)
            ] = p


class FetchTransactionAccountsMixin(LedgerEntryPreparator):
    transaction_party_model: Type[TransactionPartyMixin]

    unknown_account_message = _(
        'Transaction account %(account)s unknown. '
        'Skipped processing.'
    )

    ambiguous_account_message = _(
        'Designation %(account)s could refer to multiple accounts. '
        'Skipped processing.'
    )

    unparseable_account_message = _(
        'Designation %(account)s could not be parsed. '
        'Skipped processing.'
    )

    _by_id = None
    _by_lookup_str = None

    def get_lookup_builders(self):
        raise NotImplementedError

    def get_account(self, *, pk=None, lookup_str=None):
        if pk is not None:
            return self._by_id[pk]
        elif lookup_str is not None:
            return self._by_lookup_str[lookup_str]
        raise ValueError('You must supply either pk or lookup_str')

    def account_ids(self):
        return self._by_id.keys()

    def unknown_account(self, account_lookup_str, line_nos):
        self.error_at_lines(
            line_nos, self.unknown_account_message,
            params={'account': account_lookup_str}
        )

    def ambiguous_account(self, account_lookup_str, line_nos):
        self.error_at_lines(
            line_nos, self.ambiguous_account_message,
            params={'account': account_lookup_str}
        )

    def unparseable_account(self, account_lookup_str, line_no):
        if self.unparseable_account_message is not None:
            self.error_at_line(
                line_no, self.unparseable_account_message,
                params={'account': account_lookup_str}
            )

    def prepare(self):
        super().prepare()
        self._by_id = dict()
        self._by_lookup_str = dict()
        lookup_builders = self.get_lookup_builders()
        for info in self.transactions:
            appended = False
            for builder in lookup_builders:
                appended |= builder.append(info)
                if appended:
                    break
            if not appended:
                self.unparseable_account(
                    info.account_lookup_str, info.line_no
                )

        for builder in lookup_builders:
            builder.populate_indexes()

        # It's technically more efficient to keep the transaction dicts around
        # to refer to later, but since later calls to validate_global might
        # shrink the list of valid transactions, this is a bad idea for
        # maintainability. Amdahl.

class DuplicationProtectedPreparator(LedgerEntryPreparator):
    single_dup_message = None
    multiple_dup_message = None

    def validate_global(self, valid_transactions):
        valid_transactions = super().validate_global(valid_transactions)
        # early out, nothing to do
        if not valid_transactions:
            return []
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
            for dup_sig, transactions in import_buckets.items():
                occ_in_import = len(transactions)
                occ_in_hist = historical_buckets[dup_sig]
                dupcount = min(occ_in_hist, occ_in_import)
                if occ_in_hist:
                    # signal duplicate with an error message
                    params = self.dup_error_params(dup_sig)
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
            'date': signature_used.date,
            'amount': Money(
                signature_used.amount, settings.BOOKKEEPING_CURRENCY
            ),
        }


class ApportionmentResult:
    def __init__(self, *, fully_used_payments=None, fully_paid_debts=None,
                 remaining_payments=None, remaining_debts=None):
        self.fully_used_payments = fully_used_payments or []
        self.fully_paid_debts = fully_paid_debts or []
        self.remaining_debts = remaining_debts or []
        self.remaining_payments = remaining_payments or []

    def __iadd__(self, other):
        if not isinstance(other, ApportionmentResult):
            raise TypeError
        self.fully_used_payments += other.fully_used_payments
        self.fully_paid_debts += other.fully_paid_debts
        self.remaining_debts += other.remaining_debts
        self.remaining_payments += other.remaining_payments
        return self


ST = TypeVar('ST', bound=BaseDebtPaymentSplit)
def make_payment_splits(payments: Sequence[accounting_base.BasePaymentRecord],
                        debts: Sequence[accounting_base.BaseDebtRecord],
                        split_model: Type[ST],
                        prioritise_exact_amount_match=True,
                        exact_amount_match_only=False,
                        payment_fk_name: str=None, debt_fk_name: str=None) \
        -> Generator[ST, None, ApportionmentResult]:
    """
    This method assumes that there are no preexistent splits between
    the payments and debts involved in the computation.
    Ensure that the payments and debts are appropriately annotated for
    optimal results.
    """

    # use double-ledger introspection to figure out the right foreign
    # key names
    if payment_fk_name is None:
        payment_fk_name = split_model.get_payment_column()
    if debt_fk_name is None:
        debt_fk_name = split_model.get_debt_column()

    results = ApportionmentResult()

    # There might be a more efficient way, but let's not optimise prematurely
    if prioritise_exact_amount_match or exact_amount_match_only:
        payment_list = list(payments)
        debt_list = deque(debts)
        payments_todo = []
        for payment in payment_list:
            try:
                amt = payment.credit_remaining
                # attempt to find a debt matching the exact payment amount
                index, exact_match = next(
                    (ix, d) for ix, d in enumerate(debt_list)
                    if d.balance == amt and d.timestamp <= payment.timestamp
                    and not d.is_refund
                )
                # remove debt from the candidate list
                # this triggers another O(n) read, which feels like
                # it should be unnecessary, but it doesn't really matter
                del debt_list[index]

                # yield payment split covering this transaction
                yield split_model(**{
                    payment_fk_name: payment, debt_fk_name: exact_match,
                    'amount': amt
                })
                results.fully_used_payments.append(payment)
                results.fully_paid_debts.append(exact_match)
            except StopIteration:
                # no exact match, so defer handling
                payments_todo.append(payment)

        debts_iter = iter(debt_list)
        payments_iter = iter(payments_todo)
    else:
        payments_iter = iter(payments)
        debts_iter = iter(debts)

    if exact_amount_match_only:
        results.remaining_debts.extend(debts_iter)
        results.remaining_payments.extend(payments_iter)
        return results

    # The generic method is simple: use payments to pay off debts
    # until we either run out of debts, or of money to pay 'em
    payment = debt = None
    # credit remaining on the current payment
    # and portion of the current debt that still needs to be paid
    # (during this payment run)

    # The only subtlety is in enforcing the invariant
    # that debts cannot be retroactively paid off by past payments.
    # By ordering the payments and debts from old to new, we can easily
    # ensure that this happens.
    credit_remaining = debt_remaining = decimal_to_money(Decimal('0.00'))
    while True:
        try:
            # look for some unpaid debt
            while not debt_remaining or debt.is_refund:
                if debt is not None:  # initial step guard
                    if not debt_remaining:
                        # report debt as fully paid
                        results.fully_paid_debts.append(debt)
                    else:
                        # this shouldn't happen, but let's
                        # cover our collective asses
                        results.remaining_debts.append(debt)
                debt = next(debts_iter)
                debt_remaining = debt.balance
        except StopIteration:
            # all debts fully paid back, bail
            if credit_remaining:
                payment.spoof_matched_balance(
                    payment.total_amount.amount - credit_remaining.amount
                )
                results.remaining_payments.append(payment)
            for p in payments_iter:
                if p.credit_remaining:
                    results.remaining_payments.append(p)
                else:
                    results.fully_used_payments.append(p)
            break

        try:
            # keep trying payments until we find one that is recent enough
            # to cover the current debt.
            while not credit_remaining or payment.timestamp < debt.timestamp:
                if payment is not None:  # initial step guard
                    # report on payment status
                    if credit_remaining:
                        results.remaining_payments.append(payment)
                    else:
                        results.fully_used_payments.append(payment)
                payment = next(payments_iter)
                credit_remaining = payment.credit_remaining

        except StopIteration:
            # no money left to pay stuff, bail
            if debt_remaining:
                debt.spoof_matched_balance(
                    debt.total_amount.amount - debt_remaining.amount
                )
                results.remaining_debts.append(debt)
            for d in debts_iter:
                if d.balance:
                    results.remaining_debts.append(d)
                else:
                    results.fully_paid_debts.append(d)
            break
        # pay off as much of the current debt as we can
        # with the current balance
        amt = min(debt_remaining, credit_remaining)
        credit_remaining -= amt
        debt_remaining -= amt
        yield split_model(**{
            payment_fk_name: payment, debt_fk_name: debt, 'amount': amt
        })

    return results


def refund_overpayment(
        payments: Sequence[accounting_base.BasePaymentRecord],
        debt_kwargs: dict=None
    ) -> Optional[
        Tuple[
            accounting_base.BaseDebtRecord,
            Iterator[BaseDebtPaymentSplit]
        ]
    ]:

    payments = list(payments)
    if not payments:
        return
    p = payments[0]
    payment_model = p.__class__
    split_model, payment_fk_name = payment_model.get_split_model()
    debt_model = p.__class__.get_other_half_model()
    debt_fk_name = split_model.get_debt_column()

    credit_to_refund = sum(
        (payment.credit_remaining for payment in payments),
        decimal_to_money(Decimal('0.00'))
    )

    if not credit_to_refund:
        return

    now = timezone.now()
    kwargs = {'timestamp': now, 'processed': now}
    if debt_kwargs is not None:
        kwargs.update(debt_kwargs)
    kwargs['is_refund'] = True
    refund_object = debt_model(**kwargs)
    refund_object.clean()
    # total_amount may not be an actual field
    refund_object.total_amount = credit_to_refund

    # this generator should be triggered by the caller when
    # the refund_object and all payments have been saved.
    def splits_to_create():
        for payment in payments:
            if payment.credit_remaining:
                yield split_model(**{
                    payment_fk_name: payment, debt_fk_name: refund_object,
                    'amount': payment.credit_remaining
                })

    return refund_object, splits_to_create()



class CreditApportionmentMixin(LedgerEntryPreparator):
    overpayment_fmt_string = _(
        'Received %(total_credit)s, but only %(total_used)s '
        'can be applied to outstanding debts. '
        'Payment(s) dated %(payment_dates)s have outstanding '
        'balances.'
    )

    # optional, can be derived through reflection
    payment_fk_name = None
    debt_fk_name = None
    _trans_buckets = None

    @property
    def split_model(self) -> Type[ST]:
        return cast(Type[ST], self.model.get_split_model()[0])

    def debts_for(self, debt_key):
        raise NotImplementedError

    def transaction_buckets(self):
        raise NotImplementedError

    def overpayment_error_params(self, debt_key, total_used,
                                 total_credit, remaining_payments):
        return {
            'total_used': total_used,
            'total_credit': total_credit,
            'remaining_payments': remaining_payments,
            'payment_dates': ', '.join(
                p.timestamp.strftime('%Y-%m-%d') for p in remaining_payments
            )
        }

    @cached_property
    def refund_message(self):
        financial_globals = models.FinancialGlobals.load()
        autogenerate_refunds = financial_globals.autogenerate_refunds
        if autogenerate_refunds:
            return ugettext(
                'Refunds will be automatically created to compensate '
                'for the difference in funds. '
                'Please process these at your earliest convenience.'
            )
        else:
            return ugettext(
                'Please resolve this issue manually.'
            )

    def simulate_apportionments(self, debt_key, debts, transactions):
        payments = sorted([
                t.ledger_entry for t in transactions
            ],
            key=lambda p: p.timestamp
        )

        results: ApportionmentResult
        def _split_gen_wrapper():
            nonlocal results
            results = yield from make_payment_splits(
                payments, debts, self.split_model,
                payment_fk_name=self.payment_fk_name,
                debt_fk_name=self.debt_fk_name
            )

        total_used = sum(
            (s.amount for s in _split_gen_wrapper()),
            Money(0, settings.BOOKKEEPING_CURRENCY)
        )

        total_credit = sum(
            (p.total_amount for p in payments),
            Money(0, settings.BOOKKEEPING_CURRENCY)
        )

        if total_used < total_credit:
            self.error_at_lines(
                [t.line_no for t in transactions],
                self.overpayment_fmt_string,
                params=self.overpayment_error_params(
                    debt_key, total_used, total_credit,
                    results.remaining_payments
                )
            )

    def review(self):
        super().review()
        # compute the total credit used vs the total
        # credit established, and notify the treasurer
        self._trans_buckets = self.transaction_buckets()
        for key, transactions in self._trans_buckets.items():
            debts = self.debts_for(key)
            self.simulate_apportionments(key, debts, transactions)


class StandardCreditApportionmentMixin(CreditApportionmentMixin,
                                       FetchTransactionAccountsMixin):

    @property
    def model(self):
        return self.transaction_party_model.get_payment_model()

    def get_lookup_builders(self):
        raise NotImplementedError

    def transaction_buckets(self):
        trans_buckets = defaultdict(list)
        tpm = self.transaction_party_model
        payment_fk_name = tpm.get_payment_remote_fk_column()
        debt_fk_name = tpm.get_debt_remote_fk_column()
        for t in self.valid_transactions:
            account_id = getattr(t.ledger_entry, payment_fk_name)
            trans_buckets[account_id].append(t)
        base_qs = tpm.get_debt_model()._default_manager

        debt_qs = base_qs.filter(**{
            '%s__in' % debt_fk_name: self.account_ids()
        }).with_payments().unpaid().order_by('timestamp')

        debt_buckets = defaultdict(list)
        for debt in debt_qs:
            debt_buckets[getattr(debt, debt_fk_name)].append(debt)

        self._debt_buckets = debt_buckets

        return trans_buckets

    def debts_for(self, debt_key):
        return self._debt_buckets[debt_key]

    def model_kwargs_for_transaction(self, transaction):
        kwargs = super().model_kwargs_for_transaction(transaction)
        if kwargs is None:
            return None
        try:
            account = self._by_lookup_str[transaction.account_lookup_str]
            acct_field = self.transaction_party_model.get_payment_remote_fk()
            kwargs[acct_field] = account
            return kwargs
        except KeyError:
            # member search errors have already been logged
            # in the preparation step, so we don't care
            return None


class FinancialCSVUploadForm(CSVUploadForm):
    ledger_preparator_classes = ()
    upload_field_label = None
    csv_parser_class = None

    csv = forms.FileField()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields['csv'].label = self.upload_field_label
    
    @cached_property
    def formset_preparators(self) -> List[LedgerEntryPreparator]:
        data = self.cleaned_data['csv']
        return [prep(data) for prep in self.ledger_preparator_classes]

    # is the most common use case
    @property
    def formset_preparator(self):
        assert len(self.ledger_preparator_classes) == 1
        return self.formset_preparators[0]
 
    def render_confirmation_page(self, request, context=None):
        raise NotImplementedError

    @classmethod
    def submit_confirmation(cls, post_data):
        # call save() on the right formsets after user confirmation
        formsets = [
            prep.formset_class(post_data, prefix=prep.formset_prefix)
            for prep in cls.ledger_preparator_classes 
        ]

        dirty = False
        for formset in formsets:
            # this means someone tampered with the POST data,
            # so we have no obligation to give a nice response
            if not formset.is_valid():
                logger.error(formset.errors)
                dirty = True

        if dirty:
            raise SuspiciousOperation()

        with db_transaction.atomic():
            for formset in formsets:
                formset.save()


class BaseCreditApportionmentFormset(forms.BaseModelFormSet):
    transaction_party_model: Type[TransactionPartyMixin] = None

    def prepare_payment_instances(self) -> Tuple[
        Iterable[int], Iterable[accounting_base.BasePaymentRecord]
    ]:
        raise NotImplementedError

    def generate_splits(self, account) -> Generator[
        BaseDebtPaymentSplit, None, ApportionmentResult
    ]:
        raise NotImplementedError

    def post_debt_update(self, fully_paid_debts, remaining_debts):
        pass

    def save(self, commit=True):
        from django.db import connection
        can_bulk_save = connection.features.can_return_ids_from_bulk_insert
        global_results = ApportionmentResult()
        account_pks, all_payments = self.prepare_payment_instances()

        account_qs = self.transaction_party_model.objects.filter(
            pk__in=account_pks
        ).with_debt_annotations()
        financial_globals = models.FinancialGlobals.load()
        refund_category = financial_globals.refund_credit_gnucash_acct
        autogenerate_refunds = (
            refund_category is not None
            and financial_globals.autogenerate_refunds
        )
        debt_account_field = self.transaction_party_model.get_debt_remote_fk()
        payment_model = self.transaction_party_model.get_payment_model()
        debt_model = self.transaction_party_model.get_debt_model()

        # save payments before building splits, otherwise the ORM
        # will not set fk's correctly
        if commit:
            if can_bulk_save:
                payment_model.objects.bulk_create(all_payments)
            else:
                logger.debug(
                    'Database does not support RETURNING on bulk inserts. '
                    'Fall back to saving in a loop.'
                )
                for payment in all_payments:
                    payment.save()

        def splits_to_create() -> Iterator[BaseDebtPaymentSplit]:
            nonlocal global_results
            refunds_to_save = []
            refund_splits_to_save = []

            for account in account_qs:
                results = yield from self.generate_splits(account)
                global_results += results

                if autogenerate_refunds:
                    debt_kwargs = {
                        'gnucash_category': refund_category,
                        debt_account_field: account
                    }
                    refund_data = refund_overpayment(
                        results.remaining_payments,
                        debt_kwargs=debt_kwargs
                    )
                    # we cannot yield from the refund splits here
                    # since the refund object hasn't been saved yet
                    if refund_data is not None:
                        refund_object, refund_splits = refund_data
                        if can_bulk_save:
                            refunds_to_save.append(refund_object)
                            refund_splits_to_save.append(refund_splits)
                        else:
                            if commit:
                                refund_object.save()
                            yield from refund_splits
            if can_bulk_save:
                # save all refund objects and create/yield all refund splits
                if commit:
                    debt_model.objects.bulk_create(refunds_to_save)
                for splits in refund_splits_to_save:
                    yield from splits

        if commit:
            self.transaction_party_model.get_split_model().objects.bulk_create(
                splits_to_create()
            )

        # allow subclasses to hook into the ApportionmentResults
        self.post_debt_update(
            fully_paid_debts=global_results.fully_paid_debts,
            remaining_debts=global_results.remaining_debts
        )

        return all_payments