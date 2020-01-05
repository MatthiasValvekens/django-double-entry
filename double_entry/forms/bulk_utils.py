import abc
import dataclasses
import inspect
import logging
import datetime
from dataclasses import dataclass
from decimal import Decimal
from collections import defaultdict, deque
from enum import IntFlag
from itertools import chain
from typing import (
    TypeVar, Sequence, Generator, Type, Tuple,
    Iterator, Optional, Iterable, cast, List,
    Generic, ClassVar, Dict, Any
)

from django import forms
from django.conf import settings
from django.core.exceptions import SuspiciousOperation
from django.db import transaction as db_transaction
from django.db.models import ForeignKey
from django.utils import timezone
from django.utils.functional import cached_property
from django.utils.translation import (
    ugettext_lazy as _, ugettext,
)
from djmoney.money import Money

from double_entry.forms.csv import TransactionInfo, FinancialCSVParser
from double_entry.models import (
    TransactionPartyMixin, BaseDebtPaymentSplit
)
from double_entry import models as accounting_base, models
from double_entry.utils import decimal_to_money, consume_with_result
from double_entry.forms.utils import (
    CSVUploadForm, ErrorMixin,
    ParserErrorMixin, ErrorContextWrapper,
)

logger = logging.getLogger(__name__)

"""
Utilities for processing & displaying accounting data
originating from .csv files
"""
# TODO (wbc): user-configurable column names, or at least
# translatable ones
# TODO: unified method to mark required/optional columns


# an error prevents further processing, a warning doesn't
# TODO maybe support "overrulable" errors? Makes sense e.g. for duplicates
class ResolvedTransactionVerdict(IntFlag):
    COMMIT = 0
    SUGGEST_DISCARD = 1
    DISCARD = 3

class ResolvedTransactionMessageContext:
    """
    Verdicts saved here are merely intended for communication with the user,
    enforcement is up to the individual ledger preparators.
    """

    def __init__(self):
        self._verdict: ResolvedTransactionVerdict = \
            ResolvedTransactionVerdict.COMMIT
        self.transaction_errors = []
        self.transaction_warnings = []
        self.transaction_success = []

    def discard(self):
        self._verdict = ResolvedTransactionVerdict.DISCARD

    def suggest_skip(self):
        self._verdict |= ResolvedTransactionVerdict.SUGGEST_DISCARD

    @property
    def verdict(self) -> ResolvedTransactionVerdict:
        return self._verdict

    def error(self, msg: str, params: Optional[dict]=None):
        self.discard()
        self.transaction_errors.append(msg if params is None else msg % params)

    def warning(self, msg: str, params: Optional[dict]=None):
        self.transaction_warnings.append(msg if params is None else msg % params)

    def success(self, msg: str, params: Optional[dict]=None):
        self.transaction_success.append(msg if params is None else msg % params)

    @staticmethod
    def mass_suggest_skip(contexts):
        for c in contexts:
            c.suggest_skip()

    @staticmethod
    def mass_discard(contexts):
        for c in contexts:
            c.discard()

    @classmethod
    def broadcast_error(cls, contexts: List['ResolvedTransactionMessageContext'],
                        msg: str, params: Optional[dict]=None):
        for c in contexts:
            c.error(msg, params)

    @classmethod
    def broadcast_warning(cls, contexts: List['ResolvedTransactionMessageContext'],
                          msg: str, params: Optional[dict]):
        for c in contexts:
            c.warning(msg, params)


class TransactionWithMessages:
    message_context: ResolvedTransactionMessageContext
    do_not_skip: bool

    @property
    def to_commit(self):
        v = self.message_context.verdict
        if bool(self.do_not_skip):
            # ignore a a suggest_discard verdict
            v &= ~ResolvedTransactionVerdict.SUGGEST_DISCARD
        return bool(v)

    def discard(self):
        self.message_context.discard()

    def suggest_skip(self):
        self.message_context.suggest_skip()



def broadcast_error(transactions: List[TransactionWithMessages],
                        msg: str, params: Optional[dict]):
    if not transactions:
        raise ValueError('no transactions to which error applies')
    errs = list(tr.message_context for tr in transactions)
    err_cls = errs[0].__class__
    if any(err.__class__ != err_cls for err in errs):
        raise ValueError('Cannot combine message contexts of different types')
    err_cls.broadcast_error(errs, msg, params)

def broadcast_warning(transactions: List[TransactionWithMessages],
                    msg: str, params: Optional[dict]):
    if not transactions:
        raise ValueError('no transactions to which warning applies')
    errs = list(tr.message_context for tr in transactions)
    err_cls = errs[0].__class__
    if any(err.__class__ != err_cls for err in errs):
        raise ValueError('Cannot combine message contexts of different types')
    err_cls.broadcast_warning(errs, msg, params)


@dataclass(frozen=True)
class ResolvedTransaction(TransactionWithMessages):
    transaction_party_id: int
    amount: Money
    timestamp: datetime.datetime
    pipeline_section_id: int
    message_context: ResolvedTransactionMessageContext = dataclasses.field(compare=False, hash=False)
    do_not_skip: bool = dataclasses.field(compare=False, hash=False)

    def html_ignore(self):
        return 'message_context', 'do_no_skip'


TI = TypeVar('TI', bound=TransactionInfo)
RT = TypeVar('RT', bound=ResolvedTransaction)
TP = TypeVar('TP', bound=accounting_base.TransactionPartyMixin)
LE = TypeVar('LE', bound=accounting_base.DoubleBookModel)

class TransactionPartyIndexBuilder(Generic[TP]):

    def __init__(self, resolver: 'LedgerResolver'):
        self.resolver = resolver

    def lookup(self, account_lookup_str: str) -> Optional[TP]:
        raise NotImplementedError

    @classmethod
    def lookup_key_for_account(cls, account):
        raise NotImplementedError

    def append(self, tinfo):
        raise NotImplementedError

    def execute_query(self):
        raise NotImplementedError


class RTErrorContextFromMixin(ResolvedTransactionMessageContext):
    """
    Report errors with line numbers to a central error handler,
    and also keep track of error messages that are "local"
    to a resolved transaction.
    """

    def __init__(self, error_mixin, tinfo: TransactionInfo):
        super().__init__()
        self.error_mixin = error_mixin
        self.tinfo = tinfo

    @classmethod
    def broadcast_error(cls, contexts: List['RTErrorContextFromMixin'],
                        msg: str, params: Optional[dict]=None):
        line_nos = [c.tinfo.line_no for c in contexts]
        ctxt = contexts[0]
        ctxt.error_mixin.error_at_lines(line_nos, msg, params)
        for ctxt in contexts:
            ctxt.discard()
            ctxt.transaction_errors.append(
                msg if params is None else msg % params
            )


    def error(self, msg: str, params=Optional[dict]):
        self.error_mixin.error_at_line(self.tinfo.line_no, msg, params)
        super().error(msg, params)

    @classmethod
    def broadcast_warning(cls, contexts: List['RTErrorContextFromMixin'],
                        msg: str, params: Optional[dict]):
        line_nos = [c.tinfo.line_no for c in contexts]
        ctxt = contexts[0]
        ctxt.error_mixin.error_at_lines(line_nos, msg, params)
        for ctxt in contexts:
            ctxt.transaction_warnings.append(
                msg if params is None else msg % params
            )


    def warning(self, msg: str, params=Optional[dict]):
        self.error_mixin.error_at_line(self.tinfo.line_no, msg, params)
        super().warning(msg, params)

# TODO: implement APIErrorContext

class LedgerResolver(ErrorContextWrapper, Generic[TP, TI, RT], abc.ABC):
    transaction_party_model: ClassVar[Type[TP]] = None
    transaction_info_class: ClassVar[Type[TI]] = None
    resolved_transaction_class: ClassVar[Type[RT]] = None

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

    def __init__(self, error_context: ErrorMixin, pipeline_section_id: int):
        self.pipeline_section_id = pipeline_section_id
        super().__init__(error_context)

    def __init_subclass__(cls, *args, abstract=False, **kwargs):
        abstract |= inspect.isabstract(cls)
        if cls.transaction_party_model is None and not abstract:
            raise TypeError('Ledger resolver must set transaction_party_model')
        super().__init_subclass__(*args, **kwargs)

    @abc.abstractmethod
    def get_index_builders(self) -> List[TransactionPartyIndexBuilder[TP]]:
        raise NotImplementedError

    def resolve_account(self, tinfo: TI, transaction_party_id) -> RT:
        tinfo_dict = dataclasses.asdict(tinfo)
        del tinfo_dict['account_lookup_str']
        del tinfo_dict['line_no']
        return self.resolved_transaction_class(
            transaction_party_id=transaction_party_id,
            message_context=RTErrorContextFromMixin(self, tinfo),
            pipeline_section_id=self.pipeline_section_id,
            do_not_skip=False, **tinfo_dict
        )

    def unknown_account(self, account_lookup_str: str, line_nos: List[int]):
        self.error_at_lines(
            line_nos, self.unknown_account_message,
            params={'account': account_lookup_str}
        )

    def ambiguous_account(self, account_lookup_str: str, line_nos: List[int]):
        self.error_at_lines(
            line_nos, self.ambiguous_account_message,
            params={'account': account_lookup_str}
        )

    def unparseable_account(self, account_lookup_str: str, line_no: int):
        if self.unparseable_account_message is not None:
            self.error_at_line(
                line_no, self.unparseable_account_message,
                params={'account': account_lookup_str}
            )

    def populate_indexes(self, transactions: List[TI]) \
            -> List[TransactionPartyIndexBuilder[TP]]:
        indexes = self.get_index_builders()
        # first, prime the index builders with all lookup strings
        for info in transactions:
            appended = False
            for builder in indexes:
                appended |= builder.append(info)
                if appended:
                    break
            if not appended:
                self.unparseable_account(
                    info.account_lookup_str, info.line_no
                )
        # execute bulk lookup DB queries
        #  (the index builders are given the opportunity to not hammer the DB)
        for index in indexes:
            index.execute_query()
        return indexes

    def __call__(self, transactions: List[TI]) -> Iterable[Tuple[TP, RT]]:
        # TODO: this is kind of a silly way of doing things
        _by_id: Dict[int, TP] = {}
        _resolved_by_id: Dict[int, List[RT]] = defaultdict(list)

        indexes = self.populate_indexes(transactions)
        for info in transactions:
            # walk through indexes to collect account data
            for index in indexes:
                account = index.lookup(info.account_lookup_str)
                if account is not None:
                    _by_id[account.pk] = account
                    resolved = self.resolve_account(info, account.pk)
                    _resolved_by_id[account.pk].append(resolved)
                    break
            # no need to generate an error if we get here, the
            # index builders will have taken care of that

        for account_id, acct in _by_id.items():
            for resolved in _resolved_by_id[account_id]:
                yield acct, resolved


@dataclass(frozen=True)
class PreparedTransaction(TransactionWithMessages, Generic[LE, RT]):
    transaction: RT
    ledger_entry: LE

    @property
    def message_context(self):
        return self.transaction.message_context

    @property
    def do_not_skip(self) -> bool:
        return self.transaction.do_not_skip


PreparedTransactionList = Iterable[PreparedTransaction[LE,RT]]

class LedgerEntryPreparator(Generic[LE, TP, RT]):
    model: ClassVar[Type[LE]] = None
    transaction_party_model: ClassVar[Type[TP]] = None
    # fk to transaction party model on ledger entry model
    account_field: str = None
    _valid_transactions = None

    # This can't always be done in __init_subclass__, since messing with models
    #  in Django is very finicky until the full app registry is loaded
    # The _ensure methods will always be called in the order in which they are
    # defined, and can be overridden to perform initialisation logic if necessary
    @classmethod
    def _ensure_transaction_party_model_set(cls):
        if cls.transaction_party_model is None:
            raise TypeError('transaction_party_model must be set')

    @classmethod
    def _ensure_model_set(cls):
        if cls.model is None:
            raise TypeError('model must be set')

    @classmethod
    def get_account_field(cls):
        if cls.account_field is None:
            model: Type[LE] = cls.model
            tpm: Type[TP] = cls.transaction_party_model

            def is_candidate(field):
                if not isinstance(field, ForeignKey):
                    return False
                return field.related_model == tpm

            try:
                account_field, = (
                    f for f in model._meta.get_fields() if is_candidate(f)
                )
            except ValueError:
                raise TypeError(
                    'Could not establish a link between transaction party model '
                    'and ledger entry model. Please set the `account_field` '
                    'class attribute.'
                )
            cls.account_field = account_field.name

        return cls.account_field

    def __init__(self, resolved_transactions: Iterable[Tuple[TP, RT]]):
        self.__class__._ensure_transaction_party_model_set()
        self.__class__._ensure_model_set()
        # ensure that resolved transactions are always sorted
        # in chronological order
        self.resolved_transactions = sorted(
            resolved_transactions, key=lambda p: p[1].timestamp
        )
        # extra pass, but meh
        self._account_ix: Dict[int, TP] = {
            tp.pk: tp for tp, rt in resolved_transactions
        }

    # mainly useful for after-the-fact error reporting
    def get_account(self, account_id: int) -> TP:
        return self._account_ix[account_id]

    def account_ids(self) -> Iterable[int]:
        return self._account_ix.keys()

    def model_kwargs_for_transaction(self, acct: TP, transaction: RT) \
            -> Optional[dict]:
        # validate and build model kwargs for transaction
        if transaction.amount.amount < 0:
            err: ResolvedTransactionMessageContext = transaction.error_context
            err.error(
                _('Payment amount %(amount)s is negative.'), {
                    'amount': transaction.amount
                }
            )
            return None

        return {
            'total_amount': transaction.amount,
            'timestamp': transaction.timestamp,
            self.__class__.get_account_field(): acct
        }

    def validate_global(self, valid_transactions: PreparedTransactionList) \
            -> PreparedTransactionList:
        # this method can assume that all transactions have the
        # ledger_entry property set to something meaningful
        return valid_transactions

    def _prepare_and_validate(self):
        if self._valid_transactions is not None:
            return
        resolved: List[Tuple[TP, RT]] = self.resolved_transactions
        # initialise ORM objects when possible, and collect the valid ones
        def indiv_transactions() -> PreparedTransactionList:
            acct: TransactionPartyMixin
            for acct, t in resolved:
                kwargs = self.model_kwargs_for_transaction(acct, t)
                if kwargs is None:
                    continue
                entry: LE = self.model(**kwargs)
                entry.spoof_matched_balance(Decimal('0.00'))
                yield PreparedTransaction(t, entry)

        self._valid_transactions = list(
            self.validate_global(indiv_transactions())
        )

    @property
    def valid_transactions(self) -> List[PreparedTransaction[LE,RT]]:
        if self._valid_transactions is None:
            self._prepare_and_validate()
        return self._valid_transactions

    # Either review or commit will be called, but not both

    def review(self):
        # ensure that valid transactions get computed no matter what
        self._prepare_and_validate()
        return

    def commit(self):
        from django.db import connection
        can_bulk_save = connection.features.can_return_ids_from_bulk_insert

        all_ledger_entries = [
            t.ledger_entry for t in self.valid_transactions
        ]

        if can_bulk_save:
            self.model.objects.bulk_create(all_ledger_entries)
        else:
            logger.debug(
                'Database does not support RETURNING on bulk inserts. '
                'Fall back to saving in a loop.'
            )
            for le in all_ledger_entries:
                le.save()


class DuplicationProtectedPreparator(LedgerEntryPreparator[LE, TP, RT]):
    single_dup_message = None
    multiple_dup_message = None

    def validate_global(self, valid_transactions: PreparedTransactionList):
        valid_transactions = list(super().validate_global(valid_transactions))
        dates = [
            timezone.localdate(t.transaction.timestamp)
            for t in valid_transactions
        ]

        historical_buckets = self.model._default_manager.dupcheck_buckets(
            date_bounds=(min(dates), max(dates))
        )

        import_buckets: Dict[Any, List[PreparedTransaction]] = defaultdict(list)
        for transaction in valid_transactions:
            # have to assert this, the typing hints aren't flexible enough
            # TODO think of a cleaner way
            e = cast(accounting_base.DuplicationProtectionMixin,
                     transaction.ledger_entry)
            sig = e.dupcheck_signature
            import_buckets[sig].append(transaction)

        def strip_duplicates():
            transactions: List[PreparedTransaction]
            for dup_sig, transactions in import_buckets.items():
                occ_in_import = len(transactions)
                occ_in_hist = historical_buckets[dup_sig]
                dupcount = min(occ_in_hist, occ_in_import)
                # skip the first dupcount entries, we treat those as the
                # duplicate ones. The others will be entered into the db
                # as usual
                yield from transactions[dupcount:]
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
                    dups = transactions[:dupcount]

                    broadcast_warning(dups, msg_fmt_str, params)
                    # honour do_not_skip
                    for d in dups:
                        d.suggest_skip()
                        if d.to_commit:
                            yield d

        return strip_duplicates()
                

    def dup_error_params(self, signature_used):
        return {
            'date': signature_used.date,
            'amount': Money(
                signature_used.amount, settings.DEFAULT_CURRENCY
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

                # for consistency
                payment.spoof_matched_balance(payment.total_amount.amount)
                exact_match.spoof_matched_balance(exact_match.total_amount.amount)
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


PLE = TypeVar('PLE', bound=models.BasePaymentRecord)
class CreditApportionmentMixin(LedgerEntryPreparator[PLE, TP, RT]):

    @property
    def overpayment_fmt_string(self):
        return _(
            'Received %(total_credit)s, but only %(total_used)s '
            'can be applied to outstanding debts. '
            'Payment(s) dated %(payment_dates)s have outstanding '
            'balances.'
        )

    # optional, can be derived through reflection
    payment_fk_name = None
    debt_fk_name = None
    _trans_buckets = None

    refund_credit_gnucash_account: Optional[models.GnuCashCategory] = None
    results: ApportionmentResult = None

    @property
    def split_model(self):
        return self.model.get_split_model()[0]

    def debts_for(self, debt_key):
        raise NotImplementedError

    def transaction_buckets(self):
        raise NotImplementedError

    # by default, debt keys are simply PK's of transaction parties
    def get_account_from_debt_key(self, debt_key):
        return self.get_account(debt_key)

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

    @property
    def require_autogenerated_refunds(self):
        return False

    @cached_property
    def refund_message(self):
        if self.require_autogenerated_refunds:
            return ugettext(
                'Refunds will be automatically created to compensate '
                'for the difference in funds. '
                'Please process these at your earliest convenience.'
            )
        else:
            return ugettext(
                'Please resolve this issue manually.'
            )

    def _split_gen(self, debts, payments):
        return make_payment_splits(
            payments, debts, self.split_model,
            payment_fk_name=self.payment_fk_name,
            debt_fk_name=self.debt_fk_name
        )

    def simulate_apportionments(self, debt_key, debts, transactions) \
            -> ApportionmentResult:
        payments = [t.ledger_entry for t in transactions]
        split_generator = self._split_gen(debts, payments)
        splits, results = consume_with_result(split_generator)

        total_used = sum(
            (s.amount for s in splits), Money(0, settings.DEFAULT_CURRENCY)
        )

        total_credit = sum(
            (p.total_amount for p in payments),
            Money(0, settings.DEFAULT_CURRENCY)
        )

        if total_used < total_credit:
            broadcast_warning(
                transactions, self.overpayment_fmt_string,
                params=self.overpayment_error_params(
                    debt_key, total_used, total_credit,
                    results.remaining_payments
                )
            )
        return results

    def review(self):
        super().review()
        # compute the total credit used vs the total
        # credit established, and notify the treasurer
        self._trans_buckets = self.transaction_buckets()
        self.results = ApportionmentResult()
        for key, transactions in self._trans_buckets.items():
            debts = self.debts_for(key)
            # accumulate results for (optional) later processing
            self.results += self.simulate_apportionments(
                key, debts, transactions
            )

    def commit(self):
        # save payments before building splits, otherwise the ORM
        # will not set fk's correctly
        super().commit()

        from django.db import connection
        self._trans_buckets = self.transaction_buckets()
        can_bulk_save = connection.features.can_return_ids_from_bulk_insert
        global_results = ApportionmentResult()

        refund_category = self.refund_credit_gnucash_account
        autogenerate_refunds = refund_category is not None
        debt_account_field = self.transaction_party_model.get_debt_remote_fk()
        debt_model = self.transaction_party_model.get_debt_model()

        def splits_to_create() -> Iterator[BaseDebtPaymentSplit]:
            nonlocal global_results
            refunds_to_save = []
            refund_splits_to_save = []

            for key, transactions in self._trans_buckets.items():
                debts = self.debts_for(key)
                # accumulate results for (optional) later processing
                results = yield from self._split_gen(
                    debts, [t.ledger_entry for t in transactions]
                )
                global_results += results

                if autogenerate_refunds:
                    debt_kwargs = {
                        'gnucash_category': refund_category,
                        debt_account_field: self.get_account_from_debt_key(key)
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
                            refund_object.save()
                            yield from refund_splits
            if can_bulk_save:
                # save all refund objects and create/yield all refund splits
                debt_model.objects.bulk_create(refunds_to_save)
                for splits in refund_splits_to_save:
                    yield from splits

        self.transaction_party_model.get_split_model().objects.bulk_create(
            splits_to_create()
        )

        # allow subclasses to hook into the ApportionmentResults
        # We only set this here to avoid potential shenanigans with partial
        # state.
        self.results = global_results


class StandardCreditApportionmentMixin(CreditApportionmentMixin[LE, TP, RT]):
    transaction_party_model: Type[TP]

    @classmethod
    def _ensure_model_set(cls):
        if cls.model is None:
            cls.model = cls.transaction_party_model.get_payment_model()

    def transaction_buckets(self):
        trans_buckets = defaultdict(list)
        tpm = self.transaction_party_model
        debt_fk_name = tpm.get_debt_remote_fk_column()
        account_ids = set()
        for t in self.valid_transactions:
            account_id = t.transaction.transaction_party_id
            trans_buckets[account_id].append(t)
            account_ids.add(account_id)
        base_qs = tpm.get_debt_model()._default_manager

        debt_qs = base_qs.filter(**{
            '%s__in' % debt_fk_name: account_ids
        }).with_payments().unpaid().order_by('timestamp')

        debt_buckets = defaultdict(list)
        for debt in debt_qs:
            debt_buckets[getattr(debt, debt_fk_name)].append(debt)

        self._debt_buckets = debt_buckets

        return trans_buckets

    def debts_for(self, debt_key):
        return self._debt_buckets[debt_key]


class PaymentPipelineSection(ErrorContextWrapper, Generic[LE, TP, RT, TI]):
    resolver_class: Type[LedgerResolver[TP, TI, RT]]
    ledger_preparator_class: Type[LedgerEntryPreparator[LE, TP, RT]]

    @classmethod
    def transaction_party_queryset(cls):
        raise NotImplementedError

    def __init__(self, error_context: ErrorMixin, pipeline_section_id: int):
        self.pipeline_section_id = pipeline_section_id
        super().__init__(error_context)

    def resolve(self, parsed_data: List[TI]) -> Iterable[Tuple[TP,RT]]:
        resolver: LedgerResolver[TP, TI, RT] = self.resolver_class(
            self.error_context, self.pipeline_section_id
        )
        return resolver(parsed_data)

    def review(self, resolved: Iterable[Tuple[TP, RT]]):
        preparator = self.ledger_preparator_class(resolved)
        # accumulate review errors if necessary
        preparator.review()
        # errors/warnings are saved on the resolved transaction objects, so
        # it suffices to present these as feedback to the user
        # all other data is irrelevant to the pipeline
        return preparator

    def commit(self, resolved: Iterable[Tuple[TP, RT]]):
        preparator = self.ledger_preparator_class(resolved)
        preparator.commit()
        return preparator


PipelineResolved = List[List[Tuple[TransactionPartyMixin, ResolvedTransaction]]]

class PaymentPipelineError(ValueError):
    pass

class PaymentPipeline(ParserErrorMixin):
    pipeline_section_classes: List[Type[PaymentPipelineSection]] = []

    def __init__(self, parser=None, resolved: Optional[List[ResolvedTransaction]]=None):
        if parser is None and resolved is None:
            raise PaymentPipelineError(
                'One of \'parser\' and \'resolved\' must be non-null'
            )
        super().__init__(parser)
        self.pipeline_sections = [
            cl(self, i) for i, cl in enumerate(self.pipeline_section_classes)
        ]
        pipeline_count = len(self.pipeline_section_classes)
        if resolved is None:
            self.resolved: Optional[PipelineResolved] = None
        else:
            # divvy up the resolved transactions per pipeline section
            by_section = [[] for _i in range(pipeline_count)]
            for tr in resolved:
                try:
                    by_section[tr.pipeline_section_id].append(tr)
                except IndexError:
                    raise PaymentPipelineError(
                        '%(id)d is not a valid pipeline section ID' % {
                            'id': tr.pipeline_section_id
                        }
                    )


            def resolved_for_pipeline(pls, transactions):
                account_ids = set(r.transaction_party_id for r in transactions)

                accounts = pls.transaction_party_queryset().filter(
                    pk__in=account_ids
                )
                account_ix = {acct.pk: acct for acct in accounts}
                for rt in transactions:
                    yield account_ix[rt.transaction_party_id], rt

            self.resolved = [
                list(resolved_for_pipeline(*t))
                for t in zip(self.pipeline_section_classes, by_section)
            ]

    def run(self):
        """
        Runs the resolution part of the pipeline.
        """
        if self.resolved is not None:
            return
        self.resolved = [
            list(p.resolve(self.parser.parsed_data))
            for p in self.pipeline_sections
        ]

    def review(self):
        for res, p in zip(self.resolved, self.pipeline_sections):
            p.review(res)

    def commit(self):
        for res, p in zip(self.resolved, self.pipeline_sections):
            p.commit(res)


class FinancialCSVUploadForm(CSVUploadForm):
    pipeline_class: Type[PaymentPipeline]
    upload_field_label = None
    csv_parser_class = None

    csv = forms.FileField()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields['csv'].label = self.upload_field_label

    # is the most common use case
    @property
    def ledger_preparator(self):
        assert len(self.ledger_preparator_classes) == 1
        return self.ledger_preparators[0]

    def review(self) -> List[ResolvedTransaction]:
        parser: FinancialCSVParser = self.cleaned_data['csv']
        pipeline = self.pipeline_class(parser)
        pipeline.review()
        assert pipeline.resolved is not None
        return list(
            chain(*(res for tp, res in pipeline.resolved))
        )

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
