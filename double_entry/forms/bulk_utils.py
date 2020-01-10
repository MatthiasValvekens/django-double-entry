"""
Utilities for importing financial transactions in bulk.
For now, all code in the pipeline assumes that there is one fixed party to all
transactions (i.e. the website tenant), and one other party that may vary
by transaction. These parties are kept track of in one or more database tables.

Transactions enter the pipeline as TransactionInfo objects from a CSV parser,
and then pass through the following stages:

 - Resolution: figure out to which account the transactions belong.
   Can be extended to support multi-account transactions in the future.
   This produces ResolvedTransaction objects for all transactions for which
   we can find a corresponding account in the database. A ResolvedTransaction
   object also comes with messaging facilities to attach extra information for
   the human operator further down the processing pipeline.
 - Preparation: takes care of creating ORM objects corresponding to
   transactions and/or split objects to indicate relationships between
   transactions. Transactions that are deemed suspect or faulty can be discarded
   with an appropriate error message, or "softly" discarded in a manner that
   can be overridden by the user.
 - Commit/review: if the pipeline is run in review mode, nothing is saved to the
   database, but the results of the pipeline should then be collected and
   presented to the user so that he/she can make an informed decision on what to
   commit/alter.
   If the pipeline is run in commit mode, the ORM objects created in the
   preparation step are saved to the database, unless the corresponding
   transactions have been marked as discarded.

   The submission API (to be worked out) enters the pipeline immediately after
   the resolution step, since the API user is supposed to be aware of the primary
   keys of the transaction parties involved.
   The goal is to route all transaction review operations in the UI through
   said API, to make sure that everything ultimately passes through the same
   pipeline, which avoids interface duplication.
"""
import abc
import dataclasses
import inspect
import logging
import datetime
from dataclasses import dataclass
from decimal import Decimal
from collections import defaultdict, deque
from enum import IntFlag
from typing import (
    TypeVar, Sequence, Generator, Type, Tuple,
    Iterator, Optional, Iterable, cast, List,
    Generic, ClassVar, Dict, Any
)

import pytz
from django import forms
from django.conf import settings
from django.db.models import ForeignKey, QuerySet
from django.utils import timezone
from django.utils.translation import (
    ugettext_lazy as _,
)
from djmoney.money import Money

from double_entry.forms.csv import TransactionInfo, FinancialCSVParser
from double_entry.models import (
    TransactionPartyMixin, BaseDebtPaymentSplit
)
from double_entry import models as accounting_base, models
from double_entry.utils import (
    decimal_to_money, consume_with_result,
    _dt_fallback,
)
from double_entry.forms.utils import (
    CSVUploadForm, ErrorMixin,
    ParserErrorAggregator, ErrorContextWrapper,
    FileSizeValidator,
)

__all__ = [
    'ResolvedTransactionVerdict', 'ResolvedTransaction',
    'TransactionWithMessages', 'TransactionPartyIndexBuilder',
    'ResolvedTransactionMessageContext', 'LedgerEntryPreparator',
    'LedgerResolver', 'PreparedTransaction', 'CreditApportionmentMixin',
    'StandardCreditApportionmentMixin', 'DuplicationProtectedPreparator',
    'RTErrorContextFromMixin', 'FinancialCSVUploadForm', 'make_payment_splits',
    'refund_overpayment', 'PaymentPipeline', 'PaymentPipelineError',
    'ApportionmentResult'
]
logger = logging.getLogger(__name__)

"""
Utilities for processing & displaying accounting data
originating from .csv files
"""
# TODO (wbc): user-configurable column names, or at least
# translatable ones
# TODO: unified method to mark required/optional columns


# an error prevents further processing, a warning doesn't
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

    # XXX Hack to make asdict() work on ResolvedTransactions
    def __deepcopy__(self, memodict=None):
        return self


class TransactionWithMessages:
    message_context: ResolvedTransactionMessageContext
    do_not_skip: bool

    @property
    def to_commit(self):
        v = self.message_context.verdict
        if bool(self.do_not_skip):
            # ignore a a suggest_discard verdict
            v &= ~ResolvedTransactionVerdict.SUGGEST_DISCARD
        # COMMIT is 0, so this works
        return not bool(v)

    def discard(self):
        self.message_context.discard()

    def suggest_skip(self):
        self.message_context.suggest_skip()



def broadcast_error(transactions: List[TransactionWithMessages],
                        msg: str, params: Optional[dict]):
    _broadcast('error', transactions, msg, params)

def broadcast_warning(transactions: List[TransactionWithMessages],
                    msg: str, params: Optional[dict]):
    _broadcast('warning', transactions, msg, params)

def _broadcast(what, transactions: List[TransactionWithMessages],
                      msg: str, params: Optional[dict]):
    if not transactions:  # pragma: no cover
        return
    errs = list(tr.message_context for tr in transactions)
    err_cls = errs[0].__class__
    if any(err.__class__ != err_cls for err in errs):  # pragma: no cover
        raise ValueError('Cannot combine message contexts of different types')
    getattr(err_cls, 'broadcast_' + what)(errs, msg, params)


@dataclass(frozen=True)
class ResolvedTransaction(TransactionWithMessages):
    transaction_party_id: int
    amount: Money
    timestamp: datetime.datetime
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

    def append(self, tinfo):
        raise NotImplementedError

    def execute_query(self):
        raise NotImplementedError

    def base_query_set(self):
        return self.resolver.base_query_set()


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
    def _broadcast_message(cls, contexts: List['RTErrorContextFromMixin'],
                           msg: str, params: Optional[dict] = None):
        line_nos = [c.tinfo.line_no for c in contexts]
        ctxt = contexts[0]
        ctxt.error_mixin.error_at_lines(line_nos, msg, params)

    @classmethod
    def broadcast_error(cls, contexts: List['RTErrorContextFromMixin'],
                        msg: str, params: Optional[dict]=None):
        cls._broadcast_message(contexts, msg, params)
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
        cls._broadcast_message(contexts, msg, params)
        for ctxt in contexts:
            ctxt.transaction_warnings.append(
                msg if params is None else msg % params
            )


    def warning(self, msg: str, params=Optional[dict]):
        self.error_mixin.error_at_line(self.tinfo.line_no, msg, params)
        super().warning(msg, params)


class LedgerQuerySetBuilder(Generic[TP]):
    transaction_party_model: ClassVar[Type[TP]] = None

    @classmethod
    def base_query_set(cls):
        return LedgerQuerySetBuilder.default_ledger_query_set(
            transaction_party_model=cls.transaction_party_model
        )

    @staticmethod
    def default_ledger_query_set(transaction_party_model: Type[TP]):
        return transaction_party_model \
            ._default_manager.with_debts_and_payments()

class LedgerResolver(ErrorContextWrapper, LedgerQuerySetBuilder[TP], Generic[TP, TI, RT], abc.ABC):
    transaction_info_class: ClassVar[Type[TI]] = TransactionInfo
    resolved_transaction_class: ClassVar[Type[RT]] = ResolvedTransaction

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

    def __init__(self, error_context: ErrorMixin):
        super().__init__(error_context)

    def __init_subclass__(cls, *args, abstract=False, **kwargs):
        abstract |= inspect.isabstract(cls)
        if cls.transaction_party_model is None and not abstract:  # pragma: no cover
            raise TypeError('Ledger resolver must set transaction_party_model')
        super().__init_subclass__(*args, **kwargs)

    @abc.abstractmethod
    def get_index_builders(self) -> List[TransactionPartyIndexBuilder[TP]]:
        raise NotImplementedError

    def resolve_account(self, tinfo: TI, transaction_party_id, **extra_kwargs) -> RT:
        tinfo_dict = dataclasses.asdict(tinfo)
        del tinfo_dict['account_lookup_str']
        del tinfo_dict['line_no']
        # noinspection PyArgumentList
        return self.resolved_transaction_class(
            transaction_party_id=transaction_party_id,
            message_context=RTErrorContextFromMixin(self, tinfo),
            do_not_skip=False, **tinfo_dict, **extra_kwargs
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
        if cls.transaction_party_model is None:  # pragma: no cover
            raise TypeError('transaction_party_model must be set')

    @classmethod
    def _ensure_model_set(cls):
        if cls.model is None:  # pragma: no cover
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
            except ValueError:  # pramga: no cover
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
            err: ResolvedTransactionMessageContext = transaction.message_context
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
            t: ResolvedTransaction
            for acct, t in resolved:
                kwargs = self.model_kwargs_for_transaction(acct, t)
                if kwargs is None:
                    t.discard()  # make sure this happens
                    continue
                # if this generates a TypeError, that's on the programmer
                #  so it should percolate up the stack to a server error
                entry: LE = self.model(**kwargs)
                entry.spoof_matched_balance(Decimal('0.00'))
                yield PreparedTransaction(t, entry)

        # we enforce the skipping verdict here
        # This automatically honours do_not_skip if SUGGEST_SKIP is set,
        # and DISCARD if relevant.
        self._valid_transactions = [
            pt for pt in self.validate_global(indiv_transactions())
            if pt.to_commit
        ]

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
    multiple_dup_message = _(
        'A payment by %(account)s '
        'for amount %(amount)s on date %(date)s appears %(hist)d time(s) '
        'in history, and %(import)d time(s) in '
        'the current batch of data. '
        'Resolution: %(dupcount)d ruled as duplicate(s).'
    )

    single_dup_message = _(
        'A payment by %(account)s '
        'for amount %(amount)s on date %(date)s already appears '
        'in the payment history. '
        'Resolution: likely duplicate, skipped processing.'
    )

    def validate_global(self, valid_transactions: PreparedTransactionList):
        valid_transactions = list(super().validate_global(valid_transactions))
        dates = [
            t.transaction.timestamp.astimezone(pytz.utc).date()
            for t in valid_transactions
        ]
        if not dates:
            return []
        # create a window wide enough to accommodate timezone shenanigans
        min_ts = _dt_fallback(
            min(dates) - datetime.timedelta(days=2), default_timezone=pytz.utc
        )
        max_ts = _dt_fallback(
            max(dates) + datetime.timedelta(days=2), default_timezone=pytz.utc,
            use_max=True
        )
        historical_buckets = self.model._default_manager.dupcheck_buckets(
            timestamp_bounds=(min_ts, max_ts)
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
                    ResolvedTransactionMessageContext.mass_suggest_skip(dups)
                    yield from dups

        return strip_duplicates()
                

    def dup_error_params(self, signature_used):
        account_id = getattr(signature_used, self.account_field + '_id')
        return {
            'date': signature_used.date,
            'amount': Money(
                signature_used.amount, settings.DEFAULT_CURRENCY
            ),
            'account': str(self.get_account(account_id))
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
            raise TypeError  # pragma: no cover
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
        # noinspection DuplicatedCode
        try:
            # look for some unpaid debt
            while not debt_remaining or debt.is_refund:
                if debt is not None:  # initial step guard
                    # either the debt is a refund, or it is fully paid off
                    # since being partially paid off doesn't make sense
                    # for a refund, we can safely report the debt as fully paid
                    results.fully_paid_debts.append(debt)
                    debt.spoof_matched_balance(debt.total_amount)
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

        # noinspection DuplicatedCode
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
                        payment.spoof_matched_balance(payment.total_amount)
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
        return  # pragma: no cover
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

    prioritise_exact_amount_match = True
    exact_amount_match_only = False

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

    def get_refund_credit_gnucash_account(self, debt_key):
        """
        May be called with None if require_autogenerated_refunds is not
        overridden.
        """
        raise NotImplementedError

    @property
    def require_autogenerated_refunds(self):
        return self.get_refund_credit_gnucash_account(None) is not None

    def _split_gen(self, debts, payments):
        return make_payment_splits(
            payments, debts, self.split_model,
            payment_fk_name=self.payment_fk_name,
            debt_fk_name=self.debt_fk_name,
            prioritise_exact_amount_match=self.prioritise_exact_amount_match,
            exact_amount_match_only=self.exact_amount_match_only
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

        autogenerate_refunds = self.require_autogenerated_refunds
        debt_account_field = self.transaction_party_model.get_debt_remote_fk()
        debt_model = self.transaction_party_model.get_debt_model()

        def splits_to_create() -> Iterator[BaseDebtPaymentSplit]:
            nonlocal global_results
            refunds_to_save = []
            refund_splits_to_save = []

            for key, transactions in self._trans_buckets.items():
                refund_category = self.get_refund_credit_gnucash_account(
                    key
                )
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

    def get_refund_credit_gnucash_account(self, debt_key):
        return None

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


class SubmissionPipelineSection(Generic[LE,TP,RT]):
    def __init__(self, ledger_preparator_class: Type[LedgerEntryPreparator[LE, TP, RT]]):
        self.ledger_preparator_class = ledger_preparator_class

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

class PaymentPipelineSection(SubmissionPipelineSection, Generic[LE,TP,TI,RT]):

    def __init__(self, resolver_class: Type[LedgerResolver[TP, TI, RT]],
                    ledger_preparator_class: Type[LedgerEntryPreparator[LE, TP, RT]],
                    error_context: ErrorMixin):
        super().__init__(ledger_preparator_class)
        self.resolver_class = resolver_class
        self.error_context = error_context

    def resolve(self, parsed_data: List[TI]) -> Iterable[Tuple[TP,RT]]:
        resolver: LedgerResolver[TP, TI, RT] = self.resolver_class(
            self.error_context
        )
        return resolver(parsed_data)

ResolvedSection = List[Tuple[TransactionPartyMixin, ResolvedTransaction]]
PipelineResolved = List[ResolvedSection]
PipelinePrepared = List[List[PreparedTransaction]]
PipelineSectionClass = Tuple[
    Type[LedgerResolver[TP, TI, RT]], Type[LedgerEntryPreparator[LE, TP, RT]]
]
SubmissionPipelineSectionClass = Tuple[
    Type[RT], Type[LedgerEntryPreparator[LE, TP, RT]]
]
SubmissionSpec = List[SubmissionPipelineSectionClass]
PipelineSpec = List[PipelineSectionClass]

class PaymentPipelineError(ValueError):
    pass

def as_submission_spec(pipeline_spec: PipelineSpec) -> SubmissionSpec:
    return [
        (resolver_class.resolved_transaction_class, prep_class)
        for resolver_class, prep_class in pipeline_spec
    ]

class PaymentSubmissionPipeline:
    def __init__(self, pipeline_spec: SubmissionSpec, **kwargs):
        super().__init__(**kwargs)
        self.pipeline_sections = [
            SubmissionPipelineSection(preparator)
            for rt_class, preparator in pipeline_spec
        ]
        self.rt_classes = [rt_class for rt_class, preparator in pipeline_spec]

    def submit_resolved(self, resolved: List[Tuple[QuerySet, List[ResolvedTransaction]]]):
        def resolved_for_pipeline(qs, rt_class, transactions):
            account_ids = set(r.transaction_party_id for r in transactions)
            # no need to hit the DB if we know the result set will be empty
            if not account_ids:
                return
            wrong = [r for r in transactions if not isinstance(r, rt_class)]
            if wrong:
                broadcast_error(
                    wrong,
                    'Wrong transaction type, pipeline '
                    'expects \'%(expected)s\'.',
                    params={ 'expected': rt_class }
                )
            accounts = qs.filter(pk__in=account_ids)
            account_ix = {acct.pk: acct for acct in accounts}
            rt: ResolvedTransaction
            for rt in transactions:
                try:
                    account = account_ix[rt.transaction_party_id]
                    yield account, rt
                except KeyError:
                    rt.message_context.error(
                        _('Account with id \'%(pk)d\' not found'),
                        params={'pk': rt.transaction_party_id}
                    )

        self.resolved = [
            list(resolved_for_pipeline(qb, rt_class, transactions))
            for rt_class, (qb, transactions) in zip(self.rt_classes, resolved)
        ]

    def review(self):
        if self.resolved is None:
            raise ValueError('No resolved transactions to review')  # pragma: no cover
        def by_section():
            for res, p in zip(self.resolved, self.pipeline_sections):
                if res:
                    prep = p.review(res)
                    yield prep.valid_transactions
                else:
                    yield []
        self.prepared = list(by_section())

    def commit(self):
        if self.resolved is None:
            raise ValueError('No resolved transactions to commit')  # pragma: no cover
        def by_section():
            for res, p in zip(self.resolved, self.pipeline_sections):
                if res:
                    prep = p.commit(res)
                    yield prep.valid_transactions
                else:
                    yield []
        self.prepared = list(by_section())


class PaymentPipeline(PaymentSubmissionPipeline, ParserErrorAggregator):

    def __init__(self, pipeline_spec: PipelineSpec, parser):
        submission_spec: SubmissionSpec = [
            (res_class.resolved_transaction_class, prep_class)
            for res_class, prep_class in pipeline_spec
        ]
        super().__init__(pipeline_spec=submission_spec, parser=parser)

        self.pipeline_sections = [
            PaymentPipelineSection(resolver, preparator, self)
            for resolver, preparator in pipeline_spec
        ]
        self.prepared: Optional[PipelinePrepared] = None
        self.resolved: Optional[PipelineResolved] = None

    def resolve(self):
        if self.resolved is not None:
            return
        self.resolved = [
            list(p.resolve(self.parser.parsed_data))
            for p in self.pipeline_sections
        ]


class FinancialCSVUploadForm(CSVUploadForm):
    csv = forms.FileField(
        validators=[FileSizeValidator(
            b=getattr(settings, 'MAX_CSV_UPLOAD', 1024 * 1024)
        )]
    )

    def __init__(self, *args, pipeline_spec: PipelineSpec,
                 csv_parser_class: Type['FinancialCSVParser'],
                 upload_field_label: str=_('Upload .csv'), **kwargs):
        super().__init__(*args, **kwargs)
        self.fields['csv'].label = upload_field_label
        self.csv_parser_class = csv_parser_class
        self.pipeline_spec = pipeline_spec
        self.resolved = None

    def review(self):
        parser: FinancialCSVParser = self.cleaned_data['csv']
        pipeline = PaymentPipeline(
            pipeline_spec=self.pipeline_spec, parser=parser
        )
        pipeline.resolve()
        pipeline.review()
        # the PreparedTransactions aren't directly necessary for now
        assert pipeline.resolved is not None
        self.resolved = pipeline.resolved
