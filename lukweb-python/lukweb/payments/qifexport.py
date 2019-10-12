import logging

from decimal import Decimal
from collections import defaultdict

from django.db.models import Prefetch
from django.utils import timezone
from django.utils.functional import cached_property
from django.utils.translation import pgettext, get_language, activate

from .utils import PAYMENT_NATURE_CASH, PAYMENT_NATURE_TRANSFER
from double_entry.utils import _dt_fallback
from .. import models

logger = logging.getLogger(__name__)

__all__ = ['InternalAccountsQifFormatter','TicketSalesQifFormatter']

# TODO: make proper use of reflection methods from accounting.base

class QifFormatter:
    overpaid_category = pgettext('.qif export', 'OVERPAID')
    overpaid_memo = pgettext('.qif export', 'ERROR: OVERPAID')
    multiple_debts_memo = pgettext('.qif export', '[multiple debts]')
    accounts = {}

    def __init__(self, start, end, by_processed_ts=True):
        self.start = start
        self.end = end
        self.by_processed_ts = by_processed_ts
        self.categories_seen = set()

    @property
    def base_split_qs(self):
        raise NotImplementedError

    @cached_property
    def fin_globals(self):
        return models.FinancialGlobals.load()

    def format_transaction_party(self, payment):
        raise NotImplementedError

    def account_key(self, payment):
        raise NotImplementedError

    def format_account(self, account, transaction_type, splits_by_payment):
        yield '!Account'
        yield 'N' + account
        yield '^'
        yield '!Type:' + transaction_type
        for payment, splits in splits_by_payment.items():
            yield from self.format_transaction(payment, splits)

    def format_split_memo(self, split):
        raise NotImplementedError

    def split_category(self, split):
        raise NotImplementedError

    def format_transaction(self, payment, splits):
        yield 'D' + timezone.localdate(payment.timestamp).strftime('%d/%m/%y')
        yield 'T' + str(payment.total_amount.amount)
        yield 'M{transaction_party}: {split_memos}'.format(
            transaction_party=self.format_transaction_party(payment),
            split_memos=(
                self.multiple_debts_memo if len(splits) > 3 else
                ', '.join(self.format_split_memo(split) for split in splits)
            )
        )

        total_amt = Decimal('0.00')
        for split in splits:
            gnucash_category = self.split_category(split)
            if not gnucash_category:
                logger.error(
                    "Could not find a gnucash category for payment; "
                    "split id is %d." % split.pk
                )
                gnucash_category = "WEBSITE_ERROR"

            self.categories_seen.add(gnucash_category)
            amt = split.amount.amount
            total_amt += amt
            yield 'S' + gnucash_category
            yield 'E' + self.format_split_memo(split)
            yield '$' + str(amt)

        # This should happen only rarely, but in any case
        # the treasurer needs to know about it,
        # so we add a split in the OVERPAID category
        # (at least this should be more predicable and easier to document
        # than explaining/figuring out how unbalanced transactions are handled
        # in GnuCash on .qif imports)
        remainder = payment.total_amount.amount - total_amt
        if remainder > 0:
            self.categories_seen.add(self.overpaid_category)
            yield 'S' + self.overpaid_category
            yield 'E' + self.overpaid_memo
            yield '$' + str(remainder)

        yield '^'
        yield ''

    def _format_qif(self):
        # declare categories
        yield '!Type:Cat'
        for category in self.categories_seen:
            yield 'N' + category
            yield 'I'
            yield '^'
            yield ''

        by_account = defaultdict(lambda: defaultdict(list))
        for s in self.get_queryset():
            k = self.account_key(s.payment)
            by_account[k][s.payment].append(s)

        for k, (account, transaction_type) in self.accounts.items():
            yield from self.format_account(
                account, transaction_type, by_account[k]
            )

    def get_queryset(self):
        # This generates better SQL than prefetch_related
        # on the InternalPayment table (gets all data in 1 query)
        # TODO optimise by deferring unnecessary fields
        # (probably requires something like django-seal to test properly)
        ts_range = (
            _dt_fallback(self.start), _dt_fallback(self.end, use_max=True)
        )
        if self.by_processed_ts:
            return self.base_split_qs.filter(payment__processed__range=ts_range)
        else:
            return self.base_split_qs.filter(payment__timestamp__range=ts_range)

    def generate(self):
        old_lang = get_language()
        activate(self.fin_globals.gnucash_language)
        result = '\n'.join(self._format_qif())
        activate(old_lang)
        return result


class InternalAccountsQifFormatter(QifFormatter):

    def split_category(self, split):
        return split.debt.gnucash_category_string

    def format_split_memo(self, split):
        return split.debt.gnucash_memo

    def format_transaction_party(self, payment):
        return '%s %s' % (
            payment.member.last_name.upper(), payment.member.first_name
        )

    def account_key(self, payment):
        return payment.nature

    @property
    def accounts(self):
        return {
            PAYMENT_NATURE_TRANSFER: (
                self.fin_globals.gnucash_checking_account_name, 'Bank'
            ),
            PAYMENT_NATURE_CASH: (
                self.fin_globals.gnucash_cash_account_name, 'Cash'
            )
        }

    @property
    def base_split_qs(self):
        return models.InternalPaymentSplit.objects.select_related(
            'debt', 'payment', 'payment__member',
            'debt__activity_participation__activity',
            'debt__activity_participation__activity__gnucash_category'
        )


class TicketSalesQifFormatter(QifFormatter):

    def split_category(self, split):
        r: models.ReservationDebt = split.reservation
        try:
            r = r.reservation
        except models.Reservation.DoesNotExist:
            pass
        return r.gnucash_category_string

    def format_split_memo(self, split):
        r: models.ReservationDebt = split.reservation
        try:
            r = r.reservation
        except models.Reservation.DoesNotExist:
            pass
        return r.gnucash_memo

    def format_transaction_party(self, payment):
        return payment.customer.name

    @property
    def accounts(self):
        return {
            models.PAYMENT_METHOD_PREPAID: (
                self.fin_globals.gnucash_checking_account_name, 'Bank'
            ),
            models.PAYMENT_METHOD_ONSITE_CASH: (
                self.fin_globals.gnucash_cash_account_name, 'Cash'
            )
        }

    def account_key(self, payment):
        return payment.method

    @property
    def base_split_qs(self):
        reservation_qs = models.Reservation.objects.prefetch_related(
            Prefetch(
                'tickets',
                queryset=models.Ticket.objects.select_related('category')
            )
        ).select_related('event', 'event__ticket_sales_gnucash_category')
        reservation_debt_qs = models.ReservationDebt.objects.prefetch_related(
            Prefetch('reservation', queryset=reservation_qs)
        )
        return models.ReservationPaymentSplit.objects.select_related(
            'payment', 'payment__customer',
        ).prefetch_related(
            Prefetch('reservation', queryset=reservation_debt_qs)
        )
